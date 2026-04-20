"""End-to-end orchestration test for ``collect_anchors``."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import httpx
import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session

from headcount.db.enums import (
    AnchorType,
    CompanyRunStage,
    CompanyRunStageStatus,
    CompanyStatus,
    HeadcountValueKind,
    PriorityTier,
    RunStatus,
    SourceName,
)
from headcount.ingest.collect import collect_anchors
from headcount.ingest.http import FileCache, HttpClient
from headcount.ingest.observers import (
    ManualAnchorObserver,
    SECObserver,
    WikidataObserver,
)
from headcount.models import (
    Base,
    Company,
    CompanyAnchorObservation,
    CompanyRunStatus,
    Run,
    SourceObservation,
)

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "fixtures"


def _fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_fk(dbapi_conn, _):  # type: ignore[no-untyped-def]
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    with Session(engine, future=True) as s:
        yield s


@pytest.fixture()
def apple(session: Session) -> Company:
    company = Company(
        canonical_name="Apple Inc.",
        canonical_domain="apple.com",
        status=CompanyStatus.active,
        priority_tier=PriorityTier.P0,
    )
    session.add(company)
    session.commit()
    return company


def _handler(mapping: dict[str, tuple[int, str]]):
    def _impl(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        for prefix, (status, text) in mapping.items():
            if url.startswith(prefix):
                return httpx.Response(status, text=text)
        return httpx.Response(404)

    return _impl


@pytest.mark.asyncio
async def test_collect_anchors_persists_sec_and_wikidata_signals(
    session: Session, apple: Company, tmp_path: Path
) -> None:
    handler = _handler(
        {
            "https://www.sec.gov/files/company_tickers.json": (
                200,
                _fixture("sec_company_tickers.json"),
            ),
            "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json": (
                200,
                _fixture("sec_apple_facts.json"),
            ),
            "https://query.wikidata.org/sparql": (
                200,
                _fixture("wikidata_apple.json"),
            ),
        }
    )
    cache = FileCache(tmp_path / "cache")
    http = HttpClient(cache=cache, transport=httpx.MockTransport(handler))

    result = await collect_anchors(
        session,
        adapters=[SECObserver(), WikidataObserver()],
        companies=[apple],
        http_client=http,
    )
    session.commit()

    assert result.companies_attempted == 1
    assert result.companies_with_signals == 1
    assert result.signals_written == result.anchors_written
    assert result.anchors_written >= 4

    obs = session.execute(select(SourceObservation)).scalars().all()
    anchors = session.execute(select(CompanyAnchorObservation)).scalars().all()
    assert len(obs) == len(anchors)
    sec_obs = [o for o in obs if o.source_name is SourceName.sec]
    wd_obs = [o for o in obs if o.source_name is SourceName.wikidata]
    assert len(sec_obs) == 3
    assert len(wd_obs) >= 1
    for anchor in anchors:
        assert anchor.company_id == apple.id
        assert anchor.headcount_value_kind is HeadcountValueKind.exact

    # The most recent SEC anchor should be 2024-09-01.
    sec_anchor_months = sorted(
        {a.anchor_month for a in anchors if a.anchor_type is AnchorType.historical_statement}
    )
    assert date(2024, 9, 1) in sec_anchor_months

    stage_row = session.execute(select(CompanyRunStatus)).scalar_one()
    assert stage_row.stage is CompanyRunStage.collect_anchors
    assert stage_row.status is CompanyRunStageStatus.succeeded

    run = session.execute(select(Run)).scalar_one()
    assert run.status is RunStatus.succeeded


@pytest.mark.asyncio
async def test_collect_anchors_is_idempotent(
    session: Session, apple: Company, tmp_path: Path
) -> None:
    handler = _handler(
        {
            "https://www.sec.gov/files/company_tickers.json": (
                200,
                _fixture("sec_company_tickers.json"),
            ),
            "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json": (
                200,
                _fixture("sec_apple_facts.json"),
            ),
        }
    )
    cache = FileCache(tmp_path / "cache")
    http = HttpClient(cache=cache, transport=httpx.MockTransport(handler))

    first = await collect_anchors(
        session, adapters=[SECObserver()], companies=[apple], http_client=http
    )
    session.commit()
    second = await collect_anchors(
        session, adapters=[SECObserver()], companies=[apple], http_client=http
    )
    session.commit()

    assert first.anchors_written == 3
    assert second.anchors_written == 0
    total = session.execute(select(CompanyAnchorObservation)).scalars().all()
    assert len(total) == 3


@pytest.mark.asyncio
async def test_collect_anchors_marks_stage_failed_when_all_adapters_error(
    session: Session, apple: Company, tmp_path: Path
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    cache = FileCache(tmp_path / "cache")
    http = HttpClient(cache=cache, transport=httpx.MockTransport(handler))

    result = await collect_anchors(
        session,
        adapters=[WikidataObserver()],
        companies=[apple],
        http_client=http,
        trip_after=1,
    )
    session.commit()

    assert result.anchors_written == 0
    assert result.errors  # recorded at least one error
    stage_row = session.execute(select(CompanyRunStatus)).scalar_one()
    assert stage_row.status is CompanyRunStageStatus.failed
    run = session.execute(select(Run)).scalar_one()
    assert run.status is RunStatus.failed


@pytest.mark.asyncio
async def test_collect_anchors_with_manual_observer_no_http(
    session: Session, apple: Company, tmp_path: Path
) -> None:
    yaml_path = tmp_path / "manual.yaml"
    yaml_path.write_text(
        """
- canonical_name: Apple Inc.
  domain: apple.com
  anchor_month: 2026-04-01
  headcount:
    min: 163000
    point: 164000
    max: 165000
    kind: range
  confidence: 0.98
  note: 2026 Q1 investor update
""",
        encoding="utf-8",
    )
    cache = FileCache(tmp_path / "cache")
    http = HttpClient(cache=cache, transport=httpx.MockTransport(lambda r: httpx.Response(404)))

    result = await collect_anchors(
        session,
        adapters=[ManualAnchorObserver(path=yaml_path)],
        companies=[apple],
        http_client=http,
    )
    session.commit()

    assert result.anchors_written == 1
    anchor = session.execute(select(CompanyAnchorObservation)).scalar_one()
    assert anchor.anchor_type is AnchorType.manual_anchor
    assert anchor.headcount_value_point == 164000.0
    assert anchor.confidence == pytest.approx(0.98)
