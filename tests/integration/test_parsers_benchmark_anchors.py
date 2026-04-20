"""Integration tests for benchmark -> anchor promotion."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session

from headcount.db.enums import (
    AnchorType,
    BenchmarkMetric,
    BenchmarkProvider,
    HeadcountValueKind,
    PriorityTier,
    SourceName,
)
from headcount.models import Base, Company
from headcount.models.benchmark import BenchmarkObservation
from headcount.models.company_anchor_observation import CompanyAnchorObservation
from headcount.models.source_observation import SourceObservation
from headcount.parsers.benchmark_anchors import (
    BENCHMARK_ANCHOR_PARSER_VERSION,
    promote_benchmark_anchors,
)


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_fk(dbapi_conn, _):  # type: ignore[no-untyped-def]
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    with Session(engine, future=True) as s:
        yield s


def _make_company(session: Session) -> Company:
    company = Company(
        canonical_name="Acme Inc",
        canonical_domain="acme.com",
        priority_tier=PriorityTier.P1,
    )
    session.add(company)
    session.flush()
    return company


def _bench(
    session: Session,
    *,
    company_id: str | None,
    provider: BenchmarkProvider,
    metric: BenchmarkMetric,
    as_of: date | None,
    value_point: float | None,
    row_index: int,
    value_min: float | None = None,
    value_max: float | None = None,
) -> BenchmarkObservation:
    obs = BenchmarkObservation(
        company_id=company_id,
        source_workbook="sample.xlsx",
        source_sheet="Summary",
        source_row_index=row_index,
        source_cell_address="D5",
        source_column_name=metric.value,
        company_name_raw="Acme Inc",
        company_domain_raw="acme.com",
        provider=provider,
        metric=metric,
        as_of_month=as_of,
        value_min=value_min,
        value_point=value_point,
        value_max=value_max,
    )
    session.add(obs)
    session.flush()
    return obs


def test_promotes_headcount_rows_to_historical_anchors(session: Session) -> None:
    company = _make_company(session)
    today = date(2026, 4, 1)
    _bench(
        session,
        company_id=company.id,
        provider=BenchmarkProvider.harmonic,
        metric=BenchmarkMetric.headcount_current,
        as_of=today,
        value_point=500.0,
        row_index=1,
    )
    _bench(
        session,
        company_id=company.id,
        provider=BenchmarkProvider.harmonic,
        metric=BenchmarkMetric.headcount_6m_ago,
        as_of=date(2025, 10, 1),
        value_point=480.0,
        row_index=1,
    )
    _bench(
        session,
        company_id=company.id,
        provider=BenchmarkProvider.zeeshan,
        metric=BenchmarkMetric.headcount_1y_ago,
        as_of=date(2025, 4, 1),
        value_point=420.0,
        row_index=1,
    )

    result = promote_benchmark_anchors(session)
    session.flush()

    assert result.eligible == 3
    assert result.inserted_anchor_rows == 3
    assert result.inserted_source_rows == 3

    anchors = list(session.execute(select(CompanyAnchorObservation)).scalars())
    assert len(anchors) == 3
    by_month = {a.anchor_month: a for a in anchors}
    assert by_month[today].anchor_type is AnchorType.current_headcount_anchor
    assert by_month[date(2025, 10, 1)].anchor_type is AnchorType.historical_statement
    assert by_month[date(2025, 4, 1)].anchor_type is AnchorType.historical_statement
    # Analyst-verified (``zeeshan``) outranks automated third-party
    # feeds (``harmonic``) in our policy: when both providers report a
    # value at the same month, the analyst's number wins.
    assert by_month[date(2025, 4, 1)].confidence > by_month[today].confidence

    sources = list(session.execute(select(SourceObservation)).scalars())
    assert len(sources) == 3
    assert all(s.source_name is SourceName.benchmark for s in sources)
    assert all(s.parser_version == BENCHMARK_ANCHOR_PARSER_VERSION for s in sources)


def test_rerun_is_idempotent(session: Session) -> None:
    company = _make_company(session)
    _bench(
        session,
        company_id=company.id,
        provider=BenchmarkProvider.linkedin,
        metric=BenchmarkMetric.headcount_current,
        as_of=date(2026, 4, 1),
        value_point=302.0,
        row_index=1,
    )
    first = promote_benchmark_anchors(session)
    session.flush()
    assert first.inserted_anchor_rows == 1
    second = promote_benchmark_anchors(session)
    session.flush()
    assert second.inserted_anchor_rows == 0
    assert second.already_promoted == 1
    anchors = list(session.execute(select(CompanyAnchorObservation)).scalars())
    assert len(anchors) == 1


def test_skips_rows_without_value_or_month(session: Session) -> None:
    company = _make_company(session)
    _bench(
        session,
        company_id=company.id,
        provider=BenchmarkProvider.harmonic,
        metric=BenchmarkMetric.headcount_current,
        as_of=None,
        value_point=500.0,
        row_index=1,
    )
    _bench(
        session,
        company_id=company.id,
        provider=BenchmarkProvider.harmonic,
        metric=BenchmarkMetric.headcount_6m_ago,
        as_of=date(2025, 10, 1),
        value_point=None,
        row_index=2,
    )
    result = promote_benchmark_anchors(session)
    session.flush()
    assert result.inserted_anchor_rows == 0
    assert result.skipped_no_value == 2


def test_skips_unresolved_benchmark_rows(session: Session) -> None:
    _bench(
        session,
        company_id=None,
        provider=BenchmarkProvider.harmonic,
        metric=BenchmarkMetric.headcount_current,
        as_of=date(2026, 4, 1),
        value_point=500.0,
        row_index=1,
    )
    result = promote_benchmark_anchors(session)
    session.flush()
    # Rows without company_id are scanned but skipped by the
    # read-through guard on company_id.
    assert result.inserted_anchor_rows == 0
    assert result.skipped_no_company == 1


def test_skips_non_headcount_metrics(session: Session) -> None:
    company = _make_company(session)
    _bench(
        session,
        company_id=company.id,
        provider=BenchmarkProvider.harmonic,
        metric=BenchmarkMetric.growth_6m_pct,
        as_of=date(2026, 4, 1),
        value_point=0.15,
        row_index=1,
    )
    result = promote_benchmark_anchors(session)
    session.flush()
    # Growth-percent metrics are entirely filtered at the query level.
    assert result.scanned == 0
    assert result.inserted_anchor_rows == 0


def test_respects_company_id_filter(session: Session) -> None:
    a = _make_company(session)
    b = Company(
        canonical_name="Beacon Corp",
        canonical_domain="beacon.com",
        priority_tier=PriorityTier.P1,
    )
    session.add(b)
    session.flush()
    for row_idx, cid in enumerate((a.id, b.id), start=1):
        _bench(
            session,
            company_id=cid,
            provider=BenchmarkProvider.harmonic,
            metric=BenchmarkMetric.headcount_current,
            as_of=date(2026, 4, 1),
            value_point=500.0,
            row_index=row_idx,
        )
    result = promote_benchmark_anchors(session, company_ids=[a.id])
    session.flush()
    assert result.inserted_anchor_rows == 1
    anchors = list(session.execute(select(CompanyAnchorObservation)).scalars())
    assert len(anchors) == 1
    assert anchors[0].company_id == a.id


def test_uses_value_kind_from_observation_when_provided(session: Session) -> None:
    company = _make_company(session)
    _bench(
        session,
        company_id=company.id,
        provider=BenchmarkProvider.linkedin,
        metric=BenchmarkMetric.headcount_current,
        as_of=date(2026, 4, 1),
        value_point=300.0,
        value_min=201.0,
        value_max=500.0,
        row_index=1,
    )
    # Override value_kind on the row to mimic a bucket value.
    obs = next(iter(session.execute(select(BenchmarkObservation)).scalars()))
    obs.value_kind = HeadcountValueKind.bucket
    session.flush()

    promote_benchmark_anchors(session)
    session.flush()

    anchor = next(iter(session.execute(select(CompanyAnchorObservation)).scalars()))
    assert anchor.headcount_value_kind is HeadcountValueKind.bucket
    assert anchor.headcount_value_min == pytest.approx(201.0)
    assert anchor.headcount_value_max == pytest.approx(500.0)
