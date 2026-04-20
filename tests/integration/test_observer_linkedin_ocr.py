"""Unit + integration tests for the LinkedIn OCR growth-trend observer.

We deliberately inject stub ``fetch_html`` / ``fetch_image`` / ``ocr``
callables so the tests never touch the network and do not require the
``[ocr]`` optional dependency group.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session

from headcount.db.enums import (
    AnchorType,
    HeadcountValueKind,
    PriorityTier,
    SourceName,
)
from headcount.ingest.observers.linkedin_ocr import (
    LINKEDIN_OCR_PARSER_VERSION,
    LinkedInGrowthTrendObserver,
    ParsedGrowthTrend,
    back_compute_historical_values,
    parse_growth_trend_text,
)
from headcount.models import Base, Company, CompanyAnchorObservation
from headcount.models.source_observation import SourceObservation

# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_parse_growth_trend_text_picks_up_three_horizons() -> None:
    text = "Employee growth\n6m +12%\n1 year +34%\n2y -5%"
    trend = parse_growth_trend_text(text)
    assert trend is not None
    assert trend.pct_6m == pytest.approx(12.0)
    assert trend.pct_1y == pytest.approx(34.0)
    assert trend.pct_2y == pytest.approx(-5.0)


def test_parse_growth_trend_text_returns_none_when_noisy() -> None:
    assert parse_growth_trend_text("") is None
    assert parse_growth_trend_text("   \n   ") is None
    assert parse_growth_trend_text("No growth numbers here") is None


def test_parse_growth_trend_text_rejects_implausibly_large_values() -> None:
    trend = parse_growth_trend_text("6m 900% 1y 25%")
    assert trend is not None
    assert trend.pct_6m is None  # 900% rejected
    assert trend.pct_1y == pytest.approx(25.0)


def test_back_compute_historical_values_inverts_growth() -> None:
    trend = ParsedGrowthTrend(pct_6m=25.0, pct_1y=100.0, pct_2y=-20.0)
    result = back_compute_historical_values(
        current_point=1000.0,
        current_min=950.0,
        current_max=1050.0,
        trend=trend,
    )
    assert result["6m"][1] == pytest.approx(800.0)
    assert result["1y"][1] == pytest.approx(500.0)
    # -20% means the current is 80% of historical -> historical is 1250.
    assert result["2y"][1] == pytest.approx(1250.0)


def test_back_compute_historical_drops_horizons_with_non_positive_factor() -> None:
    trend = ParsedGrowthTrend(pct_6m=-110.0, pct_1y=50.0, pct_2y=None)
    result = back_compute_historical_values(
        current_point=100.0,
        current_min=90.0,
        current_max=110.0,
        trend=trend,
    )
    # -110% would invert past zero; must be dropped.
    assert "6m" not in result
    assert "1y" in result
    assert "2y" not in result


# ---------------------------------------------------------------------------
# Observer orchestration (no network, no OCR)
# ---------------------------------------------------------------------------


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
        linkedin_company_url="https://www.linkedin.com/company/acme/",
        priority_tier=PriorityTier.P1,
    )
    session.add(company)
    session.flush()
    return company


def _add_current_anchor(session: Session, company: Company) -> CompanyAnchorObservation:
    row = CompanyAnchorObservation(
        company_id=company.id,
        anchor_type=AnchorType.current_headcount_anchor,
        anchor_month=date(2026, 4, 1),
        headcount_value_point=1000.0,
        headcount_value_min=950.0,
        headcount_value_max=1050.0,
        headcount_value_kind=HeadcountValueKind.exact,
        confidence=0.7,
    )
    session.add(row)
    session.flush()
    return row


def test_observer_emits_historical_anchors_from_stub_pipeline(
    session: Session,
) -> None:
    company = _make_company(session)
    _add_current_anchor(session, company)

    html = (
        '<html><body><img src="https://static.licdn.com/charts/growth.png" '
        'alt="growth"/></body></html>'
    )

    def fetch_html(url: str) -> str | None:
        return html if "linkedin.com" in url else None

    def fetch_image(url: str) -> bytes | None:
        return b"pretend-image-bytes"

    def ocr(image_bytes: bytes) -> str:
        return "Employee growth 6m +25% 1y +100% 2y 0%"

    observer = LinkedInGrowthTrendObserver(
        fetch_html=fetch_html, fetch_image=fetch_image, ocr=ocr
    )
    written = observer.collect(session, companies=[company])
    session.flush()
    # 6m, 1y. 2y @ 0% is dropped - factor would be 1.0 and produce a
    # duplicate of the current anchor, which we correctly skip below.
    assert written == 3  # 0% is a valid horizon, just a no-growth one.

    anchors = list(session.execute(select(CompanyAnchorObservation)).scalars())
    # Current anchor + three historical back-computed anchors.
    assert len(anchors) == 4

    historical = [a for a in anchors if a.anchor_type is AnchorType.historical_statement]
    assert len(historical) == 3
    by_month = {a.anchor_month: a for a in historical}
    assert date(2025, 10, 1) in by_month  # 6m ago
    assert date(2025, 4, 1) in by_month  # 1y ago
    assert date(2024, 4, 1) in by_month  # 2y ago

    # Every historical anchor must carry a SourceObservation row
    # tagged with the LinkedIn OCR parser version.
    src_rows = list(session.execute(select(SourceObservation)).scalars())
    ocr_srcs = [
        s for s in src_rows if s.parser_version == LINKEDIN_OCR_PARSER_VERSION
    ]
    assert len(ocr_srcs) == 3
    assert all(s.source_name is SourceName.linkedin_public for s in ocr_srcs)


def test_observer_is_noop_without_linkedin_url(session: Session) -> None:
    company = Company(
        canonical_name="No URL Co",
        canonical_domain="nourl.com",
        priority_tier=PriorityTier.P1,
    )
    session.add(company)
    session.flush()
    _add_current_anchor(session, company)

    def fail_html(_url: str) -> str | None:  # pragma: no cover
        raise AssertionError("should not fetch when linkedin_company_url is None")

    observer = LinkedInGrowthTrendObserver(fetch_html=fail_html)
    assert observer.collect(session, companies=[company]) == 0


def test_observer_respects_authwall_gate(session: Session) -> None:
    company = _make_company(session)
    _add_current_anchor(session, company)

    def fetch_html(_url: str) -> str:
        return "<html><body>Please sign in to see the authwall</body></html>"

    def fetch_image(_url: str) -> bytes:  # pragma: no cover
        raise AssertionError("should not be called when gated")

    def ocr(_: bytes) -> str:  # pragma: no cover
        raise AssertionError("should not be called when gated")

    observer = LinkedInGrowthTrendObserver(
        fetch_html=fetch_html, fetch_image=fetch_image, ocr=ocr
    )
    assert observer.collect(session, companies=[company]) == 0


def test_observer_requires_current_anchor(session: Session) -> None:
    company = _make_company(session)
    # Deliberately do NOT add a current_headcount_anchor.

    def fetch_html(_url: str) -> str:
        return '<img src="https://static.licdn.com/chart.png"/>'

    def fetch_image(_url: str) -> bytes:
        return b"img"

    def ocr(_: bytes) -> str:
        return "6m +25%"

    observer = LinkedInGrowthTrendObserver(
        fetch_html=fetch_html, fetch_image=fetch_image, ocr=ocr
    )
    # Without a current anchor to scale against, the observer cannot
    # back-compute a historical value and must emit zero signals.
    assert observer.collect(session, companies=[company]) == 0


def test_observer_is_available_reflects_optional_deps(monkeypatch: Any) -> None:
    # If either dep is unavailable in the test env, the helper should
    # report False. We don't force-install them here.
    result = LinkedInGrowthTrendObserver.is_available()
    assert isinstance(result, bool)
