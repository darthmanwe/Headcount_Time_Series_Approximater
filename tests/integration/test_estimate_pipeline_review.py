"""Phase 8 end-to-end pipeline tests.

These walk the pipeline through the new review surface:

1. Confidence scores and component breakdowns are persisted per month.
2. Anchor-pin overrides flow into the reconciled anchor.
3. Suppress-window overrides force months into manual-review-required.
4. ReviewQueueItem rows are created for low-confidence and suppressed
   months, deduplicating across runs.
5. AuditLog rows are written when overrides are applied.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session

from headcount.db.enums import (
    AnchorType,
    ConfidenceBand,
    HeadcountValueKind,
    OverrideField,
    PriorityTier,
    ReviewReason,
    SourceName,
)
from headcount.estimate.pipeline import estimate_series
from headcount.models import (
    AuditLog,
    Base,
    Company,
    CompanyAnchorObservation,
    ConfidenceComponentScore,
    EstimateVersion,
    HeadcountEstimateMonthly,
    ManualOverride,
    Person,
    PersonEmploymentObservation,
    ReviewQueueItem,
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


def _make_company(session: Session, name: str = "Acme Inc") -> Company:
    c = Company(
        canonical_name=name,
        canonical_domain=f"{name.lower().replace(' ', '')}.com",
        priority_tier=PriorityTier.P1,
    )
    session.add(c)
    session.flush()
    return c


def _make_person(session: Session, name: str) -> Person:
    p = Person(
        source_name=SourceName.manual,
        source_person_key=f"manual::{name}",
        display_name=name,
    )
    session.add(p)
    session.flush()
    return p


def _seed_basic_company(session: Session) -> Company:
    company = _make_company(session)
    session.add(
        CompanyAnchorObservation(
            company_id=company.id,
            anchor_type=AnchorType.historical_statement,
            anchor_month=date(2023, 6, 1),
            headcount_value_point=1000,
            headcount_value_min=980,
            headcount_value_max=1020,
            headcount_value_kind=HeadcountValueKind.exact,
            confidence=0.9,
        )
    )
    for i in range(10):
        p = _make_person(session, f"Person {i}")
        session.add(
            PersonEmploymentObservation(
                person_id=p.id,
                company_id=company.id,
                start_month=date(2023, 1, 1),
                end_month=None,
                is_current_role=True,
            )
        )
    session.flush()
    return company


def test_pipeline_persists_confidence_score_and_components(session: Session) -> None:
    company = _seed_basic_company(session)
    estimate_series(
        session,
        start_month=date(2023, 1, 1),
        end_month=date(2023, 12, 1),
        as_of_month=date(2023, 12, 1),
        sample_floor=1,
    )

    version = session.execute(
        select(EstimateVersion).where(EstimateVersion.company_id == company.id)
    ).scalars().one()

    rows = session.execute(
        select(HeadcountEstimateMonthly).where(
            HeadcountEstimateMonthly.estimate_version_id == version.id
        )
    ).scalars().all()
    assert rows
    for r in rows:
        assert r.confidence_score is not None
        assert 0.0 <= r.confidence_score <= 1.0
        assert r.confidence_components_json is not None
        assert "components" in r.confidence_components_json
        assert r.confidence_components_json["scoring_version"] == "scoring_v1"

    components = session.execute(
        select(ConfidenceComponentScore).where(
            ConfidenceComponentScore.estimate_version_id == version.id
        )
    ).scalars().all()
    assert {c.component_name for c in components} == {
        "anchor_source_quality",
        "anchor_recency",
        "anchor_agreement",
        "sample_coverage",
        "event_proximity",
        "multi_source_corroboration",
    }


def test_anchor_pin_override_dominates_other_anchors(session: Session) -> None:
    company = _seed_basic_company(session)
    # The real anchor claims 1000. The analyst pin claims 2000 - with
    # manual_anchor precedence that should win the point reconciliation.
    session.add(
        ManualOverride(
            company_id=company.id,
            field_name=OverrideField.current_anchor,
            override_value_json={
                "anchor_month": "2023-06-01",
                "value_min": 1900,
                "value_point": 2000,
                "value_max": 2100,
                "confidence": 0.95,
            },
            reason="corrected via SEC filing",
        )
    )
    session.flush()

    estimate_series(
        session,
        start_month=date(2023, 1, 1),
        end_month=date(2023, 12, 1),
        as_of_month=date(2023, 12, 1),
        sample_floor=1,
    )
    version = session.execute(
        select(EstimateVersion).where(EstimateVersion.company_id == company.id)
    ).scalars().one()
    june_row = session.execute(
        select(HeadcountEstimateMonthly).where(
            HeadcountEstimateMonthly.estimate_version_id == version.id,
            HeadcountEstimateMonthly.month == date(2023, 6, 1),
        )
    ).scalars().one()
    assert june_row.estimated_headcount >= 1900


def test_suppress_window_forces_manual_review(session: Session) -> None:
    company = _seed_basic_company(session)
    session.add(
        ManualOverride(
            company_id=company.id,
            field_name=OverrideField.estimate_suppress_window,
            override_value_json={
                "start_month": "2023-02-01",
                "end_month": "2023-04-01",
            },
            reason="anchor known bad",
        )
    )
    session.flush()

    estimate_series(
        session,
        start_month=date(2023, 1, 1),
        end_month=date(2023, 12, 1),
        as_of_month=date(2023, 12, 1),
        sample_floor=1,
    )
    version = session.execute(
        select(EstimateVersion).where(EstimateVersion.company_id == company.id)
    ).scalars().one()
    rows = session.execute(
        select(HeadcountEstimateMonthly)
        .where(HeadcountEstimateMonthly.estimate_version_id == version.id)
        .order_by(HeadcountEstimateMonthly.month)
    ).scalars().all()

    in_window = [r for r in rows if date(2023, 2, 1) <= r.month <= date(2023, 4, 1)]
    assert len(in_window) == 3
    for r in in_window:
        assert r.needs_review is True
        assert r.confidence_band is ConfidenceBand.manual_review_required
        assert r.suppression_reason is not None
        assert "manual_suppress" in r.suppression_reason

    # Audit log records the override application.
    audits = session.execute(
        select(AuditLog).where(AuditLog.action == "overrides_applied")
    ).scalars().all()
    assert audits
    payload = dict(audits[0].payload_json or {})
    assert payload["company_id"] == company.id
    assert payload["n_windows"] == 1


def test_review_queue_accumulates_and_dedupes_across_runs(session: Session) -> None:
    # A company whose samples are borderline enough that review picks
    # up both low confidence AND a manual-suppress signal.
    company = _seed_basic_company(session)
    session.add(
        ManualOverride(
            company_id=company.id,
            field_name=OverrideField.estimate_suppress_window,
            override_value_json={
                "start_month": "2023-02-01",
                "end_month": "2023-04-01",
            },
            reason="audit",
        )
    )
    session.flush()

    estimate_series(
        session,
        start_month=date(2023, 1, 1),
        end_month=date(2023, 12, 1),
        as_of_month=date(2023, 12, 1),
        sample_floor=1,
    )
    first_rows = session.execute(select(ReviewQueueItem)).scalars().all()
    reasons_first = {r.review_reason for r in first_rows}
    assert ReviewReason.manual in reasons_first
    first_count = len(first_rows)

    # Re-running should not duplicate review items under the same
    # estimate_version; each run creates a fresh EstimateVersion so we
    # expect the count to grow by the number of distinct (reason) per
    # new version, not for every month.
    estimate_series(
        session,
        start_month=date(2023, 1, 1),
        end_month=date(2023, 12, 1),
        as_of_month=date(2023, 12, 1),
        sample_floor=1,
    )
    second_rows = session.execute(select(ReviewQueueItem)).scalars().all()
    # At most 4 more rows (one per possible ReviewReason used by the
    # pipeline): low_confidence, anomaly, manual, benchmark_disagreement.
    assert len(second_rows) >= first_count
    assert len(second_rows) <= first_count + 4


def test_run_without_anchors_marks_every_month_manual_review(session: Session) -> None:
    company = _make_company(session, "NoAnchor LLC")
    p = _make_person(session, "Only Person")
    session.add(
        PersonEmploymentObservation(
            person_id=p.id,
            company_id=company.id,
            start_month=date(2023, 1, 1),
            end_month=None,
            is_current_role=True,
        )
    )
    session.flush()

    estimate_series(
        session,
        start_month=date(2023, 1, 1),
        end_month=date(2023, 3, 1),
        as_of_month=date(2023, 3, 1),
    )
    rows = session.execute(select(HeadcountEstimateMonthly)).scalars().all()
    assert len(rows) == 3
    for r in rows:
        assert r.confidence_band is ConfidenceBand.manual_review_required
        assert r.needs_review is True
