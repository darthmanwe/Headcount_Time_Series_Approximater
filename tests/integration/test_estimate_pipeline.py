"""End-to-end integration tests for the Phase 7 estimation pipeline.

We build a synthetic company in an in-memory SQLite database:

- Anchors at a mix of months and provenances (including bucket ranges).
- Employment intervals for multiple "people" that overlap the window.
- An acquisition event in the middle to force a hard-break segment.

The pipeline must produce a complete :class:`HeadcountEstimateMonthly`
series plus :class:`AnchorReconciliation` per segment that actually has
anchors, mark any out-of-sample months suppressed, and close out the
``Run`` + ``CompanyRunStatus`` rows cleanly.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session

from headcount.db.enums import (
    AnchorType,
    CompanyRunStage,
    CompanyRunStageStatus,
    EstimateMethod,
    EventSourceClass,
    EventType,
    HeadcountValueKind,
    PriorityTier,
    RunStatus,
    SourceName,
)
from headcount.estimate.pipeline import estimate_series
from headcount.models import (
    AnchorReconciliation,
    Base,
    Company,
    CompanyAnchorObservation,
    CompanyEvent,
    CompanyRunStatus,
    EstimateVersion,
    HeadcountEstimateMonthly,
    Person,
    PersonEmploymentObservation,
    Run,
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
    company = Company(
        canonical_name=name,
        canonical_domain=f"{name.lower().replace(' ', '')}.com",
        priority_tier=PriorityTier.P1,
    )
    session.add(company)
    session.flush()
    return company


def _make_person(session: Session, name: str) -> Person:
    person = Person(
        source_name=SourceName.manual,
        source_person_key=f"manual::{name}",
        display_name=name,
    )
    session.add(person)
    session.flush()
    return person


def _add_anchor(
    session: Session,
    company: Company,
    *,
    month: date,
    point: float,
    vmin: float | None = None,
    vmax: float | None = None,
    kind: HeadcountValueKind = HeadcountValueKind.exact,
    type_: AnchorType = AnchorType.historical_statement,
    conf: float = 0.8,
) -> CompanyAnchorObservation:
    row = CompanyAnchorObservation(
        company_id=company.id,
        anchor_type=type_,
        anchor_month=month,
        headcount_value_point=point,
        headcount_value_min=point if vmin is None else vmin,
        headcount_value_max=point if vmax is None else vmax,
        headcount_value_kind=kind,
        confidence=conf,
    )
    session.add(row)
    session.flush()
    return row


def _add_employment(
    session: Session,
    company: Company,
    person: Person,
    *,
    start: date,
    end: date | None,
    current: bool = False,
) -> PersonEmploymentObservation:
    row = PersonEmploymentObservation(
        person_id=person.id,
        company_id=company.id,
        start_month=start,
        end_month=end,
        is_current_role=current,
    )
    session.add(row)
    session.flush()
    return row


def _seed_synthetic_company(session: Session) -> Company:
    company = _make_company(session)
    # Anchor at June 2023 from SEC (exact) and company-web (range), both
    # pointing at ~1000.
    _add_anchor(
        session,
        company,
        month=date(2023, 6, 1),
        point=1000,
        vmin=950,
        vmax=1050,
        kind=HeadcountValueKind.exact,
        type_=AnchorType.historical_statement,
        conf=0.9,
    )
    _add_anchor(
        session,
        company,
        month=date(2023, 6, 1),
        point=1000,
        vmin=900,
        vmax=1100,
        kind=HeadcountValueKind.range,
        type_=AnchorType.current_headcount_anchor,
        conf=0.6,
    )
    # Seed 10 people with staggered start/end months so monthly counts
    # grow linearly across 2023.
    for i in range(10):
        p = _make_person(session, f"Person {i}")
        _add_employment(
            session,
            company,
            p,
            start=date(2023, 1, 1) if i < 5 else date(2023, 4, 1),
            end=None if i >= 3 else date(2023, 8, 1),
            current=i >= 3,
        )
    return company


def _months(rows: Iterable[HeadcountEstimateMonthly]) -> list[date]:
    return sorted({r.month for r in rows})


def test_pipeline_produces_monthly_series(session: Session) -> None:
    company = _seed_synthetic_company(session)

    result = estimate_series(
        session,
        start_month=date(2023, 1, 1),
        end_month=date(2023, 12, 1),
        as_of_month=date(2023, 12, 1),
        sample_floor=1,
    )

    assert result.companies_attempted == 1
    assert result.companies_succeeded >= 1
    assert result.months_written == 12

    run = session.get(Run, result.run_id)
    assert run is not None
    assert run.status in {RunStatus.succeeded, RunStatus.partial}
    assert run.finished_at is not None

    version_rows = (
        session.execute(select(EstimateVersion).where(EstimateVersion.company_id == company.id))
        .scalars()
        .all()
    )
    assert len(version_rows) == 1

    estimates = (
        session.execute(
            select(HeadcountEstimateMonthly).where(
                HeadcountEstimateMonthly.estimate_version_id == version_rows[0].id
            )
        )
        .scalars()
        .all()
    )
    assert _months(estimates) == [date(2023, m, 1) for m in range(1, 13)]

    # Every monotonicity check must hold (the schema enforces it too, but
    # we assert explicitly so the test catches regressions earlier).
    for r in estimates:
        assert r.estimated_headcount_min <= r.estimated_headcount
        assert r.estimated_headcount <= r.estimated_headcount_max

    stage_row = (
        session.execute(
            select(CompanyRunStatus).where(
                CompanyRunStatus.run_id == result.run_id,
                CompanyRunStatus.company_id == company.id,
                CompanyRunStatus.stage == CompanyRunStage.estimate_series,
            )
        )
        .scalars()
        .one()
    )
    assert stage_row.status is CompanyRunStageStatus.succeeded


def test_pipeline_writes_anchor_reconciliation_row(session: Session) -> None:
    company = _seed_synthetic_company(session)

    result = estimate_series(
        session,
        start_month=date(2023, 1, 1),
        end_month=date(2023, 12, 1),
        as_of_month=date(2023, 12, 1),
        sample_floor=1,
    )
    assert result.companies_attempted == 1

    version = (
        session.execute(select(EstimateVersion).where(EstimateVersion.company_id == company.id))
        .scalars()
        .one()
    )

    reconciliations = (
        session.execute(
            select(AnchorReconciliation).where(
                AnchorReconciliation.estimate_version_id == version.id
            )
        )
        .scalars()
        .all()
    )
    assert len(reconciliations) == 1
    rec = reconciliations[0]
    assert rec.chosen_point == pytest.approx(1000.0, rel=0.05)
    assert rec.rationale
    assert isinstance(rec.inputs_json, list)


def test_hard_break_acquisition_splits_series(session: Session) -> None:
    company = _make_company(session)

    _add_anchor(
        session,
        company,
        month=date(2023, 2, 1),
        point=500,
        vmin=450,
        vmax=550,
    )
    _add_anchor(
        session,
        company,
        month=date(2023, 10, 1),
        point=1200,
        vmin=1100,
        vmax=1300,
    )
    session.add(
        CompanyEvent(
            company_id=company.id,
            event_type=EventType.acquisition,
            event_month=date(2023, 7, 1),
            source_class=EventSourceClass.first_party,
            confidence=0.9,
            description="Acme acquired BetaCorp",
        )
    )
    for i in range(8):
        p = _make_person(session, f"Pre {i}")
        _add_employment(
            session,
            company,
            p,
            start=date(2023, 1, 1),
            end=date(2023, 6, 1),
        )
    for i in range(20):
        p = _make_person(session, f"Post {i}")
        _add_employment(
            session,
            company,
            p,
            start=date(2023, 7, 1),
            end=None,
            current=True,
        )

    result = estimate_series(
        session,
        start_month=date(2023, 1, 1),
        end_month=date(2023, 12, 1),
        as_of_month=date(2023, 12, 1),
        sample_floor=1,
    )
    assert result.companies_attempted == 1

    version = (
        session.execute(select(EstimateVersion).where(EstimateVersion.company_id == company.id))
        .scalars()
        .one()
    )
    rows = (
        session.execute(
            select(HeadcountEstimateMonthly)
            .where(HeadcountEstimateMonthly.estimate_version_id == version.id)
            .order_by(HeadcountEstimateMonthly.month)
        )
        .scalars()
        .all()
    )
    assert len(rows) == 12
    pre_june = next(r for r in rows if r.month == date(2023, 6, 1))
    post_july = next(r for r in rows if r.month == date(2023, 7, 1))
    # The post-event month is scaled against the post-event anchor, which is
    # much larger than the pre-event anchor. We expect a jump across the
    # segment boundary, not a smoothed interpolation.
    assert post_july.estimated_headcount > pre_june.estimated_headcount * 1.5

    # Segment boundary jump must NOT be flagged as a mom_jump anomaly.
    assert post_july.needs_review is False

    reconciliations = (
        session.execute(
            select(AnchorReconciliation).where(
                AnchorReconciliation.estimate_version_id == version.id
            )
        )
        .scalars()
        .all()
    )
    assert len(reconciliations) == 2


def test_low_sample_months_are_suppressed_in_output(session: Session) -> None:
    company = _make_company(session)
    _add_anchor(
        session,
        company,
        month=date(2023, 6, 1),
        point=500,
        vmin=450,
        vmax=550,
    )
    # A single live employee - below the default floor of 5 -> suppressed.
    p = _make_person(session, "Solo")
    _add_employment(
        session,
        company,
        p,
        start=date(2023, 1, 1),
        end=None,
        current=True,
    )

    result = estimate_series(
        session,
        start_month=date(2023, 1, 1),
        end_month=date(2023, 6, 1),
        as_of_month=date(2023, 6, 1),
    )

    version = (
        session.execute(select(EstimateVersion).where(EstimateVersion.company_id == company.id))
        .scalars()
        .one()
    )
    rows = (
        session.execute(
            select(HeadcountEstimateMonthly).where(
                HeadcountEstimateMonthly.estimate_version_id == version.id
            )
        )
        .scalars()
        .all()
    )
    assert all(r.method is EstimateMethod.suppressed_low_sample for r in rows)
    assert all(r.needs_review is True for r in rows)
    assert result.months_flagged == len(rows)


def test_company_with_no_anchors_fails_closed(session: Session) -> None:
    company = _make_company(session)
    # No anchors, no events, no employment.
    result = estimate_series(
        session,
        start_month=date(2023, 1, 1),
        end_month=date(2023, 3, 1),
        as_of_month=date(2023, 3, 1),
    )
    assert result.companies_attempted == 1
    version = (
        session.execute(select(EstimateVersion).where(EstimateVersion.company_id == company.id))
        .scalars()
        .one()
    )
    rows = (
        session.execute(
            select(HeadcountEstimateMonthly).where(
                HeadcountEstimateMonthly.estimate_version_id == version.id
            )
        )
        .scalars()
        .all()
    )
    assert len(rows) == 3
    assert all(r.method is EstimateMethod.suppressed_low_sample for r in rows)
    assert all(r.suppression_reason == "no_anchor_in_segment" for r in rows)
