"""Model round-trip tests against an in-memory SQLite database.

Verifies every aggregate can be constructed, persisted, retrieved, and that
the interval-anchor and estimate-interval check constraints are enforced.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError, StatementError
from sqlalchemy.orm import Session

from headcount.db.enums import (
    AliasType,
    AnchorType,
    CompanyRunStage,
    CompanyRunStageStatus,
    CompanyStatus,
    ConfidenceBand,
    EstimateMethod,
    EstimateVersionStatus,
    EventSourceClass,
    EventType,
    HeadcountValueKind,
    OverrideField,
    ParseStatus,
    PriorityTier,
    RelationKind,
    ReviewReason,
    ReviewStatus,
    RunKind,
    RunStatus,
    SourceBudgetStatus,
    SourceEntityType,
    SourceName,
)
from headcount.models import (
    AnchorReconciliation,
    AuditLog,
    Base,
    Company,
    CompanyAlias,
    CompanyAnchorObservation,
    CompanyCandidate,
    CompanyEvent,
    CompanyRelation,
    CompanyRunStatus,
    CompanySourceLink,
    ConfidenceComponentScore,
    EstimateVersion,
    HeadcountEstimateMonthly,
    ManualOverride,
    Person,
    PersonEmploymentObservation,
    PersonIdentityMerge,
    ReviewQueueItem,
    Run,
    SourceBudget,
    SourceObservation,
)


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    from sqlalchemy import event

    @event.listens_for(engine, "connect")
    def _enable_fk(dbapi_conn, _):  # type: ignore[no-untyped-def]
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    with Session(engine, future=True) as s:
        yield s


def _make_company(session: Session, **overrides: object) -> Company:
    company = Company(
        canonical_name=overrides.pop("canonical_name", "Acme Corp"),
        canonical_domain=overrides.pop("canonical_domain", "acme.com"),
        status=CompanyStatus.active,
        priority_tier=PriorityTier.P1,
        **overrides,
    )
    session.add(company)
    session.flush()
    return company


def _make_observation(session: Session) -> SourceObservation:
    obs = SourceObservation(
        source_name=SourceName.company_web,
        entity_type=SourceEntityType.company,
        source_url="https://acme.com/about",
        observed_at=datetime.now(tz=UTC),
        raw_content_hash="x" * 64,
        parser_version="test-parser-v1",
        parse_status=ParseStatus.ok,
    )
    session.add(obs)
    session.flush()
    return obs


def _make_run(session: Session) -> Run:
    run = Run(
        kind=RunKind.full,
        status=RunStatus.started,
        started_at=datetime.now(tz=UTC),
        cutoff_month=date(2024, 3, 1),
        method_version="hc-v1",
        anchor_policy_version="anchor-v1",
        coverage_curve_version="coverage-v1",
        config_hash="y" * 32,
        priority_tier=PriorityTier.P1,
    )
    session.add(run)
    session.flush()
    return run


def test_company_and_alias_roundtrip(session: Session) -> None:
    company = _make_company(session)
    alias = CompanyAlias(
        company_id=company.id,
        alias_name="Acme Corporation",
        alias_type=AliasType.legal,
        confidence=0.9,
        source="manual",
    )
    session.add(alias)
    session.commit()

    stored = session.execute(
        select(CompanyAlias).where(CompanyAlias.company_id == company.id)
    ).scalar_one()
    assert stored.alias_type is AliasType.legal
    assert stored.confidence == pytest.approx(0.9)


def test_company_source_link_roundtrip(session: Session) -> None:
    company = _make_company(session)
    link = CompanySourceLink(
        company_id=company.id,
        source_name=SourceName.linkedin_public,
        source_url="https://www.linkedin.com/company/acme",
        is_primary=True,
        confidence=0.8,
    )
    session.add(link)
    session.commit()
    stored = session.execute(select(CompanySourceLink)).scalar_one()
    assert stored.source_name is SourceName.linkedin_public


def test_company_candidate_and_relation(session: Session) -> None:
    parent = _make_company(session, canonical_name="Parent Co")
    child = _make_company(session, canonical_name="Child Co")

    candidate = CompanyCandidate(
        source_workbook="High Priority Companies_01.04.2026.xlsx",
        source_sheet="Sheet1",
        source_row_index=2,
        company_name="Parent Co",
        domain="parent.example",
    )
    session.add(candidate)

    relation = CompanyRelation(
        parent_id=parent.id,
        child_id=child.id,
        kind=RelationKind.acquired,
        effective_month=date(2023, 6, 1),
        confidence=0.85,
    )
    session.add(relation)
    session.commit()

    assert session.execute(select(CompanyRelation)).scalar_one().kind is RelationKind.acquired
    assert session.execute(select(CompanyCandidate)).scalar_one().company_name == "Parent Co"


def test_anchor_observation_monotonic_ok(session: Session) -> None:
    company = _make_company(session)
    obs = _make_observation(session)
    anchor = CompanyAnchorObservation(
        company_id=company.id,
        source_observation_id=obs.id,
        anchor_type=AnchorType.current_headcount_anchor,
        headcount_value_min=200,
        headcount_value_point=350,
        headcount_value_max=500,
        headcount_value_kind=HeadcountValueKind.range,
        anchor_month=date(2024, 3, 1),
        confidence=0.7,
    )
    session.add(anchor)
    session.commit()
    stored = session.execute(select(CompanyAnchorObservation)).scalar_one()
    assert stored.headcount_value_kind is HeadcountValueKind.range


def test_anchor_observation_rejects_inverted_interval(session: Session) -> None:
    company = _make_company(session)
    obs = _make_observation(session)
    bad = CompanyAnchorObservation(
        company_id=company.id,
        source_observation_id=obs.id,
        anchor_type=AnchorType.current_headcount_anchor,
        headcount_value_min=500,
        headcount_value_point=200,
        headcount_value_max=100,
        headcount_value_kind=HeadcountValueKind.range,
        anchor_month=date(2024, 3, 1),
        confidence=0.5,
    )
    session.add(bad)
    with pytest.raises(IntegrityError):
        session.commit()


def test_employment_person_event(session: Session) -> None:
    company = _make_company(session)
    obs = _make_observation(session)
    p1 = Person(
        source_name=SourceName.linkedin_public,
        source_person_key="alice",
        display_name="Alice",
    )
    p2 = Person(
        source_name=SourceName.linkedin_public,
        source_person_key="alice-2",
        display_name="Alice (dup)",
    )
    session.add_all([p1, p2])
    session.flush()

    emp = PersonEmploymentObservation(
        person_id=p1.id,
        company_id=company.id,
        source_observation_id=obs.id,
        job_title="Engineer",
        start_month=date(2023, 1, 1),
        end_month=None,
        is_current_role=True,
        confidence=0.7,
    )
    session.add(emp)

    merge = PersonIdentityMerge(
        primary_person_id=p1.id,
        duplicate_person_id=p2.id,
        reason="manual merge",
    )
    session.add(merge)

    event = CompanyEvent(
        company_id=company.id,
        event_type=EventType.acquisition,
        event_month=date(2023, 6, 1),
        source_observation_id=obs.id,
        source_class=EventSourceClass.benchmark,
        confidence=0.8,
        description="Acquired by Symphony AI",
    )
    session.add(event)
    session.commit()

    assert session.execute(select(PersonEmploymentObservation)).scalar_one().is_current_role
    assert session.execute(select(CompanyEvent)).scalar_one().event_type is EventType.acquisition


def test_run_and_company_run_status(session: Session) -> None:
    run = _make_run(session)
    company = _make_company(session)
    status = CompanyRunStatus(
        run_id=run.id,
        company_id=company.id,
        stage=CompanyRunStage.estimate_series,
        status=CompanyRunStageStatus.running,
        attempts=1,
    )
    session.add(status)
    budget = SourceBudget(
        run_id=run.id,
        source_name=SourceName.linkedin_public,
        requests_used=0,
        requests_allowed=400,
        status=SourceBudgetStatus.open,
    )
    session.add(budget)
    session.commit()
    assert (
        session.execute(select(CompanyRunStatus)).scalar_one().status
        is CompanyRunStageStatus.running
    )
    assert session.execute(select(SourceBudget)).scalar_one().requests_allowed == 400


def test_estimate_version_reconciliation_confidence_and_rows(session: Session) -> None:
    run = _make_run(session)
    company = _make_company(session)
    version = EstimateVersion(
        company_id=company.id,
        estimation_run_id=run.id,
        method_version="hc-v1",
        anchor_policy_version="anchor-v1",
        coverage_curve_version="coverage-v1",
        source_snapshot_cutoff=date(2024, 3, 1),
        status=EstimateVersionStatus.draft,
    )
    session.add(version)
    session.flush()

    reconciliation = AnchorReconciliation(
        estimate_version_id=version.id,
        chosen_point=320.0,
        chosen_min=250.0,
        chosen_max=400.0,
        inputs_json=[
            {
                "source": "linkedin_public",
                "point": 350,
                "min": 201,
                "max": 500,
                "weight": 0.6,
                "confidence": 0.7,
            }
        ],
        weights_json={"linkedin_public": 0.6, "wikidata": 0.3, "company_web": 0.1},
        rationale="weighted by source confidence",
    )
    component = ConfidenceComponentScore(
        estimate_version_id=version.id,
        component_name="cross_source_agreement",
        component_score=0.85,
    )
    series_point = HeadcountEstimateMonthly(
        company_id=company.id,
        estimate_version_id=version.id,
        month=date(2024, 3, 1),
        estimated_headcount=320.0,
        estimated_headcount_min=250.0,
        estimated_headcount_max=400.0,
        public_profile_count=180,
        scaled_from_anchor_value=320.0,
        method=EstimateMethod.scaled_ratio_coverage_corrected,
        confidence_band=ConfidenceBand.medium,
    )
    session.add_all([reconciliation, component, series_point])
    session.commit()

    assert session.execute(select(AnchorReconciliation)).scalar_one().chosen_point == 320.0
    assert session.execute(
        select(ConfidenceComponentScore)
    ).scalar_one().component_score == pytest.approx(0.85)
    assert (
        session.execute(select(HeadcountEstimateMonthly)).scalar_one().method
        is EstimateMethod.scaled_ratio_coverage_corrected
    )


def test_estimate_interval_check_rejects_inverted(session: Session) -> None:
    run = _make_run(session)
    company = _make_company(session)
    version = EstimateVersion(
        company_id=company.id,
        estimation_run_id=run.id,
        method_version="hc-v1",
        anchor_policy_version="anchor-v1",
        coverage_curve_version="coverage-v1",
        source_snapshot_cutoff=date(2024, 3, 1),
    )
    session.add(version)
    session.flush()
    bad = HeadcountEstimateMonthly(
        company_id=company.id,
        estimate_version_id=version.id,
        month=date(2024, 3, 1),
        estimated_headcount=500.0,
        estimated_headcount_min=600.0,
        estimated_headcount_max=400.0,
        public_profile_count=100,
        scaled_from_anchor_value=500.0,
        method=EstimateMethod.scaled_ratio,
        confidence_band=ConfidenceBand.low,
    )
    session.add(bad)
    with pytest.raises(IntegrityError):
        session.commit()


def test_manual_override_review_audit(session: Session) -> None:
    run = _make_run(session)
    company = _make_company(session)
    version = EstimateVersion(
        company_id=company.id,
        estimation_run_id=run.id,
        method_version="hc-v1",
        anchor_policy_version="anchor-v1",
        coverage_curve_version="coverage-v1",
        source_snapshot_cutoff=date(2024, 3, 1),
    )
    session.add(version)
    session.flush()

    override = ManualOverride(
        company_id=company.id,
        field_name=OverrideField.current_anchor,
        override_value_json={"point": 400, "min": 350, "max": 450, "kind": "range"},
        reason="analyst",
        entered_by="analyst@example.com",
    )
    queue = ReviewQueueItem(
        company_id=company.id,
        estimate_version_id=version.id,
        review_reason=ReviewReason.benchmark_disagreement,
        priority=80,
        status=ReviewStatus.open,
        detail="benchmark delta exceeds threshold",
    )
    audit = AuditLog(
        actor_type="service",
        action="enqueue_review",
        target_type="company",
        target_id=company.id,
        payload_json={"priority": 80},
    )
    session.add_all([override, queue, audit])
    session.commit()

    assert (
        session.execute(select(ManualOverride)).scalar_one().field_name
        is OverrideField.current_anchor
    )
    assert session.execute(select(ReviewQueueItem)).scalar_one().priority == 80
    assert session.execute(select(AuditLog)).scalar_one().action == "enqueue_review"


def test_person_identity_merge_rejects_self(session: Session) -> None:
    person = Person(
        source_name=SourceName.linkedin_public,
        source_person_key="bob",
    )
    session.add(person)
    session.flush()
    bad = PersonIdentityMerge(
        primary_person_id=person.id,
        duplicate_person_id=person.id,
    )
    session.add(bad)
    with pytest.raises((IntegrityError, StatementError)):
        session.commit()
