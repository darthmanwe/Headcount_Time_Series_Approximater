"""Integration tests for the deterministic canonical resolver."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session

from headcount.db.enums import (
    AliasType,
    BenchmarkEventCandidateStatus,
    BenchmarkEventHintType,
    BenchmarkMetric,
    BenchmarkProvider,
    CandidateStatus,
    HeadcountValueKind,
    PriorityTier,
    RelationKind,
    SourceName,
)
from headcount.models import (
    Base,
    BenchmarkEventCandidate,
    BenchmarkObservation,
    Company,
    CompanyAlias,
    CompanyCandidate,
    CompanyRelation,
    CompanySourceLink,
)
from headcount.resolution import resolve_candidates


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_fk(dbapi_conn, _):  # type: ignore[no-untyped-def]
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    with Session(engine, future=True) as s:
        yield s


def _add_candidate(
    session: Session,
    *,
    row: int,
    name: str,
    domain: str | None = None,
    workbook: str = "priority.xlsx",
    sheet: str = "Sheet1",
) -> CompanyCandidate:
    cand = CompanyCandidate(
        source_workbook=workbook,
        source_sheet=sheet,
        source_row_index=row,
        company_name=name,
        domain=domain,
    )
    session.add(cand)
    session.flush()
    return cand


def _add_benchmark_obs(
    session: Session,
    candidate: CompanyCandidate,
    *,
    row: int,
    linkedin: str | None = None,
    domain: str | None = None,
) -> BenchmarkObservation:
    obs = BenchmarkObservation(
        company_candidate_id=candidate.id,
        source_workbook="bench.xlsx",
        source_sheet="Sheet1",
        source_row_index=row,
        company_name_raw=candidate.company_name,
        company_domain_raw=domain,
        linkedin_url_raw=linkedin,
        provider=BenchmarkProvider.linkedin,
        metric=BenchmarkMetric.headcount_current,
        as_of_month=date(2026, 4, 1),
        value_min=100,
        value_point=100,
        value_max=100,
        value_kind=HeadcountValueKind.exact,
    )
    session.add(obs)
    session.flush()
    return obs


def test_resolver_creates_company_with_alias(session: Session) -> None:
    cand = _add_candidate(session, row=1, name="Acme, Inc.", domain="acme.com")
    result = resolve_candidates(session)
    session.commit()

    assert result.candidates_resolved == 1
    assert result.companies_created == 1
    assert result.aliases_created == 1

    company = session.execute(select(Company)).scalar_one()
    assert company.canonical_name == "Acme, Inc."
    assert company.canonical_domain == "acme.com"
    assert company.priority_tier is PriorityTier.P1

    alias = session.execute(select(CompanyAlias)).scalar_one()
    assert alias.company_id == company.id
    assert alias.alias_name == "Acme, Inc."
    assert alias.alias_type is AliasType.legal

    session.refresh(cand)
    assert cand.company_id == company.id
    assert cand.status is CandidateStatus.resolved


def test_resolver_merges_candidates_by_domain(session: Session) -> None:
    _add_candidate(session, row=1, name="Acme Inc", domain="acme.com")
    _add_candidate(session, row=2, name="ACME", domain="www.acme.com")
    result = resolve_candidates(session)
    session.commit()

    companies = session.execute(select(Company)).scalars().all()
    assert len(companies) == 1
    assert result.candidates_resolved == 2
    assert result.companies_created == 1
    aliases = {a.alias_name for a in session.execute(select(CompanyAlias)).scalars()}
    assert aliases == {"Acme Inc", "ACME"}


def test_resolver_merges_by_name_key_when_domain_missing(session: Session) -> None:
    _add_candidate(session, row=1, name="Symphony AI")
    _add_candidate(session, row=2, name="Symphony AI, Inc.")
    result = resolve_candidates(session)
    session.commit()
    assert result.companies_created == 1


def test_resolver_merges_by_linkedin_hint_from_benchmark(session: Session) -> None:
    first = _add_candidate(session, row=1, name="Acme Holdings")
    second = _add_candidate(session, row=2, name="Acme Global")
    _add_benchmark_obs(session, first, row=1, linkedin="linkedin.com/company/acme")
    _add_benchmark_obs(session, second, row=2, linkedin="https://www.linkedin.com/company/ACME/")
    result = resolve_candidates(session)
    session.commit()
    assert result.companies_created == 1
    link = session.execute(select(CompanySourceLink)).scalar_one()
    assert link.source_name is SourceName.linkedin_public
    assert "acme" in link.source_url.lower()


def test_resolver_is_idempotent(session: Session) -> None:
    _add_candidate(session, row=1, name="Acme, Inc.", domain="acme.com")
    resolve_candidates(session)
    session.commit()
    result = resolve_candidates(session)
    session.commit()
    # default only_pending=True filters resolved rows at the SQL layer
    assert result.candidates_scanned == 0
    assert result.candidates_resolved == 0
    assert result.companies_created == 0
    assert result.aliases_created == 0
    assert len(session.execute(select(Company)).scalars().all()) == 1


def test_resolver_replay_with_all_candidates_counts_already_resolved(session: Session) -> None:
    _add_candidate(session, row=1, name="Acme, Inc.", domain="acme.com")
    resolve_candidates(session)
    session.commit()
    result = resolve_candidates(session, only_pending=False)
    session.commit()
    assert result.candidates_scanned == 1
    assert result.candidates_already_resolved == 1
    assert result.candidates_resolved == 0
    assert result.companies_created == 0


def test_resolver_backfills_benchmark_company_id(session: Session) -> None:
    cand = _add_candidate(session, row=1, name="Acme", domain="acme.com")
    obs = _add_benchmark_obs(session, cand, row=1)
    resolve_candidates(session)
    session.commit()
    session.refresh(obs)
    session.refresh(cand)
    assert obs.company_id == cand.company_id


def test_resolver_emits_acquisition_relation_when_acquirer_known(session: Session) -> None:
    parent_cand = _add_candidate(session, row=1, name="Symphony AI, Inc.", domain="symphonyai.com")
    child_cand = _add_candidate(session, row=2, name="1010data", domain="1010data.com")
    event_row = BenchmarkEventCandidate(
        company_candidate_id=child_cand.id,
        source_workbook="bench.xlsx",
        source_sheet="Sheet1",
        source_row_index=5,
        hint_type=BenchmarkEventHintType.acquisition,
        event_month_hint=date(2023, 6, 1),
        description="Acquired by Symphony AI in June 2023",
    )
    session.add(event_row)
    session.flush()

    result = resolve_candidates(session)
    session.commit()

    assert result.companies_created == 2
    assert result.relations_created == 1

    parent = session.execute(
        select(Company).where(Company.canonical_name == "Symphony AI, Inc.")
    ).scalar_one()
    child = session.execute(
        select(Company).where(Company.canonical_name == "1010data")
    ).scalar_one()
    relation = session.execute(select(CompanyRelation)).scalar_one()
    assert relation.parent_id == parent.id
    assert relation.child_id == child.id
    assert relation.kind is RelationKind.acquired
    assert relation.effective_month == date(2023, 6, 1)
    assert relation.note.startswith("Acquired by Symphony AI")
    assert relation.confidence == pytest.approx(0.6)
    session.refresh(parent_cand)
    assert parent_cand.status is CandidateStatus.resolved


def test_resolver_queues_unresolved_acquirers(session: Session) -> None:
    child = _add_candidate(session, row=1, name="Nimbus Analytics")
    event_row = BenchmarkEventCandidate(
        company_candidate_id=child.id,
        source_workbook="bench.xlsx",
        source_sheet="Sheet1",
        source_row_index=1,
        hint_type=BenchmarkEventHintType.acquisition,
        event_month_hint=None,
        description="Acquired by Unknown Parent Corp in 2024",
        status=BenchmarkEventCandidateStatus.pending_merge,
    )
    session.add(event_row)
    session.flush()

    result = resolve_candidates(session)
    session.commit()
    assert result.relations_created == 0
    assert result.unresolved_acquirers == ["Unknown Parent Corp"]


def test_resolver_does_not_self_link(session: Session) -> None:
    cand = _add_candidate(session, row=1, name="Acme")
    event_row = BenchmarkEventCandidate(
        company_candidate_id=cand.id,
        source_workbook="bench.xlsx",
        source_sheet="Sheet1",
        source_row_index=1,
        hint_type=BenchmarkEventHintType.acquisition,
        event_month_hint=None,
        description="Acquired by Acme in 2023",
    )
    session.add(event_row)
    session.flush()
    result = resolve_candidates(session)
    session.commit()
    assert result.relations_created == 0


def test_resolver_rebrand_relation(session: Session) -> None:
    old_cand = _add_candidate(session, row=1, name="LegacyCorp")
    new_cand = _add_candidate(session, row=2, name="Horizon Labs")
    event_row = BenchmarkEventCandidate(
        company_candidate_id=old_cand.id,
        source_workbook="bench.xlsx",
        source_sheet="Sheet1",
        source_row_index=1,
        hint_type=BenchmarkEventHintType.rebrand,
        event_month_hint=date(2024, 1, 1),
        description="Rebranded to Horizon Labs in 2024",
    )
    session.add(event_row)
    session.flush()
    result = resolve_candidates(session)
    session.commit()
    assert result.relations_created == 1
    relation = session.execute(select(CompanyRelation)).scalar_one()
    assert relation.kind is RelationKind.renamed
    new_company = session.execute(
        select(Company).where(Company.canonical_name == "Horizon Labs")
    ).scalar_one()
    old_company = session.execute(
        select(Company).where(Company.canonical_name == "LegacyCorp")
    ).scalar_one()
    assert relation.parent_id == new_company.id
    assert relation.child_id == old_company.id
    assert new_cand.id  # satisfy unused warning
