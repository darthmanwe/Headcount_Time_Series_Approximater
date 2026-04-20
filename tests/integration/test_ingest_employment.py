"""Integration tests for hc collect-employment's orchestrator."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session

from headcount.db.enums import (
    BenchmarkMetric,
    BenchmarkProvider,
    CompanyRunStage,
    CompanyRunStageStatus,
    PriorityTier,
    RunStatus,
    SourceName,
)
from headcount.ingest.employment import (
    collect_employment,
    import_profiles_csv,
)
from headcount.models import Base, Company, CompanyAnchorObservation, Run
from headcount.models.benchmark import BenchmarkObservation
from headcount.models.person import Person
from headcount.models.person_employment_observation import (
    PersonEmploymentObservation,
)
from headcount.models.run import CompanyRunStatus


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_fk(dbapi_conn, _):  # type: ignore[no-untyped-def]
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    with Session(engine, future=True) as s:
        yield s


def _make_company(
    session: Session, *, name: str, domain: str | None = None
) -> Company:
    company = Company(
        canonical_name=name,
        canonical_domain=domain or f"{name.lower().replace(' ', '')}.com",
        priority_tier=PriorityTier.P1,
    )
    session.add(company)
    session.flush()
    return company


def _add_bench(
    session: Session,
    company: Company,
    *,
    metric: BenchmarkMetric,
    as_of: date,
    value: float,
    row_idx: int,
) -> None:
    session.add(
        BenchmarkObservation(
            company_id=company.id,
            source_workbook="sample.xlsx",
            source_sheet="Summary",
            source_row_index=row_idx,
            source_cell_address="D5",
            source_column_name=metric.value,
            company_name_raw=company.canonical_name,
            company_domain_raw=company.canonical_domain,
            provider=BenchmarkProvider.harmonic,
            metric=metric,
            as_of_month=as_of,
            value_point=value,
        )
    )
    session.flush()


def test_promotes_benchmark_and_records_run_state(session: Session) -> None:
    acme = _make_company(session, name="Acme Inc")
    _add_bench(
        session,
        acme,
        metric=BenchmarkMetric.headcount_current,
        as_of=date(2026, 4, 1),
        value=400.0,
        row_idx=1,
    )
    _add_bench(
        session,
        acme,
        metric=BenchmarkMetric.headcount_6m_ago,
        as_of=date(2025, 10, 1),
        value=360.0,
        row_idx=1,
    )

    result = collect_employment(session)
    session.flush()

    assert result.companies_attempted == 1
    assert result.companies_succeeded == 1
    assert result.companies_failed == 0
    assert result.benchmark.inserted_anchor_rows == 2

    anchors = list(session.execute(select(CompanyAnchorObservation)).scalars())
    assert len(anchors) == 2

    run = session.get(Run, result.run_id)
    assert run is not None
    assert run.finished_at is not None
    assert run.status is RunStatus.succeeded

    stage = (
        session.execute(
            select(CompanyRunStatus).where(
                CompanyRunStatus.run_id == result.run_id,
                CompanyRunStatus.stage == CompanyRunStage.collect_employment,
            )
        )
        .scalars()
        .one()
    )
    assert stage.status is CompanyRunStageStatus.succeeded


def test_csv_import_idempotent_and_resolves_by_domain(
    session: Session, tmp_path: Path
) -> None:
    acme = _make_company(session, name="Acme Inc", domain="acme.com")
    csv_path = tmp_path / "profiles.csv"
    csv_path.write_text(
        "person_source_key,company_domain,start_month,end_month,"
        "display_name,job_title\n"
        "li:alice,acme.com,2023-04-01,,Alice A,Engineer\n"
        "li:bob,acme.com,2022-06-01,2024-01-01,Bob B,Manager\n"
        "li:nomatch,unknown.com,2023-01-01,,,\n",
        encoding="utf-8",
    )

    stats = import_profiles_csv(session, csv_path=csv_path)
    session.flush()
    assert stats.rows_read == 3
    assert stats.rows_imported == 2
    assert stats.persons_created == 2
    assert stats.rows_skipped_missing_company == 1

    rows = list(session.execute(select(PersonEmploymentObservation)).scalars())
    assert len(rows) == 2
    assert {r.company_id for r in rows} == {acme.id}

    # Re-run: everything should be a duplicate now.
    stats2 = import_profiles_csv(session, csv_path=csv_path)
    session.flush()
    assert stats2.rows_imported == 0
    assert stats2.rows_skipped_duplicate == 2
    # No new persons should have been created.
    persons = list(session.execute(select(Person)).scalars())
    assert len(persons) == 2


def test_collect_employment_with_csv_happy_path(
    session: Session, tmp_path: Path
) -> None:
    acme = _make_company(session, name="Acme Inc", domain="acme.com")
    csv_path = tmp_path / "profiles.csv"
    csv_path.write_text(
        "person_source_key,company_id,start_month\n"
        f"li:one,{acme.id},2023-01-01\n"
        f"li:two,{acme.id},2023-02-01\n",
        encoding="utf-8",
    )

    result = collect_employment(
        session, profiles_csv=csv_path
    )
    session.flush()
    assert result.csv.rows_imported == 2
    rows = list(session.execute(select(PersonEmploymentObservation)).scalars())
    assert len(rows) == 2


def test_ocr_requested_without_observer_is_noop(session: Session) -> None:
    _make_company(session, name="Acme Inc")
    result = collect_employment(session, sources=["linkedin_ocr"])
    session.flush()
    assert result.ocr_signals == 0
    # No errors should be raised just because the observer is absent.
    assert result.errors == []


def test_csv_missing_required_columns_raises(
    session: Session, tmp_path: Path
) -> None:
    csv_path = tmp_path / "bad.csv"
    csv_path.write_text(
        "person_source_key,company_id\nli:a,abc\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="missing required columns"):
        import_profiles_csv(session, csv_path=csv_path)


def test_custom_source_name_persisted(
    session: Session, tmp_path: Path
) -> None:
    acme = _make_company(session, name="Acme Inc", domain="acme.com")
    csv_path = tmp_path / "profiles.csv"
    csv_path.write_text(
        "person_source_key,company_id,start_month,source_name\n"
        f"manual:alice,{acme.id},2024-01-01,manual\n",
        encoding="utf-8",
    )
    import_profiles_csv(session, csv_path=csv_path)
    session.flush()
    person = next(iter(session.execute(select(Person)).scalars()))
    assert person.source_name is SourceName.manual
