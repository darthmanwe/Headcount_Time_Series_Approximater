"""Integration tests for :mod:`headcount.serving.benchmark_comparison`."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from headcount.db.enums import (
    AnchorType,
    BenchmarkMetric,
    BenchmarkProvider,
    HeadcountValueKind,
    PriorityTier,
    SourceName,
)
from headcount.estimate.pipeline import estimate_series
from headcount.models import (
    Base,
    BenchmarkObservation,
    Company,
    CompanyAnchorObservation,
    Person,
    PersonEmploymentObservation,
)
from headcount.serving.benchmark_comparison import compare_estimates_to_benchmarks


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_fk(dbapi_conn, _):  # type: ignore[no-untyped-def]
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    with Session(engine, future=True) as s:
        yield s


def _seed(
    session: Session,
    *,
    benchmark_value: float,
) -> Company:
    company = Company(
        canonical_name="Acme Inc",
        canonical_domain="acme.com",
        priority_tier=PriorityTier.P1,
    )
    session.add(company)
    session.flush()

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
        p = Person(
            source_name=SourceName.manual,
            source_person_key=f"manual::p{i}",
            display_name=f"Person {i}",
        )
        session.add(p)
        session.flush()
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
        end_month=date(2023, 12, 1),
        as_of_month=date(2023, 12, 1),
        sample_floor=1,
    )

    session.add(
        BenchmarkObservation(
            company_id=company.id,
            source_workbook="test.xlsx",
            source_sheet="Main",
            source_row_index=1,
            company_name_raw=company.canonical_name,
            provider=BenchmarkProvider.zeeshan,
            metric=BenchmarkMetric.headcount_current,
            as_of_month=date(2023, 6, 1),
            value_point=benchmark_value,
            value_min=benchmark_value - 5,
            value_max=benchmark_value + 5,
        )
    )
    session.flush()
    return company


def test_compare_matches_when_estimate_close_to_benchmark(session: Session) -> None:
    _seed(session, benchmark_value=1000.0)
    summary = compare_estimates_to_benchmarks(session)
    assert summary.companies_with_benchmarks == 1
    assert summary.benchmarks_total == 1
    assert summary.benchmarks_matched == 1
    assert summary.disagreements_total == 0


def test_compare_flags_disagreement_above_threshold(session: Session) -> None:
    _seed(session, benchmark_value=200.0)
    summary = compare_estimates_to_benchmarks(session, threshold=0.25)
    assert summary.disagreements_total == 1
    only = summary.per_company[0]
    assert only.disagreements[0].relative_gap > 0.25
    assert not only.disagreements[0].interval_overlap


def test_compare_tracks_company_without_version(session: Session) -> None:
    company = Company(
        canonical_name="No Version Co",
        priority_tier=PriorityTier.P2,
    )
    session.add(company)
    session.flush()
    session.add(
        BenchmarkObservation(
            company_id=company.id,
            source_workbook="test.xlsx",
            source_sheet="Main",
            source_row_index=2,
            company_name_raw=company.canonical_name,
            provider=BenchmarkProvider.harmonic,
            metric=BenchmarkMetric.headcount_current,
            as_of_month=date(2023, 6, 1),
            value_point=500.0,
        )
    )
    session.flush()

    summary = compare_estimates_to_benchmarks(session)
    assert summary.companies_with_benchmarks == 1
    assert summary.benchmarks_total == 1
    # No estimate version -> nothing matched, nothing flagged.
    assert summary.benchmarks_matched == 0
    assert summary.disagreements_total == 0


def test_to_dict_roundtrips(session: Session) -> None:
    _seed(session, benchmark_value=200.0)
    summary = compare_estimates_to_benchmarks(session)
    payload = summary.to_dict()
    assert payload["disagreements_total"] == 1
    assert payload["per_company"][0]["disagreements"]
    assert "relative_gap" in payload["per_company"][0]["disagreements"][0]
