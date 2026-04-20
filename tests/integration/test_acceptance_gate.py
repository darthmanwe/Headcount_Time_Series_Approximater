"""Phase 11 acceptance-gate integration test.

This test wires the full evaluation stack against a hand-built 5-company
benchmark fixture and asserts agreed quality thresholds. It is the
"does the product still work end-to-end?" smoke test that the docs
reference as the minimum bar for shipping changes that touch
estimation, confidence, or benchmark comparison.

Thresholds (see ``docs/EVALUATION_V1.md``)
------------------------------------------

* **Coverage (in-scope)**: at least 0.80 of seeded companies must
  receive an estimate. Real failures here usually mean the promoter
  is dropping benchmark rows silently.
* **Zeeshan MAPE on ``headcount_current``**: ≤ 0.05. We promote the
  analyst anchor into :class:`CompanyAnchorObservation` directly, so
  the estimate should land inside the analyst interval and the
  interval-overlap credit should push MAPE to (near) zero.
* **High-confidence disagreements**: exactly 0. Any row with a
  high/medium band that disagrees with analyst by > 2x signals a
  real regression worth blocking.

Each assertion prints a full ``scoreboard`` on failure to speed up
triage.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from headcount.db.enums import (
    BenchmarkMetric,
    BenchmarkProvider,
    HeadcountValueKind,
    PriorityTier,
)
from headcount.estimate.pipeline import estimate_series
from headcount.models import Base, BenchmarkObservation, Company
from headcount.parsers.benchmark_anchors import promote_benchmark_anchors
from headcount.review.evaluation import evaluate_against_benchmarks

_TEST_COMPANIES: list[dict[str, object]] = [
    # Pulled from Zeeshan April 1 sheet of the benchmark workbook;
    # midpoint of the "201-500" bucket etc.
    {
        "name": "1010data",
        "row_index": 1,
        "current": (350.5, 201.0, 500.0),
        "t_minus_6m": 341.0,
        "t_minus_1y": 333.0,
        "t_minus_2y": 318.0,
    },
    {
        "name": "1Kosmos",
        "row_index": 3,
        "current": (125.0, 51.0, 200.0),
        "t_minus_6m": 133.0,
        "t_minus_1y": 142.0,
        "t_minus_2y": 164.0,
    },
    {
        "name": "6sense",
        "row_index": 5,
        "current": (3000.0, 1001.0, 5000.0),
        "t_minus_6m": 3061.0,
        "t_minus_1y": 3125.0,
        "t_minus_2y": 3261.0,
    },
    {
        "name": "AliveCor",
        "row_index": 6,
        "current": (350.5, 201.0, 500.0),
        "t_minus_6m": 357.0,
        "t_minus_1y": 365.0,
        "t_minus_2y": 380.0,
    },
    {
        "name": "Alleva",
        "row_index": 7,
        "current": (125.0, 51.0, 200.0),
        "t_minus_6m": 130.0,
        "t_minus_1y": 136.0,
        "t_minus_2y": 149.0,
    },
]


@pytest.fixture()
def engine() -> Iterator[Engine]:
    eng = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(eng, "connect")
    def _enable_fk(dbapi_conn, _):  # type: ignore[no-untyped-def]
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture()
def factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)


def _seed(factory: sessionmaker[Session]) -> date:
    as_of = date(2026, 4, 1)
    with factory() as session:
        for spec in _TEST_COMPANIES:
            company = Company(
                canonical_name=spec["name"],
                canonical_domain=f"{str(spec['name']).lower()}.com",
                priority_tier=PriorityTier.P1,
            )
            session.add(company)
            session.flush()
            current_point, current_min, current_max = spec["current"]  # type: ignore[misc]
            rows = [
                (
                    BenchmarkMetric.headcount_current,
                    current_point,
                    current_min,
                    current_max,
                    HeadcountValueKind.range,
                ),
                (
                    BenchmarkMetric.headcount_6m_ago,
                    spec["t_minus_6m"],
                    None,
                    None,
                    HeadcountValueKind.exact,
                ),
                (
                    BenchmarkMetric.headcount_1y_ago,
                    spec["t_minus_1y"],
                    None,
                    None,
                    HeadcountValueKind.exact,
                ),
                (
                    BenchmarkMetric.headcount_2y_ago,
                    spec["t_minus_2y"],
                    None,
                    None,
                    HeadcountValueKind.exact,
                ),
            ]
            for metric, point, vmin, vmax, kind in rows:
                session.add(
                    BenchmarkObservation(
                        company_id=company.id,
                        source_workbook="Sample Employee Growth for High Priority Prospects.xlsx",
                        source_sheet="Zeeshan April 1",
                        source_row_index=int(spec["row_index"]),  # type: ignore[arg-type]
                        source_column_name=metric.value,
                        company_name_raw=str(spec["name"]),
                        provider=BenchmarkProvider.zeeshan,
                        metric=metric,
                        as_of_month=as_of,
                        value_min=vmin,
                        value_point=point,
                        value_max=vmax,
                        value_kind=kind,
                    )
                )
        session.commit()
        promote_benchmark_anchors(session)
        session.commit()
        estimate_series(
            session,
            start_month=date(2024, 4, 1),
            end_month=as_of,
            as_of_month=as_of,
            sample_floor=1,
        )
        session.commit()
    return as_of


def test_acceptance_gate_meets_thresholds(factory: sessionmaker[Session]) -> None:
    as_of = _seed(factory)
    with factory() as session:
        board = evaluate_against_benchmarks(session, as_of_month=as_of)

    payload = board.to_dict()
    assert board.companies_in_scope == len(_TEST_COMPANIES), payload
    assert board.coverage_in_scope >= 0.80, payload
    assert board.companies_with_benchmark == len(_TEST_COMPANIES), payload

    zee_current = board.accuracy.get("zeeshan", {}).get("headcount_current", {})
    mape = zee_current.get("mape")
    assert mape is not None, payload
    assert mape <= 0.05, f"zeeshan headcount_current MAPE {mape} exceeds 0.05: {payload}"

    # With analyst anchors as ground truth, no high-confidence disagreements.
    assert board.high_confidence_disagreements == 0, payload


def test_acceptance_gate_coverage_holds_under_scope_restriction(
    factory: sessionmaker[Session],
) -> None:
    """When restricted to a subset, coverage_in_scope must still be 1.0."""

    as_of = _seed(factory)
    with factory() as session:
        company_ids = [
            c.id
            for c in session.execute(select(Company).order_by(Company.canonical_name)).scalars()
        ][:3]
        board = evaluate_against_benchmarks(session, as_of_month=as_of, company_ids=company_ids)
    payload = board.to_dict()
    assert board.companies_in_scope == 3, payload
    assert board.coverage_in_scope == 1.0, payload
