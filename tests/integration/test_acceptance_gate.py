"""Phase 11 acceptance-gate integration test.

This test wires the full evaluation stack against a hand-built
5-company benchmark fixture and asserts agreed quality thresholds. It
is the "does the product still work end-to-end?" smoke test that the
docs reference as the minimum bar for shipping changes that touch
estimation, confidence, or benchmark comparison.

Fixture shape
-------------

Each company carries both a Harmonic row (point ``Headcount`` +
percentage growth for 6m and 1y - the signal we are approximating)
and a Zeeshan row (range bucket + historical points + 2y growth -
supporting evidence). Harmonic is the promotion winner and the
headline KPI target; Zeeshan covers the horizons Harmonic does not
emit. We pick five companies with distinct Harmonic growth rates so
Spearman rank correlation is well-defined.

Thresholds (see ``docs/EVALUATION_V1.md``)
------------------------------------------

* **Coverage (in-scope)**: at least 0.80 of seeded companies must
  receive an estimate.
* **Harmonic cohort coverage**: must be 1.0 - every Harmonic-seeded
  company must produce an estimate.
* **Harmonic MAPE on ``headcount_current``**: ≤ 0.05. Harmonic's
  point anchor is promoted, so the latest-month estimate must land
  on it (or inside the Zeeshan range, which overlaps) and MAPE must
  collapse to near zero.
* **Harmonic MAE on ``growth_1y_pct``**: ≤ 0.05 (5 percentage
  points). Tests that the growth window we emit tracks Harmonic's
  365-day rate.
* **Spearman rank correlation on ``growth_1y_pct`` vs Harmonic**: ≥
  0.70. Kept loose because N=5 is tiny; tighten as the Harmonic
  cohort grows toward its full ~25-company size.
* **High-confidence (Harmonic) disagreements**: exactly 0. Any
  high/medium-band row off Harmonic by > 2x is a real regression.

Each assertion prints a full scoreboard on failure to speed up
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

# Five-company calibration set. Each entry carries what each provider
# emits for that company in the reference workbook.
#
# * Harmonic: current headcount point, 6m / 1y growth percentages.
# * Zeeshan: current range bucket, historical points, 2y growth %.
# * Harmonic and Zeeshan current-month values agree (by construction)
#   so the interval-overlap credit applies cleanly. Historical points
#   are Zeeshan-only because Harmonic does not emit them.
_TEST_COMPANIES: list[dict[str, object]] = [
    {
        "name": "1010data",
        "row_index": 1,
        # Zeeshan range bucket and Harmonic point.
        "zeeshan_current": (350.5, 201.0, 500.0),
        "harmonic_current": 350.0,
        # Harmonic percentage growth.
        "harmonic_6m_pct": 2.8,
        "harmonic_1y_pct": 5.3,
        # Zeeshan historical points + 2y growth.
        "t_minus_6m": 341.0,
        "t_minus_1y": 333.0,
        "t_minus_2y": 318.0,
        "zeeshan_2y_pct": 10.2,
    },
    {
        "name": "1Kosmos",
        "row_index": 3,
        "zeeshan_current": (125.0, 51.0, 200.0),
        "harmonic_current": 125.0,
        "harmonic_6m_pct": -6.0,
        "harmonic_1y_pct": -12.0,
        "t_minus_6m": 133.0,
        "t_minus_1y": 142.0,
        "t_minus_2y": 164.0,
        "zeeshan_2y_pct": -23.8,
    },
    {
        "name": "6sense",
        "row_index": 5,
        "zeeshan_current": (3000.0, 1001.0, 5000.0),
        "harmonic_current": 3000.0,
        "harmonic_6m_pct": -2.0,
        "harmonic_1y_pct": -4.0,
        "t_minus_6m": 3061.0,
        "t_minus_1y": 3125.0,
        "t_minus_2y": 3261.0,
        "zeeshan_2y_pct": -8.0,
    },
    {
        "name": "AliveCor",
        "row_index": 6,
        "zeeshan_current": (350.5, 201.0, 500.0),
        "harmonic_current": 350.0,
        "harmonic_6m_pct": -1.8,
        "harmonic_1y_pct": -4.1,
        "t_minus_6m": 357.0,
        "t_minus_1y": 365.0,
        "t_minus_2y": 380.0,
        "zeeshan_2y_pct": -7.9,
    },
    {
        "name": "Alleva",
        "row_index": 7,
        "zeeshan_current": (125.0, 51.0, 200.0),
        "harmonic_current": 125.0,
        "harmonic_6m_pct": -3.8,
        "harmonic_1y_pct": -8.1,
        "t_minus_6m": 130.0,
        "t_minus_1y": 136.0,
        "t_minus_2y": 149.0,
        "zeeshan_2y_pct": -16.1,
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
    # Real ingest snaps Harmonic's "April 8" pull to the nearest
    # month-start so it lines up with our monthly grid; mirror that
    # here so the Harmonic anchor sits at ``as_of`` and wins the tie.
    harmonic_as_of = as_of
    workbook = "Sample Employee Growth for High Priority Prospects.xlsx"
    # Per-metric historical month offsets used to give each Zeeshan
    # historical anchor its correct ``anchor_month`` (the promoter
    # does not infer this from the metric label - it uses the raw
    # ``as_of_month`` on the benchmark row).
    historical_as_of = {
        BenchmarkMetric.headcount_current: as_of,
        BenchmarkMetric.headcount_6m_ago: date(2025, 10, 1),
        BenchmarkMetric.headcount_1y_ago: date(2025, 4, 1),
        BenchmarkMetric.headcount_2y_ago: date(2024, 4, 1),
        BenchmarkMetric.growth_2y_pct: as_of,
    }
    with factory() as session:
        for spec in _TEST_COMPANIES:
            company = Company(
                canonical_name=str(spec["name"]),
                canonical_domain=f"{str(spec['name']).lower()}.com",
                priority_tier=PriorityTier.P1,
            )
            session.add(company)
            session.flush()

            # --- Zeeshan rows ------------------------------------------------
            zc_point, zc_min, zc_max = spec["zeeshan_current"]  # type: ignore[misc]
            zeeshan_rows = [
                (
                    BenchmarkMetric.headcount_current,
                    zc_point,
                    zc_min,
                    zc_max,
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
                (
                    BenchmarkMetric.growth_2y_pct,
                    spec["zeeshan_2y_pct"],
                    None,
                    None,
                    HeadcountValueKind.exact,
                ),
            ]
            for metric, point, vmin, vmax, kind in zeeshan_rows:
                session.add(
                    BenchmarkObservation(
                        company_id=company.id,
                        source_workbook=workbook,
                        source_sheet="Zeeshan April 1",
                        source_row_index=int(spec["row_index"]),  # type: ignore[arg-type]
                        source_column_name=metric.value,
                        company_name_raw=str(spec["name"]),
                        provider=BenchmarkProvider.zeeshan,
                        metric=metric,
                        as_of_month=historical_as_of[metric],
                        value_min=vmin,
                        value_point=point,
                        value_max=vmax,
                        value_kind=kind,
                    )
                )

            # --- Harmonic rows -----------------------------------------------
            harmonic_rows = [
                (
                    BenchmarkMetric.headcount_current,
                    spec["harmonic_current"],
                ),
                (
                    BenchmarkMetric.growth_6m_pct,
                    spec["harmonic_6m_pct"],
                ),
                (
                    BenchmarkMetric.growth_1y_pct,
                    spec["harmonic_1y_pct"],
                ),
            ]
            for metric, point in harmonic_rows:
                session.add(
                    BenchmarkObservation(
                        company_id=company.id,
                        source_workbook=workbook,
                        source_sheet="Harmonic April 8",
                        source_row_index=int(spec["row_index"]),  # type: ignore[arg-type]
                        source_column_name=metric.value,
                        company_name_raw=str(spec["name"]),
                        provider=BenchmarkProvider.harmonic,
                        metric=metric,
                        as_of_month=harmonic_as_of,
                        value_point=float(point),  # type: ignore[arg-type]
                        value_kind=HeadcountValueKind.exact,
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


def test_acceptance_gate_meets_harmonic_thresholds(
    factory: sessionmaker[Session],
) -> None:
    as_of = _seed(factory)
    with factory() as session:
        board = evaluate_against_benchmarks(session, as_of_month=as_of)

    payload = board.to_dict()
    assert board.companies_in_scope == len(_TEST_COMPANIES), payload
    assert board.companies_with_benchmark == len(_TEST_COMPANIES), payload
    assert board.coverage_in_scope >= 0.80, payload

    # Harmonic cohort = every seeded company (all have Harmonic rows).
    assert board.harmonic_cohort_size == len(_TEST_COMPANIES), payload
    assert board.harmonic_cohort_evaluated == len(_TEST_COMPANIES), payload

    # --- Headline: Harmonic MAPE on headcount_current ------------------
    harmonic_current = board.accuracy.get("harmonic", {}).get("headcount_current", {})
    mape = harmonic_current.get("mape")
    assert mape is not None, payload
    assert mape <= 0.05, f"harmonic headcount_current MAPE {mape} exceeds 0.05: {payload}"

    # --- Headline: Harmonic MAE on 1y growth ---------------------------
    growth_1y = board.growth_accuracy.get("harmonic", {}).get("1y", {})
    mae_1y = growth_1y.get("mae")
    assert mae_1y is not None, payload
    assert mae_1y <= 0.05, (
        f"harmonic growth_1y_pct MAE {mae_1y} exceeds 0.05 (5 percentage points): {payload}"
    )

    # --- Headline: Spearman rank correlation vs Harmonic ---------------
    # N=5 is tiny; we keep the floor at 0.70 per implementation notes
    # in docs/EVALUATION_V1.md. Revisit as the Harmonic cohort grows
    # toward its full ~25-company size.
    spearman_1y = board.rank_correlation.get("harmonic", {}).get("1y")
    assert spearman_1y is not None, payload
    assert spearman_1y >= 0.70, f"harmonic growth_1y spearman {spearman_1y} below 0.70: {payload}"

    # --- No high-confidence Harmonic disagreements ---------------------
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
    assert board.harmonic_cohort_size == 3, payload
