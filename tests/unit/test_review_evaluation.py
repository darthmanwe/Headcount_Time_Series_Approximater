"""Unit tests for :mod:`headcount.review.evaluation`.

Exercises the pure math (``MetricBucket``, interval-overlap handling,
scoreboard aggregation) and the full DB-backed evaluator against a
hand-built company + benchmark fixture set.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from headcount.db.enums import (
    BenchmarkMetric,
    BenchmarkProvider,
    ConfidenceBand,
    EstimateMethod,
    HeadcountValueKind,
    PriorityTier,
    ReviewReason,
    ReviewStatus,
    RunKind,
    RunStatus,
)
from headcount.models import (
    Base,
    BenchmarkObservation,
    Company,
    EstimateVersion,
    HeadcountEstimateMonthly,
    ReviewQueueItem,
    Run,
)
from headcount.review.evaluation import (
    EvaluationConfig,
    MetricBucket,
    evaluate_against_benchmarks,
    persist_scoreboard,
)


@pytest.fixture()
def session() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _enable_fk(dbapi_conn, _):  # type: ignore[no-untyped-def]
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    with Session(engine, future=True) as s:
        yield s


# ---------------------------------------------------------------------------
# Pure-math: MetricBucket
# ---------------------------------------------------------------------------


def test_metric_bucket_empty_summary() -> None:
    b = MetricBucket()
    s = b.summary()
    assert s == {"n": 0, "mae": None, "mape": None, "median_abs_error": None}


def test_metric_bucket_computes_mae_and_mape() -> None:
    b = MetricBucket()
    b.add_point(estimate=110.0, benchmark=100.0)
    b.add_point(estimate=90.0, benchmark=100.0)
    b.add_point(estimate=150.0, benchmark=100.0)
    s = b.summary()
    assert s["n"] == 3
    # Errors: 10, 10, 50 -> MAE 23.333
    assert s["mae"] == pytest.approx(23.3333, rel=1e-3)
    # Pct errors: 0.1, 0.1, 0.5 -> MAPE 0.2333
    assert s["mape"] == pytest.approx(0.2333, rel=1e-3)
    assert s["median_abs_error"] == pytest.approx(10.0, rel=1e-3)


def test_metric_bucket_skips_mape_when_benchmark_zero() -> None:
    b = MetricBucket()
    b.add_point(estimate=10.0, benchmark=0.0)
    b.add_point(estimate=5.0, benchmark=10.0)
    s = b.summary()
    # MAE averages both errors (10 and 5) -> 7.5. MAPE only has 1 term.
    assert s["n"] == 2
    assert s["mae"] == pytest.approx(7.5, rel=1e-3)
    assert s["mape"] == pytest.approx(0.5, rel=1e-3)


# ---------------------------------------------------------------------------
# DB-backed evaluator
# ---------------------------------------------------------------------------


def _seed_company(session: Session, name: str) -> Company:
    c = Company(
        canonical_name=name,
        canonical_domain=f"{name.lower()}.com",
        priority_tier=PriorityTier.P1,
    )
    session.add(c)
    session.flush()
    return c


def _seed_estimate_row(
    session: Session,
    *,
    company: Company,
    version_id: str,
    month: date,
    value_point: float,
    band: ConfidenceBand = ConfidenceBand.medium,
) -> None:
    row = HeadcountEstimateMonthly(
        company_id=company.id,
        estimate_version_id=version_id,
        month=month,
        estimated_headcount=value_point,
        estimated_headcount_min=value_point * 0.5,
        estimated_headcount_max=value_point * 1.5,
        public_profile_count=0,
        scaled_from_anchor_value=value_point,
        method=EstimateMethod.interpolated_multi_anchor,
        confidence_band=band,
        needs_review=False,
        suppression_reason=None,
    )
    session.add(row)


def _seed_estimate_version(
    session: Session,
    company: Company,
    *,
    cutoff: date = date(2026, 4, 1),
) -> EstimateVersion:
    run = Run(
        kind=RunKind.full,
        status=RunStatus.succeeded,
        started_at=datetime.now(tz=UTC),
        cutoff_month=cutoff,
        method_version="method_v1",
        anchor_policy_version="anchor_v1",
        coverage_curve_version="cov_v1",
        config_hash="test",
    )
    session.add(run)
    session.flush()

    version = EstimateVersion(
        company_id=company.id,
        estimation_run_id=run.id,
        method_version=run.method_version,
        anchor_policy_version=run.anchor_policy_version,
        coverage_curve_version=run.coverage_curve_version,
        source_snapshot_cutoff=run.cutoff_month,
    )
    session.add(version)
    session.flush()
    return version


def _seed_benchmark(
    session: Session,
    *,
    company: Company,
    provider: BenchmarkProvider,
    metric: BenchmarkMetric,
    as_of: date,
    value_point: float,
    value_min: float | None = None,
    value_max: float | None = None,
    row_index: int = 1,
) -> BenchmarkObservation:
    obs = BenchmarkObservation(
        company_id=company.id,
        source_workbook="Sample Employee Growth for High Priority Prospects.xlsx",
        source_sheet="Zeeshan April 1",
        source_row_index=row_index,
        source_cell_address=f"D{row_index + 1}",
        source_column_name="Current Employee Count",
        company_name_raw=company.canonical_name,
        provider=provider,
        metric=metric,
        as_of_month=as_of,
        value_min=value_min,
        value_point=value_point,
        value_max=value_max,
        value_kind=(HeadcountValueKind.exact if value_min is None else HeadcountValueKind.range),
    )
    session.add(obs)
    session.flush()
    return obs


def test_evaluate_empty_db_returns_zero_coverage(session: Session) -> None:
    board = evaluate_against_benchmarks(session, as_of_month=date(2026, 4, 1))
    assert board.companies_in_scope == 0
    assert board.coverage_in_scope == 0.0
    assert board.accuracy == {}


def test_evaluate_matches_analyst_exactly_gives_zero_error(session: Session) -> None:
    company = _seed_company(session, "Acme")
    version = _seed_estimate_version(session, company)
    # Anchor months for zeeshan metrics when as_of=2026-04:
    # current=2026-04, 6m=2025-10, 1y=2025-04, 2y=2024-04
    _seed_estimate_row(
        session, company=company, version_id=version.id, month=date(2026, 4, 1), value_point=350.0
    )
    _seed_estimate_row(
        session, company=company, version_id=version.id, month=date(2025, 10, 1), value_point=341.0
    )
    _seed_estimate_row(
        session, company=company, version_id=version.id, month=date(2025, 4, 1), value_point=333.0
    )
    _seed_estimate_row(
        session, company=company, version_id=version.id, month=date(2024, 4, 1), value_point=318.0
    )

    # Zeeshan benchmarks aligned with those months.
    _seed_benchmark(
        session,
        company=company,
        provider=BenchmarkProvider.zeeshan,
        metric=BenchmarkMetric.headcount_current,
        as_of=date(2026, 4, 1),
        value_point=350.0,
    )
    _seed_benchmark(
        session,
        company=company,
        provider=BenchmarkProvider.zeeshan,
        metric=BenchmarkMetric.headcount_6m_ago,
        as_of=date(2026, 4, 1),
        value_point=341.0,
        row_index=2,
    )
    _seed_benchmark(
        session,
        company=company,
        provider=BenchmarkProvider.zeeshan,
        metric=BenchmarkMetric.headcount_1y_ago,
        as_of=date(2026, 4, 1),
        value_point=333.0,
        row_index=3,
    )
    _seed_benchmark(
        session,
        company=company,
        provider=BenchmarkProvider.zeeshan,
        metric=BenchmarkMetric.headcount_2y_ago,
        as_of=date(2026, 4, 1),
        value_point=318.0,
        row_index=4,
    )
    session.flush()

    board = evaluate_against_benchmarks(session, as_of_month=date(2026, 4, 1))

    assert board.companies_in_scope == 1
    assert board.companies_evaluated == 1
    assert board.companies_with_benchmark == 1
    assert board.coverage_in_scope == 1.0

    zee = board.accuracy["zeeshan"]
    assert zee["headcount_current"]["n"] == 1
    assert zee["headcount_current"]["mae"] == 0.0
    assert zee["headcount_current"]["mape"] == 0.0
    # All four metrics present.
    assert set(zee.keys()) == {
        "headcount_current",
        "headcount_6m_ago",
        "headcount_1y_ago",
        "headcount_2y_ago",
    }
    # No disagreements when values match exactly and intervals overlap.
    assert board.top_disagreements == []
    assert board.high_confidence_disagreements == 0


def test_evaluate_flags_high_confidence_disagreement(session: Session) -> None:
    """Primary-provider (Harmonic) disagreements trip ``high_confidence_disagreements``."""

    company = _seed_company(session, "Acme")
    version = _seed_estimate_version(session, company)
    # Estimate says 1000, Harmonic says 100 -> abs_ratio 9.0, far
    # beyond the default 1.0 threshold and band is ``high``.
    _seed_estimate_row(
        session,
        company=company,
        version_id=version.id,
        month=date(2026, 4, 1),
        value_point=1000.0,
        band=ConfidenceBand.high,
    )
    _seed_benchmark(
        session,
        company=company,
        provider=BenchmarkProvider.harmonic,
        metric=BenchmarkMetric.headcount_current,
        as_of=date(2026, 4, 1),
        value_point=100.0,
    )
    session.flush()

    board = evaluate_against_benchmarks(session, as_of_month=date(2026, 4, 1))
    assert board.high_confidence_disagreements == 1
    assert board.supporting_disagreements == 0
    assert len(board.top_disagreements) == 1
    d = board.top_disagreements[0]
    assert d.provider == "harmonic"
    assert d.abs_ratio >= 1.0


def test_evaluate_routes_supporting_provider_disagreements_separately(
    session: Session,
) -> None:
    """A Zeeshan 9x-off row must not trip the Harmonic-only gate."""

    company = _seed_company(session, "Acme")
    version = _seed_estimate_version(session, company)
    _seed_estimate_row(
        session,
        company=company,
        version_id=version.id,
        month=date(2026, 4, 1),
        value_point=1000.0,
        band=ConfidenceBand.high,
    )
    _seed_benchmark(
        session,
        company=company,
        provider=BenchmarkProvider.zeeshan,
        metric=BenchmarkMetric.headcount_current,
        as_of=date(2026, 4, 1),
        value_point=100.0,
    )
    session.flush()

    board = evaluate_against_benchmarks(session, as_of_month=date(2026, 4, 1))
    assert board.high_confidence_disagreements == 0
    assert board.supporting_disagreements == 1
    assert len(board.top_disagreements) == 1
    assert board.top_disagreements[0].provider == "zeeshan"


def test_evaluate_credits_interval_overlap(session: Session) -> None:
    """When both estimate and benchmark carry intervals that overlap,
    the accuracy bucket records zero error."""

    company = _seed_company(session, "Acme")
    version = _seed_estimate_version(session, company)
    row = HeadcountEstimateMonthly(
        company_id=company.id,
        estimate_version_id=version.id,
        month=date(2026, 4, 1),
        estimated_headcount=350.5,
        estimated_headcount_min=201.0,
        estimated_headcount_max=500.0,
        public_profile_count=0,
        scaled_from_anchor_value=350.5,
        method=EstimateMethod.interpolated_multi_anchor,
        confidence_band=ConfidenceBand.medium,
        needs_review=False,
        suppression_reason=None,
    )
    session.add(row)

    # Benchmark interval 201-500 fully contains the estimate interval.
    _seed_benchmark(
        session,
        company=company,
        provider=BenchmarkProvider.zeeshan,
        metric=BenchmarkMetric.headcount_current,
        as_of=date(2026, 4, 1),
        value_point=350.5,
        value_min=201.0,
        value_max=500.0,
    )
    session.flush()

    board = evaluate_against_benchmarks(session, as_of_month=date(2026, 4, 1))
    zee_current = board.accuracy["zeeshan"]["headcount_current"]
    assert zee_current["mae"] == 0.0


def test_evaluate_counts_review_queue_open(session: Session) -> None:
    company = _seed_company(session, "Acme")
    for status in (ReviewStatus.open, ReviewStatus.open, ReviewStatus.resolved):
        session.add(
            ReviewQueueItem(
                company_id=company.id,
                estimate_version_id=None,
                review_reason=ReviewReason.low_confidence,
                status=status,
                priority=1,
            )
        )
    session.flush()

    board = evaluate_against_benchmarks(session, as_of_month=date(2026, 4, 1))
    assert board.review_queue_open == 2


def test_persist_scoreboard_round_trips(session: Session) -> None:
    company = _seed_company(session, "Acme")
    version = _seed_estimate_version(session, company)
    _seed_estimate_row(
        session, company=company, version_id=version.id, month=date(2026, 4, 1), value_point=100.0
    )
    _seed_benchmark(
        session,
        company=company,
        provider=BenchmarkProvider.zeeshan,
        metric=BenchmarkMetric.headcount_current,
        as_of=date(2026, 4, 1),
        value_point=100.0,
    )
    session.flush()

    board = evaluate_against_benchmarks(session, as_of_month=date(2026, 4, 1))
    eval_id = persist_scoreboard(session, board, note="unit-test")
    session.flush()

    from headcount.models import EvaluationRun

    row = session.get(EvaluationRun, eval_id)
    assert row is not None
    assert row.evaluation_version == board.evaluation_version
    assert row.companies_evaluated == 1
    assert row.scoreboard_json["coverage"]["in_scope"] == 1.0
    assert row.note == "unit-test"


def test_evaluate_scope_restriction(session: Session) -> None:
    a = _seed_company(session, "A")
    b = _seed_company(session, "B")
    for c in (a, b):
        v = _seed_estimate_version(session, c)
        _seed_estimate_row(
            session, company=c, version_id=v.id, month=date(2026, 4, 1), value_point=100.0
        )
    session.flush()

    board_all = evaluate_against_benchmarks(session, as_of_month=date(2026, 4, 1))
    assert board_all.companies_in_scope == 2

    board_a = evaluate_against_benchmarks(session, as_of_month=date(2026, 4, 1), company_ids=[a.id])
    assert board_a.companies_in_scope == 1
    assert board_a.companies_evaluated == 1


def test_evaluation_config_threshold_controls_high_confidence_count(session: Session) -> None:
    company = _seed_company(session, "Acme")
    version = _seed_estimate_version(session, company)
    _seed_estimate_row(
        session,
        company=company,
        version_id=version.id,
        month=date(2026, 4, 1),
        value_point=100.0,
        band=ConfidenceBand.medium,
    )
    _seed_benchmark(
        session,
        company=company,
        provider=BenchmarkProvider.harmonic,
        metric=BenchmarkMetric.headcount_current,
        as_of=date(2026, 4, 1),
        value_point=150.0,
    )
    session.flush()

    # Ratio = 50 / 150 = 0.333. Default threshold (1.0) should not flag.
    board_default = evaluate_against_benchmarks(session, as_of_month=date(2026, 4, 1))
    assert board_default.high_confidence_disagreements == 0

    # Tighten threshold: 0.2 should flag.
    board_strict = evaluate_against_benchmarks(
        session,
        as_of_month=date(2026, 4, 1),
        config=EvaluationConfig(high_confidence_disagreement_ratio=0.2),
    )
    assert board_strict.high_confidence_disagreements == 1


def test_harmonic_cohort_is_tracked_separately(session: Session) -> None:
    """Only companies with a Harmonic benchmark row count as cohort."""

    c_harmonic = _seed_company(session, "H-Corp")
    c_zeeshan_only = _seed_company(session, "Z-Corp")
    for c in (c_harmonic, c_zeeshan_only):
        v = _seed_estimate_version(session, c)
        _seed_estimate_row(
            session, company=c, version_id=v.id, month=date(2026, 4, 1), value_point=100.0
        )
    _seed_benchmark(
        session,
        company=c_harmonic,
        provider=BenchmarkProvider.harmonic,
        metric=BenchmarkMetric.headcount_current,
        as_of=date(2026, 4, 1),
        value_point=100.0,
    )
    _seed_benchmark(
        session,
        company=c_zeeshan_only,
        provider=BenchmarkProvider.zeeshan,
        metric=BenchmarkMetric.headcount_current,
        as_of=date(2026, 4, 1),
        value_point=100.0,
        row_index=2,
    )
    session.flush()

    board = evaluate_against_benchmarks(session, as_of_month=date(2026, 4, 1))
    assert board.companies_with_benchmark == 2
    assert board.harmonic_cohort_size == 1
    assert board.harmonic_cohort_evaluated == 1


def test_rank_correlation_matches_harmonic_ordering(session: Session) -> None:
    """Spearman rho == 1.0 when our growth ordering matches Harmonic's exactly."""

    companies = []
    for i, (now, ya) in enumerate([(100.0, 100.0), (200.0, 100.0), (300.0, 100.0), (400.0, 100.0)]):
        c = _seed_company(session, f"Co{i}")
        companies.append(c)
        v = _seed_estimate_version(session, c)
        # Growth ratio ~= (now - ya) / ya, increasing with i.
        _seed_estimate_row(
            session, company=c, version_id=v.id, month=date(2025, 4, 1), value_point=ya
        )
        _seed_estimate_row(
            session, company=c, version_id=v.id, month=date(2026, 4, 1), value_point=now
        )
        # Harmonic 365d percentage that also increases with i.
        pct = ((now - ya) / ya) * 100.0
        _seed_benchmark(
            session,
            company=c,
            provider=BenchmarkProvider.harmonic,
            metric=BenchmarkMetric.growth_1y_pct,
            as_of=date(2026, 4, 1),
            value_point=pct,
            row_index=i + 1,
        )
    session.flush()

    board = evaluate_against_benchmarks(session, as_of_month=date(2026, 4, 1))
    rho = board.rank_correlation.get("harmonic", {}).get("1y")
    assert rho is not None
    assert rho == pytest.approx(1.0, rel=1e-6)
    assert board.headline_spearman("1y") == pytest.approx(1.0, rel=1e-6)
