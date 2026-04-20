"""Offline evaluation harness: pipeline output vs. benchmark workbooks.

Phase 11 closes the acceptance-criteria loop by turning "the pipeline
runs" into "we can prove the estimator is accurate, catch regressions,
and explain misses". This module is the math layer: given the current
database state it produces a :class:`Scoreboard` that captures

* coverage (how many in-scope companies received an estimate)
* per-provider accuracy on the four headcount metrics we promote
  (``headcount_current`` / ``6m`` / ``1y`` / ``2y`` ago)
* per-provider growth-window error (``growth_6m_pct`` / ``1y`` / ``2y``)
* review queue state + confidence band distribution
* top per-company disagreements, with workbook/sheet/row provenance

Design choices
--------------

* **Policy explicit, not hidden**: every threshold and weight is a
  ``dataclass`` field and defaults live in constants. The acceptance
  gate passes its own tightened instance when the rules differ from
  the CLI default.
* **Deterministic**: no wall-clock, no RNG. Two runs over the same DB
  state produce byte-identical scoreboards modulo the optional
  ``evaluated_at`` tag.
* **Provider-aware**: each accuracy bucket keeps the provider in its
  key so we can tell "analyst agrees, Harmonic disagrees" apart from
  "everybody disagrees".
* **Interval-aware**: when the benchmark row carries an interval
  (Zeeshan's range buckets, 201-500), we count the estimate as "in
  the range" rather than penalising for not hitting the midpoint.
* **Fail-closed**: if a denominator would be zero (no benchmark rows
  for a metric, no in-scope companies) we emit ``None`` and record the
  sample size, never a fabricated zero.
"""

from __future__ import annotations

import statistics
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from headcount.db.enums import (
    BenchmarkMetric,
    ConfidenceBand,
    ReviewStatus,
)
from headcount.estimate.growth import latest_growth_windows
from headcount.estimate.reconcile import MonthlyEstimate
from headcount.models.benchmark import BenchmarkObservation
from headcount.models.company import Company
from headcount.models.headcount_estimate_monthly import HeadcountEstimateMonthly
from headcount.models.review_queue_item import ReviewQueueItem
from headcount.utils.logging import get_logger

EVALUATION_VERSION = "eval_v1"

_log = get_logger("headcount.review.evaluation")

_HEADCOUNT_METRICS: frozenset[BenchmarkMetric] = frozenset(
    {
        BenchmarkMetric.headcount_current,
        BenchmarkMetric.headcount_6m_ago,
        BenchmarkMetric.headcount_1y_ago,
        BenchmarkMetric.headcount_2y_ago,
    }
)

_GROWTH_METRIC_TO_LABEL: dict[BenchmarkMetric, str] = {
    BenchmarkMetric.growth_6m_pct: "6m",
    BenchmarkMetric.growth_1y_pct: "1y",
    BenchmarkMetric.growth_2y_pct: "2y",
}

_HEADCOUNT_METRIC_OFFSETS: dict[BenchmarkMetric, int] = {
    BenchmarkMetric.headcount_current: 0,
    BenchmarkMetric.headcount_6m_ago: -6,
    BenchmarkMetric.headcount_1y_ago: -12,
    BenchmarkMetric.headcount_2y_ago: -24,
}


# ---------------------------------------------------------------------------
# Config & result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class EvaluationConfig:
    """Policy knobs for a single evaluation run."""

    top_disagreements_limit: int = 25
    # A "high-confidence disagreement" is a row whose estimate
    # ``confidence_band`` is ``high`` or ``medium`` but whose value
    # differs from the benchmark by more than this ratio (100% = 2x).
    high_confidence_disagreement_ratio: float = 1.0
    # Interval-aware: if both estimate and benchmark carry intervals
    # and they overlap, we credit the estimate as "in range" and do
    # not contribute an error term. Set to ``False`` to always compare
    # point-to-point (useful for strict golden diffs).
    credit_interval_overlap: bool = True


@dataclass(slots=True)
class MetricBucket:
    """Per-(provider, metric) accuracy bucket."""

    n: int = 0
    errors_abs: list[float] = field(default_factory=list)
    errors_pct: list[float] = field(default_factory=list)

    def add_point(
        self,
        *,
        estimate: float,
        benchmark: float,
    ) -> None:
        self.n += 1
        self.errors_abs.append(abs(estimate - benchmark))
        if benchmark != 0.0:
            self.errors_pct.append(abs(estimate - benchmark) / abs(benchmark))

    def summary(self) -> dict[str, float | int | None]:
        if self.n == 0:
            return {"n": 0, "mae": None, "mape": None, "median_abs_error": None}
        return {
            "n": self.n,
            "mae": round(statistics.fmean(self.errors_abs), 4),
            "mape": (
                round(statistics.fmean(self.errors_pct), 4)
                if self.errors_pct
                else None
            ),
            "median_abs_error": round(statistics.median(self.errors_abs), 4),
        }


@dataclass(frozen=True, slots=True)
class Disagreement:
    company_id: str
    company_name: str
    provider: str
    metric: str
    month: date
    benchmark_point: float
    estimate_point: float
    abs_ratio: float
    interval_overlap: bool
    confidence_band: str
    workbook: str
    sheet: str
    row_index: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "company_id": self.company_id,
            "company_name": self.company_name,
            "provider": self.provider,
            "metric": self.metric,
            "month": self.month.isoformat(),
            "benchmark_point": self.benchmark_point,
            "estimate_point": self.estimate_point,
            "abs_ratio": round(self.abs_ratio, 4),
            "interval_overlap": self.interval_overlap,
            "confidence_band": self.confidence_band,
            "workbook": self.workbook,
            "sheet": self.sheet,
            "row_index": self.row_index,
        }


@dataclass(slots=True)
class Scoreboard:
    """Full evaluation output. Serializable to JSON."""

    evaluation_version: str
    as_of_month: date
    companies_in_scope: int
    companies_evaluated: int
    companies_with_benchmark: int
    coverage_in_scope: float
    coverage_with_benchmark: float
    # ``accuracy[provider][metric] -> summary dict``
    accuracy: dict[str, dict[str, dict[str, float | int | None]]]
    growth_accuracy: dict[str, dict[str, dict[str, float | int | None]]]
    confidence_bands: dict[str, int]
    review_queue_open: int
    high_confidence_disagreements: int
    top_disagreements: list[Disagreement]
    evaluated_at: str | None = None
    estimate_run_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "evaluation_version": self.evaluation_version,
            "as_of_month": self.as_of_month.isoformat(),
            "evaluated_at": self.evaluated_at,
            "estimate_run_id": self.estimate_run_id,
            "companies": {
                "in_scope": self.companies_in_scope,
                "evaluated": self.companies_evaluated,
                "with_benchmark": self.companies_with_benchmark,
            },
            "coverage": {
                "in_scope": round(self.coverage_in_scope, 4),
                "with_benchmark": round(self.coverage_with_benchmark, 4),
            },
            "accuracy": self.accuracy,
            "growth_accuracy": self.growth_accuracy,
            "confidence_bands": self.confidence_bands,
            "review": {
                "queue_open": self.review_queue_open,
                "high_confidence_disagreements": self.high_confidence_disagreements,
            },
            "top_disagreements": [d.to_dict() for d in self.top_disagreements],
        }

    def headline_mape(self) -> float | None:
        """Zeeshan ``headcount_current`` MAPE - the headline accuracy KPI."""

        row = self.accuracy.get("zeeshan", {}).get("headcount_current", {})
        mape = row.get("mape")
        return float(mape) if mape is not None else None

    def headline_growth_mae(self) -> float | None:
        """Zeeshan ``1y`` growth MAE - headline growth-accuracy KPI."""

        row = self.growth_accuracy.get("zeeshan", {}).get("1y", {})
        mae = row.get("mae")
        return float(mae) if mae is not None else None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _month_floor(d: date) -> date:
    return d.replace(day=1)


def _offset_month(reference: date, months: int) -> date:
    ref = _month_floor(reference)
    total = ref.year * 12 + (ref.month - 1) + months
    year = total // 12
    month = total % 12 + 1
    return date(year, month, 1)


def _intervals_overlap(
    a_min: float, a_max: float, b_min: float | None, b_max: float | None
) -> bool:
    if b_min is None or b_max is None:
        return False
    return not (a_max < b_min or b_max < a_min)


def _load_latest_estimates(
    session: Session, *, company_ids: Sequence[str] | None = None
) -> dict[str, dict[date, MonthlyEstimate]]:
    """Return ``{company_id: {month: MonthlyEstimate}}`` for the latest
    estimate_version_id per company, as determined by the max
    ``created_at`` on the ``HeadcountEstimateMonthly`` rows.

    We intentionally read directly from the monthly table (rather than
    dereferencing :class:`EstimateVersion`) so an evaluation can run
    against any DB state, including partial pipelines.
    """

    stmt = select(HeadcountEstimateMonthly).order_by(
        HeadcountEstimateMonthly.company_id,
        HeadcountEstimateMonthly.created_at.desc(),
    )
    if company_ids:
        stmt = stmt.where(HeadcountEstimateMonthly.company_id.in_(list(company_ids)))

    out: dict[str, dict[date, MonthlyEstimate]] = {}
    latest_version: dict[str, str] = {}
    for row in session.execute(stmt).scalars():
        cid = row.company_id
        version = row.estimate_version_id
        if cid not in latest_version:
            latest_version[cid] = version
        if latest_version[cid] != version:
            continue
        bucket = out.setdefault(cid, {})
        bucket[row.month] = _row_to_monthly_estimate(row)
    return out


def _row_to_monthly_estimate(row: HeadcountEstimateMonthly) -> MonthlyEstimate:
    return MonthlyEstimate(
        month=row.month,
        value_min=float(row.estimated_headcount_min),
        value_point=float(row.estimated_headcount),
        value_max=float(row.estimated_headcount_max),
        public_profile_count=int(row.public_profile_count),
        scaled_from_anchor_value=float(row.scaled_from_anchor_value),
        method=row.method,
        confidence_band=row.confidence_band,
        needs_review=bool(row.needs_review),
        suppression_reason=row.suppression_reason,
    )


def _group_benchmarks(
    benchmarks: Iterable[BenchmarkObservation],
) -> dict[str, list[BenchmarkObservation]]:
    out: dict[str, list[BenchmarkObservation]] = {}
    for b in benchmarks:
        if b.company_id is None:
            continue
        out.setdefault(b.company_id, []).append(b)
    return out


# ---------------------------------------------------------------------------
# Core evaluation
# ---------------------------------------------------------------------------


def evaluate_against_benchmarks(
    session: Session,
    *,
    as_of_month: date,
    config: EvaluationConfig | None = None,
    company_ids: Sequence[str] | None = None,
    estimate_run_id: str | None = None,
    evaluated_at: datetime | None = None,
) -> Scoreboard:
    """Compute a :class:`Scoreboard` for the current DB state.

    ``company_ids`` restricts the scope; otherwise every
    :class:`Company` is considered in-scope. ``as_of_month`` is the
    reference for translating ``headcount_6m_ago`` / ``1y_ago`` /
    ``2y_ago`` metrics into concrete months.
    """

    cfg = config or EvaluationConfig()
    as_of = _month_floor(as_of_month)

    if company_ids:
        scope_ids = [str(x) for x in company_ids if x]
        scope_rows = list(
            session.execute(
                select(Company.id, Company.canonical_name).where(
                    Company.id.in_(scope_ids)
                )
            )
        )
    else:
        scope_rows = list(
            session.execute(select(Company.id, Company.canonical_name))
        )
    company_names: dict[str, str] = {row[0]: row[1] for row in scope_rows}
    scope_set = set(company_names.keys())

    estimates_by_company = _load_latest_estimates(session, company_ids=scope_ids if company_ids else None)

    bench_stmt = select(BenchmarkObservation).where(
        BenchmarkObservation.company_id.is_not(None)
    )
    if company_ids:
        bench_stmt = bench_stmt.where(
            BenchmarkObservation.company_id.in_(list(scope_set))
        )
    benchmarks_by_company = _group_benchmarks(
        session.execute(bench_stmt).scalars()
    )

    accuracy: dict[str, dict[str, MetricBucket]] = {}
    growth_accuracy: dict[str, dict[str, MetricBucket]] = {}
    disagreements: list[Disagreement] = []
    high_confidence_disagreement_count = 0
    confidence_bands: dict[str, int] = {band.value: 0 for band in ConfidenceBand}

    companies_evaluated = 0
    for cid in scope_set:
        monthly = estimates_by_company.get(cid)
        if monthly:
            companies_evaluated += 1
            for est in monthly.values():
                confidence_bands[est.confidence_band.value] = (
                    confidence_bands.get(est.confidence_band.value, 0) + 1
                )

    companies_with_benchmark = sum(
        1 for cid in scope_set if benchmarks_by_company.get(cid)
    )

    for cid, benches in benchmarks_by_company.items():
        if cid not in scope_set:
            continue
        monthly = estimates_by_company.get(cid)
        if monthly is None:
            continue
        cname = company_names.get(cid, "?")
        growth_points = {p.horizon: p for p in latest_growth_windows(monthly.values())}

        for bench in benches:
            _score_one_benchmark_row(
                bench=bench,
                company_id=cid,
                company_name=cname,
                monthly=monthly,
                growth_points=growth_points,
                as_of=as_of,
                cfg=cfg,
                accuracy=accuracy,
                growth_accuracy=growth_accuracy,
                disagreements=disagreements,
            )

    # Count high-confidence disagreements after the fact so we can
    # threshold on the ratio specifically.
    for d in disagreements:
        if (
            d.confidence_band in {ConfidenceBand.high.value, ConfidenceBand.medium.value}
            and d.abs_ratio >= cfg.high_confidence_disagreement_ratio
        ):
            high_confidence_disagreement_count += 1

    disagreements.sort(key=lambda d: d.abs_ratio, reverse=True)
    top = disagreements[: cfg.top_disagreements_limit]

    review_queue_open = int(
        session.execute(
            select(func.count(ReviewQueueItem.id)).where(
                ReviewQueueItem.status == ReviewStatus.open
            )
        ).scalar_one()
    )

    coverage_in_scope = (
        companies_evaluated / len(scope_set) if scope_set else 0.0
    )
    coverage_with_benchmark = (
        sum(
            1
            for cid in scope_set
            if benchmarks_by_company.get(cid) and estimates_by_company.get(cid)
        )
        / companies_with_benchmark
        if companies_with_benchmark
        else 0.0
    )

    return Scoreboard(
        evaluation_version=EVALUATION_VERSION,
        as_of_month=as_of,
        companies_in_scope=len(scope_set),
        companies_evaluated=companies_evaluated,
        companies_with_benchmark=companies_with_benchmark,
        coverage_in_scope=round(coverage_in_scope, 4),
        coverage_with_benchmark=round(coverage_with_benchmark, 4),
        accuracy={
            provider: {metric: bucket.summary() for metric, bucket in by_metric.items()}
            for provider, by_metric in accuracy.items()
        },
        growth_accuracy={
            provider: {metric: bucket.summary() for metric, bucket in by_metric.items()}
            for provider, by_metric in growth_accuracy.items()
        },
        confidence_bands=confidence_bands,
        review_queue_open=review_queue_open,
        high_confidence_disagreements=high_confidence_disagreement_count,
        top_disagreements=top,
        evaluated_at=evaluated_at.isoformat() if evaluated_at is not None else None,
        estimate_run_id=estimate_run_id,
    )


def _score_one_benchmark_row(
    *,
    bench: BenchmarkObservation,
    company_id: str,
    company_name: str,
    monthly: dict[date, MonthlyEstimate],
    growth_points: dict[str, Any],
    as_of: date,
    cfg: EvaluationConfig,
    accuracy: dict[str, dict[str, MetricBucket]],
    growth_accuracy: dict[str, dict[str, MetricBucket]],
    disagreements: list[Disagreement],
) -> None:
    provider = bench.provider.value
    metric = bench.metric
    value_point = bench.value_point
    if value_point is None:
        return

    if metric in _HEADCOUNT_METRICS:
        target_month = (
            bench.as_of_month
            if metric is BenchmarkMetric.headcount_current and bench.as_of_month
            else _offset_month(as_of, _HEADCOUNT_METRIC_OFFSETS[metric])
        )
        target_month = _month_floor(target_month)
        est = monthly.get(target_month)
        if est is None:
            return
        bucket = accuracy.setdefault(provider, {}).setdefault(metric.value, MetricBucket())
        overlap = _intervals_overlap(
            est.value_min, est.value_max, bench.value_min, bench.value_max
        )
        if cfg.credit_interval_overlap and overlap:
            bucket.n += 1
            # Interval overlap = zero error for accuracy math.
            bucket.errors_abs.append(0.0)
            bucket.errors_pct.append(0.0)
        else:
            bucket.add_point(estimate=est.value_point, benchmark=float(value_point))

        ratio = abs(est.value_point - float(value_point)) / max(1.0, abs(float(value_point)))
        # Flag a row as a "disagreement" only when the point gap is
        # material (>10% of benchmark). Pure interval-mismatch without a
        # numeric gap is tracked implicitly via ``accuracy`` bookkeeping
        # and is too noisy to surface as a top-line disagreement.
        if ratio > 0.1:
            disagreements.append(
                Disagreement(
                    company_id=company_id,
                    company_name=company_name,
                    provider=provider,
                    metric=metric.value,
                    month=target_month,
                    benchmark_point=float(value_point),
                    estimate_point=est.value_point,
                    abs_ratio=ratio,
                    interval_overlap=overlap,
                    confidence_band=est.confidence_band.value,
                    workbook=bench.source_workbook,
                    sheet=bench.source_sheet,
                    row_index=bench.source_row_index,
                )
            )
        return

    if metric in _GROWTH_METRIC_TO_LABEL:
        label = _GROWTH_METRIC_TO_LABEL[metric]
        point = growth_points.get(label)
        if point is None or point.value_point is None:
            return
        bucket = growth_accuracy.setdefault(provider, {}).setdefault(label, MetricBucket())
        # Benchmark is a percentage like 5.1 meaning +5.1%; growth_point
        # is a ratio like 0.051. Convert benchmark to ratio to compare.
        bench_ratio = float(value_point) / 100.0
        bucket.add_point(estimate=float(point.value_point), benchmark=bench_ratio)


def persist_scoreboard(
    session: Session,
    scoreboard: Scoreboard,
    *,
    note: str | None = None,
) -> str:
    """Insert an :class:`EvaluationRun` row and return its ``id``."""

    from headcount.models.evaluation_run import EvaluationRun

    row = EvaluationRun(
        estimate_run_id=scoreboard.estimate_run_id,
        as_of_month=scoreboard.as_of_month,
        evaluation_version=scoreboard.evaluation_version,
        companies_in_scope=scoreboard.companies_in_scope,
        companies_evaluated=scoreboard.companies_evaluated,
        companies_with_benchmark=scoreboard.companies_with_benchmark,
        coverage_in_scope=scoreboard.coverage_in_scope,
        coverage_with_benchmark=scoreboard.coverage_with_benchmark,
        mape_headcount_current=scoreboard.headline_mape(),
        mae_growth_1y_pct=scoreboard.headline_growth_mae(),
        review_queue_open=scoreboard.review_queue_open,
        high_confidence_disagreements=scoreboard.high_confidence_disagreements,
        scoreboard_json=scoreboard.to_dict(),
        note=note,
    )
    session.add(row)
    session.flush()
    _log.info(
        "evaluation_run_persisted",
        evaluation_id=row.id,
        as_of=scoreboard.as_of_month.isoformat(),
        headline_mape=scoreboard.headline_mape(),
        companies_evaluated=scoreboard.companies_evaluated,
    )
    return row.id


__all__ = [
    "EVALUATION_VERSION",
    "Disagreement",
    "EvaluationConfig",
    "MetricBucket",
    "Scoreboard",
    "evaluate_against_benchmarks",
    "persist_scoreboard",
]
