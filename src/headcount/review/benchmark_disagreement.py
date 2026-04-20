"""Benchmark vs estimate disagreement detection.

The benchmark spreadsheets in ``test_source/`` are an acceptance-test
snapshot: they tell us what "ground truth" looked like at a particular
month for a particular company. We never train against them, but we
*do* want review to flag months where our estimate sits far outside the
benchmark interval - that usually means the anchors drifted, a event
was missed, or the LinkedIn sample diverged.

Detection rules
---------------

For every :class:`BenchmarkObservation` with a resolvable
``as_of_month`` and ``value_point`` (``headcount_current`` /
``headcount_6m_ago`` / ``headcount_1y_ago`` / ``headcount_2y_ago``):

1. Translate the metric's relative offset into a concrete month against
   a caller-supplied reference (the benchmark's snapshot month).
2. Find the :class:`HeadcountEstimateMonthly` row at that month.
3. Compute disagreement as
   ``max(0, abs(benchmark.value_point - estimate.value_point) /
   max(1, benchmark.value_point))``.
4. If disagreement exceeds ``threshold`` (default 25%) OR the estimate
   interval does not overlap the benchmark interval, flag the row.

Output is a list of :class:`BenchmarkDisagreement`; the review queue
module converts them into :class:`ReviewQueueItem` rows scoped to the
benchmark's provider.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from headcount.db.enums import BenchmarkMetric
from headcount.estimate.reconcile import MonthlyEstimate
from headcount.models.benchmark import BenchmarkObservation

BENCHMARK_DISAGREEMENT_VERSION = "bench_disagreement_v1"
"""Bumped if rules or thresholds change materially."""


_METRIC_MONTH_OFFSET: dict[BenchmarkMetric, int] = {
    BenchmarkMetric.headcount_current: 0,
    BenchmarkMetric.headcount_6m_ago: -6,
    BenchmarkMetric.headcount_1y_ago: -12,
    BenchmarkMetric.headcount_2y_ago: -24,
}


@dataclass(frozen=True, slots=True)
class BenchmarkDisagreement:
    benchmark_id: str
    company_id: str
    month: date
    benchmark_point: float
    estimate_point: float
    relative_gap: float
    interval_overlap: bool
    provider: str
    metric: str


def _offset_month(reference: date, months: int) -> date:
    # Normalize to the first day of the month first so arithmetic is
    # stable across reference days.
    ref = reference.replace(day=1)
    total = ref.year * 12 + (ref.month - 1) + months
    year = total // 12
    month = total % 12 + 1
    return date(year, month, 1)


def _intervals_overlap(
    a_min: float | None,
    a_max: float | None,
    b_min: float,
    b_max: float,
) -> bool:
    if a_min is None or a_max is None:
        return False
    return not (a_max < b_min or b_max < a_min)


def detect_benchmark_disagreement(
    benchmarks: Iterable[BenchmarkObservation],
    estimates_by_month: dict[date, MonthlyEstimate],
    *,
    threshold: float = 0.25,
) -> list[BenchmarkDisagreement]:
    """Return one entry per (benchmark, estimate) pair that disagrees.

    Benchmarks without resolvable months, without numeric points, or
    without a matching estimate month are silently skipped - review
    queue surfaces those as coverage problems, not disagreements.
    """

    out: list[BenchmarkDisagreement] = []
    for b in benchmarks:
        if b.metric not in _METRIC_MONTH_OFFSET:
            continue
        if b.value_point is None or b.company_id is None:
            continue
        reference_month = b.as_of_month
        if reference_month is None:
            # No anchor we can align to; skip. The review queue
            # separately flags these as coverage_missing.
            continue
        offset = _METRIC_MONTH_OFFSET[b.metric]
        target = _offset_month(reference_month, offset)

        estimate = estimates_by_month.get(target)
        if estimate is None:
            continue

        bench_point = float(b.value_point)
        est_point = float(estimate.value_point)
        gap = abs(bench_point - est_point) / max(1.0, bench_point)

        overlap = _intervals_overlap(
            b.value_min,
            b.value_max,
            estimate.value_min,
            estimate.value_max,
        )

        if gap > threshold or not overlap:
            out.append(
                BenchmarkDisagreement(
                    benchmark_id=b.id,
                    company_id=b.company_id,
                    month=target,
                    benchmark_point=bench_point,
                    estimate_point=est_point,
                    relative_gap=gap,
                    interval_overlap=overlap,
                    provider=b.provider.value,
                    metric=b.metric.value,
                )
            )
    return out


__all__ = [
    "BENCHMARK_DISAGREEMENT_VERSION",
    "BenchmarkDisagreement",
    "detect_benchmark_disagreement",
]
