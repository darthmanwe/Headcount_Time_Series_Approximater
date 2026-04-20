"""Unit tests for :mod:`headcount.review.benchmark_disagreement`."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from headcount.db.enums import (
    BenchmarkMetric,
    BenchmarkProvider,
    ConfidenceBand,
    EstimateMethod,
    HeadcountValueKind,
)
from headcount.estimate.reconcile import MonthlyEstimate
from headcount.review.benchmark_disagreement import (
    BENCHMARK_DISAGREEMENT_VERSION,
    detect_benchmark_disagreement,
)


@dataclass
class _FakeBenchmark:
    """Stand-in with just the fields the detector actually reads."""

    id: str
    company_id: str | None
    provider: BenchmarkProvider
    metric: BenchmarkMetric
    as_of_month: date | None
    value_min: float | None
    value_point: float | None
    value_max: float | None
    value_kind: HeadcountValueKind | None = HeadcountValueKind.exact


def _est(month: date, point: float, spread: float = 20.0) -> MonthlyEstimate:
    return MonthlyEstimate(
        month=month,
        value_min=point - spread,
        value_point=point,
        value_max=point + spread,
        public_profile_count=100,
        scaled_from_anchor_value=point,
        method=EstimateMethod.scaled_ratio_coverage_corrected,
        confidence_band=ConfidenceBand.high,
    )


def test_version_stable() -> None:
    assert BENCHMARK_DISAGREEMENT_VERSION == "bench_disagreement_v1"


def test_close_estimate_produces_no_disagreement() -> None:
    est = _est(date(2023, 6, 1), 1000)
    b = _FakeBenchmark(
        id="b1",
        company_id="c1",
        provider=BenchmarkProvider.zeeshan,
        metric=BenchmarkMetric.headcount_current,
        as_of_month=date(2023, 6, 1),
        value_min=980,
        value_point=1010,
        value_max=1040,
    )
    out = detect_benchmark_disagreement([b], {date(2023, 6, 1): est})
    assert out == []


def test_far_estimate_produces_disagreement() -> None:
    est = _est(date(2023, 6, 1), 1500)
    b = _FakeBenchmark(
        id="b1",
        company_id="c1",
        provider=BenchmarkProvider.zeeshan,
        metric=BenchmarkMetric.headcount_current,
        as_of_month=date(2023, 6, 1),
        value_min=980,
        value_point=1000,
        value_max=1020,
    )
    out = detect_benchmark_disagreement([b], {date(2023, 6, 1): est})
    assert len(out) == 1
    dis = out[0]
    assert dis.company_id == "c1"
    assert dis.month == date(2023, 6, 1)
    assert dis.relative_gap > 0.25
    assert dis.interval_overlap is False


def test_1y_ago_metric_resolves_to_prior_year_month() -> None:
    est = _est(date(2022, 6, 1), 500)
    b = _FakeBenchmark(
        id="b1",
        company_id="c1",
        provider=BenchmarkProvider.harmonic,
        metric=BenchmarkMetric.headcount_1y_ago,
        as_of_month=date(2023, 6, 1),
        value_min=1000,
        value_point=1200,
        value_max=1400,
    )
    out = detect_benchmark_disagreement([b], {date(2022, 6, 1): est})
    assert len(out) == 1
    assert out[0].month == date(2022, 6, 1)


def test_benchmark_without_month_is_skipped() -> None:
    est = _est(date(2023, 6, 1), 1000)
    b = _FakeBenchmark(
        id="b1",
        company_id="c1",
        provider=BenchmarkProvider.zeeshan,
        metric=BenchmarkMetric.headcount_current,
        as_of_month=None,
        value_min=1000,
        value_point=2000,
        value_max=3000,
    )
    out = detect_benchmark_disagreement([b], {date(2023, 6, 1): est})
    assert out == []


def test_missing_estimate_month_is_skipped() -> None:
    b = _FakeBenchmark(
        id="b1",
        company_id="c1",
        provider=BenchmarkProvider.zeeshan,
        metric=BenchmarkMetric.headcount_current,
        as_of_month=date(2023, 6, 1),
        value_min=1000,
        value_point=1100,
        value_max=1200,
    )
    out = detect_benchmark_disagreement([b], {})
    assert out == []


def test_interval_non_overlap_flags_even_below_threshold() -> None:
    est = _est(date(2023, 6, 1), 1100, spread=10)
    b = _FakeBenchmark(
        id="b1",
        company_id="c1",
        provider=BenchmarkProvider.zeeshan,
        metric=BenchmarkMetric.headcount_current,
        as_of_month=date(2023, 6, 1),
        value_min=1150,
        value_point=1200,
        value_max=1250,
    )
    out = detect_benchmark_disagreement(
        [b], {date(2023, 6, 1): est}, threshold=0.50
    )
    assert len(out) == 1
    assert out[0].interval_overlap is False
