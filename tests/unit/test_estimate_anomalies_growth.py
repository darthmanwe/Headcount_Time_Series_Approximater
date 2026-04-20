"""Unit tests for anomaly detection and growth series."""

from __future__ import annotations

from datetime import date

import pytest

from headcount.db.enums import ConfidenceBand, EstimateMethod
from headcount.estimate.anomalies import (
    ANOMALIES_VERSION,
    detect_anomalies,
)
from headcount.estimate.growth import (
    GROWTH_VERSION,
    compute_growth_series,
)
from headcount.estimate.reconcile import MonthlyEstimate


def _ok(
    month: date, point: float, *, vmin: float | None = None, vmax: float | None = None
) -> MonthlyEstimate:
    return MonthlyEstimate(
        month=month,
        value_min=point * 0.95 if vmin is None else vmin,
        value_point=point,
        value_max=point * 1.05 if vmax is None else vmax,
        public_profile_count=100,
        scaled_from_anchor_value=point,
        method=EstimateMethod.scaled_ratio_coverage_corrected,
        confidence_band=ConfidenceBand.high,
    )


def test_version_constants_stable() -> None:
    assert ANOMALIES_VERSION == "anomalies_v1"
    assert GROWTH_VERSION == "growth_v1"


def test_no_anomalies_on_smooth_series() -> None:
    series = [_ok(date(2023, m, 1), 1000 + m * 5) for m in range(1, 7)]
    flags = detect_anomalies(series)
    assert all(not f.needs_review for f in flags)


def test_wide_interval_triggers_width_flag() -> None:
    e = _ok(date(2023, 6, 1), 1000, vmin=200, vmax=2500)
    flags = detect_anomalies([e])
    assert flags[0].interval_too_wide is True
    assert flags[0].needs_review is True


def test_mom_jump_flagged_when_not_on_segment_break() -> None:
    series = [
        _ok(date(2023, 1, 1), 1000),
        _ok(date(2023, 2, 1), 1900),  # +90%
    ]
    flags = detect_anomalies(series)
    assert flags[0].mom_jump is False
    assert flags[1].mom_jump is True


def test_mom_jump_suppressed_on_segment_break() -> None:
    series = [
        _ok(date(2023, 1, 1), 1000),
        _ok(date(2023, 2, 1), 1900),  # acquisition lands here
    ]
    flags = detect_anomalies(series, segment_break_months={date(2023, 2, 1)})
    assert flags[1].mom_jump is False


def test_sample_floor_violation_mirrored_from_suppression_reason() -> None:
    e = MonthlyEstimate(
        month=date(2023, 1, 1),
        value_min=450,
        value_point=500,
        value_max=550,
        public_profile_count=2,
        scaled_from_anchor_value=500,
        method=EstimateMethod.suppressed_low_sample,
        confidence_band=ConfidenceBand.manual_review_required,
        needs_review=True,
        suppression_reason="profiles_below_floor(2<5)",
    )
    flags = detect_anomalies([e])
    assert flags[0].sample_floor_violation is True


def test_coverage_floor_hit_flag() -> None:
    # coverage_factor = 1 / 0.15 = 6.666... saturates the default floor.
    e = MonthlyEstimate(
        month=date(2023, 1, 1),
        value_min=450,
        value_point=500,
        value_max=550,
        public_profile_count=100,
        scaled_from_anchor_value=500,
        method=EstimateMethod.scaled_ratio_coverage_corrected,
        confidence_band=ConfidenceBand.medium,
        coverage_factor=1.0 / 0.15,
    )
    flags = detect_anomalies([e])
    assert flags[0].coverage_floor_hit is True


def test_growth_series_computes_mom_qoq_yoy() -> None:
    series = [
        _ok(date(2022, 1, 1), 100),
        _ok(date(2022, 4, 1), 110),
        _ok(date(2022, 12, 1), 140),
        _ok(date(2023, 1, 1), 150),
    ]
    points = compute_growth_series(series)
    horizons = {(p.month, p.horizon) for p in points}
    assert (date(2023, 1, 1), "1m") in horizons
    assert (date(2023, 1, 1), "3m") not in horizons  # no Oct 2022 in series
    assert (date(2023, 1, 1), "12m") in horizons
    yoy = next(p for p in points if p.month == date(2023, 1, 1) and p.horizon == "12m")
    assert yoy.value_point == pytest.approx(0.5)


def test_growth_returns_nothing_for_suppressed_endpoints() -> None:
    prev = _ok(date(2023, 1, 1), 1000)
    low = MonthlyEstimate(
        month=date(2023, 2, 1),
        value_min=950,
        value_point=1000,
        value_max=1050,
        public_profile_count=0,
        scaled_from_anchor_value=1000,
        method=EstimateMethod.suppressed_low_sample,
        confidence_band=ConfidenceBand.manual_review_required,
    )
    points = compute_growth_series([prev, low], horizons=(1,))
    assert points == []
