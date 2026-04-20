"""Unit tests for ratio-scaling / reconcile."""

from __future__ import annotations

from datetime import date

import pytest

from headcount.db.enums import ConfidenceBand, EstimateMethod
from headcount.estimate.anchors import ReconciledAnchor
from headcount.estimate.coverage import CoverageCurve
from headcount.estimate.reconcile import (
    DEFAULT_SAMPLE_FLOOR,
    METHOD_VERSION,
    reconcile_series,
)
from headcount.estimate.segments import Segment


def _flat_coverage() -> CoverageCurve:
    # Coverage identical at every age so ratio math is easy to reason about.
    return CoverageCurve(
        coverage_at_recent=0.5,
        plateau_start=0,
        coverage_plateau=0.5,
        plateau_end=1000,
        old_age=1001,
        coverage_at_old=0.5,
    )


def _anchor(
    point: float = 1000,
    vmin: float = 900,
    vmax: float = 1100,
    month: date = date(2023, 6, 1),
) -> ReconciledAnchor:
    return ReconciledAnchor(
        anchor_month=month,
        value_point=point,
        value_min=vmin,
        value_max=vmax,
        contributing_ids=("x",),
        weights={"x": 1.0},
        rationale="test",
    )


def test_version_constant_stable() -> None:
    assert METHOD_VERSION == "method_v1"


def test_ratio_scaling_scales_all_three_bounds() -> None:
    segment = Segment(start_month=date(2023, 1, 1), end_month=date(2023, 6, 1))
    profiles = {
        date(2023, 1, 1): 50,
        date(2023, 2, 1): 60,
        date(2023, 3, 1): 70,
        date(2023, 4, 1): 80,
        date(2023, 5, 1): 90,
        date(2023, 6, 1): 100,  # anchor month
    }
    anchor = _anchor(point=1000, vmin=900, vmax=1100, month=date(2023, 6, 1))

    rows = reconcile_series(
        segment,
        anchor=anchor,
        monthly_profiles=profiles,
        coverage=_flat_coverage(),
        as_of_month=date(2023, 6, 1),
    )

    by_month = {r.month: r for r in rows}
    # At anchor_month ratio is 1.0 -> identical interval.
    r6 = by_month[date(2023, 6, 1)]
    assert r6.ratio == pytest.approx(1.0)
    assert r6.value_point == pytest.approx(1000.0)
    assert r6.value_min == pytest.approx(900.0)
    assert r6.value_max == pytest.approx(1100.0)
    assert r6.method is EstimateMethod.scaled_ratio_coverage_corrected
    # At jan ratio is 50/100 = 0.5.
    r1 = by_month[date(2023, 1, 1)]
    assert r1.ratio == pytest.approx(0.5)
    assert r1.value_point == pytest.approx(500.0)
    assert r1.value_min == pytest.approx(450.0)
    assert r1.value_max == pytest.approx(550.0)


def test_low_sample_months_are_suppressed() -> None:
    segment = Segment(start_month=date(2023, 1, 1), end_month=date(2023, 3, 1))
    profiles = {
        date(2023, 1, 1): 2,  # below floor
        date(2023, 2, 1): 30,
        date(2023, 3, 1): 50,
    }
    anchor = _anchor(point=500, vmin=450, vmax=550, month=date(2023, 3, 1))

    rows = reconcile_series(
        segment,
        anchor=anchor,
        monthly_profiles=profiles,
        coverage=_flat_coverage(),
        as_of_month=date(2023, 3, 1),
        sample_floor=DEFAULT_SAMPLE_FLOOR,
    )
    by_month = {r.month: r for r in rows}

    assert by_month[date(2023, 1, 1)].method is EstimateMethod.suppressed_low_sample
    assert by_month[date(2023, 1, 1)].needs_review is True
    assert "profiles_below_floor" in (by_month[date(2023, 1, 1)].suppression_reason or "")
    assert by_month[date(2023, 2, 1)].method is EstimateMethod.scaled_ratio_coverage_corrected
    assert by_month[date(2023, 3, 1)].method is EstimateMethod.scaled_ratio_coverage_corrected


def test_anchor_month_no_profiles_falls_back_to_current_only() -> None:
    segment = Segment(start_month=date(2023, 1, 1), end_month=date(2023, 3, 1))
    profiles = dict.fromkeys([date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1)], 0)
    anchor = _anchor(point=500, vmin=450, vmax=550, month=date(2023, 3, 1))

    rows = reconcile_series(
        segment,
        anchor=anchor,
        monthly_profiles=profiles,
        coverage=_flat_coverage(),
        as_of_month=date(2023, 3, 1),
    )

    for r in rows:
        assert r.method is EstimateMethod.degraded_current_only
        assert r.value_point == pytest.approx(500.0)
        assert r.needs_review is True


def test_missing_anchor_marks_all_months_suppressed() -> None:
    segment = Segment(start_month=date(2023, 1, 1), end_month=date(2023, 3, 1))
    profiles = {
        date(2023, 1, 1): 100,
        date(2023, 2, 1): 120,
        date(2023, 3, 1): 150,
    }

    rows = reconcile_series(
        segment,
        anchor=None,
        monthly_profiles=profiles,
        coverage=_flat_coverage(),
        as_of_month=date(2023, 3, 1),
    )
    assert all(r.method is EstimateMethod.suppressed_low_sample for r in rows)
    assert all(r.needs_review for r in rows)
    assert all(r.value_point == 0.0 for r in rows)


def test_confidence_band_reflects_interval_width() -> None:
    segment = Segment(start_month=date(2023, 6, 1), end_month=date(2023, 6, 1))
    profiles = {date(2023, 6, 1): 100}
    # Wide interval -> low/manual_review_required.
    wide = _anchor(point=1000, vmin=500, vmax=2500, month=date(2023, 6, 1))
    # Tight interval -> high.
    tight = _anchor(point=1000, vmin=980, vmax=1020, month=date(2023, 6, 1))

    wide_row = reconcile_series(
        segment,
        anchor=wide,
        monthly_profiles=profiles,
        coverage=_flat_coverage(),
        as_of_month=date(2023, 6, 1),
    )[0]
    tight_row = reconcile_series(
        segment,
        anchor=tight,
        monthly_profiles=profiles,
        coverage=_flat_coverage(),
        as_of_month=date(2023, 6, 1),
    )[0]

    assert tight_row.confidence_band is ConfidenceBand.high
    assert wide_row.confidence_band in {
        ConfidenceBand.low,
        ConfidenceBand.manual_review_required,
    }
