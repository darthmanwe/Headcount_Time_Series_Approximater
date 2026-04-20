"""Unit tests for the coverage curve."""

from __future__ import annotations

from datetime import date

import pytest

from headcount.estimate.coverage import (
    COVERAGE_CURVE_VERSION,
    DEFAULT_MIN_COVERAGE,
    CoverageCurve,
    build_default_coverage_curve,
    months_between,
)


def test_version_constant_stable() -> None:
    assert COVERAGE_CURVE_VERSION == "coverage_default_v1"


def test_default_curve_shape() -> None:
    c = build_default_coverage_curve()
    at_recent = c.at_age(0)
    plateau_start = c.at_age(c.plateau_start)
    plateau_mid = c.at_age((c.plateau_start + c.plateau_end) // 2)
    old = c.at_age(c.old_age + 24)

    assert 0.0 < at_recent < plateau_mid
    assert plateau_mid == pytest.approx(plateau_start, rel=1e-6)
    assert old < plateau_mid
    assert old >= DEFAULT_MIN_COVERAGE


def test_interpolation_monotone_inside_ramp() -> None:
    c = build_default_coverage_curve()
    vals = [c.at_age(a) for a in range(0, c.plateau_start + 1)]
    assert vals == sorted(vals), vals


def test_correction_factor_is_reciprocal() -> None:
    c = CoverageCurve(
        coverage_at_recent=0.5,
        plateau_start=6,
        coverage_plateau=0.8,
        plateau_end=36,
        old_age=96,
        coverage_at_old=0.4,
    )
    for age in (0, 3, 12, 60, 120):
        assert c.correction_factor(age) == pytest.approx(1.0 / c.at_age(age))


def test_floor_never_breached() -> None:
    c = CoverageCurve(coverage_at_old=0.01)  # below the floor
    assert c.at_age(500) == pytest.approx(DEFAULT_MIN_COVERAGE)


def test_months_between_monotone() -> None:
    base = date(2024, 6, 1)
    assert months_between(date(2024, 6, 1), base) == 0
    assert months_between(date(2024, 5, 1), base) == 1
    assert months_between(date(2023, 6, 1), base) == 12
    # Future months floor to zero (never negative).
    assert months_between(date(2025, 1, 1), base) == 0
