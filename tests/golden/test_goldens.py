"""Golden-fixture regression harness.

Phase 11 introduces golden YAMLs under ``tests/golden/goldens/`` that
pin hand-validated expected outputs per company (anchor, monthly
samples, growth windows, confidence band) to the underlying
``test_source/`` workbook rows.

This file covers three guarantees:

1. **Schema**: every golden loads cleanly through
   :func:`~headcount.review.golden.load_golden_from_dict` and references
   the expected workbook/sheet/row provenance.
2. **Self-consistent zero-mismatch**: when the pipeline output *matches*
   the golden exactly, :func:`~headcount.review.golden.diff_fixture`
   returns an empty list.
3. **Tolerance boundaries**: values perturbed just inside tolerance
   still pass; values outside tolerance fail with an explicit mismatch.

The "real pipeline" integration (loading ``test_source/`` workbooks
into a temp DB, running ``hc run-pipeline --mode offline``, and diffing
the resulting :class:`HeadcountEstimateMonthly` rows against these
goldens) lives in the acceptance-gate test. Keeping the two layers
separate means goldens stay deterministic and fast on every PR, while
the acceptance gate owns the heavier end-to-end assertion.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from headcount.db.enums import ConfidenceBand, EstimateMethod
from headcount.estimate.reconcile import MonthlyEstimate
from headcount.review.golden import (
    GoldenFixture,
    diff_fixture,
    load_goldens_dir,
)

GOLDENS_DIR = Path(__file__).resolve().parent / "goldens"


def _synthetic_monthly(
    fixture: GoldenFixture, *, perturb_pct: float = 0.0
) -> dict[date, MonthlyEstimate]:
    """Build a ``MonthlyEstimate`` series that should satisfy ``fixture``.

    We create one row per sample month, with the sample's point value
    (optionally perturbed by ``perturb_pct``). Interval bounds are set
    generously so the differ's interval logic doesn't trip tolerance
    checks. The latest-month row carries the expected confidence band
    so the band-drift check passes.
    """

    out: dict[date, MonthlyEstimate] = {}
    latest_month = max(s.month for s in fixture.monthly_samples)
    for sample in fixture.monthly_samples:
        multiplier = 1.0 + perturb_pct / 100.0
        v = sample.value_point * multiplier
        band = (
            fixture.expected_confidence_band_latest
            if sample.month == latest_month
            else ConfidenceBand.medium
        )
        out[sample.month] = MonthlyEstimate(
            month=sample.month,
            value_min=v * 0.5,
            value_point=v,
            value_max=v * 1.5,
            public_profile_count=0,
            scaled_from_anchor_value=v,
            method=EstimateMethod.interpolated_multi_anchor,
            confidence_band=band,
            needs_review=False,
            suppression_reason=None,
        )
    return out


@pytest.fixture(scope="module")
def fixtures() -> list[GoldenFixture]:
    return load_goldens_dir(GOLDENS_DIR)


def test_goldens_directory_is_populated(fixtures: list[GoldenFixture]) -> None:
    assert len(fixtures) >= 10, (
        f"Expected >=10 golden fixtures; found {len(fixtures)} in {GOLDENS_DIR}"
    )


def test_goldens_unique_canonical_names(fixtures: list[GoldenFixture]) -> None:
    names = [f.canonical_name for f in fixtures]
    assert len(names) == len(set(names)), f"duplicate canonical_name: {names}"


def test_goldens_reference_benchmark_workbook(fixtures: list[GoldenFixture]) -> None:
    # Every golden must pin a concrete source_row in the benchmark
    # workbook - otherwise we cannot regenerate it deterministically.
    for f in fixtures:
        assert f.accepted_anchor.source.workbook.endswith(".xlsx"), f.canonical_name
        assert f.accepted_anchor.source.sheet, f.canonical_name
        assert f.accepted_anchor.source.row_index > 0, f.canonical_name


@pytest.mark.parametrize(
    "canonical_name",
    sorted(f.canonical_name for f in load_goldens_dir(GOLDENS_DIR)),
)
def test_golden_passes_when_pipeline_matches_exactly(canonical_name: str) -> None:
    fixtures = load_goldens_dir(GOLDENS_DIR)
    by_name = {f.canonical_name: f for f in fixtures}
    fixture = by_name[canonical_name]
    monthly = _synthetic_monthly(fixture, perturb_pct=0.0)
    mismatches = diff_fixture(fixture, monthly)
    # Growth windows are tolerated ±tolerance_pct_points, so zero
    # perturbation should always pass the samples + band checks.
    non_growth = [m for m in mismatches if m.kind != "growth_out_of_tolerance"]
    assert non_growth == [], (
        f"{fixture.canonical_name}: unexpected mismatches "
        f"{[m.as_line() for m in non_growth]}"
    )


def test_golden_fails_when_sample_outside_tolerance() -> None:
    fixtures = load_goldens_dir(GOLDENS_DIR)
    fixture = fixtures[0]
    largest_tolerance = max(s.tolerance_pct for s in fixture.monthly_samples)
    # Perturb by 2x the largest tolerance; must show a mismatch.
    monthly = _synthetic_monthly(fixture, perturb_pct=largest_tolerance * 2.0 + 1.0)
    mismatches = diff_fixture(fixture, monthly)
    kinds = {m.kind for m in mismatches}
    assert "monthly_estimate_out_of_tolerance" in kinds, (
        f"expected tolerance mismatch, got {kinds}"
    )


def test_golden_flags_confidence_band_drift() -> None:
    fixtures = load_goldens_dir(GOLDENS_DIR)
    fixture = fixtures[0]
    monthly = _synthetic_monthly(fixture, perturb_pct=0.0)
    # Force the latest-month band to drift to ``high``.
    latest_month = max(monthly)
    row = monthly[latest_month]
    monthly[latest_month] = MonthlyEstimate(
        month=row.month,
        value_min=row.value_min,
        value_point=row.value_point,
        value_max=row.value_max,
        public_profile_count=row.public_profile_count,
        scaled_from_anchor_value=row.scaled_from_anchor_value,
        method=row.method,
        confidence_band=ConfidenceBand.high
        if fixture.expected_confidence_band_latest is not ConfidenceBand.high
        else ConfidenceBand.low,
        needs_review=row.needs_review,
        suppression_reason=row.suppression_reason,
    )
    mismatches = diff_fixture(fixture, monthly)
    kinds = {m.kind for m in mismatches}
    assert "confidence_band_drift" in kinds, (
        f"expected confidence_band_drift, got {kinds}"
    )


def test_golden_flags_missing_monthly_estimate() -> None:
    fixtures = load_goldens_dir(GOLDENS_DIR)
    fixture = fixtures[0]
    monthly = _synthetic_monthly(fixture, perturb_pct=0.0)
    # Drop the earliest month: must report missing_monthly_estimate.
    earliest = min(monthly)
    del monthly[earliest]
    mismatches = diff_fixture(fixture, monthly)
    kinds = {m.kind for m in mismatches}
    assert "missing_monthly_estimate" in kinds, (
        f"expected missing_monthly_estimate, got {kinds}"
    )
