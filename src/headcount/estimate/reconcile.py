"""Ratio-scale employment counts and reconcile into monthly intervals.

Given:

- A reconciled anchor interval ``(min, point, max)`` for each segment and
  an ``anchor_month`` inside that segment (from
  :mod:`headcount.estimate.anchors`).
- A monthly dict ``{month: public_profile_count}`` (from
  :mod:`headcount.estimate.employment`).
- A :class:`CoverageCurve` (from :mod:`headcount.estimate.coverage`).

This module produces one :class:`MonthlyEstimate` per month in the
requested window. The core math:

    coverage_corrected_count(m) = count(m) * correction_factor(age(m))
    ratio(m) = coverage_corrected_count(m) / coverage_corrected_count(anchor_month)
    point(m) = anchor.point * ratio(m)
    min(m)   = anchor.min   * ratio(m)
    max(m)   = anchor.max   * ratio(m)

Fail-closed behaviour
---------------------

- If the anchor month has zero profile count we cannot ratio-scale; every
  month in that segment is emitted as ``degraded_current_only`` holding
  the raw anchor interval flat.
- If a target month's profile count is below ``sample_floor`` we mark it
  ``suppressed_low_sample`` and still emit the month but with
  ``needs_review=True``. The point / interval from the anchor are held
  flat so downstream consumers get something to look at; the anomaly
  detector flags it.
- Hard break segmentation means every segment is reconciled
  independently: the anchor_month for one segment never contributes to
  another segment's ratio.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date

from headcount.db.enums import ConfidenceBand, EstimateMethod
from headcount.estimate.anchors import ReconciledAnchor
from headcount.estimate.coverage import CoverageCurve, months_between
from headcount.estimate.segments import Segment

METHOD_VERSION = "method_v1"
"""Stamped onto :class:`EstimateVersion.method_version`."""

DEFAULT_SAMPLE_FLOOR = 5
"""Months with fewer profiles than this are suppressed."""


@dataclass(frozen=True, slots=True)
class MonthlyEstimate:
    """One monthly output row before it hits the DB."""

    month: date
    value_min: float
    value_point: float
    value_max: float
    public_profile_count: int
    scaled_from_anchor_value: float
    method: EstimateMethod
    confidence_band: ConfidenceBand
    needs_review: bool = False
    suppression_reason: str | None = None
    coverage_factor: float = 1.0
    ratio: float = 1.0
    anchor_month: date | None = None
    contributing_anchor_ids: tuple[str, ...] = field(default_factory=tuple)


def _initial_band_from_width(point: float, span: float) -> ConfidenceBand:
    """Translate interval width to a coarse band.

    Phase 8 will refine this with dedicated confidence-component scoring;
    for now we emit a reasonable default so callers have something to show.
    """

    if point <= 0:
        return ConfidenceBand.manual_review_required
    relative = span / max(1.0, point)
    if relative < 0.15:
        return ConfidenceBand.high
    if relative < 0.35:
        return ConfidenceBand.medium
    if relative < 0.75:
        return ConfidenceBand.low
    return ConfidenceBand.manual_review_required


def _finite_positive(value: float) -> bool:
    return math.isfinite(value) and value > 0.0


def reconcile_series(
    segment: Segment,
    *,
    anchor: ReconciledAnchor | None,
    monthly_profiles: dict[date, int],
    coverage: CoverageCurve,
    as_of_month: date,
    sample_floor: int = DEFAULT_SAMPLE_FLOOR,
) -> list[MonthlyEstimate]:
    """Compute one :class:`MonthlyEstimate` per month in ``segment``."""

    months = segment.months()
    out: list[MonthlyEstimate] = []

    if anchor is None:
        for m in months:
            count = monthly_profiles.get(m, 0)
            out.append(
                MonthlyEstimate(
                    month=m,
                    value_min=0.0,
                    value_point=0.0,
                    value_max=0.0,
                    public_profile_count=count,
                    scaled_from_anchor_value=0.0,
                    method=EstimateMethod.suppressed_low_sample,
                    confidence_band=ConfidenceBand.manual_review_required,
                    needs_review=True,
                    suppression_reason="no_anchor_in_segment",
                )
            )
        return out

    anchor_month = anchor.anchor_month
    anchor_raw_count = monthly_profiles.get(anchor_month, 0)
    coverage_at_anchor = coverage.at_age(months_between(anchor_month, as_of_month))
    anchor_corrected = anchor_raw_count / coverage_at_anchor if coverage_at_anchor > 0 else 0.0

    can_ratio_scale = _finite_positive(anchor_corrected) and anchor_raw_count > 0

    for m in months:
        raw_count = monthly_profiles.get(m, 0)
        coverage_at_m = coverage.at_age(months_between(m, as_of_month))
        coverage_factor = 1.0 / coverage_at_m if coverage_at_m > 0 else 1.0
        corrected = raw_count * coverage_factor

        if not can_ratio_scale:
            out.append(
                MonthlyEstimate(
                    month=m,
                    value_min=anchor.value_min,
                    value_point=anchor.value_point,
                    value_max=anchor.value_max,
                    public_profile_count=raw_count,
                    scaled_from_anchor_value=anchor.value_point,
                    method=EstimateMethod.degraded_current_only,
                    confidence_band=ConfidenceBand.low,
                    needs_review=True,
                    suppression_reason="anchor_month_has_no_profiles",
                    coverage_factor=coverage_factor,
                    ratio=1.0,
                    anchor_month=anchor_month,
                    contributing_anchor_ids=anchor.contributing_ids,
                )
            )
            continue

        if raw_count < sample_floor:
            out.append(
                MonthlyEstimate(
                    month=m,
                    value_min=anchor.value_min,
                    value_point=anchor.value_point,
                    value_max=anchor.value_max,
                    public_profile_count=raw_count,
                    scaled_from_anchor_value=anchor.value_point,
                    method=EstimateMethod.suppressed_low_sample,
                    confidence_band=ConfidenceBand.manual_review_required,
                    needs_review=True,
                    suppression_reason=f"profiles_below_floor({raw_count}<{sample_floor})",
                    coverage_factor=coverage_factor,
                    ratio=1.0,
                    anchor_month=anchor_month,
                    contributing_anchor_ids=anchor.contributing_ids,
                )
            )
            continue

        ratio = corrected / anchor_corrected
        value_point = anchor.value_point * ratio
        value_min = anchor.value_min * ratio
        value_max = anchor.value_max * ratio
        span = max(0.0, value_max - value_min)
        band = _initial_band_from_width(value_point, span)

        out.append(
            MonthlyEstimate(
                month=m,
                value_min=value_min,
                value_point=value_point,
                value_max=value_max,
                public_profile_count=raw_count,
                scaled_from_anchor_value=anchor.value_point,
                method=EstimateMethod.scaled_ratio_coverage_corrected,
                confidence_band=band,
                needs_review=False,
                suppression_reason=None,
                coverage_factor=coverage_factor,
                ratio=ratio,
                anchor_month=anchor_month,
                contributing_anchor_ids=anchor.contributing_ids,
            )
        )

    return out


__all__ = [
    "DEFAULT_SAMPLE_FLOOR",
    "METHOD_VERSION",
    "MonthlyEstimate",
    "reconcile_series",
]
