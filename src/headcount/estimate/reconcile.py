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

Anchor-interpolation fallback
-----------------------------

When no per-person employment signal is available *but* the segment has
two or more anchor observations at distinct months (for example, the
benchmark promoter emitted ``T-24m``, ``T-12m``, ``T-6m``, and ``T``
anchors from the analyst workbook), :func:`interpolate_series_from_anchors`
produces a monthly series by linearly interpolating the anchor
``(min, point, max)`` interval between consecutive anchors. Months before
the earliest anchor and after the latest anchor are held flat at the
edge anchor's value (conservative; these are marked ``needs_review`` to
nudge analysts). Emitted rows use :attr:`EstimateMethod.interpolated_multi_anchor`.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date

from headcount.db.enums import ConfidenceBand, EstimateMethod
from headcount.estimate.anchors import AnchorCandidate, ReconciledAnchor
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


def _months_between_inclusive(start: date, end: date) -> int:
    """Signed month distance treating the first of each month as an ordinal."""

    return (end.year - start.year) * 12 + (end.month - start.month)


def _distinct_anchor_months(anchors: Sequence[AnchorCandidate]) -> list[date]:
    return sorted({a.anchor_month for a in anchors})


def has_employment_signal(
    months: Sequence[date],
    monthly_profiles: dict[date, int],
    *,
    sample_floor: int = DEFAULT_SAMPLE_FLOOR,
) -> bool:
    """True when *any* month in the segment has profiles at/above the floor.

    The pipeline uses this as the gate between ratio-scaling and the
    anchor-interpolation fallback: if there is no usable employment
    signal anywhere in the segment, ratio-scaling cannot run honestly.
    """

    return any(monthly_profiles.get(m, 0) >= sample_floor for m in months)


def _band_from_interpolation_distance(distance: int) -> ConfidenceBand:
    """Pick a confidence band based on distance to the nearest anchor.

    The ratio-scale path derives bands from interval width; the
    interpolated path has no employment evidence per-month, so distance
    to the nearest anchor is the strongest usable signal we have.
    """

    if distance <= 1:
        return ConfidenceBand.medium
    if distance <= 6:
        return ConfidenceBand.low
    return ConfidenceBand.manual_review_required


def _interp_linear(
    *,
    target: date,
    left: AnchorCandidate,
    right: AnchorCandidate,
) -> tuple[float, float, float]:
    """Linear interpolation of ``(min, point, max)`` between two anchors."""

    span = _months_between_inclusive(left.anchor_month, right.anchor_month)
    if span <= 0:
        return left.value_min, left.value_point, left.value_max
    t = _months_between_inclusive(left.anchor_month, target) / span
    t = max(0.0, min(1.0, t))
    vmin = left.value_min + (right.value_min - left.value_min) * t
    vpt = left.value_point + (right.value_point - left.value_point) * t
    vmax = left.value_max + (right.value_max - left.value_max) * t
    if vmin > vpt:
        vmin = vpt
    if vmax < vpt:
        vmax = vpt
    return vmin, vpt, vmax


def _pick_flanking_anchors(
    month: date,
    anchors_by_month: list[AnchorCandidate],
) -> tuple[AnchorCandidate, AnchorCandidate | None, int]:
    """Return ``(left, right, distance_to_nearest)``.

    ``anchors_by_month`` must be sorted by ``anchor_month`` and deduped
    to at most one anchor per month. When ``month`` is before every
    anchor or after every anchor, ``right`` is ``None`` and ``left`` is
    the single flanking anchor (used for flat extrapolation).
    """

    first = anchors_by_month[0]
    last = anchors_by_month[-1]
    if month <= first.anchor_month:
        return first, None, abs(_months_between_inclusive(month, first.anchor_month))
    if month >= last.anchor_month:
        return last, None, abs(_months_between_inclusive(last.anchor_month, month))
    left = anchors_by_month[0]
    right: AnchorCandidate | None = None
    for a in anchors_by_month:
        if a.anchor_month <= month:
            left = a
        else:
            right = a
            break
    if right is None:
        return left, None, abs(_months_between_inclusive(left.anchor_month, month))
    dist = min(
        abs(_months_between_inclusive(month, left.anchor_month)),
        abs(_months_between_inclusive(month, right.anchor_month)),
    )
    return left, right, dist


def interpolate_series_from_anchors(
    segment: Segment,
    *,
    segment_anchors: Sequence[AnchorCandidate],
) -> list[MonthlyEstimate] | None:
    """Linearly interpolate a monthly series between multiple anchors.

    Returns ``None`` when the segment has fewer than two distinct anchor
    months; the caller should fall through to the existing
    ratio-scaling / degraded path in that case.
    """

    if not segment_anchors:
        return None
    distinct_months = _distinct_anchor_months(segment_anchors)
    if len(distinct_months) < 2:
        return None

    # Keep the highest-confidence anchor at each distinct month so
    # the interpolation math sees a single (min, point, max) per month.
    best_per_month: dict[date, AnchorCandidate] = {}
    for a in segment_anchors:
        prev = best_per_month.get(a.anchor_month)
        if prev is None or float(a.confidence or 0.0) > float(prev.confidence or 0.0):
            best_per_month[a.anchor_month] = a
    anchors_by_month = sorted(best_per_month.values(), key=lambda a: a.anchor_month)

    out: list[MonthlyEstimate] = []
    first_month = anchors_by_month[0].anchor_month
    last_month = anchors_by_month[-1].anchor_month
    for m in segment.months():
        left, right, dist = _pick_flanking_anchors(m, anchors_by_month)
        extrapolated = m < first_month or m > last_month
        if right is None:
            vmin, vpt, vmax = left.value_min, left.value_point, left.value_max
            anchor_month_used = left.anchor_month
            contributing: tuple[str, ...] = (
                (left.observation_id,) if left.observation_id else ()
            )
        else:
            vmin, vpt, vmax = _interp_linear(target=m, left=left, right=right)
            anchor_month_used = left.anchor_month if (
                abs(_months_between_inclusive(m, left.anchor_month))
                <= abs(_months_between_inclusive(m, right.anchor_month))
            ) else right.anchor_month
            contributing = tuple(
                oid
                for oid in (left.observation_id, right.observation_id)
                if oid
            )

        band = _band_from_interpolation_distance(dist)
        needs_review = extrapolated or band is ConfidenceBand.manual_review_required
        suppression = "extrapolated_beyond_anchor_range" if extrapolated else None

        out.append(
            MonthlyEstimate(
                month=m,
                value_min=float(vmin),
                value_point=float(vpt),
                value_max=float(vmax),
                public_profile_count=0,
                scaled_from_anchor_value=float(vpt),
                method=EstimateMethod.interpolated_multi_anchor,
                confidence_band=band,
                needs_review=needs_review,
                suppression_reason=suppression,
                coverage_factor=1.0,
                ratio=1.0,
                anchor_month=anchor_month_used,
                contributing_anchor_ids=contributing,
            )
        )
    return out


__all__ = [
    "DEFAULT_SAMPLE_FLOOR",
    "METHOD_VERSION",
    "MonthlyEstimate",
    "has_employment_signal",
    "interpolate_series_from_anchors",
    "reconcile_series",
]
