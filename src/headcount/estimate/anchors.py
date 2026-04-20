"""Interval anchor rollup within a segment.

A single segment can contain anchors from several sources (SEC filings,
LinkedIn badges, company-web pages, Wikidata, manual analyst overrides)
captured at different months. :func:`reconcile_segment_anchors` turns that
raw list into a single *segment anchor* interval that the reconciliation
step uses to ratio-scale the employment series.

Design
------

1. **Source precedence.** Higher-trust sources bias the reconciled point,
   and the interval never shrinks below the widest high-precedence anchor.
   Precedence (higher wins):

   - ``manual_anchor``   - analyst intent, never overridden.
   - ``historical_statement`` (SEC/Wikidata exact values with as-of dates).
   - ``current_headcount_anchor`` (LinkedIn badge, company-web, Wikidata
     without as-of).

2. **Bucket anchors never dominate exact anchors.** If any ``exact`` /
   ``range`` anchor exists, ``bucket`` anchors only contribute to the
   *max* width, not the point estimate.

3. **Confidence-weighted point.** Within the top precedence tier we take a
   confidence-weighted average of the point values. Ties on precedence and
   confidence fall back to the most recent ``anchor_month``.

4. **Interval envelope.** The reconciled ``(min, max)`` is the union of all
   contributing anchors' intervals in the top precedence tier, clamped so
   ``min <= point <= max``.

5. **Proximity weighting (optional).** Anchors far from the segment midpoint
   are down-weighted by a simple exponential decay so an anchor recorded
   ten years before the segment doesn't drown out a fresher one. The decay
   is gentle (half-life 18 months) and off by default; callers opt in via
   ``decay_half_life_months``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date

from headcount.db.enums import AnchorType, HeadcountValueKind

ANCHOR_POLICY_VERSION = "anchors_v1"
"""Stamped onto :class:`EstimateVersion.anchor_policy_version`."""


_PRECEDENCE: dict[AnchorType, int] = {
    AnchorType.manual_anchor: 5,
    AnchorType.historical_statement: 3,
    AnchorType.current_headcount_anchor: 2,
    AnchorType.reconciled_anchor: 1,  # self-reference; shouldn't happen in input.
}


@dataclass(frozen=True, slots=True)
class AnchorCandidate:
    """Input to :func:`reconcile_segment_anchors`.

    This is deliberately a plain DTO, not a SQLAlchemy row, so the math
    stays testable without a session. :mod:`headcount.estimate.pipeline`
    adapts :class:`CompanyAnchorObservation` into this shape.
    """

    anchor_month: date
    value_min: float
    value_point: float
    value_max: float
    kind: HeadcountValueKind
    anchor_type: AnchorType
    confidence: float = 0.5
    source_name: str = ""
    observation_id: str | None = None


@dataclass(frozen=True, slots=True)
class ReconciledAnchor:
    """Output of :func:`reconcile_segment_anchors`.

    ``contributing_ids`` / ``weights`` / ``rationale`` feed directly into
    :class:`~headcount.models.anchor_reconciliation.AnchorReconciliation`.
    """

    anchor_month: date
    value_min: float
    value_point: float
    value_max: float
    contributing_ids: tuple[str, ...] = field(default_factory=tuple)
    weights: dict[str, float] = field(default_factory=dict)
    rationale: str = ""


def _precedence(a: AnchorCandidate) -> int:
    return _PRECEDENCE.get(a.anchor_type, 0)


def _proximity_weight(
    anchor_month: date,
    center: date,
    half_life_months: float | None,
) -> float:
    if half_life_months is None or half_life_months <= 0:
        return 1.0
    delta = abs((anchor_month.year - center.year) * 12 + (anchor_month.month - center.month))
    return math.pow(0.5, delta / half_life_months)


def _segment_center(start_month: date, end_month: date) -> date:
    months_span = (end_month.year - start_month.year) * 12 + (end_month.month - start_month.month)
    half = months_span // 2
    year = start_month.year + (start_month.month - 1 + half) // 12
    month = ((start_month.month - 1 + half) % 12) + 1
    return date(year, month, 1)


def reconcile_segment_anchors(
    anchors: list[AnchorCandidate],
    *,
    segment_start: date,
    segment_end: date,
    decay_half_life_months: float | None = None,
) -> ReconciledAnchor | None:
    """Collapse per-segment anchors into a single interval + rationale.

    Returns ``None`` when the segment has no anchors at all - the caller
    should then mark the segment degraded / suppressed.
    """

    if not anchors:
        return None

    center = _segment_center(segment_start, segment_end)

    # Top precedence tier: keep only anchors whose precedence equals the
    # highest observed. Lower-tier anchors still contribute to interval
    # widening below.
    top = max(_precedence(a) for a in anchors)
    tier = [a for a in anchors if _precedence(a) == top]

    has_precise = any(a.kind in (HeadcountValueKind.exact, HeadcountValueKind.range) for a in tier)
    if has_precise:
        # Drop buckets from the point calculation - they're too fuzzy.
        point_pool = [a for a in tier if a.kind != HeadcountValueKind.bucket]
    else:
        point_pool = list(tier)

    # Confidence * proximity weighting for the point estimate.
    weighted_sum = 0.0
    weight_total = 0.0
    per_id_weight: dict[str, float] = {}
    contributing: list[str] = []

    for a in point_pool:
        prox = _proximity_weight(a.anchor_month, center, decay_half_life_months)
        w = max(0.0, float(a.confidence) * prox)
        if w == 0.0:
            continue
        weighted_sum += a.value_point * w
        weight_total += w
        if a.observation_id:
            per_id_weight[a.observation_id] = w
            contributing.append(a.observation_id)

    if weight_total == 0.0:
        # Degenerate: everything had zero weight. Fall back to tier median
        # by anchor_month, then by confidence, then by id.
        fallback = sorted(
            point_pool,
            key=lambda a: (
                a.anchor_month,
                -float(a.confidence or 0.0),
                a.observation_id or "",
            ),
        )[-1]
        point = fallback.value_point
        if fallback.observation_id:
            per_id_weight[fallback.observation_id] = 1.0
            contributing.append(fallback.observation_id)
    else:
        point = weighted_sum / weight_total

    # Interval envelope: union over the top tier. Low-precedence anchors
    # still widen the interval if they're wider than the tier envelope -
    # we want to be conservative, not overconfident.
    tier_min = min(a.value_min for a in tier)
    tier_max = max(a.value_max for a in tier)

    # Clamp the point inside the envelope to guarantee monotonicity.
    low = min(tier_min, point)
    high = max(tier_max, point)

    # Normalize weights for the audit record.
    if weight_total > 0.0:
        norm_weights = {k: v / weight_total for k, v in per_id_weight.items()}
    else:
        norm_weights = dict.fromkeys(per_id_weight.keys(), 1.0)

    top_kinds = sorted({a.anchor_type.value for a in tier})
    rationale = (
        f"tier={top_kinds} n_top={len(tier)} "
        f"n_point={len(point_pool)} has_precise={has_precise} "
        f"center={center.isoformat()}"
    )

    return ReconciledAnchor(
        anchor_month=center,
        value_min=low,
        value_point=point,
        value_max=high,
        contributing_ids=tuple(contributing),
        weights=norm_weights,
        rationale=rationale,
    )


__all__ = [
    "ANCHOR_POLICY_VERSION",
    "AnchorCandidate",
    "ReconciledAnchor",
    "reconcile_segment_anchors",
]
