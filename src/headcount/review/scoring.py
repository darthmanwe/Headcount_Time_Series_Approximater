"""Per-month confidence scoring.

The estimator writes a coarse :class:`~headcount.db.enums.ConfidenceBand`
on every :class:`HeadcountEstimateMonthly` row, but "high/medium/low" is
not enough for review triage - analysts need to know *why* a month was
downgraded so they know where to look. This module produces a
reproducible 0..1 score plus a structured component breakdown.

Components (each returns a value in ``[0, 1]``)
-----------------------------------------------

- ``anchor_source_quality``: top-tier anchor precedence, normalized.
  Manual analyst anchors land at 1.0; historical statements
  (SEC/Wikidata with as-of dates) at 0.75; current-headcount badges /
  unanchored Wikidata at 0.5; everything else at 0.25.
- ``anchor_recency``: exponential decay on distance (in months) from
  the target month to the nearest contributing anchor. Half-life 18
  months by default; months at the anchor itself score 1.0.
- ``anchor_agreement``: ``1 - clip(spread, 0, 1)`` where
  ``spread = (value_max - value_min) / max(1, value_point)``. Tight
  intervals score near 1.0; wide intervals drop toward 0.
- ``sample_coverage``: ``min(1, raw_profile_count / expected_min)``
  where ``expected_min = sample_floor / coverage_at_age``. Months
  above the expected live-profile floor score 1.0; months at or below
  the floor drop linearly.
- ``event_proximity``: penalty for being close to a hard-break event.
  ``1 - exp(-d / proximity_half_life)`` where ``d`` is the distance in
  months to the nearest segment break. Months on a break (``d == 0``)
  score 0.0 and escalate toward 1.0 as the segment ages.
- ``multi_source_corroboration``: bonus when 2+ distinct source
  classes contributed to the segment anchor. ``0.0`` for one source,
  ``1.0`` for 2+. (This is the one component that can "rescue" a
  single-anchor segment that would otherwise look thin.)

Band mapping
------------

Default thresholds::

    score >= 0.80 -> high
    score >= 0.55 -> medium
    score >= 0.30 -> low
    score <  0.30 -> manual_review_required

Overrides:

- If the reconciler flagged
  :class:`~headcount.db.enums.EstimateMethod.suppressed_low_sample` the
  band is forced to ``manual_review_required`` regardless of score.
- :class:`~headcount.db.enums.EstimateMethod.degraded_current_only`
  caps the band at ``low``.

Changing any weight, threshold, or component definition requires
bumping ``SCORING_VERSION`` so replayed estimates are comparable to
their stored score.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date

from headcount.db.enums import AnchorType, ConfidenceBand, EstimateMethod
from headcount.estimate.anchors import AnchorCandidate
from headcount.estimate.coverage import CoverageCurve, months_between
from headcount.estimate.reconcile import MonthlyEstimate

SCORING_VERSION = "scoring_v1"
"""Bumped when weights, thresholds, or component semantics change."""


# Per-component weights. MUST sum to 1.0 to keep the final score in
# ``[0, 1]``.
DEFAULT_WEIGHTS: dict[str, float] = {
    "anchor_source_quality": 0.25,
    "anchor_recency": 0.20,
    "anchor_agreement": 0.20,
    "sample_coverage": 0.15,
    "event_proximity": 0.10,
    "multi_source_corroboration": 0.10,
}


# score -> band thresholds (strictly greater-than-equal).
DEFAULT_BAND_THRESHOLDS: dict[ConfidenceBand, float] = {
    ConfidenceBand.high: 0.80,
    ConfidenceBand.medium: 0.55,
    ConfidenceBand.low: 0.30,
}


_ANCHOR_SOURCE_QUALITY: dict[AnchorType, float] = {
    AnchorType.manual_anchor: 1.0,
    AnchorType.historical_statement: 0.75,
    AnchorType.current_headcount_anchor: 0.50,
    AnchorType.reconciled_anchor: 0.50,
}


@dataclass(frozen=True, slots=True)
class ConfidenceInputs:
    """Everything the scorer needs for one month.

    This deliberately excludes any SQLAlchemy types so the function is
    importable from tests without a session.
    """

    estimate: MonthlyEstimate
    segment_anchors: tuple[AnchorCandidate, ...]
    segment_break_months: tuple[date, ...]
    distinct_source_classes: int
    as_of_month: date
    coverage: CoverageCurve
    sample_floor: int


@dataclass(frozen=True, slots=True)
class ConfidenceBreakdown:
    """Output of :func:`score_confidence`, for one month."""

    month: date
    score: float
    band: ConfidenceBand
    components: dict[str, float] = field(default_factory=dict)
    note: str | None = None

    def as_json(self) -> dict[str, object]:
        """Shape persisted into
        ``HeadcountEstimateMonthly.confidence_components_json``."""

        return {
            "scoring_version": SCORING_VERSION,
            "score": round(self.score, 6),
            "band": self.band.value,
            "components": {k: round(v, 6) for k, v in self.components.items()},
            "note": self.note,
        }


def _anchor_source_quality(anchors: tuple[AnchorCandidate, ...]) -> float:
    if not anchors:
        return 0.0
    return max(_ANCHOR_SOURCE_QUALITY.get(a.anchor_type, 0.25) for a in anchors)


def _anchor_recency(
    month: date,
    anchors: tuple[AnchorCandidate, ...],
    half_life_months: float,
) -> float:
    if not anchors or half_life_months <= 0:
        return 0.0 if not anchors else 1.0
    distances = [
        abs((a.anchor_month.year - month.year) * 12 + (a.anchor_month.month - month.month))
        for a in anchors
    ]
    nearest = min(distances)
    return math.pow(0.5, nearest / half_life_months)


def _anchor_agreement(estimate: MonthlyEstimate) -> float:
    point = max(1.0, estimate.value_point)
    spread = max(0.0, estimate.value_max - estimate.value_min) / point
    return max(0.0, 1.0 - min(1.0, spread))


def _sample_coverage(
    estimate: MonthlyEstimate,
    as_of_month: date,
    coverage: CoverageCurve,
    sample_floor: int,
) -> float:
    if estimate.public_profile_count <= 0:
        return 0.0
    coverage_at_age = coverage.at_age(months_between(estimate.month, as_of_month))
    if coverage_at_age <= 0:
        return 0.0
    # Months that barely clear the suppression floor should not score
    # 1.0 - they should sit near 0.0 and climb as the raw count grows.
    expected_min = max(1.0, sample_floor / coverage_at_age)
    return min(1.0, estimate.public_profile_count / expected_min)


def _event_proximity(
    month: date,
    breaks: tuple[date, ...],
    proximity_half_life_months: float,
) -> float:
    if not breaks:
        return 1.0
    distances = [abs((b.year - month.year) * 12 + (b.month - month.month)) for b in breaks]
    nearest = min(distances)
    if proximity_half_life_months <= 0:
        return 1.0 if nearest > 0 else 0.0
    # 0 at the break month, asymptoting to 1 as the segment ages.
    return 1.0 - math.exp(-nearest / proximity_half_life_months)


def _multi_source_corroboration(distinct_source_classes: int) -> float:
    if distinct_source_classes >= 2:
        return 1.0
    if distinct_source_classes == 1:
        return 0.4
    return 0.0


def _band_for_score(
    score: float,
    thresholds: dict[ConfidenceBand, float],
) -> ConfidenceBand:
    if score >= thresholds[ConfidenceBand.high]:
        return ConfidenceBand.high
    if score >= thresholds[ConfidenceBand.medium]:
        return ConfidenceBand.medium
    if score >= thresholds[ConfidenceBand.low]:
        return ConfidenceBand.low
    return ConfidenceBand.manual_review_required


def score_confidence(
    inputs: ConfidenceInputs,
    *,
    weights: dict[str, float] | None = None,
    band_thresholds: dict[ConfidenceBand, float] | None = None,
    anchor_recency_half_life_months: float = 18.0,
    event_proximity_half_life_months: float = 6.0,
) -> ConfidenceBreakdown:
    """Score a single monthly estimate.

    The scorer is deterministic and pure; the pipeline is responsible
    for gathering the inputs from SQLAlchemy rows.
    """

    w = dict(weights or DEFAULT_WEIGHTS)
    t = dict(band_thresholds or DEFAULT_BAND_THRESHOLDS)

    components: dict[str, float] = {
        "anchor_source_quality": _anchor_source_quality(inputs.segment_anchors),
        "anchor_recency": _anchor_recency(
            inputs.estimate.month,
            inputs.segment_anchors,
            anchor_recency_half_life_months,
        ),
        "anchor_agreement": _anchor_agreement(inputs.estimate),
        "sample_coverage": _sample_coverage(
            inputs.estimate,
            inputs.as_of_month,
            inputs.coverage,
            inputs.sample_floor,
        ),
        "event_proximity": _event_proximity(
            inputs.estimate.month,
            inputs.segment_break_months,
            event_proximity_half_life_months,
        ),
        "multi_source_corroboration": _multi_source_corroboration(inputs.distinct_source_classes),
    }

    score = sum(components[k] * w.get(k, 0.0) for k in components)
    score = max(0.0, min(1.0, score))
    band = _band_for_score(score, t)
    note = None

    # Hard overrides based on the reconciler's decision.
    if inputs.estimate.method is EstimateMethod.suppressed_low_sample:
        band = ConfidenceBand.manual_review_required
        note = "suppressed_low_sample"
    elif inputs.estimate.method is EstimateMethod.degraded_current_only:
        if band in (ConfidenceBand.high, ConfidenceBand.medium):
            band = ConfidenceBand.low
        note = "degraded_current_only"

    return ConfidenceBreakdown(
        month=inputs.estimate.month,
        score=score,
        band=band,
        components=components,
        note=note,
    )


__all__ = [
    "DEFAULT_BAND_THRESHOLDS",
    "DEFAULT_WEIGHTS",
    "SCORING_VERSION",
    "ConfidenceBreakdown",
    "ConfidenceInputs",
    "score_confidence",
]
