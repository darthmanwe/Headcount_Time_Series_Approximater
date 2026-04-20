"""Unit tests for :mod:`headcount.review.scoring`."""

from __future__ import annotations

from datetime import date

import pytest

from headcount.db.enums import (
    AnchorType,
    ConfidenceBand,
    EstimateMethod,
    HeadcountValueKind,
)
from headcount.estimate.anchors import AnchorCandidate
from headcount.estimate.coverage import build_default_coverage_curve
from headcount.estimate.reconcile import MonthlyEstimate
from headcount.review.scoring import (
    SCORING_VERSION,
    ConfidenceInputs,
    score_confidence,
)


def _candidate(
    month: date,
    *,
    anchor_type: AnchorType = AnchorType.historical_statement,
    source_name: str = "sec",
    confidence: float = 0.9,
    value_point: float = 1000.0,
    spread: float = 20.0,
) -> AnchorCandidate:
    return AnchorCandidate(
        anchor_month=month,
        value_min=value_point - spread,
        value_point=value_point,
        value_max=value_point + spread,
        kind=HeadcountValueKind.exact,
        anchor_type=anchor_type,
        confidence=confidence,
        source_name=source_name,
        observation_id=f"obs_{source_name}_{month.isoformat()}",
    )


def _estimate(
    month: date,
    *,
    point: float = 1000.0,
    spread: float = 40.0,
    profile_count: int = 100,
    method: EstimateMethod = EstimateMethod.scaled_ratio_coverage_corrected,
    coverage_factor: float = 1.0,
) -> MonthlyEstimate:
    return MonthlyEstimate(
        month=month,
        value_min=point - spread,
        value_point=point,
        value_max=point + spread,
        public_profile_count=profile_count,
        scaled_from_anchor_value=point,
        method=method,
        confidence_band=ConfidenceBand.high,
        coverage_factor=coverage_factor,
    )


@pytest.fixture()
def coverage():
    return build_default_coverage_curve()


def test_version_stable() -> None:
    assert SCORING_VERSION == "scoring_v1"


def test_strong_evidence_scores_high(coverage) -> None:
    anchors = (
        _candidate(date(2023, 6, 1), source_name="sec"),
        _candidate(
            date(2023, 6, 1),
            anchor_type=AnchorType.current_headcount_anchor,
            source_name="linkedin_public",
            confidence=0.7,
        ),
    )
    est = _estimate(date(2023, 6, 1), point=1000, spread=20, profile_count=200)
    out = score_confidence(
        ConfidenceInputs(
            estimate=est,
            segment_anchors=anchors,
            segment_break_months=(),
            distinct_source_classes=2,
            as_of_month=date(2023, 6, 1),
            coverage=coverage,
            sample_floor=5,
        )
    )
    assert out.band is ConfidenceBand.high
    assert out.score >= 0.80
    assert out.components["multi_source_corroboration"] == 1.0


def test_wide_interval_drags_score_down(coverage) -> None:
    anchor = _candidate(date(2023, 6, 1))
    est = _estimate(date(2023, 6, 1), point=1000, spread=1200)
    out = score_confidence(
        ConfidenceInputs(
            estimate=est,
            segment_anchors=(anchor,),
            segment_break_months=(),
            distinct_source_classes=1,
            as_of_month=date(2023, 6, 1),
            coverage=coverage,
            sample_floor=5,
        )
    )
    assert out.components["anchor_agreement"] < 0.1
    assert out.band in (
        ConfidenceBand.low,
        ConfidenceBand.manual_review_required,
        ConfidenceBand.medium,
    )


def test_event_proximity_penalty(coverage) -> None:
    anchor = _candidate(date(2023, 1, 1))
    est = _estimate(date(2023, 6, 1))
    near = score_confidence(
        ConfidenceInputs(
            estimate=est,
            segment_anchors=(anchor,),
            segment_break_months=(date(2023, 6, 1),),
            distinct_source_classes=1,
            as_of_month=date(2023, 6, 1),
            coverage=coverage,
            sample_floor=5,
        )
    )
    far = score_confidence(
        ConfidenceInputs(
            estimate=est,
            segment_anchors=(anchor,),
            segment_break_months=(date(2020, 1, 1),),
            distinct_source_classes=1,
            as_of_month=date(2023, 6, 1),
            coverage=coverage,
            sample_floor=5,
        )
    )
    assert near.components["event_proximity"] < far.components["event_proximity"]


def test_suppressed_low_sample_forces_manual_review_band(coverage) -> None:
    anchor = _candidate(date(2023, 6, 1))
    est = _estimate(
        date(2023, 6, 1),
        point=1000,
        spread=20,
        profile_count=200,
        method=EstimateMethod.suppressed_low_sample,
    )
    out = score_confidence(
        ConfidenceInputs(
            estimate=est,
            segment_anchors=(anchor,),
            segment_break_months=(),
            distinct_source_classes=2,
            as_of_month=date(2023, 6, 1),
            coverage=coverage,
            sample_floor=5,
        )
    )
    assert out.band is ConfidenceBand.manual_review_required
    assert out.note == "suppressed_low_sample"


def test_degraded_current_only_caps_band_at_low(coverage) -> None:
    anchor = _candidate(date(2023, 6, 1))
    est = _estimate(
        date(2023, 6, 1),
        point=1000,
        spread=20,
        profile_count=200,
        method=EstimateMethod.degraded_current_only,
    )
    out = score_confidence(
        ConfidenceInputs(
            estimate=est,
            segment_anchors=(anchor,),
            segment_break_months=(),
            distinct_source_classes=2,
            as_of_month=date(2023, 6, 1),
            coverage=coverage,
            sample_floor=5,
        )
    )
    assert out.band is ConfidenceBand.low
    assert out.note == "degraded_current_only"


def test_no_anchors_scores_zero_on_source_components(coverage) -> None:
    est = _estimate(date(2023, 6, 1), point=1000, spread=20)
    out = score_confidence(
        ConfidenceInputs(
            estimate=est,
            segment_anchors=(),
            segment_break_months=(),
            distinct_source_classes=0,
            as_of_month=date(2023, 6, 1),
            coverage=coverage,
            sample_floor=5,
        )
    )
    assert out.components["anchor_source_quality"] == 0.0
    assert out.components["anchor_recency"] == 0.0
    assert out.components["multi_source_corroboration"] == 0.0


def test_as_json_shape_is_stable(coverage) -> None:
    anchor = _candidate(date(2023, 6, 1))
    est = _estimate(date(2023, 6, 1))
    out = score_confidence(
        ConfidenceInputs(
            estimate=est,
            segment_anchors=(anchor,),
            segment_break_months=(),
            distinct_source_classes=1,
            as_of_month=date(2023, 6, 1),
            coverage=coverage,
            sample_floor=5,
        )
    )
    js = out.as_json()
    assert js["scoring_version"] == SCORING_VERSION
    assert set(js["components"].keys()) == {
        "anchor_source_quality",
        "anchor_recency",
        "anchor_agreement",
        "sample_coverage",
        "event_proximity",
        "multi_source_corroboration",
    }
    assert 0.0 <= js["score"] <= 1.0
