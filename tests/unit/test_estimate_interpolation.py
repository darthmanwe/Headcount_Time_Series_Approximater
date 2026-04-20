"""Unit tests for the anchor-interpolation fallback in reconcile.py."""

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
from headcount.estimate.reconcile import (
    has_employment_signal,
    interpolate_series_from_anchors,
)
from headcount.estimate.segments import Segment


def _anchor(
    *,
    month: date,
    point: float,
    spread: float = 10.0,
    observation_id: str = "",
    confidence: float = 0.6,
    kind: HeadcountValueKind = HeadcountValueKind.exact,
) -> AnchorCandidate:
    return AnchorCandidate(
        anchor_month=month,
        value_min=point - spread,
        value_point=point,
        value_max=point + spread,
        kind=kind,
        anchor_type=AnchorType.historical_statement,
        confidence=confidence,
        source_name="benchmark",
        observation_id=observation_id,
    )


def test_returns_none_without_two_distinct_months() -> None:
    seg = Segment(start_month=date(2024, 1, 1), end_month=date(2024, 12, 1))
    # Empty -> None.
    assert (
        interpolate_series_from_anchors(seg, segment_anchors=[]) is None
    )
    # Single anchor -> None.
    single = [_anchor(month=date(2024, 6, 1), point=500, observation_id="a")]
    assert interpolate_series_from_anchors(seg, segment_anchors=single) is None
    # Two anchors but at the same month -> None.
    same_month = [
        _anchor(month=date(2024, 6, 1), point=500, observation_id="a"),
        _anchor(month=date(2024, 6, 1), point=510, observation_id="b"),
    ]
    assert (
        interpolate_series_from_anchors(seg, segment_anchors=same_month) is None
    )


def test_linear_interpolation_between_two_anchors() -> None:
    seg = Segment(start_month=date(2024, 1, 1), end_month=date(2024, 7, 1))
    anchors = [
        _anchor(month=date(2024, 1, 1), point=100, spread=0, observation_id="a"),
        _anchor(month=date(2024, 7, 1), point=700, spread=0, observation_id="b"),
    ]
    rows = interpolate_series_from_anchors(seg, segment_anchors=anchors)
    assert rows is not None
    assert len(rows) == 7
    by_month = {r.month: r for r in rows}
    assert by_month[date(2024, 1, 1)].value_point == pytest.approx(100.0)
    assert by_month[date(2024, 4, 1)].value_point == pytest.approx(400.0)
    assert by_month[date(2024, 7, 1)].value_point == pytest.approx(700.0)
    assert all(
        r.method is EstimateMethod.interpolated_multi_anchor for r in rows
    )


def test_flat_extrapolation_before_first_and_after_last() -> None:
    seg = Segment(start_month=date(2023, 1, 1), end_month=date(2024, 12, 1))
    anchors = [
        _anchor(month=date(2023, 6, 1), point=300, observation_id="a"),
        _anchor(month=date(2024, 6, 1), point=600, observation_id="b"),
    ]
    rows = interpolate_series_from_anchors(seg, segment_anchors=anchors)
    assert rows is not None
    by_month = {r.month: r for r in rows}

    pre = by_month[date(2023, 2, 1)]
    assert pre.value_point == pytest.approx(300.0)
    assert pre.needs_review
    assert pre.suppression_reason == "extrapolated_beyond_anchor_range"

    post = by_month[date(2024, 11, 1)]
    assert post.value_point == pytest.approx(600.0)
    assert post.needs_review


def test_interior_months_have_non_manual_band() -> None:
    seg = Segment(start_month=date(2024, 1, 1), end_month=date(2024, 12, 1))
    anchors = [
        _anchor(month=date(2024, 1, 1), point=100, observation_id="a"),
        _anchor(month=date(2024, 12, 1), point=1200, observation_id="b"),
    ]
    rows = interpolate_series_from_anchors(seg, segment_anchors=anchors)
    assert rows is not None
    by_month = {r.month: r for r in rows}
    # Exactly on an anchor -> band medium (distance 0).
    assert by_month[date(2024, 1, 1)].confidence_band is ConfidenceBand.medium
    # 3 months from nearest anchor (march-ish) -> low.
    assert by_month[date(2024, 4, 1)].confidence_band is ConfidenceBand.low


def test_contributing_anchor_ids_are_flanking_pair() -> None:
    seg = Segment(start_month=date(2024, 1, 1), end_month=date(2024, 12, 1))
    anchors = [
        _anchor(month=date(2024, 1, 1), point=100, observation_id="a"),
        _anchor(month=date(2024, 6, 1), point=600, observation_id="b"),
        _anchor(month=date(2024, 12, 1), point=1200, observation_id="c"),
    ]
    rows = interpolate_series_from_anchors(seg, segment_anchors=anchors)
    assert rows is not None
    by_month = {r.month: r for r in rows}
    # March -> flanked by (a, b)
    assert by_month[date(2024, 3, 1)].contributing_anchor_ids == ("a", "b")
    # Sept -> flanked by (b, c)
    assert by_month[date(2024, 9, 1)].contributing_anchor_ids == ("b", "c")
    # Extrapolation edge -> single flanking anchor.
    # (seg starts at the earliest anchor so this is that anchor only).
    assert by_month[date(2024, 1, 1)].contributing_anchor_ids == ("a",)


def test_has_employment_signal_detects_presence_vs_absence() -> None:
    months = [date(2024, i, 1) for i in range(1, 7)]
    assert has_employment_signal(months, {}, sample_floor=5) is False
    assert (
        has_employment_signal(
            months,
            {date(2024, 3, 1): 2, date(2024, 4, 1): 1},
            sample_floor=5,
        )
        is False
    )
    assert (
        has_employment_signal(
            months,
            {date(2024, 3, 1): 6},
            sample_floor=5,
        )
        is True
    )


def test_highest_confidence_anchor_wins_when_same_month() -> None:
    seg = Segment(start_month=date(2024, 1, 1), end_month=date(2024, 12, 1))
    anchors = [
        _anchor(
            month=date(2024, 6, 1),
            point=500,
            observation_id="low",
            confidence=0.3,
        ),
        _anchor(
            month=date(2024, 6, 1),
            point=580,
            observation_id="high",
            confidence=0.9,
        ),
        _anchor(month=date(2024, 12, 1), point=600, observation_id="c"),
    ]
    rows = interpolate_series_from_anchors(seg, segment_anchors=anchors)
    assert rows is not None
    by_month = {r.month: r for r in rows}
    # At month 6 we should see the high-confidence anchor's value.
    assert by_month[date(2024, 6, 1)].value_point == pytest.approx(580.0)
