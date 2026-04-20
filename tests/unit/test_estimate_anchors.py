"""Unit tests for anchor reconciliation within a segment."""

from __future__ import annotations

from datetime import date

import pytest

from headcount.db.enums import AnchorType, HeadcountValueKind
from headcount.estimate.anchors import (
    ANCHOR_POLICY_VERSION,
    AnchorCandidate,
    reconcile_segment_anchors,
)


def _a(
    *,
    month: date = date(2023, 1, 1),
    point: float = 500.0,
    vmin: float | None = None,
    vmax: float | None = None,
    kind: HeadcountValueKind = HeadcountValueKind.exact,
    type_: AnchorType = AnchorType.historical_statement,
    conf: float = 0.7,
    oid: str = "a",
) -> AnchorCandidate:
    return AnchorCandidate(
        anchor_month=month,
        value_point=point,
        value_min=point if vmin is None else vmin,
        value_max=point if vmax is None else vmax,
        kind=kind,
        anchor_type=type_,
        confidence=conf,
        observation_id=oid,
    )


def test_version_constant_stable() -> None:
    assert ANCHOR_POLICY_VERSION == "anchors_v1"


def test_empty_list_returns_none() -> None:
    assert (
        reconcile_segment_anchors([], segment_start=date(2022, 1, 1), segment_end=date(2022, 12, 1))
        is None
    )


def test_single_anchor_passthrough() -> None:
    a = _a(point=500, vmin=450, vmax=550)
    r = reconcile_segment_anchors(
        [a], segment_start=date(2023, 1, 1), segment_end=date(2023, 12, 1)
    )
    assert r is not None
    assert r.value_point == pytest.approx(500.0)
    assert r.value_min == pytest.approx(450.0)
    assert r.value_max == pytest.approx(550.0)
    assert r.contributing_ids == ("a",)
    assert r.weights == {"a": pytest.approx(1.0)}


def test_manual_anchor_dominates_historical() -> None:
    manual = _a(point=800, type_=AnchorType.manual_anchor, conf=0.4, oid="manual")
    sec = _a(point=600, type_=AnchorType.historical_statement, conf=0.9, oid="sec")
    r = reconcile_segment_anchors(
        [manual, sec],
        segment_start=date(2022, 1, 1),
        segment_end=date(2022, 12, 1),
    )
    assert r is not None
    # Manual is top tier alone; sec contributes to envelope but not point.
    assert r.value_point == pytest.approx(800.0)
    assert r.contributing_ids == ("manual",)


def test_bucket_excluded_from_point_when_exact_present() -> None:
    exact = _a(point=500, vmin=450, vmax=550, oid="ex")
    bucket = _a(
        point=5000,
        vmin=1001,
        vmax=5000,
        kind=HeadcountValueKind.bucket,
        type_=AnchorType.historical_statement,
        conf=0.9,
        oid="bk",
    )
    r = reconcile_segment_anchors(
        [exact, bucket],
        segment_start=date(2023, 1, 1),
        segment_end=date(2023, 12, 1),
    )
    assert r is not None
    assert "bk" not in r.contributing_ids
    assert r.value_point == pytest.approx(500.0)
    # Bucket's envelope still widens max.
    assert r.value_max >= 5000.0


def test_confidence_weighted_point_among_tier() -> None:
    a = _a(point=400, conf=0.2, oid="a")
    b = _a(point=600, conf=0.8, oid="b")
    r = reconcile_segment_anchors(
        [a, b], segment_start=date(2023, 1, 1), segment_end=date(2023, 12, 1)
    )
    assert r is not None
    assert r.value_point == pytest.approx((400 * 0.2 + 600 * 0.8) / (0.2 + 0.8))
    assert r.weights["a"] + r.weights["b"] == pytest.approx(1.0)
    assert r.weights["b"] > r.weights["a"]


def test_interval_envelope_is_union_of_tier() -> None:
    a = _a(point=500, vmin=480, vmax=520, oid="a")
    b = _a(point=700, vmin=650, vmax=800, oid="b")
    r = reconcile_segment_anchors(
        [a, b], segment_start=date(2023, 1, 1), segment_end=date(2023, 12, 1)
    )
    assert r is not None
    assert r.value_min == pytest.approx(480.0)
    assert r.value_max == pytest.approx(800.0)


def test_decay_half_life_downweights_distant_anchors() -> None:
    old = _a(point=400, month=date(2020, 1, 1), conf=0.9, oid="old")
    new = _a(point=800, month=date(2023, 6, 1), conf=0.9, oid="new")
    no_decay = reconcile_segment_anchors(
        [old, new],
        segment_start=date(2023, 1, 1),
        segment_end=date(2023, 12, 1),
        decay_half_life_months=None,
    )
    with_decay = reconcile_segment_anchors(
        [old, new],
        segment_start=date(2023, 1, 1),
        segment_end=date(2023, 12, 1),
        decay_half_life_months=6.0,
    )
    assert no_decay is not None and with_decay is not None
    assert with_decay.value_point > no_decay.value_point
