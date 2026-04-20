"""Unit tests for event-aware hard-break segmentation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from headcount.db.enums import EventType
from headcount.estimate.segments import (
    SEGMENTS_VERSION,
    Segment,
    is_break_event,
    segment_for_month,
    split_into_segments,
)


@dataclass
class _StubEvent:
    """Minimal duck-typed CompanyEvent - we only read type + month."""

    event_type: EventType
    event_month: date

    def __getattr__(self, name: str) -> Any:  # pragma: no cover - defensive
        raise AttributeError(name)


def test_version_constant_stable() -> None:
    assert SEGMENTS_VERSION == "segments_v1"


def test_is_break_event_whitelist() -> None:
    assert is_break_event(EventType.acquisition) is True
    assert is_break_event(EventType.merger) is True
    assert is_break_event(EventType.spinout) is True
    assert is_break_event(EventType.layoff) is True
    assert is_break_event(EventType.parent_sub_reassignment) is True
    # Name-only changes never break the series.
    assert is_break_event(EventType.rebrand) is False
    assert is_break_event(EventType.stealth_to_public) is False


def test_no_events_returns_single_segment() -> None:
    segs = split_into_segments([], start_month=date(2022, 1, 1), end_month=date(2023, 6, 1))
    assert segs == [Segment(start_month=date(2022, 1, 1), end_month=date(2023, 6, 1))]


def test_single_break_creates_two_segments() -> None:
    ev = _StubEvent(event_type=EventType.acquisition, event_month=date(2022, 7, 1))
    segs = split_into_segments([ev], start_month=date(2022, 1, 1), end_month=date(2022, 12, 1))
    assert len(segs) == 2
    assert segs[0].start_month == date(2022, 1, 1)
    assert segs[0].end_month == date(2022, 6, 1)
    assert segs[0].opening_event_types == ()
    assert segs[1].start_month == date(2022, 7, 1)
    assert segs[1].end_month == date(2022, 12, 1)
    assert segs[1].opening_event_types == (EventType.acquisition,)


def test_events_on_start_month_attach_to_first_segment() -> None:
    ev = _StubEvent(event_type=EventType.spinout, event_month=date(2022, 1, 1))
    segs = split_into_segments([ev], start_month=date(2022, 1, 1), end_month=date(2022, 6, 1))
    assert len(segs) == 1
    assert segs[0].opening_event_types == (EventType.spinout,)


def test_events_outside_window_are_ignored() -> None:
    before = _StubEvent(EventType.acquisition, date(2019, 5, 1))
    after = _StubEvent(EventType.merger, date(2030, 12, 1))
    segs = split_into_segments(
        [before, after],
        start_month=date(2022, 1, 1),
        end_month=date(2022, 12, 1),
    )
    assert len(segs) == 1


def test_non_break_event_does_not_split() -> None:
    rebrand = _StubEvent(EventType.rebrand, date(2022, 6, 1))
    segs = split_into_segments([rebrand], start_month=date(2022, 1, 1), end_month=date(2022, 12, 1))
    assert len(segs) == 1
    assert segs[0].opening_event_types == ()


def test_two_breaks_in_same_month_collapse() -> None:
    a = _StubEvent(EventType.acquisition, date(2022, 6, 1))
    b = _StubEvent(EventType.parent_sub_reassignment, date(2022, 6, 1))
    segs = split_into_segments([a, b], start_month=date(2022, 1, 1), end_month=date(2022, 12, 1))
    assert len(segs) == 2
    assert segs[1].opening_event_types == (
        EventType.acquisition,
        EventType.parent_sub_reassignment,
    )


def test_many_breaks_sorted() -> None:
    ev1 = _StubEvent(EventType.acquisition, date(2021, 3, 1))
    ev2 = _StubEvent(EventType.spinout, date(2022, 1, 1))
    ev3 = _StubEvent(EventType.merger, date(2023, 9, 1))
    segs = split_into_segments(
        [ev3, ev1, ev2],
        start_month=date(2020, 1, 1),
        end_month=date(2024, 6, 1),
    )
    assert [(s.start_month, s.end_month) for s in segs] == [
        (date(2020, 1, 1), date(2021, 2, 1)),
        (date(2021, 3, 1), date(2021, 12, 1)),
        (date(2022, 1, 1), date(2023, 8, 1)),
        (date(2023, 9, 1), date(2024, 6, 1)),
    ]


def test_end_before_start_returns_empty() -> None:
    assert split_into_segments([], start_month=date(2023, 1, 1), end_month=date(2022, 12, 1)) == []


def test_segment_months_enumerates_correctly() -> None:
    seg = Segment(start_month=date(2022, 11, 1), end_month=date(2023, 2, 1))
    assert seg.months() == [
        date(2022, 11, 1),
        date(2022, 12, 1),
        date(2023, 1, 1),
        date(2023, 2, 1),
    ]


def test_segment_for_month() -> None:
    segs = [
        Segment(start_month=date(2022, 1, 1), end_month=date(2022, 6, 1)),
        Segment(start_month=date(2022, 7, 1), end_month=date(2022, 12, 1)),
    ]
    assert segment_for_month(segs, date(2022, 3, 15)) is segs[0]
    assert segment_for_month(segs, date(2022, 7, 1)) is segs[1]
    assert segment_for_month(segs, date(2099, 1, 1)) is None
