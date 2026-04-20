"""Event-aware hard-break segmentation.

A company's monthly headcount series must not be smoothed or interpolated
across a regime change - acquisitions, mergers, rebrands, spinouts, and
parent/sub reassignments all create discontinuities that overwhelm any
honest rate of natural growth. ``split_into_segments`` takes an ordered
list of :class:`~headcount.models.company_event.CompanyEvent` rows plus a
requested ``[start_month, end_month]`` window and yields the list of
contiguous :class:`Segment` intervals that the caller can reconcile
independently.

Semantics (all months are first-of-month :class:`datetime.date` values):

- Event month is **included** in the *post-event* segment. Reasoning: if we
  learn Acme acquired BetaCorp in June 2023, June 2023 already reflects the
  combined workforce.
- Events before the window open are ignored; events after the window close
  are ignored.
- Events on the exact ``start_month`` still open a fresh segment for
  bookkeeping but yield a single segment covering the whole window.
- Two events in the same month collapse to a single break; their event_type
  names are preserved for auditing.
- Only event types that truly change workforce composition break the series.
  ``rebrand`` and ``stealth_to_public`` are name / public-visibility changes
  and do **not** create a break. ``parent_sub_reassignment`` does.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date

from headcount.db.enums import EventType
from headcount.models.company_event import CompanyEvent

SEGMENTS_VERSION = "segments_v1"
"""Bumped when the break-event whitelist or boundary semantics change."""

_BREAK_EVENT_TYPES: frozenset[EventType] = frozenset(
    {
        EventType.acquisition,
        EventType.merger,
        EventType.spinout,
        EventType.layoff,
        EventType.parent_sub_reassignment,
    }
)


def is_break_event(event_type: EventType) -> bool:
    """Return ``True`` iff ``event_type`` hard-breaks the series."""

    return event_type in _BREAK_EVENT_TYPES


@dataclass(frozen=True, slots=True)
class Segment:
    """Closed-on-both-ends monthly segment [start_month, end_month]."""

    start_month: date
    end_month: date
    opening_event_types: tuple[EventType, ...] = field(default_factory=tuple)

    def months(self) -> list[date]:
        """All first-of-month dates between ``start_month`` and ``end_month``."""

        out: list[date] = []
        cur = self.start_month
        while cur <= self.end_month:
            out.append(cur)
            cur = _next_month(cur)
        return out

    def contains(self, month: date) -> bool:
        return self.start_month <= month <= self.end_month


def _next_month(m: date) -> date:
    if m.month == 12:
        return date(m.year + 1, 1, 1)
    return date(m.year, m.month + 1, 1)


def _prev_month(m: date) -> date:
    if m.month == 1:
        return date(m.year - 1, 12, 1)
    return date(m.year, m.month - 1, 1)


def _month_floor(d: date) -> date:
    return d.replace(day=1)


def split_into_segments(
    events: Iterable[CompanyEvent],
    *,
    start_month: date,
    end_month: date,
) -> list[Segment]:
    """Split ``[start_month, end_month]`` at each breaking event.

    Parameters
    ----------
    events:
        Iterable of :class:`CompanyEvent` rows (order doesn't matter; we
        sort internally).
    start_month, end_month:
        Both inclusive and first-of-month. If the caller passes a
        non-first-of-month date we floor silently.

    Returns
    -------
    list[Segment]
        Segments in chronological order covering the full window exactly.
        For a window with no breaking events the list has a single element.
    """

    start = _month_floor(start_month)
    end = _month_floor(end_month)
    if end < start:
        return []

    # Collect break months strictly inside the open interval (start, end].
    # An event on start_month does not create an empty leading segment; it
    # simply attaches its event_type to the first (and only) segment.
    events_by_month: dict[date, list[EventType]] = {}
    leading_event_types: list[EventType] = []
    for ev in events:
        if not is_break_event(ev.event_type):
            continue
        m = _month_floor(ev.event_month)
        if m < start or m > end:
            continue
        if m == start:
            leading_event_types.append(ev.event_type)
            continue
        events_by_month.setdefault(m, []).append(ev.event_type)

    break_months = sorted(events_by_month.keys())

    segments: list[Segment] = []
    cursor = start
    opening_types: tuple[EventType, ...] = tuple(dict.fromkeys(leading_event_types))

    for bm in break_months:
        # Segment ends at the month before the break; the break month opens
        # the next segment.
        segments.append(
            Segment(
                start_month=cursor,
                end_month=_prev_month(bm),
                opening_event_types=opening_types,
            )
        )
        cursor = bm
        # Deterministic dedup while preserving first-seen order.
        opening_types = tuple(dict.fromkeys(events_by_month[bm]))

    segments.append(
        Segment(
            start_month=cursor,
            end_month=end,
            opening_event_types=opening_types,
        )
    )

    return segments


def segment_for_month(segments: list[Segment], month: date) -> Segment | None:
    """Find the segment containing ``month``; ``None`` if outside all segments."""

    target = _month_floor(month)
    for seg in segments:
        if seg.contains(target):
            return seg
    return None


__all__ = [
    "SEGMENTS_VERSION",
    "Segment",
    "is_break_event",
    "segment_for_month",
    "split_into_segments",
]
