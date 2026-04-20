"""Expand employment observations into monthly public-profile counts.

:class:`~headcount.models.person_employment_observation.PersonEmploymentObservation`
rows are interval-valued (``start_month``..``end_month``, or open-ended
when ``is_current_role=True``). Summing how many such rows are *live* in a
given month gives the monthly public-profile count that downstream
ratio-scaling will divide by. We also dedupe per-person per-month so a
person who has two overlapping roles at the same company is counted once.

Semantics
---------

- Months are first-of-month. Both ``start_month`` and ``end_month`` are
  inclusive.
- Open-ended intervals (``end_month is None``) run through the provided
  ``as_of_month`` (typically "today" floored to the first of the month).
- We intentionally do **not** extend past ``as_of_month`` even when
  ``is_current_role=True`` - you can't count a profile you haven't seen.
- Confidence weighting is applied separately in the reconcile step; here
  we only produce integer counts, because the anchor "now" value is an
  integer headcount and the ratio is what carries uncertainty.

Pure-functional: no session, no I/O. The pipeline module adapts rows into
:class:`EmploymentInterval` before calling.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

EMPLOYMENT_EXPANSION_VERSION = "employment_v1"
"""Bumped when dedup/open-end semantics change."""


@dataclass(frozen=True, slots=True)
class EmploymentInterval:
    """DTO adapter for :class:`PersonEmploymentObservation`."""

    person_id: str
    start_month: date
    end_month: date | None
    is_current_role: bool = False
    confidence: float = 0.5

    def live_in(self, month: date, *, as_of_month: date) -> bool:
        start = _month_floor(self.start_month)
        if month < start:
            return False
        effective_end = self.end_month
        if effective_end is None:
            if not self.is_current_role:
                # Conservatively treat missing end + not-current as closed
                # at start_month.
                return month == start
            effective_end = as_of_month
        effective_end = _month_floor(effective_end)
        return month <= effective_end


def _month_floor(d: date) -> date:
    return d.replace(day=1)


def _next_month(m: date) -> date:
    if m.month == 12:
        return date(m.year + 1, 1, 1)
    return date(m.year, m.month + 1, 1)


def monthly_public_profile_counts(
    intervals: Iterable[EmploymentInterval],
    *,
    start_month: date,
    end_month: date,
    as_of_month: date,
) -> dict[date, int]:
    """Return ``{month: count}`` across ``[start_month, end_month]`` inclusive.

    A person contributes at most once per month regardless of how many
    overlapping intervals they have. Months with zero live intervals are
    still present in the output mapping with value ``0`` so downstream
    consumers don't have to reason about missing keys.
    """

    start = _month_floor(start_month)
    end = _month_floor(end_month)
    cutoff = _month_floor(as_of_month)

    if end < start:
        return {}

    per_month: dict[date, set[str]] = defaultdict(set)
    months: list[date] = []
    cur = start
    while cur <= end:
        months.append(cur)
        cur = _next_month(cur)

    for interval in intervals:
        for m in months:
            if m > cutoff:
                break
            if interval.live_in(m, as_of_month=cutoff):
                per_month[m].add(interval.person_id)

    return {m: len(per_month.get(m, set())) for m in months}


__all__ = [
    "EMPLOYMENT_EXPANSION_VERSION",
    "EmploymentInterval",
    "monthly_public_profile_counts",
]
