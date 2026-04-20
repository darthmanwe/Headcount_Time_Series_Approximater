"""Month-level time helpers used throughout the estimator.

Everything in the estimator operates at month granularity ([docs]). A
"month" is always represented as a ``datetime.date`` on the first day of
that month in UTC. Keeping this single invariant collapses a class of
subtle bugs around timezone drift and partial months.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime


@dataclass(frozen=True, slots=True)
class MonthRange:
    """Inclusive closed-closed month range ``[start, end]``."""

    start: date
    end: date

    def __post_init__(self) -> None:
        if self.start != month_floor(self.start):
            raise ValueError(f"start must be on month boundary, got {self.start!r}")
        if self.end != month_floor(self.end):
            raise ValueError(f"end must be on month boundary, got {self.end!r}")
        if self.end < self.start:
            raise ValueError(f"end < start ({self.end} < {self.start})")

    def months(self) -> Iterator[date]:
        """Yield each month boundary in the range, inclusive."""
        cur = self.start
        while cur <= self.end:
            yield cur
            cur = next_month(cur)

    def length(self) -> int:
        return (self.end.year - self.start.year) * 12 + (self.end.month - self.start.month) + 1


def month_floor(value: date | datetime) -> date:
    """Return the first day of the month containing ``value``."""
    if isinstance(value, datetime):
        value = value.astimezone(UTC).date() if value.tzinfo else value.date()
    return date(value.year, value.month, 1)


def next_month(value: date) -> date:
    """Return the first day of the month after ``value``."""
    floored = month_floor(value)
    if floored.month == 12:
        return date(floored.year + 1, 1, 1)
    return date(floored.year, floored.month + 1, 1)


def prev_month(value: date) -> date:
    """Return the first day of the month before ``value``."""
    floored = month_floor(value)
    if floored.month == 1:
        return date(floored.year - 1, 12, 1)
    return date(floored.year, floored.month - 1, 1)


def add_months(value: date, delta: int) -> date:
    """Return ``value`` shifted by ``delta`` months (may be negative)."""
    floored = month_floor(value)
    total = floored.year * 12 + (floored.month - 1) + delta
    year, month_index = divmod(total, 12)
    return date(year, month_index + 1, 1)


def month_diff(a: date, b: date) -> int:
    """Return ``month_floor(a) - month_floor(b)`` in whole months."""
    a_m = month_floor(a)
    b_m = month_floor(b)
    return (a_m.year - b_m.year) * 12 + (a_m.month - b_m.month)


def month_range(start: date, end: date) -> MonthRange:
    """Build a validated inclusive month range, flooring both ends."""
    return MonthRange(start=month_floor(start), end=month_floor(end))


def utc_today_month() -> date:
    """First day of the current UTC month."""
    return month_floor(datetime.now(tz=UTC))
