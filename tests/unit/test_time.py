"""Month-level time helper tests."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from headcount.utils.time import (
    MonthRange,
    add_months,
    month_diff,
    month_floor,
    month_range,
    next_month,
    prev_month,
)


def test_month_floor_from_date() -> None:
    assert month_floor(date(2024, 5, 17)) == date(2024, 5, 1)


def test_month_floor_from_datetime_utc() -> None:
    dt = datetime(2024, 5, 17, 15, 0, tzinfo=UTC)
    assert month_floor(dt) == date(2024, 5, 1)


def test_next_and_prev_month_cross_year_boundary() -> None:
    assert next_month(date(2024, 12, 1)) == date(2025, 1, 1)
    assert prev_month(date(2024, 1, 1)) == date(2023, 12, 1)


def test_add_months_negative_and_positive() -> None:
    assert add_months(date(2024, 3, 1), 10) == date(2025, 1, 1)
    assert add_months(date(2024, 3, 1), -5) == date(2023, 10, 1)


def test_month_diff() -> None:
    assert month_diff(date(2024, 6, 1), date(2024, 1, 1)) == 5
    assert month_diff(date(2023, 1, 1), date(2024, 6, 1)) == -17


def test_month_range_validation() -> None:
    rng = month_range(date(2023, 1, 15), date(2023, 4, 10))
    assert rng.start == date(2023, 1, 1)
    assert rng.end == date(2023, 4, 1)
    assert rng.length() == 4
    assert list(rng.months()) == [
        date(2023, 1, 1),
        date(2023, 2, 1),
        date(2023, 3, 1),
        date(2023, 4, 1),
    ]


def test_month_range_rejects_inverted() -> None:
    with pytest.raises(ValueError):
        MonthRange(start=date(2023, 5, 1), end=date(2023, 2, 1))
