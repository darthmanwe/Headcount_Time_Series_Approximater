"""Unit tests for employment-interval expansion."""

from __future__ import annotations

from datetime import date

from headcount.estimate.employment import (
    EMPLOYMENT_EXPANSION_VERSION,
    EmploymentInterval,
    monthly_public_profile_counts,
)


def test_version_constant_stable() -> None:
    assert EMPLOYMENT_EXPANSION_VERSION == "employment_v1"


def test_empty_intervals_yield_zero_counts() -> None:
    counts = monthly_public_profile_counts(
        [],
        start_month=date(2022, 1, 1),
        end_month=date(2022, 3, 1),
        as_of_month=date(2022, 6, 1),
    )
    assert counts == {
        date(2022, 1, 1): 0,
        date(2022, 2, 1): 0,
        date(2022, 3, 1): 0,
    }


def test_closed_interval_counts_each_month_once() -> None:
    i = EmploymentInterval(person_id="p1", start_month=date(2022, 2, 1), end_month=date(2022, 4, 1))
    counts = monthly_public_profile_counts(
        [i],
        start_month=date(2022, 1, 1),
        end_month=date(2022, 6, 1),
        as_of_month=date(2022, 12, 1),
    )
    assert counts == {
        date(2022, 1, 1): 0,
        date(2022, 2, 1): 1,
        date(2022, 3, 1): 1,
        date(2022, 4, 1): 1,
        date(2022, 5, 1): 0,
        date(2022, 6, 1): 0,
    }


def test_open_ended_current_role_runs_to_as_of() -> None:
    i = EmploymentInterval(
        person_id="p1",
        start_month=date(2022, 3, 1),
        end_month=None,
        is_current_role=True,
    )
    counts = monthly_public_profile_counts(
        [i],
        start_month=date(2022, 1, 1),
        end_month=date(2022, 6, 1),
        as_of_month=date(2022, 5, 1),
    )
    # Runs Mar..May (as_of), Jun is past as_of.
    assert counts[date(2022, 2, 1)] == 0
    assert counts[date(2022, 3, 1)] == 1
    assert counts[date(2022, 4, 1)] == 1
    assert counts[date(2022, 5, 1)] == 1
    assert counts[date(2022, 6, 1)] == 0


def test_open_ended_without_current_flag_is_pinned_to_start() -> None:
    i = EmploymentInterval(
        person_id="p1",
        start_month=date(2022, 3, 1),
        end_month=None,
        is_current_role=False,
    )
    counts = monthly_public_profile_counts(
        [i],
        start_month=date(2022, 1, 1),
        end_month=date(2022, 6, 1),
        as_of_month=date(2022, 12, 1),
    )
    # Only the start month is counted; we don't assume an open end.
    assert counts[date(2022, 3, 1)] == 1
    assert counts[date(2022, 4, 1)] == 0


def test_dedup_per_person_per_month() -> None:
    # Two overlapping intervals for the same person (think of a role change
    # that was recorded twice) must not double-count.
    i1 = EmploymentInterval(
        person_id="p1", start_month=date(2022, 1, 1), end_month=date(2022, 6, 1)
    )
    i2 = EmploymentInterval(
        person_id="p1", start_month=date(2022, 3, 1), end_month=date(2022, 9, 1)
    )
    counts = monthly_public_profile_counts(
        [i1, i2],
        start_month=date(2022, 1, 1),
        end_month=date(2022, 12, 1),
        as_of_month=date(2022, 12, 1),
    )
    assert counts[date(2022, 4, 1)] == 1
    assert counts[date(2022, 8, 1)] == 1
    assert counts[date(2022, 1, 1)] == 1


def test_multiple_people_summed() -> None:
    i1 = EmploymentInterval(
        person_id="p1", start_month=date(2022, 1, 1), end_month=date(2022, 6, 1)
    )
    i2 = EmploymentInterval(
        person_id="p2", start_month=date(2022, 3, 1), end_month=date(2022, 9, 1)
    )
    counts = monthly_public_profile_counts(
        [i1, i2],
        start_month=date(2022, 1, 1),
        end_month=date(2022, 9, 1),
        as_of_month=date(2022, 12, 1),
    )
    assert counts[date(2022, 1, 1)] == 1
    assert counts[date(2022, 3, 1)] == 2
    assert counts[date(2022, 6, 1)] == 2
    assert counts[date(2022, 7, 1)] == 1


def test_months_past_as_of_always_zero() -> None:
    i = EmploymentInterval(
        person_id="p1",
        start_month=date(2022, 1, 1),
        end_month=None,
        is_current_role=True,
    )
    counts = monthly_public_profile_counts(
        [i],
        start_month=date(2022, 1, 1),
        end_month=date(2023, 6, 1),
        as_of_month=date(2022, 12, 1),
    )
    assert counts[date(2023, 1, 1)] == 0
    assert counts[date(2023, 6, 1)] == 0
    assert counts[date(2022, 12, 1)] == 1
