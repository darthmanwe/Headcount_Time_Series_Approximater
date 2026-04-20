"""Unit tests for :mod:`headcount.parsers.dates`."""

from __future__ import annotations

from datetime import date

import pytest

from headcount.parsers.dates import (
    DATES_PARSER_VERSION,
    ParsedMonth,
    ParsedMonthRange,
    parse_month,
    parse_month_range,
)


def test_parser_version_stable() -> None:
    assert DATES_PARSER_VERSION == "dates_v1"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2023-08", date(2023, 8, 1)),
        ("2023/8", date(2023, 8, 1)),
        ("08-2023", date(2023, 8, 1)),
        ("8/2023", date(2023, 8, 1)),
        ("Aug 2023", date(2023, 8, 1)),
        ("August 2023", date(2023, 8, 1)),
        ("Aug. 2023", date(2023, 8, 1)),
        ("Aug, 2023", date(2023, 8, 1)),
        ("SEPT 2019", date(2019, 9, 1)),
        ("December 2024", date(2024, 12, 1)),
    ],
)
def test_parse_month_happy_paths(raw: str, expected: date) -> None:
    parsed = parse_month(raw)
    assert parsed is not None
    assert parsed.month == expected
    assert parsed.confidence_reduced is False


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2023", date(2023, 1, 1)),
        ("2019", date(2019, 1, 1)),
    ],
)
def test_parse_month_year_only_is_flagged(raw: str, expected: date) -> None:
    parsed = parse_month(raw)
    assert parsed is not None
    assert parsed.month == expected
    assert parsed.confidence_reduced is True
    assert parsed.note == "year_only"


@pytest.mark.parametrize(
    ("raw", "expected_month"),
    [
        ("Q1 2023", 1),
        ("Q2 2023", 4),
        ("Q3 2023", 7),
        ("Q4 2023", 10),
        ("Q4 '23", 10),
    ],
)
def test_parse_month_quarter(raw: str, expected_month: int) -> None:
    parsed = parse_month(raw)
    assert parsed is not None
    assert parsed.month == date(2023, expected_month, 1)
    assert parsed.confidence_reduced is True


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "   ",
        "Spring 2023",
        "Summer",
        "not a date",
        "Aug",  # month without a year is ambiguous
        "1800-01",  # well before our window
        "3099-01",  # far future
        "13/2020",  # invalid month
        "present",
        "current",
    ],
)
def test_parse_month_rejects(raw: str) -> None:
    assert parse_month(raw) is None


def test_parse_month_respects_max_year() -> None:
    assert parse_month("2030-01", max_year=2025) is None
    assert parse_month("2025-01", max_year=2025) == ParsedMonth(
        month=date(2025, 1, 1), raw="2025-01"
    )


def test_parse_month_range_two_named() -> None:
    parsed = parse_month_range("Aug 2021 - Dec 2023")
    assert parsed == ParsedMonthRange(
        start=date(2021, 8, 1),
        end=date(2023, 12, 1),
        end_open=False,
        raw="Aug 2021 - Dec 2023",
    )


def test_parse_month_range_iso() -> None:
    parsed = parse_month_range("2021-08 to 2023-12")
    assert parsed is not None
    assert parsed.start == date(2021, 8, 1)
    assert parsed.end == date(2023, 12, 1)
    assert parsed.end_open is False


def test_parse_month_range_en_dash() -> None:
    parsed = parse_month_range("Aug 2021 \u2013 Dec 2023")
    assert parsed is not None
    assert parsed.end == date(2023, 12, 1)


def test_parse_month_range_present_without_cutoff() -> None:
    parsed = parse_month_range("Aug 2022 - present")
    assert parsed is not None
    assert parsed.start == date(2022, 8, 1)
    assert parsed.end is None
    assert parsed.end_open is True


def test_parse_month_range_present_with_cutoff_clamps() -> None:
    cutoff = date(2026, 3, 1)
    parsed = parse_month_range("Aug 2022 - current", cutoff=cutoff)
    assert parsed is not None
    assert parsed.end == cutoff
    assert parsed.end_open is True


def test_parse_month_range_flags_reduced_from_year_only() -> None:
    parsed = parse_month_range("2021 - 2023")
    assert parsed is not None
    assert parsed.start == date(2021, 1, 1)
    assert parsed.end == date(2023, 1, 1)
    assert parsed.confidence_reduced is True


def test_parse_month_range_rejects_inverted() -> None:
    assert parse_month_range("Dec 2023 - Jan 2023") is None


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "Aug 2021",  # no separator
        "Aug 2021 - garbage",
        "garbage - Aug 2021",
    ],
)
def test_parse_month_range_rejects_bad_input(raw: str) -> None:
    assert parse_month_range(raw) is None
