"""Golden tests for the headcount-value parser."""

from __future__ import annotations

import pytest

from headcount.db.enums import HeadcountValueKind
from headcount.parsers.headcount_value import parse_headcount_value


@pytest.mark.parametrize(
    ("raw", "min_", "point", "max_", "kind"),
    [
        (65.0, 65.0, 65.0, 65.0, HeadcountValueKind.exact),
        (1565, 1565.0, 1565.0, 1565.0, HeadcountValueKind.exact),
        ("65", 65.0, 65.0, 65.0, HeadcountValueKind.exact),
        ("1,565", 1565.0, 1565.0, 1565.0, HeadcountValueKind.exact),
        ("201-500", 201.0, 350.5, 500.0, HeadcountValueKind.range),
        ("201 - 500", 201.0, 350.5, 500.0, HeadcountValueKind.range),
        ("1,000-5,000", 1000.0, 3000.0, 5000.0, HeadcountValueKind.range),
        ("51-200", 51.0, 125.5, 200.0, HeadcountValueKind.range),
        ("500-201", 201.0, 350.5, 500.0, HeadcountValueKind.range),
    ],
)
def test_parser_ranges_and_exact(
    raw: object,
    min_: float,
    point: float,
    max_: float,
    kind: HeadcountValueKind,
) -> None:
    parsed = parse_headcount_value(raw)
    assert parsed is not None
    assert parsed.value_min == min_
    assert parsed.value_point == point
    assert parsed.value_max == max_
    assert parsed.kind is kind


def test_parser_bucket_with_open_upper_bound() -> None:
    parsed = parse_headcount_value("10000+")
    assert parsed is not None
    assert parsed.value_min == 10000.0
    assert parsed.value_max == 20000.0
    assert parsed.kind is HeadcountValueKind.bucket


@pytest.mark.parametrize("raw", [None, "", "   ", "n/a", "not a number", float("nan"), -5])
def test_parser_returns_none_for_bad_inputs(raw: object) -> None:
    assert parse_headcount_value(raw) is None


def test_parser_rejects_booleans() -> None:
    assert parse_headcount_value(True) is None
    assert parse_headcount_value(False) is None
