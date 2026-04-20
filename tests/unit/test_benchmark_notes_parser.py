"""Benchmark note-hint extraction tests."""

from __future__ import annotations

from datetime import date

from headcount.db.enums import BenchmarkEventHintType
from headcount.parsers.benchmark_notes import parse_note_hint


def test_acquisition_in_june_2023() -> None:
    hint = parse_note_hint("Acquired by Symphony AI in June 2023")
    assert hint is not None
    assert hint.hint_type is BenchmarkEventHintType.acquisition
    assert hint.event_month_hint == date(2023, 6, 1)
    assert hint.description == "Acquired by Symphony AI in June 2023"


def test_rebrand_without_date() -> None:
    hint = parse_note_hint("Rebranded last year")
    assert hint is not None
    assert hint.hint_type is BenchmarkEventHintType.rebrand
    assert hint.event_month_hint is None


def test_merger_with_short_month() -> None:
    hint = parse_note_hint("Merger completed Aug 2024")
    assert hint is not None
    assert hint.hint_type is BenchmarkEventHintType.merger
    assert hint.event_month_hint == date(2024, 8, 1)


def test_unknown_hint_preserves_text() -> None:
    hint = parse_note_hint("Doing great things")
    assert hint is not None
    assert hint.hint_type is BenchmarkEventHintType.unknown


def test_returns_none_for_empty_input() -> None:
    assert parse_note_hint(None) is None
    assert parse_note_hint("") is None
    assert parse_note_hint("   ") is None
