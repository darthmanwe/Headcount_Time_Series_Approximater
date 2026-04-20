"""Extract event hints from benchmark note columns.

The ``LinkedIn April 13`` sheet carries free-text notes like
``"Acquired by Symphony AI in June 2023"``. The parser is intentionally
conservative: if anything looks ambiguous, ``hint_type`` is
``unknown`` and the raw text flows downstream unchanged so analysts can
resolve it in Phase 8. Months are resolved to the first day of the named
month.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

from headcount.db.enums import BenchmarkEventHintType

_MONTH_WORDS: dict[str, int] = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sept": 9,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

_HINT_PATTERNS: list[tuple[re.Pattern[str], BenchmarkEventHintType]] = [
    (re.compile(r"\bacquired\b", re.IGNORECASE), BenchmarkEventHintType.acquisition),
    (re.compile(r"\bacquisition\b", re.IGNORECASE), BenchmarkEventHintType.acquisition),
    (re.compile(r"\brebrand(?:ed)?\b", re.IGNORECASE), BenchmarkEventHintType.rebrand),
    (re.compile(r"\brenamed\b", re.IGNORECASE), BenchmarkEventHintType.rebrand),
    (re.compile(r"\bmerged\b", re.IGNORECASE), BenchmarkEventHintType.merger),
    (re.compile(r"\bmerger\b", re.IGNORECASE), BenchmarkEventHintType.merger),
]

_DATE_RE = re.compile(
    r"\b(?P<month>"
    + "|".join(sorted(_MONTH_WORDS.keys(), key=len, reverse=True))
    + r")\s+(?P<year>\d{4})\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class ParsedNoteHint:
    hint_type: BenchmarkEventHintType
    event_month_hint: date | None
    description: str


def parse_note_hint(note: str | None) -> ParsedNoteHint | None:
    if note is None:
        return None
    text = note.strip()
    if not text:
        return None

    hint_type = BenchmarkEventHintType.unknown
    for pattern, candidate in _HINT_PATTERNS:
        if pattern.search(text):
            hint_type = candidate
            break

    event_month: date | None = None
    match = _DATE_RE.search(text)
    if match:
        month = _MONTH_WORDS[match.group("month").lower()]
        year = int(match.group("year"))
        event_month = date(year, month, 1)

    return ParsedNoteHint(
        hint_type=hint_type,
        event_month_hint=event_month,
        description=text,
    )
