"""Conservative date parsing.

Rules of the game
-----------------

- All parsed months are *first-of-month* ``datetime.date`` values so
  downstream estimation can treat months as atomic units without
  juggling day offsets.
- When the input only pins a year (e.g. "2023"), or a season / quarter
  that is genuinely ambiguous about the exact month, we still return a
  month but flip ``confidence_reduced=True`` so callers can lower their
  own confidence score accordingly. We do NOT silently widen to a
  range in the scalar parser; the range parser handles that.
- We never guess across eras. Two-digit years, years that evaluate
  outside ``[1900, today.year + 1]``, and anything that would round to
  a future month beyond the run cutoff return ``None``.
- Output is deterministic: the same input string always produces the
  same ``ParsedMonth`` regardless of system locale.

Supported shapes (case-insensitive, whitespace-tolerant):

- ``2023-08``, ``2023/08``, ``08-2023``, ``08/2023``
- ``Aug 2023``, ``August 2023``, ``Aug. 2023``, ``Aug, 2023``
- ``2023``                     -> month=1, ``confidence_reduced=True``
- ``Q1 2023`` / ``Q4 '23``     -> first month of the quarter,
  ``confidence_reduced=True``
- ``Spring 2023`` / ``Summer`` -> suppressed (returns ``None``)
- ``present`` / ``current``    -> handled by :func:`parse_month_range`
  as the right edge; scalar parser returns ``None``.

See :func:`parse_month_range` for two-sided shapes like
``"Aug 2021 - Dec 2023"``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

DATES_PARSER_VERSION = "dates_v1"

_MONTH_ALIASES: dict[str, int] = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

# Right-bound sentinels that :func:`parse_month_range` accepts to mean
# "still current as of the run cutoff". Scalar parser rejects them.
_PRESENT_TOKENS = frozenset({"present", "current", "now", "today"})

_MONTH_NAME_RE = re.compile(r"(?i)\b(?P<month>[A-Za-z]{3,9})\.?\s*,?\s*(?P<year>\d{4})\b")
_YEAR_MONTH_RE = re.compile(r"\b(?P<year>\d{4})[/-](?P<month>\d{1,2})\b")
_MONTH_YEAR_RE = re.compile(r"\b(?P<month>\d{1,2})[/-](?P<year>\d{4})\b")
_YEAR_ONLY_RE = re.compile(r"^(?P<year>\d{4})$")
_QUARTER_RE = re.compile(r"(?i)\bQ(?P<q>[1-4])\s*['\u2019]?(?P<year>\d{2,4})\b")


@dataclass(frozen=True, slots=True)
class ParsedMonth:
    """Successful scalar month parse."""

    month: date
    raw: str
    confidence_reduced: bool = False
    note: str | None = None


@dataclass(frozen=True, slots=True)
class ParsedMonthRange:
    """Successful month-range parse (inclusive on both ends).

    ``end_open`` means the right edge was a "present"/"current" token
    and should be clamped to the run cutoff by the caller. In that case
    ``end`` is set to ``None``.
    """

    start: date
    end: date | None
    end_open: bool
    raw: str
    confidence_reduced: bool = False
    note: str | None = None


def _year_is_sane(year: int, *, max_year: int | None = None) -> bool:
    upper = max_year if max_year is not None else date.today().year + 1
    return 1900 <= year <= upper


def _normalize_two_digit_year(digits: str) -> int | None:
    """Return a four-digit year from a two- or four-digit string, else None."""
    if len(digits) == 4:
        try:
            return int(digits)
        except ValueError:  # pragma: no cover - regex enforces digits
            return None
    if len(digits) == 2:
        try:
            yy = int(digits)
        except ValueError:  # pragma: no cover
            return None
        # Deliberately narrow: we only accept two-digit years in the
        # "quarter" shape where Q1 '23 is unambiguous in context. Pick
        # 2000-2099 as the anchor - our data set starts at 2000.
        return 2000 + yy
    return None


def parse_month(raw: str, *, max_year: int | None = None) -> ParsedMonth | None:
    """Parse a single month token into a first-of-month :class:`date`.

    Returns ``None`` for unrecognized inputs so callers can decide
    whether to flag a review item or drop the observation.
    """
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered in _PRESENT_TOKENS:
        return None

    # 1. Year-month formats (unambiguous).
    m = _YEAR_MONTH_RE.search(text)
    if m:
        year = int(m.group("year"))
        month = int(m.group("month"))
        if _year_is_sane(year, max_year=max_year) and 1 <= month <= 12:
            return ParsedMonth(month=date(year, month, 1), raw=raw)

    m = _MONTH_YEAR_RE.search(text)
    if m:
        year = int(m.group("year"))
        month = int(m.group("month"))
        if _year_is_sane(year, max_year=max_year) and 1 <= month <= 12:
            return ParsedMonth(month=date(year, month, 1), raw=raw)

    # 2. "Aug 2023" / "August 2023".
    m = _MONTH_NAME_RE.search(text)
    if m:
        name = m.group("month").lower()
        year = int(m.group("year"))
        if name in _MONTH_ALIASES and _year_is_sane(year, max_year=max_year):
            return ParsedMonth(month=date(year, _MONTH_ALIASES[name], 1), raw=raw)

    # 3. Quarters: Q1 2023 / Q4 '23. First month of quarter; flag reduced.
    m = _QUARTER_RE.search(text)
    if m:
        q = int(m.group("q"))
        normalized_year = _normalize_two_digit_year(m.group("year"))
        if normalized_year is not None and _year_is_sane(normalized_year, max_year=max_year):
            return ParsedMonth(
                month=date(normalized_year, 1 + 3 * (q - 1), 1),
                raw=raw,
                confidence_reduced=True,
                note=f"quarter=Q{q}",
            )

    # 4. Bare year: pin to January, flag reduced.
    m = _YEAR_ONLY_RE.match(text)
    if m:
        year = int(m.group("year"))
        if _year_is_sane(year, max_year=max_year):
            return ParsedMonth(
                month=date(year, 1, 1),
                raw=raw,
                confidence_reduced=True,
                note="year_only",
            )

    return None


# Range separators we allow between the two endpoints. Hyphens must be
# whitespace-bounded so we don't chop inside ``2021-08``; unicode dashes
# and the word separators are tolerant of either whitespace pattern.
_RANGE_SEP_RE = re.compile(
    r"(?:\s+-{1,2}\s+|\s*[\u2013\u2014]\s*|\s+(?:to|through|until)\s+)",
    flags=re.IGNORECASE,
)


def parse_month_range(
    raw: str,
    *,
    max_year: int | None = None,
    cutoff: date | None = None,
) -> ParsedMonthRange | None:
    """Parse a two-sided month range.

    The right endpoint may be a "present"/"current" sentinel, in which
    case :attr:`ParsedMonthRange.end_open` is ``True`` and ``end`` is
    left at ``None`` for the caller to clamp to its run cutoff.
    """
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None

    parts = _RANGE_SEP_RE.split(text, maxsplit=1)
    if len(parts) != 2:
        return None
    left_raw, right_raw = parts[0].strip(), parts[1].strip()
    if not left_raw or not right_raw:
        return None

    left = parse_month(left_raw, max_year=max_year)
    if left is None:
        return None

    right_lower = right_raw.lower()
    end_open = right_lower in _PRESENT_TOKENS
    if end_open:
        end_date: date | None = None
        # Run cutoff clamping is the caller's job; we surface the flag.
        if cutoff is not None:
            end_date = cutoff
        return ParsedMonthRange(
            start=left.month,
            end=end_date,
            end_open=True,
            raw=raw,
            confidence_reduced=left.confidence_reduced,
            note=left.note,
        )

    right = parse_month(right_raw, max_year=max_year)
    if right is None:
        return None
    if right.month < left.month:
        # Inverted ranges are always data errors; refuse to guess.
        return None

    return ParsedMonthRange(
        start=left.month,
        end=right.month,
        end_open=False,
        raw=raw,
        confidence_reduced=left.confidence_reduced or right.confidence_reduced,
        note="; ".join(n for n in (left.note, right.note) if n) or None,
    )
