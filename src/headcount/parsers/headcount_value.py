"""Parse heterogenous headcount cell values into (min, point, max, kind).

Benchmark workbooks surface headcount in three shapes:

* ``exact`` scalar ("65", "602", 1565.0) — stored as min == point == max.
* ``range`` string ("201-500", "1,000-5,000") — min/max literal, point = midpoint.
* ``bucket`` label (LinkedIn Employee Range buckets like ``"51-200"``) —
  treated as a range when bounds parse, bucket otherwise.

Anything unrecognized returns ``None`` so callers can record ``raw_value_text``
for audit instead of silently inventing numbers.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Final

from headcount.db.enums import HeadcountValueKind

PARSER_VERSION: Final[str] = "headcount-value-v1"

_NUMBER_CLEAN_RE = re.compile(r"[,\s]")
_RANGE_RE = re.compile(r"^\s*(?P<low>\d[\d,]*)\s*[-\u2010-\u2015]\s*(?P<high>\d[\d,]*)\s*\+?\s*$")
_PLUS_RE = re.compile(r"^\s*(?P<low>\d[\d,]*)\s*\+\s*$")
_OPEN_HIGH_BUCKETS = {"10000+", "10,000+", "5001+", "5,001+"}


@dataclass(frozen=True, slots=True)
class ParsedHeadcount:
    value_min: float
    value_point: float
    value_max: float
    kind: HeadcountValueKind
    raw_value_text: str


def _clean_int(token: str) -> int:
    return int(_NUMBER_CLEAN_RE.sub("", token))


def parse_headcount_value(raw: object) -> ParsedHeadcount | None:
    """Return a parsed interval or ``None`` if the cell cannot be interpreted.

    ``raw`` may be ``None``, a number, or a string produced by a spreadsheet
    export. Excel surfaces "1,000-5,000" for large buckets, "201-500" for
    small ones, and occasionally "10000+" with an open upper bound — treat
    the open upper bound as bucket and double the floor as an explicit
    over-estimate.
    """
    if raw is None:
        return None

    if isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        if isinstance(raw, float) and math.isnan(raw):
            return None
        value = float(raw)
        if value < 0:
            return None
        return ParsedHeadcount(
            value_min=value,
            value_point=value,
            value_max=value,
            kind=HeadcountValueKind.exact,
            raw_value_text=str(raw),
        )

    text = str(raw).strip()
    if not text:
        return None

    plus = _PLUS_RE.match(text)
    if plus or text.lower() in {s.lower() for s in _OPEN_HIGH_BUCKETS}:
        lo_raw = plus.group("low") if plus else text.split("+", 1)[0]
        low = float(_clean_int(lo_raw))
        return ParsedHeadcount(
            value_min=low,
            value_point=low,
            value_max=low * 2.0,
            kind=HeadcountValueKind.bucket,
            raw_value_text=text,
        )

    rng = _RANGE_RE.match(text)
    if rng:
        low = float(_clean_int(rng.group("low")))
        high = float(_clean_int(rng.group("high")))
        if high < low:
            low, high = high, low
        mid = (low + high) / 2.0
        return ParsedHeadcount(
            value_min=low,
            value_point=mid,
            value_max=high,
            kind=HeadcountValueKind.range,
            raw_value_text=text,
        )

    try:
        scalar = float(_NUMBER_CLEAN_RE.sub("", text))
    except ValueError:
        return None
    if scalar < 0:
        return None
    return ParsedHeadcount(
        value_min=scalar,
        value_point=scalar,
        value_max=scalar,
        kind=HeadcountValueKind.exact,
        raw_value_text=text,
    )
