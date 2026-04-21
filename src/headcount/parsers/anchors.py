"""Per-source anchor parsing, as pure functions.

Observers in :mod:`headcount.ingest.observers` are thin fetchers that
handle I/O, caching, rate-limiting, and gate detection flow. The actual
*parse* - the regex, the XBRL unpacking, the SPARQL row flattening - all
lives here so:

- We can replay parsing over already-persisted ``source_observation``
  rows when ``parser_version`` bumps (Phase 7+).
- Tests exercise the parse in isolation with fixture strings only.
- Each source has a clearly versioned ``parser_version`` constant that
  observers surface as-is on every :class:`RawAnchorSignal` they emit.

Nothing in this module touches a database, a filesystem, or the network.
The only runtime dependencies are the standard library and the enum set
shared with the observers.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from urllib.parse import urlparse

from headcount.db.enums import HeadcountValueKind
from headcount.utils.time import month_floor

# ---------------------------------------------------------------------------
# Version constants
# ---------------------------------------------------------------------------

LINKEDIN_PUBLIC_PARSER_VERSION = "linkedin_public_v2"
COMPANY_WEB_PARSER_VERSION = "company_web_v1"
SEC_PARSER_VERSION = "sec_v1"
WIKIDATA_PARSER_VERSION = "wikidata_v1"

# ---------------------------------------------------------------------------
# LinkedIn logged-out public page
# ---------------------------------------------------------------------------

LINKEDIN_GATE_MARKERS: tuple[str, ...] = (
    "authwall",
    "sign in to see",
    "join linkedin to see",
    "session_redirect",
    "captcha",
    "unusual activity",
    "please verify you are a human",
)
_LOGIN_PATH_PREFIXES: tuple[str, ...] = (
    "/login",
    "/checkpoint",
    "/authwall",
    "/uas/login",
)

_LINKEDIN_BUCKETS: tuple[tuple[int, int, str], ...] = (
    (1, 10, "1-10"),
    (2, 10, "2-10"),
    (11, 50, "11-50"),
    (51, 200, "51-200"),
    (201, 500, "201-500"),
    (501, 1000, "501-1,000"),
    (1001, 5000, "1,001-5,000"),
    (5001, 10000, "5,001-10,000"),
    (10001, 50000, "10,001+"),
)
_LINKEDIN_RANGE_RE = re.compile(
    r"(?i)(?:Company\s+size|Employees?)"
    r"[^0-9]{0,30}"
    r"(?P<low>\d{1,3}(?:,\d{3})*)"
    r"\s*(?:-|to|\u2013)\s*"
    r"(?P<high>\d{1,3}(?:,\d{3})*)"
    r"\s*(?:\+\s*)?employees"
)
_LINKEDIN_OPEN_RE = re.compile(
    r"(?i)(?:Company\s+size|Employees?)"
    r"[^0-9]{0,30}"
    r"(?P<low>\d{1,3}(?:,\d{3})*)\+\s*employees"
)
_LINKEDIN_EXACT_RE = re.compile(
    r"(?i)\b(?P<n>\d{1,3}(?:,\d{3})+|\d{2,6})"
    r"\s*(?:employees?|associated\s+members?|members?)\b"
)


@dataclass(frozen=True, slots=True)
class ParsedBadge:
    """LinkedIn company-size badge parse result."""

    low: int
    high: int
    open_ended: bool
    phrase: str


def looks_gated_linkedin(status_code: int, text: str, final_url: str) -> str | None:
    """Return a structured gate reason for a LinkedIn response, else None."""
    if status_code == 429:
        return "rate_limited"
    if status_code == 403:
        return "forbidden"
    if status_code in (401, 407):
        return "auth_required"
    parsed = urlparse(final_url)
    if any(parsed.path.startswith(p) for p in _LOGIN_PATH_PREFIXES):
        return "login_redirect"
    lowered = text.lower()
    for marker in LINKEDIN_GATE_MARKERS:
        if marker in lowered:
            return f"marker:{marker.replace(' ', '_')}"
    return None


def extract_linkedin_badge(text: str) -> ParsedBadge | None:
    """Return the first company-size badge in ``text``."""
    for m in _LINKEDIN_RANGE_RE.finditer(text):
        low = int(m.group("low").replace(",", ""))
        high = int(m.group("high").replace(",", ""))
        if high <= low:
            continue
        return ParsedBadge(low=low, high=high, open_ended=False, phrase=m.group(0))
    for m in _LINKEDIN_OPEN_RE.finditer(text):
        low = int(m.group("low").replace(",", ""))
        return ParsedBadge(
            low=low,
            high=max(low * 5, low + 1),
            open_ended=True,
            phrase=m.group(0),
        )
    return None


def extract_linkedin_exact_count(text: str) -> tuple[int, str] | None:
    """Return ``(count, phrase)`` for an exact people-page count, else None."""
    for m in _LINKEDIN_EXACT_RE.finditer(text):
        raw = m.group("n").replace(",", "")
        try:
            value = int(raw)
        except ValueError:  # pragma: no cover
            continue
        if 2 <= value <= 10_000_000:
            return value, m.group(0)
    return None


def linkedin_bucket_label(low: int, high: int) -> str:
    for b_low, b_high, label in _LINKEDIN_BUCKETS:
        if b_low == low and b_high == high:
            return label
    return f"{low}-{high}"


# ---------------------------------------------------------------------------
# LinkedIn JSON-LD (lever L2)
# ---------------------------------------------------------------------------
#
# LinkedIn still ships a ``<script type="application/ld+json">`` block on
# public ``/company/<slug>/`` pages even when the visible body is a login
# wall: the SEO crew needs the structured data so that Google, Bing, and
# AI-crawl bots can index the org. We mine that block first because it's
# far more stable than the visible-text badge: the regex has to survive
# A/B copy changes ("Employees", "Company size", "Team members"),
# whereas the JSON-LD schema is an external contract.
#
# Expected shapes inside the Organization node:
#   "numberOfEmployees": 1234
#   "numberOfEmployees": {"@type": "QuantitativeValue", "value": 1234}
#   "numberOfEmployees": {"@type": "QuantitativeValue",
#                         "minValue": 51, "maxValue": 200}
# The min/max variant is what LinkedIn uses in practice and it lines up
# with the 10-bucket ladder we already model.

_JSONLD_SCRIPT_RE = re.compile(
    r"(?is)<script\b[^>]*type\s*=\s*[\"']application/ld\+json[\"'][^>]*>(.*?)</script>"
)


@dataclass(frozen=True, slots=True)
class LinkedInJsonLdEmployees:
    """One ``numberOfEmployees`` statement lifted from LinkedIn JSON-LD."""

    low: int
    high: int
    point: float
    kind: HeadcountValueKind  # exact when low == high, else bucket
    phrase: str
    org_name: str | None
    org_url: str | None


def _iter_jsonld_objects(html: str) -> list[Any]:
    """Return every JSON object parsed from ``application/ld+json`` blocks.

    Each block can legally hold either a single object or a list (common
    when a page advertises multiple schema types). We surface every
    object so the walker below can look for Organization regardless of
    nesting shape.
    """

    docs: list[Any] = []
    for match in _JSONLD_SCRIPT_RE.finditer(html):
        raw = match.group(1).strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, list):
            docs.extend(parsed)
        else:
            docs.append(parsed)
    return docs


def _is_organization_node(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    atype = obj.get("@type")
    if isinstance(atype, str):
        return "Organization" in atype or atype == "Corporation"
    if isinstance(atype, list):
        return any(isinstance(t, str) and "Organization" in t for t in atype)
    return False


def _employees_from_value(node: Any) -> tuple[int, int] | None:
    """Return ``(low, high)`` from a ``numberOfEmployees`` subtree, else None."""

    if isinstance(node, (int, float)):
        value = int(node)
        if 1 <= value <= 10_000_000:
            return value, value
        return None
    if isinstance(node, str):
        digits = node.replace(",", "").strip()
        if digits.isdigit():
            value = int(digits)
            if 1 <= value <= 10_000_000:
                return value, value
        return None
    if isinstance(node, dict):
        # QuantitativeValue with min/max.
        try:
            min_raw = node.get("minValue")
            max_raw = node.get("maxValue")
        except AttributeError:
            min_raw = max_raw = None
        if min_raw is not None and max_raw is not None:
            try:
                low = int(float(min_raw))
                high = int(float(max_raw))
            except (TypeError, ValueError):
                return None
            if low > 0 and high >= low:
                return low, high
        # QuantitativeValue with value (or plain {"value": N}).
        value_raw = node.get("value")
        if value_raw is not None:
            try:
                value = int(float(value_raw))
            except (TypeError, ValueError):
                return None
            if 1 <= value <= 10_000_000:
                return value, value
    return None


def extract_linkedin_jsonld_employees(html: str) -> LinkedInJsonLdEmployees | None:
    """Return the first valid ``numberOfEmployees`` statement found in JSON-LD.

    Walks every ``application/ld+json`` block and, within each block,
    every nested object. Stops at the first Organization node that has
    a parseable ``numberOfEmployees`` value; favours exact counts over
    buckets and rejects obviously invalid values (<=0 or absurd
    upper-bounds).
    """

    best_exact: LinkedInJsonLdEmployees | None = None
    best_bucket: LinkedInJsonLdEmployees | None = None

    def _walk(obj: Any) -> None:
        nonlocal best_exact, best_bucket
        if isinstance(obj, list):
            for item in obj:
                _walk(item)
            return
        if not isinstance(obj, dict):
            return

        if _is_organization_node(obj) and "numberOfEmployees" in obj:
            bounds = _employees_from_value(obj["numberOfEmployees"])
            if bounds is not None:
                low, high = bounds
                kind = (
                    HeadcountValueKind.exact
                    if low == high
                    else HeadcountValueKind.bucket
                )
                phrase = (
                    f"numberOfEmployees={low}"
                    if low == high
                    else f"numberOfEmployees={low}-{high}"
                )
                org_name = obj.get("name") if isinstance(obj.get("name"), str) else None
                org_url = obj.get("url") if isinstance(obj.get("url"), str) else None
                record = LinkedInJsonLdEmployees(
                    low=low,
                    high=high,
                    point=(low + high) / 2.0,
                    kind=kind,
                    phrase=phrase,
                    org_name=org_name,
                    org_url=org_url,
                )
                if kind is HeadcountValueKind.exact and best_exact is None:
                    best_exact = record
                elif best_bucket is None:
                    best_bucket = record

        # Keep walking nested structures (schema graphs often nest).
        for value in obj.values():
            if isinstance(value, (dict, list)):
                _walk(value)

    for doc in _iter_jsonld_objects(html):
        _walk(doc)
        if best_exact is not None:
            return best_exact

    return best_exact or best_bucket


# ---------------------------------------------------------------------------
# First-party company-website scraping
# ---------------------------------------------------------------------------

_HTML_TAG_RE = re.compile(r"(?is)<script.*?</script>|<style.*?</style>|<[^>]+>")
_WS_RE = re.compile(r"\s+")

_COMPANY_EXACT_RE = re.compile(
    r"\b(?P<n>\d{1,3}(?:,\d{3})+|\d{2,7})\s+"
    r"(?:employees|people|team\s+members|staff)\b",
    flags=re.IGNORECASE,
)
_COMPANY_QUALIFIED_RE = re.compile(
    r"\b(?P<qual>over|more\s+than|approximately|approx\.?|about|around|nearly)\s+"
    r"(?P<n>\d{1,3}(?:,\d{3})+|\d{2,7})\s+"
    r"(?:employees|people|team\s+members|staff)\b",
    flags=re.IGNORECASE,
)
_COMPANY_TEAM_OF_RE = re.compile(r"\bteam\s+of\s+(?P<n>\d{2,6})\b", flags=re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class CompanyWebMatch:
    """One headcount mention parsed from a first-party company page."""

    value_min: float
    value_point: float
    value_max: float
    kind: HeadcountValueKind
    phrase: str
    qualifier: str | None


def clean_html_to_text(html: str) -> str:
    stripped = _HTML_TAG_RE.sub(" ", html)
    return _WS_RE.sub(" ", stripped).strip()


def parse_company_web_text(text: str) -> list[CompanyWebMatch]:
    """Return every headcount mention found in ``text``.

    Order is meaningful: qualified mentions first (so dedup can drop
    the bare exact match when it's the tail of a qualified phrase),
    then exact, then the "team of N" shorthand.
    """
    matches: list[CompanyWebMatch] = []

    def _to_float(raw: str) -> float:
        return float(raw.replace(",", ""))

    for m in _COMPANY_QUALIFIED_RE.finditer(text):
        n = _to_float(m.group("n"))
        qual = m.group("qual").lower()
        if qual in {"over", "more than"}:
            matches.append(
                CompanyWebMatch(
                    value_min=n,
                    value_point=n * 1.1,
                    value_max=n * 1.25,
                    kind=HeadcountValueKind.range,
                    phrase=m.group(0),
                    qualifier=qual,
                )
            )
        else:
            matches.append(
                CompanyWebMatch(
                    value_min=n * 0.9,
                    value_point=n,
                    value_max=n * 1.1,
                    kind=HeadcountValueKind.range,
                    phrase=m.group(0),
                    qualifier=qual,
                )
            )

    for m in _COMPANY_EXACT_RE.finditer(text):
        phrase = m.group(0)
        # Skip when this exact phrase is already the tail of a qualified
        # one (e.g. "over 500 employees" already captured the "500
        # employees" span).
        if any(existing.phrase.lower().endswith(phrase.lower()) for existing in matches):
            continue
        n = _to_float(m.group("n"))
        matches.append(
            CompanyWebMatch(
                value_min=n,
                value_point=n,
                value_max=n,
                kind=HeadcountValueKind.exact,
                phrase=phrase,
                qualifier=None,
            )
        )

    for m in _COMPANY_TEAM_OF_RE.finditer(text):
        n = _to_float(m.group("n"))
        matches.append(
            CompanyWebMatch(
                value_min=n * 0.9,
                value_point=n,
                value_max=n * 1.15,
                kind=HeadcountValueKind.range,
                phrase=m.group(0),
                qualifier="team of",
            )
        )

    return matches


# ---------------------------------------------------------------------------
# SEC EDGAR company facts
# ---------------------------------------------------------------------------

_SEC_EMPLOYEE_CONCEPTS: frozenset[str] = frozenset(
    {
        "EntityCommonStockSharesOutstanding",
        "EmployeeNumberOfEmployees",
        "NumberOfEmployees",
        "EntityNumberOfEmployees",
        "EmployeeEquivalentsFullTimeAndPartTimeTotalNumberOfEmployees",
    }
)


@dataclass(frozen=True, slots=True)
class SecEmployeeRow:
    """One employee-count fact row parsed out of XBRL company facts."""

    concept: str
    end: date
    value: float
    fy: int | None
    fp: str | None
    filed: str | None


def parse_sec_company_facts(payload: str | dict[str, Any]) -> list[SecEmployeeRow]:
    """Parse the JSON payload from ``/api/xbrl/companyfacts/CIK*.json``.

    Returns one row per distinct employee-count fact report, sorted by
    ``end`` descending then ``filed`` descending so callers can take the
    top N without re-sorting.
    """
    if isinstance(payload, str):
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            return []
    else:
        data = payload
    facts_bucket = data.get("facts", {}) if isinstance(data, dict) else {}
    rows: list[SecEmployeeRow] = []
    for taxonomy, concepts in facts_bucket.items():
        if not isinstance(concepts, dict):
            continue
        for concept, details in concepts.items():
            if concept not in _SEC_EMPLOYEE_CONCEPTS:
                continue
            qualified = f"{taxonomy}:{concept}"
            units = details.get("units", {}) if isinstance(details, dict) else {}
            for unit_rows in units.values():
                if not isinstance(unit_rows, list):
                    continue
                for entry in unit_rows:
                    if not isinstance(entry, dict):
                        continue
                    try:
                        end = datetime.fromisoformat(entry["end"]).date()
                    except (KeyError, ValueError, TypeError):
                        continue
                    if "val" not in entry:
                        continue
                    try:
                        value = float(entry["val"])
                    except (TypeError, ValueError):
                        continue
                    rows.append(
                        SecEmployeeRow(
                            concept=qualified,
                            end=end,
                            value=value,
                            fy=entry.get("fy"),
                            fp=entry.get("fp"),
                            filed=entry.get("filed"),
                        )
                    )
    rows.sort(key=lambda r: (r.end, r.filed or ""), reverse=True)
    return rows


# ---------------------------------------------------------------------------
# Wikidata SPARQL rows
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class WikidataAnchor:
    """One P1128 employee-count statement flattened from a SPARQL row."""

    qid: str
    label: str
    employees: float
    asof: str | None
    anchor_month: date
    match_reason: str
    is_historical: bool


def parse_wikidata_row(row: dict[str, Any], *, reason: str) -> WikidataAnchor | None:
    """Flatten one ``results.bindings`` row from a SPARQL query, else None."""
    try:
        employees_raw = row["employees"]["value"]
        employees = float(employees_raw)
    except (KeyError, ValueError, TypeError):
        return None
    if employees <= 0:
        return None
    qid = row.get("company", {}).get("value", "")
    label = row.get("companyLabel", {}).get("value", "")
    asof_raw = row.get("asof", {}).get("value") if "asof" in row else None
    anchor_month = _parse_asof(asof_raw)
    return WikidataAnchor(
        qid=qid,
        label=label,
        employees=employees,
        asof=asof_raw,
        anchor_month=anchor_month,
        match_reason=reason,
        is_historical=bool(asof_raw),
    )


def _parse_asof(value: str | None) -> date:
    if not value:
        return month_floor(date.today())
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return month_floor(parsed.date())
    except ValueError:
        return month_floor(date.today())
