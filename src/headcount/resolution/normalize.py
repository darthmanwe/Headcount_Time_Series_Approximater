"""Deterministic text normalization helpers for canonical resolution.

Keep all behavior pure, Unicode-aware, and side-effect-free so the resolver
can be re-run idempotently. Normalization is intentionally conservative: we
prefer false negatives (create a new canonical row) over false positives
(merge unrelated companies), because the review queue can cheaply fix the
former but correcting a bad merge is expensive.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from urllib.parse import urlparse

_LEGAL_SUFFIXES: tuple[str, ...] = (
    "incorporated",
    "corporation",
    "limited liability company",
    "limited",
    "holdings",
    "holding",
    "company",
    "corp.",
    "corp",
    "inc.",
    "inc",
    "llc",
    "ltd.",
    "ltd",
    "plc",
    "gmbh",
    "ag",
    "sarl",
    "sa",
    "bv",
    "nv",
    "oy",
    "ab",
    "pty",
    "kk",
    "co.",
    "co",
    "lp",
    "llp",
)

_LEGAL_SUFFIX_RE = re.compile(
    r"(?:(?:,\s*|\s+)(?:" + "|".join(re.escape(s) for s in _LEGAL_SUFFIXES) + r"))+\s*$",
    flags=re.IGNORECASE,
)

_PUNCT_RE = re.compile(r"[^a-z0-9]+")
_LINKEDIN_SLUG_RE = re.compile(r"linkedin\.com/company/([^/?#]+)", flags=re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class NormalizedCompany:
    """Normalized handles for matching a company across sources."""

    display_name: str
    name_key: str
    domain: str | None
    domain_key: str | None
    linkedin_slug: str | None
    legal_suffix: str | None


def strip_diacritics(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def clean_display_name(raw: str) -> str:
    stripped = raw.strip()
    return re.sub(r"\s+", " ", stripped)


def extract_legal_suffix(raw: str) -> str | None:
    match = _LEGAL_SUFFIX_RE.search(raw.strip())
    if match is None:
        return None
    return match.group(0).strip(" ,").strip()


def strip_legal_suffix(raw: str) -> str:
    return _LEGAL_SUFFIX_RE.sub("", raw.strip()).strip(" ,.")


def normalize_name_key(raw: str) -> str:
    """Deterministic lower-case, diacritic-free, punctuation-free match key."""
    if not raw:
        return ""
    folded = strip_diacritics(raw).lower()
    folded = strip_legal_suffix(folded)
    compact = _PUNCT_RE.sub("", folded)
    return compact


def normalize_domain(raw: str | None) -> str | None:
    if raw is None:
        return None
    candidate = str(raw).strip().lower()
    if not candidate:
        return None
    if "://" in candidate:
        parsed = urlparse(candidate)
        candidate = parsed.netloc or parsed.path
    candidate = candidate.split("/", 1)[0]
    candidate = candidate.removeprefix("www.")
    candidate = candidate.strip(".")
    return candidate or None


def normalize_domain_key(raw: str | None) -> str | None:
    return normalize_domain(raw)


def normalize_linkedin_slug(raw: str | None) -> str | None:
    if raw is None:
        return None
    candidate = str(raw).strip()
    if not candidate:
        return None
    match = _LINKEDIN_SLUG_RE.search(candidate)
    if match is not None:
        return match.group(1).strip("/").lower() or None
    if "/" in candidate or "." in candidate:
        return None
    return candidate.lower() or None


def normalize_company(
    raw_name: str,
    *,
    raw_domain: str | None = None,
    raw_linkedin: str | None = None,
) -> NormalizedCompany:
    display = clean_display_name(raw_name)
    return NormalizedCompany(
        display_name=display,
        name_key=normalize_name_key(display),
        domain=normalize_domain(raw_domain),
        domain_key=normalize_domain_key(raw_domain),
        linkedin_slug=normalize_linkedin_slug(raw_linkedin),
        legal_suffix=extract_legal_suffix(display),
    )
