"""LinkedIn company-slug resolver.

The candidate seed workbooks almost never carry a LinkedIn URL, and
without one ``LinkedInPublicObserver`` short-circuits silently (see
BUG-A in docs/HARMONIC_COHORT_LIVE_RUN.md). This module infers the
slug from a company's domain and canonical name by probing
``linkedin.com/company/<candidate>`` and verifying the page's ``<title>``
matches the target name.

The resolver is intentionally conservative: we only claim a slug if the
HTTP probe returns 200 and the page title fuzzily matches the target.
On auth-wall / 429 / 403 responses we treat the candidate as ``gated``
rather than wrongly accepting or rejecting it, so re-runs stay cheap.

Public API
----------
- ``slug_candidates(name, domain)`` - ordered slug candidates to try.
- ``resolve_linkedin_slug(company, *, http)`` - returns the first
  verified ``LinkedInSlugResult`` or ``None``.
- ``backfill_linkedin_slugs(session, *, company_ids, http)`` - iterate
  over companies missing ``linkedin_company_url`` and persist anything
  the resolver can verify.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from html import unescape

from sqlalchemy import select
from sqlalchemy.orm import Session

from headcount.db.enums import SourceName
from headcount.ingest.http import CachedResponse, HttpClient
from headcount.models.company import Company
from headcount.utils.logging import get_logger

_log = get_logger("headcount.resolution.linkedin_resolver")

COMPANY_URL_TEMPLATE = "https://www.linkedin.com/company/{slug}/"

_TITLE_RE = re.compile(
    r"<title[^>]*>(?P<title>.*?)</title>",
    flags=re.IGNORECASE | re.DOTALL,
)
_OG_TITLE_RE = re.compile(
    r"<meta[^>]+property=['\"]og:title['\"][^>]+content=['\"](?P<title>[^'\"]+)['\"]",
    flags=re.IGNORECASE,
)
_NON_ALNUM = re.compile(r"[^a-z0-9]+")

# LinkedIn uses 999 as an informal "bot/auth wall" status on logged-out
# requests to logged-in-only surfaces. Treat it as "cannot verify" rather
# than "definitely wrong".
_GATED_STATUSES = frozenset({401, 403, 429, 451, 999})
_NOT_FOUND_STATUSES = frozenset({404, 410})


@dataclass(frozen=True)
class LinkedInSlugResult:
    """A verified LinkedIn slug for a company."""

    slug: str
    url: str
    title: str
    method: str  # e.g. "domain_label", "name_slug", "name_concat"
    confidence: float


def _name_tokens(raw: str) -> list[str]:
    lowered = raw.lower()
    cleaned = _NON_ALNUM.sub(" ", lowered).strip()
    return [t for t in cleaned.split(" ") if t]


def _name_slug(raw: str) -> str | None:
    cleaned = _NON_ALNUM.sub("-", raw.lower()).strip("-")
    return cleaned or None


def _name_concat(raw: str) -> str | None:
    cleaned = _NON_ALNUM.sub("", raw.lower())
    return cleaned or None


def _domain_label(domain: str) -> str | None:
    label = domain.lower().split(".", 1)[0]
    label = _NON_ALNUM.sub("-", label).strip("-")
    return label or None


def slug_candidates(name: str, domain: str | None) -> list[tuple[str, str]]:
    """Return ordered ``(slug, method)`` candidates for ``name`` / ``domain``.

    Order roughly matches expected hit-rate: domain labels first (most
    companies align their LinkedIn slug with their primary domain), then
    name-derived variants.
    """

    out: list[tuple[str, str]] = []
    seen: set[str] = set()

    def _push(slug: str | None, method: str) -> None:
        if not slug or slug in seen:
            return
        out.append((slug, method))
        seen.add(slug)

    if domain:
        _push(_domain_label(domain), "domain_label")
    if name:
        _push(_name_slug(name), "name_slug")
        _push(_name_concat(name), "name_concat")
    return out


def _extract_title(body: str) -> str | None:
    """Pull the first human-facing title from an HTML document."""

    og = _OG_TITLE_RE.search(body)
    if og is not None:
        return unescape(og.group("title")).strip()
    tag = _TITLE_RE.search(body)
    if tag is not None:
        return unescape(tag.group("title")).strip()
    return None


def _strip_linkedin_suffix(title: str) -> str:
    """Drop the trailing ``" | LinkedIn"`` marketing suffix."""

    lowered = title.lower()
    for marker in (" | linkedin", " - linkedin"):
        idx = lowered.rfind(marker)
        if idx >= 0:
            return title[:idx]
    return title


def title_matches_name(title: str, name: str) -> bool:
    """Fuzzy token-overlap test between LinkedIn page title and target name.

    Requires at least one target token (>=3 chars) to appear in the
    stripped title, which is permissive enough to tolerate suffix noise
    like "Inc." / "Systems" / "Technologies" but tight enough to reject
    a mismatched company.
    """

    stripped = _strip_linkedin_suffix(title)
    title_tokens = set(_name_tokens(stripped))
    name_tokens = _name_tokens(name)
    if not title_tokens or not name_tokens:
        return False
    significant = [t for t in name_tokens if len(t) >= 3]
    if not significant:
        significant = name_tokens
    return any(tok in title_tokens for tok in significant)


def _classify(response: CachedResponse) -> str:
    status = response.status_code
    if status == 200:
        return "ok"
    if status in _NOT_FOUND_STATUSES:
        return "not_found"
    if status in _GATED_STATUSES:
        return "gated"
    return f"status_{status}"


async def _probe(
    http: HttpClient, *, slug: str, target_name: str, method: str
) -> tuple[LinkedInSlugResult | None, str]:
    url = COMPANY_URL_TEMPLATE.format(slug=slug)
    try:
        response = await http.get(SourceName.linkedin_public, url)
    except Exception as exc:  # pragma: no cover - defensive
        return None, f"fetch_error:{exc!r}"

    verdict = _classify(response)
    if verdict != "ok":
        return None, verdict

    body = response.text or ""
    title = _extract_title(body)
    if title is None:
        return None, "no_title"
    if not title_matches_name(title, target_name):
        return None, f"title_mismatch:{title!r}"

    confidence = {
        "domain_label": 0.85,
        "name_slug": 0.75,
        "name_concat": 0.70,
    }.get(method, 0.65)
    return (
        LinkedInSlugResult(
            slug=slug, url=url, title=title, method=method, confidence=confidence
        ),
        "ok",
    )


async def resolve_linkedin_slug(
    *,
    name: str,
    domain: str | None,
    http: HttpClient,
) -> LinkedInSlugResult | None:
    """Return the first candidate slug whose LinkedIn page verifies, or None."""

    attempts: list[tuple[str, str, str]] = []
    for slug, method in slug_candidates(name, domain):
        result, verdict = await _probe(
            http, slug=slug, target_name=name, method=method
        )
        attempts.append((slug, method, verdict))
        if result is not None:
            _log.info(
                "linkedin_slug_resolved",
                name=name,
                domain=domain,
                slug=result.slug,
                method=result.method,
                title=result.title,
                attempts=attempts,
            )
            return result

    _log.info(
        "linkedin_slug_unresolved",
        name=name,
        domain=domain,
        attempts=attempts,
    )
    return None


def backfill_linkedin_slugs(
    session: Session,
    *,
    company_ids: Iterable[str] | None,
    http: HttpClient,
) -> dict[str, int]:
    """Populate ``Company.linkedin_company_url`` for companies missing it.

    Runs the async resolver synchronously (one company at a time) so it
    fits inside the existing sync resolver pipeline. The HttpClient
    enforces per-source concurrency and caching so re-running over the
    same workstation is essentially free until TTL rolls.

    Returns per-company counts: ``scanned``, ``resolved``, ``unresolved``.
    """

    import asyncio

    stats = {"scanned": 0, "resolved": 0, "unresolved": 0, "skipped": 0}

    stmt = select(Company).where(Company.linkedin_company_url.is_(None))
    if company_ids is not None:
        ids = list(company_ids)
        if not ids:
            return stats
        stmt = stmt.where(Company.id.in_(ids))
    companies = list(session.execute(stmt).scalars())

    async def _run() -> None:
        async with http:
            for company in companies:
                stats["scanned"] += 1
                if not company.canonical_name and not company.canonical_domain:
                    stats["skipped"] += 1
                    continue
                result = await resolve_linkedin_slug(
                    name=company.canonical_name,
                    domain=company.canonical_domain,
                    http=http,
                )
                if result is None:
                    stats["unresolved"] += 1
                    continue
                company.linkedin_company_url = result.url
                stats["resolved"] += 1

    asyncio.run(_run())
    return stats
