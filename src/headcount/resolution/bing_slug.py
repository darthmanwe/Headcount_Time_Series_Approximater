"""Bing SERP fallback for LinkedIn company-slug discovery (lever L6).

When the heuristic slug candidates (``domain_label``, ``name_slug``,
``name_concat``) all fail - either with 404, gate, or title mismatch -
we still want a chance at finding the real slug. Bing's web search
indexes ``linkedin.com/company/<slug>/`` URLs and returns them in plain
HTML, on a different host with a much friendlier bot policy than
LinkedIn itself.

This module is small and intentionally narrow:

- One outbound request per company at most: a single
  ``site:linkedin.com/company "<name>"`` query against
  ``www.bing.com/search``.
- HTML-only parse via regex; no JS execution, no third-party deps.
- Returns up to ``max_candidates`` distinct slugs in result-order so
  the resolver can probe them through the same verification path
  (with the same disambiguation guards) that the heuristic candidates
  go through.
- All requests flow through the shared :class:`HttpClient`, so the
  cache + concurrency policy of ``SourceName.company_web`` applies.
  Bing is rate-tolerant for low volume, but treating it like any
  other web fetch keeps the budget honest.

The output is *candidate slugs*. Verification, breaker accounting and
disambiguation all stay in :mod:`headcount.resolution.linkedin_resolver`
so Bing-sourced slugs can never bypass the same checks heuristic
candidates pay.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from urllib.parse import quote_plus, unquote, urlparse

from headcount.db.enums import SourceName
from headcount.ingest.http import HttpClient
from headcount.utils.logging import get_logger

_log = get_logger("headcount.resolution.bing_slug")

BING_SEARCH_URL = "https://www.bing.com/search?q={query}&form=QBLH"

# Permissive enough to catch /company/<slug> links inside both Bing's
# canonical anchor href and the bing-redirect "u" parameter. The slug
# pattern matches LinkedIn's documented charset (alphanum + hyphen +
# percent-encoded unicode).
_BING_LINKEDIN_HREF_RE = re.compile(
    r"https?://(?:www\.)?linkedin\.com/company/(?P<slug>[A-Za-z0-9%\-_]+)/?",
    flags=re.IGNORECASE,
)

# Bing wraps result clicks in a redirect of the form
# ``https://www.bing.com/ck/a?...&u=<base64>...`` where the actual
# target URL is base64-encoded. We decode the simpler "u=<urlencoded>"
# variant first (still in use on the desktop SERP) and only fall back
# to scanning raw substrings if no decoded match is found.
_BING_REDIRECT_RE = re.compile(
    r"href=\"(?P<href>https?://www\.bing\.com/ck/a\?[^\"]+)\"",
    flags=re.IGNORECASE,
)
_REDIRECT_U_PARAM_RE = re.compile(r"[?&]u=(?P<u>[^&\"]+)")

# Slugs Bing surfaces but that are never useful for our purposes
# (LinkedIn taxonomy, marketing, ...). Filtering these reduces wasted
# verification probes.
_SLUG_DENYLIST: frozenset[str] = frozenset(
    {
        "linkedin",
        "linkedin-corporation",
        "company",
        "showcase",
    }
)


def _build_query(name: str, domain: str | None) -> str:
    """Bias Bing toward LinkedIn company pages for ``name``."""

    parts = [f'"{name.strip()}"']
    if domain:
        parts.append(domain.strip())
    parts.append("site:linkedin.com/company")
    return " ".join(parts)


def _decode_redirect(href: str) -> str | None:
    """Pull the unwrapped target URL from a Bing redirect anchor.

    The ``u`` query parameter on bing.com/ck/a is the original click
    target, urlencoded. We only handle the urlencoded variant; the
    base64 variant on mobile Bing is not worth the parser complexity
    because the same SERP usually ships the urlencoded form too.
    """

    match = _REDIRECT_U_PARAM_RE.search(href)
    if match is None:
        return None
    raw = match.group("u")
    try:
        decoded = unquote(raw)
    except Exception:
        return None
    return decoded


def extract_linkedin_slugs(body: str, *, max_candidates: int = 5) -> list[str]:
    """Pull deduped LinkedIn ``/company/<slug>`` slugs from a Bing SERP body.

    Searches both direct ``linkedin.com/company/<slug>`` references and
    Bing's wrapped click redirects. Returns slugs in document order,
    truncated to ``max_candidates``.
    """

    seen: set[str] = set()
    out: list[str] = []

    def _push(slug: str) -> None:
        slug = slug.strip("/")
        if not slug:
            return
        slug_lc = slug.lower()
        if slug_lc in _SLUG_DENYLIST or slug_lc in seen:
            return
        seen.add(slug_lc)
        out.append(slug)

    # Direct hits first - cleaner and require no decode.
    for m in _BING_LINKEDIN_HREF_RE.finditer(body):
        _push(m.group("slug"))
        if len(out) >= max_candidates:
            return out

    # Then walk redirect anchors and decode their ``u`` param.
    for redirect in _BING_REDIRECT_RE.finditer(body):
        target = _decode_redirect(redirect.group("href"))
        if target is None:
            continue
        # The decoded target should itself contain /company/<slug>.
        m = _BING_LINKEDIN_HREF_RE.search(target)
        if m is None:
            continue
        _push(m.group("slug"))
        if len(out) >= max_candidates:
            return out

    return out


async def fetch_bing_slug_candidates(
    *,
    name: str,
    domain: str | None,
    http: HttpClient,
    max_candidates: int = 5,
) -> list[str]:
    """Issue one Bing query for ``name`` and return candidate slugs.

    Network + cache + rate limits are inherited from ``HttpClient``'s
    config for ``SourceName.company_web`` - Bing is friendly enough to
    tolerate this and treating it as a generic web fetch keeps us off
    a separate budget.
    """

    if not name or not name.strip():
        return []
    query = _build_query(name, domain)
    url = BING_SEARCH_URL.format(query=quote_plus(query))
    try:
        response = await http.get(SourceName.company_web, url)
    except Exception as exc:  # pragma: no cover - defensive
        _log.warning(
            "bing_slug_fetch_failed", name=name, domain=domain, error=repr(exc)
        )
        return []

    if response.status_code != 200:
        _log.info(
            "bing_slug_non_200",
            name=name,
            domain=domain,
            status=response.status_code,
        )
        return []

    candidates = extract_linkedin_slugs(
        response.text or "", max_candidates=max_candidates
    )
    if not candidates:
        # Bing redirect anchors sometimes hide the target inside a
        # different attribute; one last sweep across the raw body
        # picks those up at the cost of being slightly noisier.
        candidates = _scan_raw_targets(response.text or "", max_candidates)
    _log.info(
        "bing_slug_candidates",
        name=name,
        domain=domain,
        count=len(candidates),
    )
    return candidates


def _scan_raw_targets(body: str, max_candidates: int) -> list[str]:
    """Last-resort scan for slugs anywhere in the SERP body."""

    seen: set[str] = set()
    out: list[str] = []
    for m in _BING_LINKEDIN_HREF_RE.finditer(body):
        slug = m.group("slug").strip("/")
        slug_lc = slug.lower()
        if not slug or slug_lc in _SLUG_DENYLIST or slug_lc in seen:
            continue
        seen.add(slug_lc)
        out.append(slug)
        if len(out) >= max_candidates:
            break
    return out


def slugs_from_bing_iter(body: str) -> Iterable[str]:
    """Iterator alias for :func:`extract_linkedin_slugs` (test ergonomics)."""

    return iter(extract_linkedin_slugs(body))


def host_is_linkedin(url: str) -> bool:
    """True if ``url`` resolves to ``linkedin.com`` / ``www.linkedin.com``."""

    try:
        host = urlparse(url).hostname or ""
    except Exception:
        return False
    return host.lower() in {"linkedin.com", "www.linkedin.com"}
