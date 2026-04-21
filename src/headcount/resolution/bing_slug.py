"""Search-engine SERP fallback for LinkedIn company-slug discovery
(lever L6).

When the heuristic slug candidates (``domain_label``, ``name_slug``,
``name_concat``) all fail - either with 404, gate, or title mismatch -
we still want a chance at finding the real slug. DuckDuckGo's HTML
endpoint and Bing's web search both index
``linkedin.com/company/<slug>/`` URLs and return them in plain HTML
on hosts with much friendlier bot policies than LinkedIn itself.

The module tries DuckDuckGo first (``html.duckduckgo.com``) because
it returns an unconditional HTML SERP without JS, captcha, or
throttling for low-volume low-rate usage, even from residential IPs
that Bing routinely captchas. Bing is kept as a secondary to squeeze
a few more slugs out of edge cases where DDG has a shallower index.

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
# html.duckduckgo.com is a JS-free HTML SERP that DDG explicitly ships
# for lightweight clients (originally for text-browser support). It's
# bot-tolerant in a way the main ``duckduckgo.com`` endpoint is not,
# and it returns real LinkedIn result URLs wrapped in DDG's redirect.
DUCKDUCKGO_HTML_URL = "https://html.duckduckgo.com/html/?q={query}"

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

# DuckDuckGo HTML wraps organic results as
# ``<a class="result__a" href="//duckduckgo.com/l/?uddg=<urlenc>&...`` -
# we extract the ``uddg`` param, URL-decode it, and run the same
# LinkedIn-slug regex against the decoded target.
_DDG_REDIRECT_RE = re.compile(
    r"href=\"(?P<href>//duckduckgo\.com/l/\?[^\"]+)\"", flags=re.IGNORECASE
)
_DDG_UDDG_PARAM_RE = re.compile(r"[?&]uddg=(?P<u>[^&\"]+)")

# Bing's CAPTCHA interstitial - detecting this saves us from returning
# a long list of slugs extracted from unrelated Bing UI chrome.
_BING_CAPTCHA_MARKERS = (
    "captcha",
    "recaptcha",
    "verify you are a human",
)

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
    """Bias a SERP toward LinkedIn company pages for ``name``."""

    parts = [f'"{name.strip()}"']
    if domain:
        parts.append(domain.strip())
    parts.append("site:linkedin.com/company")
    return " ".join(parts)


def _looks_like_bing_captcha(body: str) -> bool:
    """Return True if the Bing SERP body looks like an anti-bot interstitial.

    Bing returns a 200-status page with its captcha challenge when it
    flags the client as automated. The page still contains the word
    ``linkedin`` and ``company`` in navigation chrome, so a naive slug
    sweep pulls back noise; detecting the marker avoids that.
    """

    low = body.lower()
    for marker in _BING_CAPTCHA_MARKERS:
        if marker in low:
            return True
    return False


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
    """Pull deduped LinkedIn ``/company/<slug>`` slugs from a SERP body.

    Searches, in order:

    1. Direct ``linkedin.com/company/<slug>`` references in plain HTML.
    2. Bing's wrapped ``bing.com/ck/a`` redirects with urlencoded ``u``.
    3. DuckDuckGo's ``duckduckgo.com/l/?uddg=`` redirects.

    Returns slugs in document order, truncated to ``max_candidates``.
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

    # Then walk redirect anchors and decode their ``u`` param (Bing).
    for redirect in _BING_REDIRECT_RE.finditer(body):
        target = _decode_redirect(redirect.group("href"))
        if target is None:
            continue
        m = _BING_LINKEDIN_HREF_RE.search(target)
        if m is None:
            continue
        _push(m.group("slug"))
        if len(out) >= max_candidates:
            return out

    # Finally walk DDG redirect anchors and decode their ``uddg`` param.
    for redirect in _DDG_REDIRECT_RE.finditer(body):
        target = _decode_ddg_redirect(redirect.group("href"))
        if target is None:
            continue
        m = _BING_LINKEDIN_HREF_RE.search(target)
        if m is None:
            continue
        _push(m.group("slug"))
        if len(out) >= max_candidates:
            return out

    return out


def _decode_ddg_redirect(href: str) -> str | None:
    """Pull the unwrapped target URL from a DDG result anchor."""

    match = _DDG_UDDG_PARAM_RE.search(href)
    if match is None:
        return None
    raw = match.group("u")
    try:
        return unquote(raw)
    except Exception:
        return None


async def _fetch_one_serp(
    *,
    engine: str,
    url: str,
    http: HttpClient,
    name: str,
    domain: str | None,
    max_candidates: int,
) -> list[str]:
    """Issue a single SERP request and return candidate slugs."""

    try:
        response = await http.get(SourceName.company_web, url)
    except Exception as exc:  # pragma: no cover - defensive
        _log.warning(
            "serp_slug_fetch_failed",
            engine=engine,
            name=name,
            domain=domain,
            error=repr(exc),
        )
        return []

    if response.status_code != 200:
        _log.info(
            "serp_slug_non_200",
            engine=engine,
            name=name,
            domain=domain,
            status=response.status_code,
        )
        return []

    body = response.text or ""
    # Bing sometimes 200s with a captcha challenge instead of results;
    # refuse to parse that because slug extraction on it is noise.
    if engine == "bing" and _looks_like_bing_captcha(body):
        _log.info(
            "serp_slug_bing_captcha",
            name=name,
            domain=domain,
        )
        return []

    candidates = extract_linkedin_slugs(body, max_candidates=max_candidates)
    if not candidates:
        candidates = _scan_raw_targets(body, max_candidates)
    if candidates:
        _log.info(
            "serp_slug_candidates",
            engine=engine,
            name=name,
            domain=domain,
            count=len(candidates),
        )
    return candidates


async def fetch_bing_slug_candidates(
    *,
    name: str,
    domain: str | None,
    http: HttpClient,
    max_candidates: int = 5,
) -> list[str]:
    """Issue SERP queries for ``name`` and return candidate LinkedIn slugs.

    Tries DuckDuckGo's HTML endpoint first because Bing frequently
    serves anti-bot captcha pages from residential IPs; falls back to
    Bing only when DDG returns no useful slugs. Network + cache +
    rate limits are inherited from ``HttpClient``'s config for
    ``SourceName.company_web``.

    The function name is kept for backwards compatibility; callers
    should read it as "search-engine slug candidates".
    """

    if not name or not name.strip():
        return []
    query = _build_query(name, domain)

    ddg_url = DUCKDUCKGO_HTML_URL.format(query=quote_plus(query))
    ddg_candidates = await _fetch_one_serp(
        engine="duckduckgo",
        url=ddg_url,
        http=http,
        name=name,
        domain=domain,
        max_candidates=max_candidates,
    )
    if ddg_candidates:
        return ddg_candidates

    bing_url = BING_SEARCH_URL.format(query=quote_plus(query))
    bing_candidates = await _fetch_one_serp(
        engine="bing",
        url=bing_url,
        http=http,
        name=name,
        domain=domain,
        max_candidates=max_candidates,
    )
    return bing_candidates


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
