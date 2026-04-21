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

Bot-defence integration
-----------------------
The resolver shares a :class:`LinkedInRateGuard` with the public
observer when the orchestrator passes one in. That gives a single
process one daily LinkedIn budget, one jitter clock, and one circuit
breaker across resolver + observer. When called without a guard (unit
tests, ad-hoc backfills) it constructs a private guard from settings.

Disambiguation
--------------
"Arable" (an agritech company) and "Arable Consulting" (an advisory
firm) both look like reasonable matches for the ``arable`` slug under
the original token-overlap test. The resolver layers extra checks on
top of :func:`title_matches_name` to reject these single-token
false-positives:

- If the target name is a *single* significant token and the title
  introduces a generic business qualifier ("consulting", "partners",
  "advisors", ...), the candidate is rejected.
- If a canonical domain is provided and it appears verbatim in the
  body, that is treated as a strong positive that overrides the
  qualifier veto - because the LinkedIn page itself is pointing at
  the same web property we're targeting.

Public API
----------
- ``slug_candidates(name, domain)`` - ordered slug candidates to try.
- ``resolve_linkedin_slug(company, *, http, rate_guard=None,
  company_id=None)`` - returns the first verified
  ``LinkedInSlugResult`` or ``None``.
- ``backfill_linkedin_slugs(session, *, company_ids, http,
  rate_guard=None)`` - iterate over companies missing
  ``linkedin_company_url`` and persist anything the resolver can
  verify.
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
from headcount.ingest.linkedin_guard import LinkedInRateGuard
from headcount.models.company import Company
from headcount.utils.logging import get_logger
from headcount.utils.metrics import linkedin_gate_total

_log = get_logger("headcount.resolution.linkedin_resolver")

COMPANY_URL_TEMPLATE = "https://www.linkedin.com/company/{slug}/"

# Generic business-type tokens that, when they appear in a LinkedIn
# title alongside a single-token target name, almost always indicate a
# different organisation that just happens to own the slug. The list is
# deliberately narrow (no "labs"/"studio" because plenty of real
# matches use those).
_AMBIGUOUS_QUALIFIERS: frozenset[str] = frozenset(
    {
        "consulting",
        "consultants",
        "consultancy",
        "advisors",
        "advisory",
        "partners",
        "associates",
        "holdings",
        "group",
        "ventures",
        "capital",
        "agency",
        "services",
        "solutions",
    }
)

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


def _domain_appears_in_body(body: str, domain: str | None) -> bool:
    """True if ``domain`` is mentioned anywhere in the response body.

    LinkedIn's public company page links the company website (often
    multiple times: nav, about copy, JSON-LD ``url`` field). A literal
    domain match is the strongest disambiguation signal we get without
    parsing JSON-LD here, and it costs almost nothing.
    """

    if not domain or not body:
        return False
    return domain.lower() in body.lower()


def disambiguate_match(
    *,
    title: str,
    name: str,
    domain: str | None,
    body: str,
) -> tuple[bool, str]:
    """Stricter verifier returning ``(accepted, reason)``.

    Layered on top of :func:`title_matches_name` to catch single-token
    false positives like ``arable`` -> "Arable Consulting". Always
    accepts when the canonical domain literally appears in the body,
    since at that point the LinkedIn page is unambiguously pointing at
    the same web property we're targeting.
    """

    if not title_matches_name(title, name):
        return False, "no_token_overlap"

    if _domain_appears_in_body(body, domain):
        return True, "domain_in_body"

    stripped = _strip_linkedin_suffix(title)
    title_tokens = _name_tokens(stripped)
    name_tokens = _name_tokens(name)
    sig_name = [t for t in name_tokens if len(t) >= 3] or name_tokens
    sig_title = [t for t in title_tokens if len(t) >= 3] or title_tokens

    # Tokens the title introduces that are NOT in the target name.
    extras = [t for t in sig_title if t not in set(sig_name)]
    if len(sig_name) == 1 and any(extra in _AMBIGUOUS_QUALIFIERS for extra in extras):
        offending = next(e for e in extras if e in _AMBIGUOUS_QUALIFIERS)
        return False, f"ambiguous_qualifier:{offending}"

    # Single-token names are the most collision-prone case on LinkedIn
    # (generic English words like "Alloy", "Alleva", "Arable" map to
    # many unrelated companies). If the caller provided a canonical
    # domain but the LinkedIn page does not mention it anywhere, refuse
    # the match. The resolver will fall through to Bing / DDG and try
    # to find the real slug. False rejections are preferable to false
    # positives here because misattributed headcounts end up as real
    # numeric errors in the scoreboard.
    if len(sig_name) == 1 and domain:
        return False, "single_token_no_domain_in_body"

    return True, "ok"


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
    http: HttpClient,
    *,
    slug: str,
    target_name: str,
    target_domain: str | None,
    method: str,
    guard: LinkedInRateGuard,
) -> tuple[LinkedInSlugResult | None, str]:
    url = COMPANY_URL_TEMPLATE.format(slug=slug)

    if guard.is_budget_exhausted():
        # Self-imposed cap. Don't burn the rest of the budget on slug
        # discovery; the observer is the higher-value caller.
        linkedin_gate_total.labels(reason="budget_exhausted").inc()
        return None, "budget_exhausted"

    await guard.before_request()
    try:
        response = await http.get(SourceName.linkedin_public, url)
    except Exception as exc:  # pragma: no cover - defensive
        return None, f"fetch_error:{exc!r}"

    guard.record_response(from_cache=bool(getattr(response, "from_cache", False)))

    verdict = _classify(response)
    if verdict != "ok":
        if verdict == "gated":
            # Status-level gate (999 / 429 / 403 / ...). Feed the breaker
            # so a streak across the slug-discovery phase doesn't burn
            # the observer's first attempts.
            tripped = guard.note_gate()
            if tripped:
                _log.error(
                    "linkedin_resolver_circuit_tripped",
                    threshold=guard.circuit_threshold,
                    streak=guard.consecutive_gates,
                )
        return None, verdict

    body = response.text or ""
    title = _extract_title(body)
    if title is None:
        return None, "no_title"

    accepted, reason = disambiguate_match(
        title=title, name=target_name, domain=target_domain, body=body
    )
    if not accepted:
        return None, f"verify_rejected:{reason}:{title!r}"

    # A verified hit is treated as a "success" for the breaker so the
    # streak resets cleanly when slug discovery starts working again.
    guard.note_success()

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
    rate_guard: LinkedInRateGuard | None = None,
    company_id: str | None = None,
) -> LinkedInSlugResult | None:
    """Return the first candidate slug whose LinkedIn page verifies, or None.

    ``rate_guard`` is optional: when omitted, the resolver builds a
    private guard from settings, preserving the historical ad-hoc
    callsite behaviour. Cohort runs always pass a shared guard so that
    one daily LinkedIn budget covers both slug discovery and the
    observer.

    ``company_id`` is used to park the company in ``rate_guard`` when
    the breaker is open, so the orchestrator can replay it after the
    cooldown elapses.
    """

    guard = rate_guard if rate_guard is not None else LinkedInRateGuard.from_settings()

    if guard.is_circuit_open():
        if company_id:
            guard.defer_company(company_id)
        linkedin_gate_total.labels(reason="circuit_open").inc()
        _log.warning(
            "linkedin_resolver_circuit_open_skip",
            name=name,
            domain=domain,
            company_id=company_id,
            cooldown_remaining=guard.cooldown_remaining(),
        )
        return None

    attempts: list[tuple[str, str, str]] = []
    seen_slugs: set[str] = set()

    async def _try_slug(slug: str, method: str) -> LinkedInSlugResult | None:
        if slug in seen_slugs:
            return None
        seen_slugs.add(slug)
        result, verdict = await _probe(
            http,
            slug=slug,
            target_name=name,
            target_domain=domain,
            method=method,
            guard=guard,
        )
        attempts.append((slug, method, verdict))
        return result

    heuristic_gated = False
    for slug, method in slug_candidates(name, domain):
        result = await _try_slug(slug, method)
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
        if guard.is_circuit_open():
            if company_id:
                guard.defer_company(company_id)
            _log.warning(
                "linkedin_resolver_circuit_tripped_mid_resolve",
                name=name,
                domain=domain,
                attempts=attempts,
            )
            return None
        # If the first heuristic probe for this company was gated, the
        # remaining candidates will almost certainly 999 too (same host,
        # same IP, seconds apart). Burn the whole slug budget here and
        # we get fewer successful probes per cohort. Fast-fail the
        # heuristic loop and let Bing/DDG try a fresh path; if that also
        # fails the caller will defer the company for the recovery pass.
        if attempts and attempts[-1][2] == "gated":
            heuristic_gated = True
            break

    # Lever L6: heuristic candidates exhausted. Ask Bing.
    # Bing is queried at most once per company, on a different host
    # with a friendlier policy. Discovered slugs go through the same
    # ``_probe`` path so they pay the same disambiguation + breaker
    # checks heuristic candidates do.
    if not guard.is_circuit_open():
        from headcount.resolution.bing_slug import fetch_bing_slug_candidates

        try:
            bing_slugs = await fetch_bing_slug_candidates(
                name=name, domain=domain, http=http
            )
        except Exception as exc:  # pragma: no cover - defensive
            bing_slugs = []
            attempts.append(("(bing)", "bing_serp", f"fetch_error:{exc!r}"))
        for slug in bing_slugs:
            result = await _try_slug(slug, "bing_serp")
            if result is not None:
                _log.info(
                    "linkedin_slug_resolved_via_bing",
                    name=name,
                    domain=domain,
                    slug=result.slug,
                    title=result.title,
                    attempts=attempts,
                )
                return result
            if guard.is_circuit_open():
                if company_id:
                    guard.defer_company(company_id)
                break

    # If any attempt was blocked by LinkedIn (status 999/429/403), park
    # the company on the guard's deferred queue so the breaker-recovery
    # pass can replay slug discovery after the cooldown. Without this,
    # the first N companies whose probes return 999 before the breaker
    # trips (threshold is N by construction) are permanently lost.
    any_gated = any(verdict == "gated" for _slug, _method, verdict in attempts)
    if any_gated and company_id:
        guard.defer_company(company_id)

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
    rate_guard: LinkedInRateGuard | None = None,
) -> dict[str, int]:
    """Populate ``Company.linkedin_company_url`` for companies missing it.

    Runs the async resolver synchronously (one company at a time) so it
    fits inside the existing sync resolver pipeline. The HttpClient
    enforces per-source concurrency and caching so re-running over the
    same workstation is essentially free until TTL rolls.

    Returns per-company counts: ``scanned``, ``resolved``, ``unresolved``,
    plus ``deferred`` for companies parked because the breaker was open.
    """

    import asyncio

    guard = rate_guard if rate_guard is not None else LinkedInRateGuard.from_settings()

    stats = {
        "scanned": 0,
        "resolved": 0,
        "unresolved": 0,
        "skipped": 0,
        "deferred": 0,
    }

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
                deferred_before = len(guard.deferred_companies)
                result = await resolve_linkedin_slug(
                    name=company.canonical_name,
                    domain=company.canonical_domain,
                    http=http,
                    rate_guard=guard,
                    company_id=company.id,
                )
                if result is None:
                    if len(guard.deferred_companies) > deferred_before:
                        stats["deferred"] += 1
                    else:
                        stats["unresolved"] += 1
                    continue
                company.linkedin_company_url = result.url
                stats["resolved"] += 1

    asyncio.run(_run())
    return stats
