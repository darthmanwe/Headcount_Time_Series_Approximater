"""Logged-out public LinkedIn observer.

Hard constraints (enforced in code, not conventions)
----------------------------------------------------

1. **Logged-out only.** No cookies, no session, no credentials of any
   kind are ever attached to outbound requests. The HTTP layer is a
   plain :class:`httpx.AsyncClient` whose only configured header is our
   advertised User-Agent.
2. **Fail-closed.** Any indication we've been gated (login wall, 403,
   429, CAPTCHA markup, 401/407, or an unexpected redirect away from
   ``/company/<slug>``) on the *primary* URL raises
   :class:`AdapterGatedError`. The orchestrator converts that into a
   ``linkedin_gated`` review item and, when LinkedIn was the only
   source that produced nothing, a :class:`CompanyRunStageStatus.gated`
   stage row. The run continues with other companies / sources - the
   LinkedIn observer is never load-bearing.
3. **Soft-gate the /people/ path.** LinkedIn's ``/company/<slug>/people/``
   almost always walls logged-out traffic; we still try it once (so we
   can pick up an exact headcount on the rare occasions it renders),
   but a gate there does *not* fail the adapter if the company page
   already produced a badge. We only log + increment a metric.
4. **No slug guessing.** If the seeded company has no LinkedIn URL we
   return ``[]`` rather than invent a slug from the company name -
   that's how you scrape an unrelated org.
5. **Bounded budget.** Maximum RPM and per-run request caps are sourced
   from settings (``LINKEDIN_PUBLIC_MAX_RPM``,
   ``LINKEDIN_PUBLIC_MAX_REQUESTS_PER_RUN``). The caller (typically the
   orchestrator) enforces them via :class:`TokenBucket` +
   :class:`SourceBudgetStore`; the adapter never creates its own.

What we extract
---------------
- ``/company/<slug>/`` and, as a fallback when the primary page has no
  badge, ``/company/<slug>/about/``. We look for the visible headcount
  badge ``Company size: 51-200 employees`` (range) or
  ``Company size: 10,001+ employees`` (open-ended). Parsed as an
  interval-valued anchor with :class:`HeadcountValueKind.bucket`.
- ``/company/<slug>/people/``: best-effort exact count such as
  ``1,250 employees`` or ``1,250 associated members``. Parsed as an
  interval-valued anchor with :class:`HeadcountValueKind.exact` (min =
  max = point). Only emitted when that page renders cleanly.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any
from urllib.parse import urlparse

from headcount.db.enums import (
    AnchorType,
    HeadcountValueKind,
    ParseStatus,
    SourceEntityType,
    SourceName,
)
from headcount.ingest.base import (
    AdapterFetchError,
    AdapterGatedError,
    AnchorSourceAdapter,
    CompanyTarget,
    FetchContext,
    RawAnchorSignal,
)
from headcount.resolution.normalize import normalize_linkedin_slug
from headcount.utils.logging import get_logger
from headcount.utils.metrics import linkedin_gate_total
from headcount.utils.time import month_floor

_log = get_logger("headcount.ingest.observers.linkedin_public")

COMPANY_URL = "https://www.linkedin.com/company/{slug}/"
COMPANY_ABOUT_URL = "https://www.linkedin.com/company/{slug}/about/"
COMPANY_PEOPLE_URL = "https://www.linkedin.com/company/{slug}/people/"

_GATE_MARKERS: tuple[str, ...] = (
    "authwall",
    "sign in to see",
    "join linkedin to see",
    "session_redirect",
    "captcha",
    "unusual activity",
    "please verify you are a human",
)
_LOGIN_PATH_PREFIXES: tuple[str, ...] = ("/login", "/checkpoint", "/authwall", "/uas/login")

# Canonical LinkedIn company-size buckets as of v1. If the badge text
# matches a (low, high) pair exactly we surface the canonical label for
# analyst readability; any other pair is stored verbatim.
_BUCKET_SIZES: tuple[tuple[int, int, str], ...] = (
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

# Range form: "Company size: 51-200 employees" (also accepts "to" and the
# unicode en dash U+2013). We require "employees" as a terminator so we
# don't match arbitrary number ranges on the page.
_EMPLOYEES_RANGE_RE = re.compile(
    r"(?i)(?:Company\s+size|Employees?)"
    r"[^0-9]{0,30}"
    r"(?P<low>\d{1,3}(?:,\d{3})*)"
    r"\s*(?:-|to|\u2013)\s*"
    r"(?P<high>\d{1,3}(?:,\d{3})*)"
    r"\s*(?:\+\s*)?employees"
)
# Open-ended form: "10,001+ employees".
_EMPLOYEES_OPEN_RE = re.compile(
    r"(?i)(?:Company\s+size|Employees?)"
    r"[^0-9]{0,30}"
    r"(?P<low>\d{1,3}(?:,\d{3})*)\+\s*employees"
)
# People-page exact form: "1,250 employees" or "1,250 associated members".
_EMPLOYEES_EXACT_RE = re.compile(
    r"(?i)\b(?P<n>\d{1,3}(?:,\d{3})+|\d{2,6})"
    r"\s*(?:employees?|associated\s+members?|members?)\b"
)


@dataclass(slots=True)
class _BadgeMatch:
    low: int
    high: int
    open_ended: bool
    phrase: str


def _extract_badge(text: str) -> _BadgeMatch | None:
    """Return the first plausible company-size badge in ``text``."""
    for m in _EMPLOYEES_RANGE_RE.finditer(text):
        low = int(m.group("low").replace(",", ""))
        high = int(m.group("high").replace(",", ""))
        if high <= low:
            continue
        return _BadgeMatch(low=low, high=high, open_ended=False, phrase=m.group(0))
    for m in _EMPLOYEES_OPEN_RE.finditer(text):
        low = int(m.group("low").replace(",", ""))
        # Open-ended top is unknown. We cap it at low*5 as a worst-case
        # bound so the interval is still well-defined - Phase 7 treats
        # wide bands as low confidence anyway.
        return _BadgeMatch(low=low, high=max(low * 5, low + 1), open_ended=True, phrase=m.group(0))
    return None


def _extract_exact_count(text: str) -> tuple[int, str] | None:
    """Return ``(count, phrase)`` for an exact count visible on /people/."""
    for m in _EMPLOYEES_EXACT_RE.finditer(text):
        raw = m.group("n").replace(",", "")
        try:
            value = int(raw)
        except ValueError:  # pragma: no cover - regex enforces digits
            continue
        if 2 <= value <= 10_000_000:
            return value, m.group(0)
    return None


def _looks_gated(status_code: int, text: str, final_url: str) -> str | None:
    """Return a structured gate reason if the response looks walled, else None."""
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
    for marker in _GATE_MARKERS:
        if marker in lowered:
            return f"marker:{marker.replace(' ', '_')}"
    return None


def _resolve_slug(target: CompanyTarget) -> str | None:
    """Return the LinkedIn slug for ``target`` or ``None`` if unknown."""
    return normalize_linkedin_slug(target.linkedin_company_url)


def _nearest_bucket_label(low: int, high: int) -> str:
    for b_low, b_high, label in _BUCKET_SIZES:
        if b_low == low and b_high == high:
            return label
    return f"{low}-{high}"


class LinkedInPublicObserver(AnchorSourceAdapter):
    """Logged-out ``/company/<slug>`` headcount-badge observer."""

    source_name = SourceName.linkedin_public
    parser_version = "linkedin-public-v1"

    def __init__(self, *, anchor_month: date | None = None) -> None:
        super().__init__()
        self._anchor_month = anchor_month

    async def fetch_current_anchor(
        self,
        target: CompanyTarget,
        *,
        context: FetchContext,
    ) -> list[RawAnchorSignal]:
        slug = _resolve_slug(target)
        if not slug:
            return []

        anchor_month = self._anchor_month or month_floor(date.today())
        signals: list[RawAnchorSignal] = []

        primary_url = COMPANY_URL.format(slug=slug)
        primary = await self._fetch(context, primary_url, purpose="company")
        if primary is None:
            # Soft failure (e.g. 404): nothing to do, no error.
            return []
        primary_body = primary.text or ""
        badge = _extract_badge(primary_body)

        if badge is None:
            about_url = COMPANY_ABOUT_URL.format(slug=slug)
            about = await self._fetch(context, about_url, purpose="about")
            if about is None:
                return []
            badge = _extract_badge(about.text or "")
            if badge is None:
                return []
            badge_source_url = about_url
        else:
            badge_source_url = primary_url

        signals.append(self._badge_signal(slug, badge, badge_source_url, anchor_month))

        # Best-effort /people/ probe. Gates here are soft - they do not
        # invalidate the badge signal we already captured. Any other
        # error is swallowed for the same reason.
        try:
            people_url = COMPANY_PEOPLE_URL.format(slug=slug)
            people = await self._fetch(context, people_url, purpose="people", soft_gate=True)
        except AdapterFetchError:
            people = None
        if people is not None:
            extra = _extract_exact_count(people.text or "")
            if extra is not None:
                count, phrase = extra
                signals.append(self._exact_signal(slug, count, phrase, people_url, anchor_month))

        return signals

    async def _fetch(
        self,
        context: FetchContext,
        url: str,
        *,
        purpose: str,
        soft_gate: bool = False,
    ) -> Any:
        """Fetch ``url`` and enforce gate detection.

        ``soft_gate=True`` means a gate hit is logged + metered but
        returns ``None`` instead of raising - used for the /people/
        probe so a wall there does not discard the main badge signal.
        """
        try:
            response = await context.http.get(self.source_name, url)
        except Exception as exc:  # pragma: no cover - network guard
            raise AdapterFetchError(
                f"linkedin_public {purpose} fetch failed for {url}: {exc!r}"
            ) from exc

        body = response.text or ""
        final_url = response.url or url
        gate_reason = _looks_gated(response.status_code, body, final_url)
        if gate_reason is not None:
            linkedin_gate_total.labels(reason=gate_reason).inc()
            _log.warning(
                "linkedin_gated",
                purpose=purpose,
                url=url,
                reason=gate_reason,
                status=response.status_code,
                soft=soft_gate,
            )
            if soft_gate:
                return None
            raise AdapterGatedError(f"{gate_reason} on {url}")

        if response.status_code == 404:
            return None
        if response.status_code >= 400:
            # Non-gate 4xx/5xx: soft-gate requests swallow these too.
            if soft_gate:
                return None
            raise AdapterFetchError(f"linkedin_public HTTP {response.status_code} for {url}")
        return response

    def _badge_signal(
        self,
        slug: str,
        badge: _BadgeMatch,
        source_url: str,
        anchor_month: date,
    ) -> RawAnchorSignal:
        bucket_label = _nearest_bucket_label(badge.low, badge.high)
        point = (badge.low + badge.high) / 2.0
        note_parts = [f"slug={slug}", f"bucket={bucket_label}"]
        if badge.open_ended:
            note_parts.append("open_ended")
        return RawAnchorSignal(
            source_name=self.source_name,
            entity_type=SourceEntityType.company,
            source_url=source_url,
            anchor_month=anchor_month,
            anchor_type=AnchorType.current_headcount_anchor,
            headcount_value_min=float(badge.low),
            headcount_value_point=point,
            headcount_value_max=float(badge.high),
            headcount_value_kind=HeadcountValueKind.bucket,
            confidence=0.45,
            raw_text=badge.phrase,
            parser_version=self.parser_version,
            parse_status=ParseStatus.ok,
            note=" ".join(note_parts),
            normalized_payload={
                "slug": slug,
                "kind": "badge",
                "bucket_low": badge.low,
                "bucket_high": badge.high,
                "open_ended": badge.open_ended,
                "phrase": badge.phrase,
            },
        )

    def _exact_signal(
        self,
        slug: str,
        count: int,
        phrase: str,
        source_url: str,
        anchor_month: date,
    ) -> RawAnchorSignal:
        # Public-profile counts logged-out are noisy - we mark them
        # exact but give them a low confidence so Phase 7 ranks them
        # below SEC / Wikidata numerics.
        return RawAnchorSignal(
            source_name=self.source_name,
            entity_type=SourceEntityType.company,
            source_url=source_url,
            anchor_month=anchor_month,
            anchor_type=AnchorType.current_headcount_anchor,
            headcount_value_min=float(count),
            headcount_value_point=float(count),
            headcount_value_max=float(count),
            headcount_value_kind=HeadcountValueKind.exact,
            confidence=0.35,
            raw_text=phrase,
            parser_version=self.parser_version,
            parse_status=ParseStatus.ok,
            note=f"slug={slug} people_page_exact",
            normalized_payload={
                "slug": slug,
                "kind": "people_exact",
                "count": count,
                "phrase": phrase,
            },
        )
