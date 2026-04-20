"""Logged-out public LinkedIn observer.

Thin fetch/orchestration layer. All parse logic - badge regex, gate
marker list, /people exact count, bucket labels - lives in
:mod:`headcount.parsers.anchors` so parser versions can be bumped and
replayed without touching this file.

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
   stage row.
3. **Soft-gate the /people/ path.** LinkedIn's ``/company/<slug>/people/``
   almost always walls logged-out traffic; we still try it once so we
   can pick up an exact headcount on the rare occasions it renders,
   but a gate there does *not* fail the adapter if the company page
   already produced a badge. We only log + increment a metric.
4. **No slug guessing.** If the seeded company has no LinkedIn URL we
   return ``[]`` rather than invent a slug from the company name.
5. **Bounded budget.** Maximum RPM and per-run request caps are sourced
   from settings; the caller enforces them via :class:`TokenBucket` +
   :class:`SourceBudgetStore`.
"""

from __future__ import annotations

from datetime import date
from typing import Any

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
from headcount.parsers.anchors import (
    LINKEDIN_PUBLIC_PARSER_VERSION,
    ParsedBadge,
    extract_linkedin_badge,
    extract_linkedin_exact_count,
    linkedin_bucket_label,
    looks_gated_linkedin,
)
from headcount.resolution.normalize import normalize_linkedin_slug
from headcount.utils.logging import get_logger
from headcount.utils.metrics import linkedin_gate_total
from headcount.utils.time import month_floor

_log = get_logger("headcount.ingest.observers.linkedin_public")

COMPANY_URL = "https://www.linkedin.com/company/{slug}/"
COMPANY_ABOUT_URL = "https://www.linkedin.com/company/{slug}/about/"
COMPANY_PEOPLE_URL = "https://www.linkedin.com/company/{slug}/people/"


def _resolve_slug(target: CompanyTarget) -> str | None:
    """Return the LinkedIn slug for ``target`` or ``None`` if unknown."""
    return normalize_linkedin_slug(target.linkedin_company_url)


class LinkedInPublicObserver(AnchorSourceAdapter):
    """Logged-out ``/company/<slug>`` headcount-badge observer."""

    source_name = SourceName.linkedin_public
    parser_version = LINKEDIN_PUBLIC_PARSER_VERSION

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
            return []
        primary_body = primary.text or ""
        badge = extract_linkedin_badge(primary_body)

        if badge is None:
            about_url = COMPANY_ABOUT_URL.format(slug=slug)
            about = await self._fetch(context, about_url, purpose="about")
            if about is None:
                return []
            badge = extract_linkedin_badge(about.text or "")
            if badge is None:
                return []
            badge_source_url = about_url
        else:
            badge_source_url = primary_url

        signals.append(self._badge_signal(slug, badge, badge_source_url, anchor_month))

        try:
            people_url = COMPANY_PEOPLE_URL.format(slug=slug)
            people = await self._fetch(context, people_url, purpose="people", soft_gate=True)
        except AdapterFetchError:
            people = None
        if people is not None:
            extra = extract_linkedin_exact_count(people.text or "")
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
        gate_reason = looks_gated_linkedin(response.status_code, body, final_url)
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
            if soft_gate:
                return None
            raise AdapterFetchError(f"linkedin_public HTTP {response.status_code} for {url}")
        return response

    def _badge_signal(
        self,
        slug: str,
        badge: ParsedBadge,
        source_url: str,
        anchor_month: date,
    ) -> RawAnchorSignal:
        bucket_label = linkedin_bucket_label(badge.low, badge.high)
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
