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

import random
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
from headcount.ingest.linkedin_guard import LinkedInRateGuard
from headcount.parsers.anchors import (
    LINKEDIN_PUBLIC_PARSER_VERSION,
    LinkedInJsonLdEmployees,
    ParsedBadge,
    extract_linkedin_badge,
    extract_linkedin_exact_count,
    extract_linkedin_jsonld_employees,
    linkedin_bucket_label,
    looks_gated_linkedin_content,
    looks_gated_linkedin_status,
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
    """Logged-out ``/company/<slug>`` headcount-badge observer.

    Ships a lightweight circuit breaker (lever L4 in
    ``docs/LINKEDIN_BOT_WALL_STRATEGY.md``): after a configurable streak
    of gate responses on the primary URL we stop issuing LinkedIn
    requests for the rest of the run. The streak resets to zero each
    time a company yields any parsed signal, so a single transient
    soft-gate does not poison the observer for a whole batch.
    """

    source_name = SourceName.linkedin_public
    parser_version = LINKEDIN_PUBLIC_PARSER_VERSION

    def __init__(
        self,
        *,
        anchor_month: date | None = None,
        circuit_threshold: int | None = None,
        daily_request_budget: int | None = None,
        jitter_ms_range: tuple[int, int] | None = None,
        rng: random.Random | None = None,
        rate_guard: LinkedInRateGuard | None = None,
    ) -> None:
        super().__init__()
        self._anchor_month = anchor_month

        # When a shared guard is injected (cohort runs, multi-caller setups)
        # we use it as-is so the resolver and observer share one budget,
        # one streak and one cooldown clock. When omitted (unit tests,
        # one-off invocations) we build a private guard from settings,
        # honouring any per-call overrides for backwards compatibility.
        if rate_guard is not None:
            self._guard = rate_guard
            self._owns_guard = False
        else:
            self._guard = LinkedInRateGuard.from_settings(
                circuit_threshold=circuit_threshold,
                daily_request_budget=daily_request_budget,
                jitter_ms=jitter_ms_range,
                rng=rng,
            )
            self._owns_guard = True

    @property
    def rate_guard(self) -> LinkedInRateGuard:
        """The guard backing this observer (shared or private)."""

        return self._guard

    @property
    def consecutive_gates(self) -> int:
        """Number of successive gated primary fetches since last success."""

        return self._guard.consecutive_gates

    @property
    def circuit_open(self) -> bool:
        """True once the breaker is open and still inside its cooldown."""

        return self._guard.is_circuit_open()

    @property
    def requests_made(self) -> int:
        """Count of outbound HTTP requests issued through this guard."""

        return self._guard.requests_made

    def _budget_exhausted(self) -> bool:
        return self._guard.is_budget_exhausted()

    async def _apply_jitter(self) -> None:
        """Sleep a human-scale random amount before the next HTTP call."""

        await self._guard.before_request()

    async def fetch_current_anchor(
        self,
        target: CompanyTarget,
        *,
        context: FetchContext,
    ) -> list[RawAnchorSignal]:
        slug = _resolve_slug(target)
        if not slug:
            _log.info(
                "linkedin_public_skipped",
                reason="no_linkedin_slug",
                company_id=target.company_id,
                canonical_name=target.canonical_name,
                canonical_domain=target.canonical_domain,
            )
            return []

        if self._guard.is_circuit_open():
            # Lever L4: while the breaker is open we skip LinkedIn
            # entirely instead of sending requests that are statistically
            # certain to be 999-walled (the exact pattern LinkedIn uses
            # to extend the ban window). The company id is parked in the
            # guard's deferred queue so the orchestrator can replay it
            # after the cooldown window elapses.
            linkedin_gate_total.labels(reason="circuit_open").inc()
            self._guard.defer_company(target.company_id)
            _log.warning(
                "linkedin_public_circuit_open_skip",
                company_id=target.company_id,
                slug=slug,
                cooldown_remaining=self._guard.cooldown_remaining(),
            )
            return []

        if self._budget_exhausted():
            # Lever L3: daily request cap. Stop silently so the batch
            # completes cleanly on other sources; the breaker does not
            # need to flip because this is a self-imposed limit, not a
            # server-side signal.
            linkedin_gate_total.labels(reason="budget_exhausted").inc()
            _log.warning(
                "linkedin_public_budget_exhausted",
                company_id=target.company_id,
                slug=slug,
                budget=self._guard.daily_request_budget,
                requests_made=self._guard.requests_made,
            )
            return []

        anchor_month = self._anchor_month or month_floor(date.today())
        signals: list[RawAnchorSignal] = []

        primary_url = COMPANY_URL.format(slug=slug)
        try:
            primary = await self._fetch(context, primary_url, purpose="company")
        except AdapterGatedError:
            self._note_primary_gate()
            raise
        if primary is None:
            return []
        primary_body = primary.text or ""
        primary_final_url = primary.url or primary_url

        # Precedence (lever L2): prefer JSON-LD over the visible-text
        # badge. LinkedIn's logged-out company page interleaves the
        # auth-wall sales copy ("sign in to see") with a perfectly
        # parseable application/ld+json block carrying numberOfEmployees,
        # so we *must* attempt structured extraction before letting
        # content-level gate markers veto the response. /about is only
        # consulted when both extractors come back empty.
        jsonld = extract_linkedin_jsonld_employees(primary_body)
        badge: ParsedBadge | None = None
        signal_source_url = primary_url
        if jsonld is None:
            badge = extract_linkedin_badge(primary_body)
        if jsonld is None and badge is None:
            # Primary yielded nothing structured. Now (and only now) is
            # it safe to honour the content-level gate marker on the
            # primary body, because if we'd parsed something we would
            # have already returned.
            primary_gate = looks_gated_linkedin_content(primary_body, primary_final_url)
            if primary_gate is not None:
                linkedin_gate_total.labels(reason=primary_gate).inc()
                _log.warning(
                    "linkedin_gated",
                    purpose="company",
                    url=primary_url,
                    reason=primary_gate,
                    deferred=True,
                )
                self._note_primary_gate()
                raise AdapterGatedError(f"{primary_gate} on {primary_url}")

            about_url = COMPANY_ABOUT_URL.format(slug=slug)
            try:
                about = await self._fetch(context, about_url, purpose="about")
            except AdapterGatedError:
                self._note_primary_gate()
                raise
            if about is None:
                return []
            about_body = about.text or ""
            about_final_url = about.url or about_url
            jsonld = extract_linkedin_jsonld_employees(about_body)
            if jsonld is None:
                badge = extract_linkedin_badge(about_body)
            if jsonld is None and badge is None:
                about_gate = looks_gated_linkedin_content(about_body, about_final_url)
                if about_gate is not None:
                    linkedin_gate_total.labels(reason=about_gate).inc()
                    _log.warning(
                        "linkedin_gated",
                        purpose="about",
                        url=about_url,
                        reason=about_gate,
                        deferred=True,
                    )
                    self._note_primary_gate()
                    raise AdapterGatedError(f"{about_gate} on {about_url}")
                return []
            signal_source_url = about_url

        if jsonld is not None:
            signals.append(
                self._jsonld_signal(slug, jsonld, signal_source_url, anchor_month)
            )
        else:
            assert badge is not None  # narrowed above; kept for mypy
            signals.append(self._badge_signal(slug, badge, signal_source_url, anchor_month))

        # Any successfully parsed signal clears the streak: LinkedIn is
        # still talking to us, whatever transient blip tripped earlier
        # attempts has passed.
        self._note_success()

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

    def _note_primary_gate(self) -> None:
        """Bump the gate streak and trip the breaker when it saturates."""

        self._guard.note_gate()

    def _note_success(self) -> None:
        """Reset the breaker once we actually parsed a signal."""

        self._guard.note_success()

    async def _fetch(
        self,
        context: FetchContext,
        url: str,
        *,
        purpose: str,
        soft_gate: bool = False,
    ) -> Any:
        """Fetch ``url`` and enforce *status-level* gate detection.

        Only HTTP-status gates (999 / 429 / 403 / 401 / 407) raise
        eagerly here, because those are unambiguous server refusals
        and the body is whatever error template they wanted to ship.
        Content-level markers ("sign in to see", login redirects) are
        intentionally NOT raised in this layer: callers parse the body
        for JSON-LD or badge data first, since LinkedIn routinely
        embeds usable structured data alongside the auth-wall copy.

        ``soft_gate=True`` still suppresses the eager raise for status
        gates, used by the ``/people/`` probe so a wall there does not
        discard the main signal.
        """

        # Lever L3: jitter + budget. We only sleep and count calls that
        # will actually hit the network; HttpClient.get returns a
        # CachedResponse with from_cache=True when served locally. The
        # jitter only fires once we know the *previous* request went to
        # the network, so a run made entirely of cache hits stays fast.
        if self._guard.is_budget_exhausted():
            linkedin_gate_total.labels(reason="budget_exhausted").inc()
            raise AdapterGatedError(f"budget_exhausted on {url}")
        await self._guard.before_request()
        try:
            response = await context.http.get(self.source_name, url)
        except Exception as exc:  # pragma: no cover - network guard
            raise AdapterFetchError(
                f"linkedin_public {purpose} fetch failed for {url}: {exc!r}"
            ) from exc

        self._guard.record_response(
            from_cache=bool(getattr(response, "from_cache", False))
        )

        status_gate = looks_gated_linkedin_status(response.status_code)
        if status_gate is not None:
            linkedin_gate_total.labels(reason=status_gate).inc()
            _log.warning(
                "linkedin_gated",
                purpose=purpose,
                url=url,
                reason=status_gate,
                status=response.status_code,
                soft=soft_gate,
            )
            if soft_gate:
                return None
            raise AdapterGatedError(f"{status_gate} on {url}")

        if response.status_code == 404:
            return None
        if response.status_code >= 400:
            if soft_gate:
                return None
            raise AdapterFetchError(f"linkedin_public HTTP {response.status_code} for {url}")

        # Soft callers (e.g. the /people probe) opt into immediate
        # content-gate handling because the regex they run will not
        # find anything in an auth-walled body anyway. Hard callers
        # (the /company and /about flows) defer this check so the
        # JSON-LD parser gets a turn before we throw the body away.
        if soft_gate:
            content_gate = looks_gated_linkedin_content(
                response.text or "", response.url or url
            )
            if content_gate is not None:
                linkedin_gate_total.labels(reason=content_gate).inc()
                _log.warning(
                    "linkedin_gated",
                    purpose=purpose,
                    url=url,
                    reason=content_gate,
                    status=response.status_code,
                    soft=True,
                )
                return None
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

    def _jsonld_signal(
        self,
        slug: str,
        jsonld: LinkedInJsonLdEmployees,
        source_url: str,
        anchor_month: date,
    ) -> RawAnchorSignal:
        """Emit a signal from LinkedIn's embedded ``numberOfEmployees``.

        Confidence is deliberately higher than the badge path: the
        JSON-LD block is an explicit machine-readable contract that
        LinkedIn maintains for search engines, whereas the visible
        badge lives in prose that rephrases itself across experiments.
        Exact counts get a further bump over bucketed ranges.
        """

        is_exact = jsonld.kind is HeadcountValueKind.exact
        confidence = 0.60 if is_exact else 0.50
        bucket_label = (
            f"{jsonld.low}" if is_exact else linkedin_bucket_label(jsonld.low, jsonld.high)
        )
        note_parts = [f"slug={slug}", f"bucket={bucket_label}", "source=jsonld"]
        return RawAnchorSignal(
            source_name=self.source_name,
            entity_type=SourceEntityType.company,
            source_url=source_url,
            anchor_month=anchor_month,
            anchor_type=AnchorType.current_headcount_anchor,
            headcount_value_min=float(jsonld.low),
            headcount_value_point=jsonld.point,
            headcount_value_max=float(jsonld.high),
            headcount_value_kind=jsonld.kind,
            confidence=confidence,
            raw_text=jsonld.phrase,
            parser_version=self.parser_version,
            parse_status=ParseStatus.ok,
            note=" ".join(note_parts),
            normalized_payload={
                "slug": slug,
                "kind": "jsonld_exact" if is_exact else "jsonld_bucket",
                "bucket_low": jsonld.low,
                "bucket_high": jsonld.high,
                "phrase": jsonld.phrase,
                "org_name": jsonld.org_name,
                "org_url": jsonld.org_url,
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
