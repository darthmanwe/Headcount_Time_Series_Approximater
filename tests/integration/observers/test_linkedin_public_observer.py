"""Integration tests for :class:`LinkedInPublicObserver`.

All fixtures are synthetic and resemble the public LinkedIn structure
without being real captures. The entire test suite runs offline via
:class:`httpx.MockTransport`.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from headcount.db.enums import HeadcountValueKind, SourceName
from headcount.ingest.base import AdapterGatedError, CompanyTarget
from headcount.ingest.observers.linkedin_public import LinkedInPublicObserver
from headcount.utils.metrics import linkedin_gate_total

LINKEDIN_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "linkedin"


def _li_text(name: str) -> str:
    return (LINKEDIN_FIXTURES / name).read_text(encoding="utf-8")


def _target(
    *,
    slug: str = "acme-inc",
    company_id: str = "c-li",
    domain: str | None = "acme.example",
) -> CompanyTarget:
    return CompanyTarget(
        company_id=company_id,
        canonical_name="Acme",
        canonical_domain=domain,
        linkedin_company_url=f"https://www.linkedin.com/company/{slug}/",
    )


def _gate_counter(reason: str) -> float:
    metric = linkedin_gate_total.labels(reason=reason)
    # prometheus_client Counter exposes a ._value wrapper; expose the
    # current float for before/after assertions without depending on
    # metric text formatting.
    return metric._value.get()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_linkedin_parses_company_page_badge(fetch_context) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/company/acme-inc/":
            return httpx.Response(200, text=_li_text("company_acme.html"))
        return httpx.Response(404)

    client, context = fetch_context(handler)
    async with client:
        signals = await LinkedInPublicObserver().fetch_current_anchor(_target(), context=context)
    assert len(signals) == 1
    sig = signals[0]
    assert sig.source_name is SourceName.linkedin_public
    assert sig.headcount_value_kind is HeadcountValueKind.bucket
    assert sig.headcount_value_min == 51
    assert sig.headcount_value_max == 200
    assert sig.headcount_value_point == pytest.approx(125.5)
    assert sig.normalized_payload["bucket_low"] == 51
    assert sig.normalized_payload["bucket_high"] == 200
    assert sig.normalized_payload["open_ended"] is False


@pytest.mark.asyncio
async def test_linkedin_parses_open_ended_bucket(fetch_context) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/company/giant-inc/":
            return httpx.Response(200, text=_li_text("company_giant_open.html"))
        return httpx.Response(404)

    client, context = fetch_context(handler)
    async with client:
        signals = await LinkedInPublicObserver().fetch_current_anchor(
            _target(slug="giant-inc", company_id="c-giant"), context=context
        )
    assert len(signals) == 1
    sig = signals[0]
    assert sig.headcount_value_min == 10001
    assert sig.headcount_value_max == pytest.approx(10001 * 5)
    assert sig.normalized_payload["open_ended"] is True


@pytest.mark.asyncio
async def test_linkedin_falls_back_to_about_page(fetch_context) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/company/fallbackable/":
            return httpx.Response(200, text=_li_text("company_no_badge.html"))
        if path == "/company/fallbackable/about/":
            return httpx.Response(200, text=_li_text("about_with_badge.html"))
        return httpx.Response(404)

    client, context = fetch_context(handler)
    async with client:
        signals = await LinkedInPublicObserver().fetch_current_anchor(
            _target(slug="fallbackable", company_id="c-fb"), context=context
        )
    assert len(signals) == 1
    sig = signals[0]
    assert sig.headcount_value_min == 201
    assert sig.headcount_value_max == 500
    assert sig.source_url.endswith("/about/")


@pytest.mark.asyncio
async def test_linkedin_gated_on_authwall(fetch_context) -> None:
    before = _gate_counter("marker:authwall")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=_li_text("authwall.html"))

    client, context = fetch_context(handler)
    async with client:
        with pytest.raises(AdapterGatedError):
            await LinkedInPublicObserver().fetch_current_anchor(
                _target(slug="blocked", company_id="c-blocked"), context=context
            )

    after = _gate_counter("marker:authwall")
    assert after >= before + 1


@pytest.mark.asyncio
async def test_linkedin_gated_on_captcha(fetch_context) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=_li_text("captcha.html"))

    client, context = fetch_context(handler)
    async with client:
        with pytest.raises(AdapterGatedError):
            await LinkedInPublicObserver().fetch_current_anchor(
                _target(slug="captchad", company_id="c-cap"), context=context
            )


@pytest.mark.asyncio
async def test_linkedin_gated_on_429(fetch_context) -> None:
    before = _gate_counter("rate_limited")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, text="Too Many Requests")

    client, context = fetch_context(handler)
    async with client:
        with pytest.raises(AdapterGatedError):
            await LinkedInPublicObserver().fetch_current_anchor(
                _target(slug="rated", company_id="c-rate"), context=context
            )
    after = _gate_counter("rate_limited")
    assert after >= before + 1


@pytest.mark.asyncio
async def test_linkedin_gated_on_403(fetch_context) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, text="Forbidden")

    client, context = fetch_context(handler)
    async with client:
        with pytest.raises(AdapterGatedError):
            await LinkedInPublicObserver().fetch_current_anchor(
                _target(slug="forb", company_id="c-forb"), context=context
            )


@pytest.mark.asyncio
async def test_linkedin_404_returns_empty(fetch_context) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="Not found")

    client, context = fetch_context(handler)
    async with client:
        signals = await LinkedInPublicObserver().fetch_current_anchor(
            _target(slug="missing", company_id="c-miss"), context=context
        )
    assert signals == []


@pytest.mark.asyncio
async def test_linkedin_soft_gate_on_people_does_not_discard_badge(
    fetch_context,
) -> None:
    before = _gate_counter("marker:authwall")

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/company/acme-inc/":
            return httpx.Response(200, text=_li_text("company_acme.html"))
        if path == "/company/acme-inc/people/":
            return httpx.Response(200, text=_li_text("authwall.html"))
        return httpx.Response(404)

    client, context = fetch_context(handler)
    async with client:
        signals = await LinkedInPublicObserver().fetch_current_anchor(_target(), context=context)
    assert len(signals) == 1  # badge survives; /people gate does not raise
    assert signals[0].headcount_value_kind is HeadcountValueKind.bucket
    after = _gate_counter("marker:authwall")
    assert after >= before + 1  # soft gate was still metered


@pytest.mark.asyncio
async def test_linkedin_emits_people_exact_when_available(fetch_context) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/company/acme-inc/":
            return httpx.Response(200, text=_li_text("company_acme.html"))
        if path == "/company/acme-inc/people/":
            return httpx.Response(200, text=_li_text("people_exact.html"))
        return httpx.Response(404)

    client, context = fetch_context(handler)
    async with client:
        signals = await LinkedInPublicObserver().fetch_current_anchor(_target(), context=context)
    assert len(signals) == 2
    kinds = sorted(s.headcount_value_kind.value for s in signals)
    assert kinds == ["bucket", "exact"]
    exact = next(s for s in signals if s.headcount_value_kind is HeadcountValueKind.exact)
    assert exact.headcount_value_point == 1250
    assert exact.normalized_payload["count"] == 1250


@pytest.mark.asyncio
async def test_linkedin_prefers_jsonld_exact_over_visible_bucket(
    fetch_context,
) -> None:
    """L2: exact JSON-LD numberOfEmployees beats the visible badge.

    The fixture embeds an exact JSON-LD value of 1250 alongside a
    visible ``501-1,000 employees`` badge. The parser must pick the
    JSON-LD path so the resulting signal is point-valued and carries
    the higher confidence.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/company/acme-inc/":
            return httpx.Response(200, text=_li_text("company_acme_jsonld.html"))
        return httpx.Response(404)

    client, context = fetch_context(handler)
    async with client:
        signals = await LinkedInPublicObserver().fetch_current_anchor(_target(), context=context)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.headcount_value_kind is HeadcountValueKind.exact
    assert sig.headcount_value_point == 1250
    assert sig.headcount_value_min == sig.headcount_value_max == 1250
    assert sig.confidence >= 0.55
    assert sig.normalized_payload["kind"] == "jsonld_exact"
    assert sig.normalized_payload["org_name"] == "Acme"


@pytest.mark.asyncio
async def test_linkedin_uses_jsonld_range_when_visible_badge_absent(
    fetch_context,
) -> None:
    """L2: JSON-LD range renders a bucket signal even with no badge text."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/company/acme-inc/":
            return httpx.Response(200, text=_li_text("company_acme_jsonld_range.html"))
        return httpx.Response(404)

    client, context = fetch_context(handler)
    async with client:
        signals = await LinkedInPublicObserver().fetch_current_anchor(_target(), context=context)

    assert len(signals) == 1
    sig = signals[0]
    assert sig.headcount_value_kind is HeadcountValueKind.bucket
    assert sig.headcount_value_min == 51
    assert sig.headcount_value_max == 200
    assert sig.normalized_payload["kind"] == "jsonld_bucket"


@pytest.mark.asyncio
async def test_linkedin_999_is_classified_as_bot_wall(fetch_context) -> None:
    """L4 pre-req: HTTP 999 must route through the gate classifier.

    Without this, 999 falls into the generic 4xx branch and raises
    ``AdapterFetchError``, which bypasses the breaker entirely.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(999, text="we-do-not-talk-to-bots")

    client, context = fetch_context(handler)
    async with client:
        with pytest.raises(AdapterGatedError):
            await LinkedInPublicObserver().fetch_current_anchor(
                _target(slug="bot-walled", company_id="c-bw"), context=context
            )


@pytest.mark.asyncio
async def test_linkedin_circuit_trips_after_threshold(fetch_context) -> None:
    """L4: the breaker trips after N consecutive primary-gate responses.

    We configure the observer with ``circuit_threshold=2`` and feed it
    three companies that all hit a 999 wall. The third company must
    short-circuit without making any HTTP request and return [] with a
    ``circuit_open`` gauge bump - confirming both the trip and the
    skip-next behaviour.
    """

    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(999, text="bot wall")

    observer = LinkedInPublicObserver(circuit_threshold=2)
    client, context = fetch_context(handler)
    async with client:
        with pytest.raises(AdapterGatedError):
            await observer.fetch_current_anchor(
                _target(slug="a", company_id="c-a"), context=context
            )
        assert observer.consecutive_gates == 1
        assert observer.circuit_open is False

        with pytest.raises(AdapterGatedError):
            await observer.fetch_current_anchor(
                _target(slug="b", company_id="c-b"), context=context
            )
        assert observer.consecutive_gates == 2
        assert observer.circuit_open is True

        calls_before_skip = call_count
        # Third company: circuit open, must skip HTTP entirely.
        signals = await observer.fetch_current_anchor(
            _target(slug="c", company_id="c-c"), context=context
        )

    assert signals == []
    assert call_count == calls_before_skip


@pytest.mark.asyncio
async def test_linkedin_circuit_resets_on_successful_parse(fetch_context) -> None:
    """Any successful parse must zero the streak for the rest of the run."""

    def handler(request: httpx.Request) -> httpx.Response:
        # First company: gated. Second: clean page with JSON-LD.
        if request.url.path.startswith("/company/walled"):
            return httpx.Response(999, text="bot wall")
        if request.url.path == "/company/acme-inc/":
            return httpx.Response(200, text=_li_text("company_acme_jsonld.html"))
        return httpx.Response(404)

    observer = LinkedInPublicObserver(circuit_threshold=3)
    client, context = fetch_context(handler)
    async with client:
        with pytest.raises(AdapterGatedError):
            await observer.fetch_current_anchor(
                _target(slug="walled", company_id="c-wall"), context=context
            )
        assert observer.consecutive_gates == 1

        signals = await observer.fetch_current_anchor(_target(), context=context)

    assert signals, "clean primary page should still produce a signal"
    assert observer.consecutive_gates == 0
    assert observer.circuit_open is False


@pytest.mark.asyncio
async def test_linkedin_no_url_skips_http(fetch_context) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, text="nope")

    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-noli",
        canonical_name="NoUrl Co",
        canonical_domain="nourl.example",
        linkedin_company_url=None,
    )
    async with client:
        signals = await LinkedInPublicObserver().fetch_current_anchor(target, context=context)
    assert signals == []
    assert calls == 0
