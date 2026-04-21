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
