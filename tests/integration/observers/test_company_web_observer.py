"""Integration tests for :class:`CompanyWebObserver`."""

from __future__ import annotations

import httpx
import pytest

from headcount.db.enums import HeadcountValueKind, SourceName
from headcount.ingest.base import AdapterGatedError, CompanyTarget
from headcount.ingest.observers.company_web import CompanyWebObserver

from .conftest import fixture_text


@pytest.mark.asyncio
async def test_company_web_parses_headcount_from_about_page(fetch_context) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/":
            return httpx.Response(404)
        if path in {"/about", "/about-us"}:
            return httpx.Response(200, text=fixture_text("company_web_acme_about.html"))
        return httpx.Response(404)

    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-acme",
        canonical_name="Acme",
        canonical_domain="acme.example",
        linkedin_company_url=None,
    )
    async with client:
        signals = await CompanyWebObserver().fetch_current_anchor(target, context=context)
    assert signals, "expected at least one headcount signal"
    assert all(s.source_name is SourceName.company_web for s in signals)
    # we should have parsed three distinct matches from the fixture
    phrases = {s.raw_text.lower() for s in signals}
    assert any("over 1,250" in p for p in phrases)
    assert any("team of 320" in p for p in phrases)
    assert any("approximately 500" in p for p in phrases)

    over = next(s for s in signals if "over" in s.raw_text.lower())
    assert over.headcount_value_min == 1250
    assert over.headcount_value_max == pytest.approx(1250 * 1.25)
    assert over.headcount_value_kind is HeadcountValueKind.range


@pytest.mark.asyncio
async def test_company_web_gated_on_403(fetch_context) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, text="forbidden")

    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-gated",
        canonical_name="Gated Co",
        canonical_domain="gated.example",
        linkedin_company_url=None,
    )
    async with client:
        with pytest.raises(AdapterGatedError):
            await CompanyWebObserver().fetch_current_anchor(target, context=context)


@pytest.mark.asyncio
async def test_company_web_returns_empty_without_domain(fetch_context) -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - should not fire
        raise AssertionError("handler should not be called when domain is missing")

    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-nodomain",
        canonical_name="Whatever",
        canonical_domain=None,
        linkedin_company_url=None,
    )
    async with client:
        signals = await CompanyWebObserver().fetch_current_anchor(target, context=context)
    assert signals == []


@pytest.mark.asyncio
async def test_company_web_returns_empty_when_no_matches(fetch_context) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html><body>Welcome.</body></html>")

    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-empty",
        canonical_name="Empty",
        canonical_domain="empty.example",
        linkedin_company_url=None,
    )
    async with client:
        signals = await CompanyWebObserver().fetch_current_anchor(target, context=context)
    assert signals == []
