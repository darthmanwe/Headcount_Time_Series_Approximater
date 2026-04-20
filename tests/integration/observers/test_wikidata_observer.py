"""Integration tests for :class:`WikidataObserver` using MockTransport."""

from __future__ import annotations

from datetime import date

import httpx
import pytest

from headcount.db.enums import AnchorType, HeadcountValueKind, SourceName
from headcount.ingest.base import CompanyTarget
from headcount.ingest.observers.wikidata import WIKIDATA_SPARQL_URL, WikidataObserver

from .conftest import fixture_text


@pytest.mark.asyncio
async def test_wikidata_returns_signals_from_sparql(fetch_context) -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.url.path == "/sparql"
        return httpx.Response(200, text=fixture_text("wikidata_apple.json"))

    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-apple",
        canonical_name="Apple Inc.",
        canonical_domain="apple.com",
        linkedin_company_url=None,
    )
    async with client:
        signals = await WikidataObserver(endpoint=WIKIDATA_SPARQL_URL).fetch_current_anchor(
            target, context=context
        )
    assert len(signals) == 1
    sig = signals[0]
    assert sig.source_name is SourceName.wikidata
    assert sig.headcount_value_point == 164000.0
    assert sig.headcount_value_kind is HeadcountValueKind.exact
    assert sig.anchor_type is AnchorType.historical_statement
    assert sig.anchor_month == date(2024, 9, 1)
    assert sig.normalized_payload["match_reason"] == "domain"
    assert sig.normalized_payload["qid"].endswith("Q312")


@pytest.mark.asyncio
async def test_wikidata_falls_back_to_name_query(fetch_context) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.url.params.get("query", "")
        calls.append(body)
        if "P856" in body:
            return httpx.Response(200, text=fixture_text("wikidata_empty.json"))
        return httpx.Response(200, text=fixture_text("wikidata_apple.json"))

    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-apple",
        canonical_name="Apple Inc.",
        canonical_domain="apple.com",
        linkedin_company_url=None,
    )
    async with client:
        signals = await WikidataObserver().fetch_current_anchor(target, context=context)
    assert len(signals) == 1
    assert signals[0].normalized_payload["match_reason"] == "name"
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_wikidata_raises_on_http_500(fetch_context) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="kaboom")

    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-x",
        canonical_name="Globex",
        canonical_domain="globex.com",
        linkedin_company_url=None,
    )
    from headcount.ingest.base import AdapterFetchError

    async with client:
        with pytest.raises(AdapterFetchError):
            await WikidataObserver().fetch_current_anchor(target, context=context)


@pytest.mark.asyncio
async def test_wikidata_empty_returns_nothing(fetch_context) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=fixture_text("wikidata_empty.json"))

    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-empty",
        canonical_name="Nobody",
        canonical_domain="nobody.example",
        linkedin_company_url=None,
    )
    async with client:
        signals = await WikidataObserver().fetch_current_anchor(target, context=context)
    assert signals == []
