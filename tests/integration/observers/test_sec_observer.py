"""Integration tests for :class:`SECObserver`."""

from __future__ import annotations

from datetime import date

import httpx
import pytest

from headcount.db.enums import AnchorType, HeadcountValueKind, SourceName
from headcount.ingest.base import CompanyTarget
from headcount.ingest.observers.sec import FACTS_URL, TICKER_URL, SECObserver

from .conftest import fixture_text


def _handler(url_to_body: dict[str, tuple[int, str]]):
    def _impl(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        for prefix, (status, text) in url_to_body.items():
            if url.startswith(prefix):
                return httpx.Response(status, text=text)
        return httpx.Response(404, text="not found")

    return _impl


@pytest.mark.asyncio
async def test_sec_returns_three_most_recent_reports(fetch_context) -> None:
    handler = _handler(
        {
            TICKER_URL: (200, fixture_text("sec_company_tickers.json")),
            FACTS_URL.format(cik="0000320193"): (
                200,
                fixture_text("sec_apple_facts.json"),
            ),
        }
    )
    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-apple",
        canonical_name="Apple Inc.",
        canonical_domain="apple.com",
        linkedin_company_url=None,
    )
    async with client:
        signals = await SECObserver().fetch_current_anchor(target, context=context)
    assert len(signals) == 3
    points = [s.headcount_value_point for s in signals]
    assert points == [164000.0, 161000.0, 164000.0]
    assert signals[0].source_name is SourceName.sec
    assert signals[0].headcount_value_kind is HeadcountValueKind.exact
    assert signals[0].anchor_month == date(2024, 9, 1)
    assert signals[0].anchor_type is AnchorType.historical_statement
    assert signals[0].normalized_payload["cik"] == "0000320193"


@pytest.mark.asyncio
async def test_sec_returns_empty_when_cik_not_found(fetch_context) -> None:
    handler = _handler({TICKER_URL: (200, fixture_text("sec_company_tickers.json"))})
    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-unknown",
        canonical_name="Acme LLC",
        canonical_domain="acme.example",
        linkedin_company_url=None,
    )
    async with client:
        signals = await SECObserver().fetch_current_anchor(target, context=context)
    assert signals == []


@pytest.mark.asyncio
async def test_sec_handles_facts_404(fetch_context) -> None:
    handler = _handler(
        {
            TICKER_URL: (200, fixture_text("sec_company_tickers.json")),
            FACTS_URL.format(cik="0000320193"): (404, ""),
        }
    )
    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-apple",
        canonical_name="Apple Inc.",
        canonical_domain="apple.com",
        linkedin_company_url=None,
    )
    async with client:
        signals = await SECObserver().fetch_current_anchor(target, context=context)
    assert signals == []


@pytest.mark.asyncio
async def test_sec_matches_by_ticker_from_domain_stem(fetch_context) -> None:
    # tickers.json maps title "Microsoft Corporation" -> MSFT. The domain
    # stem "microsoft" should normalize to a name key that hits.
    handler = _handler(
        {
            TICKER_URL: (200, fixture_text("sec_company_tickers.json")),
            FACTS_URL.format(cik="0000789019"): (200, fixture_text("sec_apple_facts.json")),
        }
    )
    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-ms",
        canonical_name="Microsoft Corporation",
        canonical_domain="microsoft.com",
        linkedin_company_url=None,
    )
    async with client:
        signals = await SECObserver().fetch_current_anchor(target, context=context)
    assert len(signals) == 3
