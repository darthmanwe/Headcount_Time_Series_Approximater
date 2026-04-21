"""Tests for the Bing-SERP fallback slug discovery (lever L6)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest

from headcount.db.enums import SourceName
from headcount.ingest.http import FileCache, HttpClient, HttpClientConfig
from headcount.ingest.linkedin_guard import LinkedInRateGuard
from headcount.resolution.bing_slug import (
    extract_linkedin_slugs,
    fetch_bing_slug_candidates,
    host_is_linkedin,
)
from headcount.resolution.linkedin_resolver import resolve_linkedin_slug


def _make_http(tmp_path: Path, handler) -> HttpClient:
    cache = FileCache(tmp_path / "cache")
    return HttpClient(
        cache=cache,
        configs={
            SourceName.linkedin_public: HttpClientConfig(max_concurrency=1),
            SourceName.company_web: HttpClientConfig(max_concurrency=1),
        },
        transport=httpx.MockTransport(handler),
    )


class TestExtractLinkedInSlugs:
    def test_picks_direct_anchor(self) -> None:
        body = (
            '<html><a href="https://www.linkedin.com/company/acme-corp/">Acme'
            "</a></html>"
        )
        assert extract_linkedin_slugs(body) == ["acme-corp"]

    def test_dedupes_across_anchors(self) -> None:
        body = (
            '<a href="https://linkedin.com/company/acme/">a</a>'
            '<a href="https://www.linkedin.com/company/acme/about/">b</a>'
            '<a href="https://www.linkedin.com/company/beta/">c</a>'
        )
        assert extract_linkedin_slugs(body) == ["acme", "beta"]

    def test_filters_taxonomy_slugs(self) -> None:
        body = (
            '<a href="https://www.linkedin.com/company/linkedin/">li</a>'
            '<a href="https://www.linkedin.com/company/showcase/">sh</a>'
            '<a href="https://www.linkedin.com/company/real-target/">rt</a>'
        )
        assert extract_linkedin_slugs(body) == ["real-target"]

    def test_decodes_bing_redirect(self) -> None:
        body = (
            '<a href="https://www.bing.com/ck/a?!&u=https%3a%2f%2fwww.linkedin'
            '.com%2fcompany%2fhidden-corp%2f&ntb=1">Hidden</a>'
        )
        assert extract_linkedin_slugs(body) == ["hidden-corp"]

    def test_decodes_ddg_redirect(self) -> None:
        # DDG wraps organic results as //duckduckgo.com/l/?uddg=<enc>&...
        body = (
            '<a class="result__a" href="//duckduckgo.com/l/?uddg='
            "https%3A%2F%2Fwww.linkedin.com%2Fcompany%2Fddg-found%2F"
            '&rut=abc">DDG-Found</a>'
        )
        assert extract_linkedin_slugs(body) == ["ddg-found"]

    def test_mixed_ddg_and_bing(self) -> None:
        body = (
            '<a href="//duckduckgo.com/l/?uddg='
            "https%3A%2F%2Fwww.linkedin.com%2Fcompany%2Fddg-co%2F"
            '&rut=x">one</a>'
            '<a href="https://www.bing.com/ck/a?!&u='
            "https%3a%2f%2fwww.linkedin.com%2fcompany%2fbing-co%2f&ntb=1"
            '">two</a>'
        )
        assert set(extract_linkedin_slugs(body)) == {"ddg-co", "bing-co"}

    def test_respects_max_candidates(self) -> None:
        body = "".join(
            f'<a href="https://www.linkedin.com/company/c{i}/">x</a>'
            for i in range(20)
        )
        slugs = extract_linkedin_slugs(body, max_candidates=3)
        assert len(slugs) == 3

    def test_empty_body(self) -> None:
        assert extract_linkedin_slugs("") == []


class TestHostIsLinkedIn:
    def test_yes(self) -> None:
        assert host_is_linkedin("https://www.linkedin.com/company/x")
        assert host_is_linkedin("http://linkedin.com/")

    def test_no(self) -> None:
        assert not host_is_linkedin("https://example.com/linkedin.com")


class TestFetchBingCandidates:
    def test_ddg_is_primary_source(self, tmp_path: Path) -> None:
        """DDG is tried first; its hits short-circuit Bing entirely."""

        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(str(request.url))
            return httpx.Response(
                200,
                text=(
                    '<html><body><a href="https://www.linkedin.com/company/'
                    'acme-corp/">Acme</a></body></html>'
                ),
            )

        http = _make_http(tmp_path, handler)

        async def _run():
            async with http:
                return await fetch_bing_slug_candidates(
                    name="Acme", domain="acme.io", http=http
                )

        slugs = asyncio.run(_run())
        assert slugs == ["acme-corp"]
        assert len(seen) == 1, "Bing should NOT be called when DDG returns hits"
        assert "duckduckgo.com" in seen[0]
        assert "site%3Alinkedin.com%2Fcompany" in seen[0]

    def test_falls_back_to_bing_when_ddg_empty(self, tmp_path: Path) -> None:
        """DDG returning zero slugs triggers a Bing fallback query."""

        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(str(request.url))
            host = request.url.host
            if "duckduckgo" in host:
                return httpx.Response(200, text="<html>no linkedin here</html>")
            return httpx.Response(
                200,
                text=(
                    '<html><body><a href="https://www.linkedin.com/company/'
                    'acme-bing/">Acme</a></body></html>'
                ),
            )

        http = _make_http(tmp_path, handler)

        async def _run():
            async with http:
                return await fetch_bing_slug_candidates(
                    name="Acme", domain="acme.io", http=http
                )

        slugs = asyncio.run(_run())
        assert slugs == ["acme-bing"]
        # One DDG probe, one Bing probe.
        assert len(seen) == 2
        assert "duckduckgo.com" in seen[0]
        assert "bing.com/search" in seen[1]

    def test_bing_captcha_page_returns_no_slugs(self, tmp_path: Path) -> None:
        """Bing CAPTCHA interstitial must not be parsed for slugs."""

        def handler(request: httpx.Request) -> httpx.Response:
            host = request.url.host
            if "duckduckgo" in host:
                return httpx.Response(200, text="")
            return httpx.Response(
                200,
                text=(
                    "<html><body>Please complete the captcha to verify "
                    "you are a human. <a href='https://www.linkedin.com/"
                    "company/some-taxonomy/'>chrome</a></body></html>"
                ),
            )

        http = _make_http(tmp_path, handler)

        async def _run():
            async with http:
                return await fetch_bing_slug_candidates(
                    name="Acme", domain="acme.io", http=http
                )

        assert asyncio.run(_run()) == []


class TestResolverFallsBackToBing:
    """End-to-end: heuristic candidates 404, Bing finds the real slug."""

    def test_bing_provides_slug_after_404s(self, tmp_path: Path) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            host = request.url.host
            path = request.url.path
            if "duckduckgo" in host or (
                host.endswith("bing.com") and path == "/search"
            ):
                return httpx.Response(
                    200,
                    text=(
                        '<a href="https://www.linkedin.com/company/'
                        'hidden-acme/">Acme</a>'
                    ),
                )
            if host.endswith("linkedin.com") and path == "/company/hidden-acme/":
                return httpx.Response(
                    200,
                    text=(
                        "<html><head><title>Acme | LinkedIn</title></head>"
                        "<body><a href='https://acme.io'>Site</a></body></html>"
                    ),
                )
            return httpx.Response(404, text="not found")

        http = _make_http(tmp_path, handler)
        guard = LinkedInRateGuard(
            circuit_threshold=10,
            daily_request_budget=0,
            jitter_ms=(0, 0),
            cooldown_seconds=10.0,
        )

        async def _run():
            async with http:
                return await resolve_linkedin_slug(
                    name="Acme",
                    domain="acme.io",
                    http=http,
                    rate_guard=guard,
                )

        result = asyncio.run(_run())
        assert result is not None
        assert result.slug == "hidden-acme"
        assert result.method == "bing_serp"

    def test_bing_skipped_when_breaker_open(self, tmp_path: Path) -> None:
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(str(request.url))
            return httpx.Response(999)

        http = _make_http(tmp_path, handler)
        guard = LinkedInRateGuard(
            circuit_threshold=1,
            daily_request_budget=0,
            jitter_ms=(0, 0),
            cooldown_seconds=10.0,
        )
        # Pre-trip the breaker so the resolver short-circuits before
        # the heuristic candidates AND before Bing.
        guard.note_gate()
        assert guard.is_circuit_open()

        async def _run():
            async with http:
                return await resolve_linkedin_slug(
                    name="Acme",
                    domain="acme.io",
                    http=http,
                    rate_guard=guard,
                    company_id="c-1",
                )

        result = asyncio.run(_run())
        assert result is None
        assert seen == [], "no requests should fire while breaker is open"


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
