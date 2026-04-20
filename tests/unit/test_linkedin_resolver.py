"""Unit tests for ``headcount.resolution.linkedin_resolver``."""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest

from headcount.db.enums import SourceName
from headcount.ingest.http import FileCache, HttpClient, HttpClientConfig
from headcount.resolution.linkedin_resolver import (
    LinkedInSlugResult,
    resolve_linkedin_slug,
    slug_candidates,
    title_matches_name,
)


class TestSlugCandidates:
    def test_domain_label_first(self) -> None:
        cands = slug_candidates("Acme Corp", "acme.io")
        assert cands[0] == ("acme", "domain_label")

    def test_name_variants_follow_domain(self) -> None:
        # Pick inputs where every variant is distinct so all three methods
        # appear in the output (dedup preserves first-seen).
        cands = slug_candidates("Acme Holdings Group", "acme.com")
        methods = [m for _, m in cands]
        assert "domain_label" in methods
        assert "name_slug" in methods
        assert "name_concat" in methods

    def test_deduplicates_identical_slugs(self) -> None:
        cands = slug_candidates("alpaca", "alpaca.markets")
        slugs = [s for s, _ in cands]
        assert slugs == list(dict.fromkeys(slugs))

    def test_empty_name_and_domain_yields_nothing(self) -> None:
        assert slug_candidates("", None) == []

    def test_domain_only(self) -> None:
        cands = slug_candidates("", "example.com")
        assert cands == [("example", "domain_label")]

    def test_name_with_punctuation(self) -> None:
        cands = slug_candidates("15Five, Inc.", None)
        slugs = {s for s, _ in cands}
        assert "15five-inc" in slugs
        assert "15fiveinc" in slugs


class TestTitleMatchesName:
    def test_exact_match_after_suffix_strip(self) -> None:
        assert title_matches_name("Acme Corp | LinkedIn", "Acme Corp") is True

    def test_partial_token_match(self) -> None:
        assert title_matches_name("Acme Biotechnologies | LinkedIn", "Acme Bio") is True

    def test_mismatch(self) -> None:
        assert title_matches_name("Unrelated Company | LinkedIn", "Acme Corp") is False

    def test_tolerates_legal_suffix_in_name(self) -> None:
        assert (
            title_matches_name("AlphaSense | LinkedIn", "AlphaSense, Inc.")
            is True
        )

    def test_short_tokens_not_alone_sufficient(self) -> None:
        # "LinkedIn Company Page" page title with a 2-letter target name
        # should not be a false positive just because "AI" appears in
        # both strings.
        assert title_matches_name("Something else entirely | LinkedIn", "AI") is False


def _make_http(tmp_path: Path, handler) -> HttpClient:
    cache = FileCache(tmp_path / "cache")
    return HttpClient(
        cache=cache,
        configs={SourceName.linkedin_public: HttpClientConfig(max_concurrency=1)},
        transport=httpx.MockTransport(handler),
    )


def _resolve(http: HttpClient, *, name: str, domain: str | None) -> LinkedInSlugResult | None:
    """Open the client and run the resolver once."""

    async def _run() -> LinkedInSlugResult | None:
        async with http:
            return await resolve_linkedin_slug(name=name, domain=domain, http=http)

    return asyncio.run(_run())


class TestResolveLinkedInSlug:
    def test_domain_label_hits_first(self, tmp_path: Path) -> None:
        seen_urls: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen_urls.append(str(request.url))
            if "/company/acme/" in str(request.url):
                return httpx.Response(
                    200,
                    text=(
                        "<html><head><title>Acme Corp | LinkedIn</title>"
                        "</head></html>"
                    ),
                )
            return httpx.Response(404, text="not found")

        http = _make_http(tmp_path, handler)
        result = _resolve(http, name="Acme Corp", domain="acme.io")
        assert isinstance(result, LinkedInSlugResult)
        assert result.slug == "acme"
        assert result.method == "domain_label"
        assert result.url.endswith("/company/acme/")
        # Should short-circuit: only one probe.
        assert len(seen_urls) == 1

    def test_falls_back_to_name_slug(self, tmp_path: Path) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            if "/company/acme/" in url:
                return httpx.Response(404)
            if "/company/acme-corp/" in url:
                return httpx.Response(
                    200,
                    text=(
                        "<html><head>"
                        '<meta property="og:title" content="Acme Corp">'
                        "<title>Acme Corp | LinkedIn</title>"
                        "</head></html>"
                    ),
                )
            return httpx.Response(404)

        http = _make_http(tmp_path, handler)
        result = _resolve(http, name="Acme Corp", domain="acme.io")
        assert result is not None
        assert result.slug == "acme-corp"
        assert result.method == "name_slug"

    def test_rejects_wrong_company_on_same_slug(self, tmp_path: Path) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                text="<html><head><title>Someone Else | LinkedIn</title></head></html>",
            )

        http = _make_http(tmp_path, handler)
        result = _resolve(http, name="Acme Corp", domain="acme.io")
        assert result is None

    def test_gated_status_does_not_claim_slug(self, tmp_path: Path) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(999, text="")

        http = _make_http(tmp_path, handler)
        result = _resolve(http, name="Acme Corp", domain="acme.io")
        assert result is None

    def test_no_title_is_unverified(self, tmp_path: Path) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, text="<html><body>no title here</body></html>")

        http = _make_http(tmp_path, handler)
        result = _resolve(http, name="Acme Corp", domain="acme.io")
        assert result is None


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
