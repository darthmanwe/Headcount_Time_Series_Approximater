"""Unit tests for ``headcount.resolution.linkedin_resolver``."""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest

from headcount.db.enums import SourceName
from headcount.ingest.http import FileCache, HttpClient, HttpClientConfig
from headcount.ingest.linkedin_guard import LinkedInRateGuard
from headcount.resolution.linkedin_resolver import (
    LinkedInSlugResult,
    disambiguate_match,
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


def _resolve(
    http: HttpClient,
    *,
    name: str,
    domain: str | None,
    rate_guard: LinkedInRateGuard | None = None,
    company_id: str | None = None,
) -> LinkedInSlugResult | None:
    """Open the client and run the resolver once."""

    async def _run() -> LinkedInSlugResult | None:
        async with http:
            return await resolve_linkedin_slug(
                name=name,
                domain=domain,
                http=http,
                rate_guard=rate_guard,
                company_id=company_id,
            )

    return asyncio.run(_run())


def _quiet_guard(**overrides) -> LinkedInRateGuard:
    """Build a guard with jitter disabled and a generous budget for tests."""

    defaults = {
        "circuit_threshold": 100,
        "daily_request_budget": 0,
        "jitter_ms": (0, 0),
        "cooldown_seconds": 60.0,
    }
    defaults.update(overrides)
    return LinkedInRateGuard(**defaults)


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


class TestDisambiguateMatch:
    """Stricter verifier that catches single-token false positives."""

    def test_accepts_clean_overlap(self) -> None:
        accepted, reason = disambiguate_match(
            title="Acme Corp | LinkedIn",
            name="Acme Corp",
            domain="acme.io",
            body="<html>...</html>",
        )
        assert accepted is True
        assert reason in {"ok", "domain_in_body"}

    def test_rejects_arable_consulting_for_arable(self) -> None:
        # The motivating false positive: target is the agritech "Arable",
        # but the slug 'arable' on LinkedIn is owned by "Arable Consulting".
        # Domain hint is NOT mentioned in the body, so the qualifier-veto
        # must fire.
        accepted, reason = disambiguate_match(
            title="Arable Consulting | LinkedIn",
            name="Arable",
            domain="arable.com",
            body="<html><body>About Arable Consulting LLC.</body></html>",
        )
        assert accepted is False
        assert reason.startswith("ambiguous_qualifier")

    def test_domain_in_body_overrides_qualifier_veto(self) -> None:
        # If the LinkedIn page literally links the target's website, that
        # IS the same company even if the title carries a qualifier.
        accepted, reason = disambiguate_match(
            title="Acme Holdings | LinkedIn",
            name="Acme",
            domain="acme.io",
            body="<html><a href='https://acme.io'>Website</a></html>",
        )
        assert accepted is True
        assert reason == "domain_in_body"

    def test_rejects_no_overlap_at_all(self) -> None:
        accepted, reason = disambiguate_match(
            title="Totally Different Company | LinkedIn",
            name="Acme",
            domain="acme.io",
            body="<html></html>",
        )
        assert accepted is False
        assert reason == "no_token_overlap"

    def test_multi_token_name_not_blocked_by_qualifier(self) -> None:
        # Veto only fires for single-token names. "Acme Bio" + "Acme Bio
        # Partners" should still match.
        accepted, _ = disambiguate_match(
            title="Acme Bio Partners | LinkedIn",
            name="Acme Bio",
            domain=None,
            body="<html></html>",
        )
        assert accepted is True

    def test_rejects_single_token_when_domain_missing_from_body(self) -> None:
        # "Alleva" slug on LinkedIn points to the Belgian ALLEVA company,
        # not the US helloalleva.com target. The title matches, but
        # helloalleva.com never appears in the body, so we must refuse.
        accepted, reason = disambiguate_match(
            title="ALLEVA | LinkedIn",
            name="Alleva",
            domain="helloalleva.com",
            body=(
                "<html><head><title>ALLEVA | LinkedIn</title></head>"
                "<body><p>Located in Belgium</p></body></html>"
            ),
        )
        assert accepted is False
        assert reason == "single_token_no_domain_in_body"

    def test_accepts_single_token_when_domain_in_body(self) -> None:
        # Same single-token name, but the canonical domain literally
        # appears in the body - that's a clean positive match.
        accepted, reason = disambiguate_match(
            title="15Five | LinkedIn",
            name="15Five",
            domain="15five.com",
            body="<html><body><a href='https://15five.com'>Site</a></body></html>",
        )
        assert accepted is True
        assert reason == "domain_in_body"

    def test_allows_single_token_without_domain_input(self) -> None:
        # When the caller gives us no canonical domain, single-token
        # names cannot be domain-verified, so we must not reject on
        # that axis - the old behaviour is preserved.
        accepted, reason = disambiguate_match(
            title="Acme | LinkedIn",
            name="Acme",
            domain=None,
            body="<html></html>",
        )
        assert accepted is True
        assert reason == "ok"


class TestResolverWithGuard:
    """Resolver integration with the shared :class:`LinkedInRateGuard`."""

    def test_circuit_open_short_circuits_without_http(self, tmp_path: Path) -> None:
        seen_urls: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen_urls.append(str(request.url))
            return httpx.Response(200, text="<title>Acme | LinkedIn</title>")

        guard = _quiet_guard(circuit_threshold=1)
        # Pre-trip the breaker.
        guard.note_gate()
        assert guard.is_circuit_open()

        http = _make_http(tmp_path, handler)
        result = _resolve(
            http,
            name="Acme",
            domain="acme.io",
            rate_guard=guard,
            company_id="c-1",
        )
        assert result is None
        assert seen_urls == [], "no HTTP requests should have been issued"
        assert "c-1" in guard.deferred_companies

    def test_status_gate_feeds_breaker(self, tmp_path: Path) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if "linkedin.com" in (request.url.host or ""):
                return httpx.Response(999, text="bot wall")
            # Bing/DDG hosts: return a non-200 so the fallback produces
            # no slugs and the resolver completes without a hit.
            return httpx.Response(200, text="<html></html>")

        guard = _quiet_guard(circuit_threshold=1)
        http = _make_http(tmp_path, handler)
        # With fast-fail-on-gate, only the *first* heuristic probe runs
        # per resolve before we punt to Bing/DDG. One gate is enough to
        # trip a threshold=1 breaker. This mirrors real runs where a
        # 999 on the first candidate is a reliable "skip this company"
        # signal.
        result = _resolve(
            http,
            name="Acme Corp",
            domain="acme.io",
            rate_guard=guard,
            company_id="c-acme",
        )
        assert result is None
        assert guard.consecutive_gates >= 1
        assert guard.is_circuit_open()
        assert "c-acme" in guard.deferred_companies

    def test_budget_exhausted_short_circuits_probe(self, tmp_path: Path) -> None:
        seen_urls: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen_urls.append(str(request.url))
            return httpx.Response(200, text="<title>Acme | LinkedIn</title>")

        guard = _quiet_guard()
        guard.daily_request_budget = 1
        # Pre-spend the budget so the resolver has to short-circuit on
        # the very first probe.
        guard.record_response(from_cache=False)
        assert guard.is_budget_exhausted()

        http = _make_http(tmp_path, handler)
        result = _resolve(
            http, name="Acme Corp", domain="acme.io", rate_guard=guard
        )
        assert result is None
        # Budget cap is LinkedIn-specific; Bing fallback is allowed.
        from urllib.parse import urlparse

        linkedin_calls = [
            u
            for u in seen_urls
            if (urlparse(u).hostname or "").endswith("linkedin.com")
        ]
        assert linkedin_calls == [], (
            f"budget cap must skip LinkedIn calls, saw {linkedin_calls}"
        )

    def test_defers_company_when_gated_below_breaker_threshold(
        self, tmp_path: Path
    ) -> None:
        """Companies whose slug probe 999s should be deferred even when the
        breaker hasn't tripped, so the recovery pass retries them.

        Without this, the first N companies to be gated (where N is the
        breaker threshold - 1) are permanently lost because the breaker
        hasn't opened yet so no ``defer_company`` fires.
        """

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(999, text="bot wall")

        guard = _quiet_guard(circuit_threshold=99)  # deliberately far away
        http = _make_http(tmp_path, handler)
        result = _resolve(
            http,
            name="AliveCor",
            domain="alivecor.com",
            rate_guard=guard,
            company_id="company-livecor",
        )
        assert result is None
        assert not guard.is_circuit_open(), (
            "breaker threshold is 99, should not trip on a single company"
        )
        assert "company-livecor" in guard.deferred_companies, (
            "slug-gated companies must be parked for the recovery pass"
        )

    def test_resolver_clears_streak_on_verified_hit(self, tmp_path: Path) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                text="<html><head><title>Acme Corp | LinkedIn</title></head></html>",
            )

        guard = _quiet_guard(circuit_threshold=5)
        guard.note_gate()
        guard.note_gate()
        assert guard.consecutive_gates == 2

        http = _make_http(tmp_path, handler)
        result = _resolve(
            http, name="Acme Corp", domain="acme.io", rate_guard=guard
        )
        assert result is not None
        assert guard.consecutive_gates == 0


class TestResolverDisambigInLoop:
    """End-to-end: a 200 OK with the wrong company is rejected by name."""

    def test_arable_consulting_is_not_arable(self, tmp_path: Path) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                text=(
                    "<html><head>"
                    "<title>Arable Consulting | LinkedIn</title>"
                    "</head><body>About Arable Consulting LLC.</body></html>"
                ),
            )

        http = _make_http(tmp_path, handler)
        result = _resolve(http, name="Arable", domain="arable.com")
        assert result is None, "disambig should reject the qualifier-titled page"

    def test_arable_with_matching_website_link_accepted(
        self, tmp_path: Path
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                text=(
                    "<html><head>"
                    "<title>Arable | LinkedIn</title>"
                    "</head><body>"
                    "<a href='https://arable.com/about'>Website</a>"
                    "</body></html>"
                ),
            )

        http = _make_http(tmp_path, handler)
        result = _resolve(http, name="Arable", domain="arable.com")
        assert result is not None
        assert result.slug == "arable"


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
