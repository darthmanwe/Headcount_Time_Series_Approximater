"""Tests for the browser fingerprint profile used on gated endpoints."""

from __future__ import annotations

import random

from headcount.db.enums import SourceName
from headcount.ingest.browser_fingerprint import (
    BROWSER_NAV_HEADERS,
    BROWSER_USER_AGENTS,
    pick_user_agent,
)
from headcount.ingest.collect import default_http_configs


def test_browser_user_agents_look_like_real_browsers() -> None:
    assert len(BROWSER_USER_AGENTS) >= 3
    for ua in BROWSER_USER_AGENTS:
        assert ua.startswith("Mozilla/5.0")
        assert "Headcount-Estimator" not in ua
        assert any(token in ua for token in ("Chrome/", "Firefox/"))


def test_nav_headers_cover_minimum_chromium_set() -> None:
    required = {
        "Accept",
        "Accept-Language",
        "Accept-Encoding",
        "Upgrade-Insecure-Requests",
        "Sec-Fetch-Dest",
        "Sec-Fetch-Mode",
        "Sec-Fetch-Site",
        "Sec-Fetch-User",
    }
    assert required.issubset(BROWSER_NAV_HEADERS.keys())

    # Must not advertise br: httpx has no default Brotli decoder and
    # Content-Encoding: br would corrupt response.text downstream.
    assert "br" not in BROWSER_NAV_HEADERS["Accept-Encoding"]

    # Navigation, not subresource or iframe load.
    assert BROWSER_NAV_HEADERS["Sec-Fetch-Dest"] == "document"
    assert BROWSER_NAV_HEADERS["Sec-Fetch-Mode"] == "navigate"


def test_pick_user_agent_is_deterministic_with_seeded_rng() -> None:
    rng_a = random.Random(42)
    rng_b = random.Random(42)
    assert pick_user_agent(rng_a) == pick_user_agent(rng_b)


def test_pick_user_agent_ranges_over_the_pool() -> None:
    seen = {pick_user_agent(random.Random(seed)) for seed in range(200)}
    # With 4 UAs and 200 seeds we expect to hit every bucket; if not,
    # the pool or the RNG wiring regressed.
    assert seen == set(BROWSER_USER_AGENTS)


def test_default_http_configs_applies_browser_fingerprint_to_linkedin() -> None:
    cfg = default_http_configs()[SourceName.linkedin_public]
    assert cfg.user_agent in BROWSER_USER_AGENTS
    # Header bundle must be merged in intact (dict() copy to compare).
    merged = dict(cfg.default_headers)
    for key, value in BROWSER_NAV_HEADERS.items():
        assert merged.get(key) == value


def test_other_sources_keep_their_polite_uas() -> None:
    # L1 only re-fingerprints linkedin_public; everyone else stays on
    # the identifying UA so SEC + Wikidata compliance does not regress.
    configs = default_http_configs()
    for source in (SourceName.sec, SourceName.wikidata, SourceName.company_web):
        ua = configs[source].user_agent
        assert "Headcount" in ua
        assert ua not in BROWSER_USER_AGENTS
