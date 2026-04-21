"""Integration tests for :class:`WaybackObserver`.

Uses ``httpx.MockTransport`` to simulate the Wayback Machine's direct
``/web/<ts>id_/<url>`` redirect pattern: a single GET per horizon gets
redirected (302) to the closest archived capture, and the body comes
back raw because the ``id_`` flag is preserved across the redirect.
"""

from __future__ import annotations

from datetime import date

import httpx
import pytest

from headcount.db.enums import AnchorType, HeadcountValueKind, SourceName
from headcount.ingest.base import CompanyTarget
from headcount.ingest.observers.wayback import (
    WaybackObserver,
    _within_drift_tolerance,
    parse_availability_response,
)


# ---------------------------------------------------------------------------
# parse_availability_response (kept as a compatibility helper; not used by
# the direct-redirect fetch path but still exercised because it remains a
# named module export).
# ---------------------------------------------------------------------------


def test_parse_availability_response_returns_snapshot_when_available() -> None:
    payload = {
        "archived_snapshots": {
            "closest": {
                "available": True,
                "url": "http://acme.example/about",
                "timestamp": "20240301120000",
                "status": "200",
            }
        }
    }
    snap = parse_availability_response(payload)
    assert snap is not None
    assert snap.available
    assert snap.timestamp == "20240301120000"
    assert snap.url == "http://acme.example/about"


def test_parse_availability_response_returns_none_when_empty() -> None:
    assert parse_availability_response({"archived_snapshots": {}}) is None
    assert parse_availability_response({}) is None
    assert parse_availability_response("not-json") is None


# ---------------------------------------------------------------------------
# Drift tolerance (pure helper)
# ---------------------------------------------------------------------------


def test_within_drift_tolerance_accepts_same_month() -> None:
    assert _within_drift_tolerance("20240401120000", date(2024, 4, 1)) is True


def test_within_drift_tolerance_accepts_small_drift() -> None:
    assert _within_drift_tolerance("20240801120000", date(2024, 4, 1)) is True


def test_within_drift_tolerance_rejects_large_drift() -> None:
    assert _within_drift_tolerance("20160101120000", date(2024, 4, 1)) is False


def test_within_drift_tolerance_accepts_missing_timestamp() -> None:
    assert _within_drift_tolerance(None, date(2024, 4, 1)) is True


# ---------------------------------------------------------------------------
# Full fetch path
# ---------------------------------------------------------------------------


def _redirect(location: str) -> httpx.Response:
    """302 to the given URL, with the Wayback ``id_`` flag preserved
    (because the caller built it that way)."""
    return httpx.Response(302, headers={"Location": location})


def _ok(body: str) -> httpx.Response:
    return httpx.Response(200, text=body)


@pytest.mark.asyncio
async def test_wayback_emits_historical_signals_from_company_web_jsonld(
    fetch_context,
) -> None:
    """A 6m / 1y / 2y pass should produce three historical anchors, one
    per horizon, when Wayback redirects each bucket timestamp to a real
    snapshot in the same month."""

    body = (
        '<html><head><script type="application/ld+json">'
        '{"@context":"https://schema.org","@type":"Organization",'
        '"name":"Acme","numberOfEmployees":700}'
        "</script></head><body></body></html>"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "web.archive.org/web/" not in url:
            return httpx.Response(404)
        # bucket request: redirect to a "real" snapshot URL in the same
        # calendar month. Request shape: web/{YYYYMM01000000}id_/{origin}
        if "acme.example/about" not in url:
            return httpx.Response(404)
        # Parse bucket timestamp -> build a mid-month "snapshot".
        tail = url.split("/web/")[1]
        bucket_ts = tail.split("id_/")[0]
        if bucket_ts.endswith("01000000"):  # still a bucket request
            snapshot_ts = f"{bucket_ts[:6]}15120000"
            return _redirect(
                f"https://web.archive.org/web/{snapshot_ts}id_/https://acme.example/about"
            )
        return _ok(body)

    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-acme",
        canonical_name="Acme",
        canonical_domain="acme.example",
        linkedin_company_url=None,
    )
    obs = WaybackObserver(
        anchor_month=date(2026, 4, 1),
        include_linkedin=False,
        about_paths=("/about",),
    )
    async with client:
        signals = await obs.fetch_current_anchor(target, context=context)

    assert len(signals) == 3, "expected one signal per horizon"
    assert all(s.source_name is SourceName.wayback for s in signals)
    assert all(s.anchor_type is AnchorType.historical_statement for s in signals)
    assert all(s.headcount_value_kind is HeadcountValueKind.exact for s in signals)
    months = sorted(s.anchor_month for s in signals)
    assert months == [date(2024, 4, 1), date(2025, 4, 1), date(2025, 10, 1)]
    assert all(0.5 <= s.confidence <= 0.6 for s in signals)
    for s in signals:
        assert s.normalized_payload["origin_kind"] == "company_web"
        assert s.normalized_payload["jsonld"] is True
        assert s.headcount_value_point == pytest.approx(700.0)
        # Provenance points at the *actual* snapshot, not the bucket.
        assert s.normalized_payload["snapshot_timestamp"].endswith("15120000")


@pytest.mark.asyncio
async def test_wayback_falls_back_to_text_parser_when_jsonld_missing(
    fetch_context,
) -> None:
    body = "<html><body><p>Our team of 42 builders ship fast.</p></body></html>"

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "web.archive.org/web/" not in url:
            return httpx.Response(404)
        tail = url.split("/web/")[1]
        bucket_ts = tail.split("id_/")[0]
        if bucket_ts.endswith("01000000"):
            snapshot_ts = f"{bucket_ts[:6]}15120000"
            return _redirect(
                f"https://web.archive.org/web/{snapshot_ts}id_/https://acme.example/about"
            )
        return _ok(body)

    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-acme",
        canonical_name="Acme",
        canonical_domain="acme.example",
        linkedin_company_url=None,
    )
    obs = WaybackObserver(
        anchor_month=date(2026, 4, 1),
        include_linkedin=False,
        horizons_months=(12,),
        about_paths=("/about",),
    )
    async with client:
        signals = await obs.fetch_current_anchor(target, context=context)

    assert len(signals) == 1
    s = signals[0]
    assert s.source_name is SourceName.wayback
    assert s.anchor_type is AnchorType.historical_statement
    assert s.headcount_value_point == 42.0
    assert s.confidence == pytest.approx(0.45)
    assert s.normalized_payload["jsonld"] is False


@pytest.mark.asyncio
async def test_wayback_emits_linkedin_historical_from_archived_jsonld(
    fetch_context,
) -> None:
    body = (
        '<html><head><script type="application/ld+json">'
        '{"@type":"Organization","name":"Acme",'
        '"numberOfEmployees":{"@type":"QuantitativeValue",'
        '"minValue":51,"maxValue":200}}'
        "</script></head></html>"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "web.archive.org/web/" not in url or "linkedin.com" not in url:
            return httpx.Response(404)
        tail = url.split("/web/")[1]
        bucket_ts = tail.split("id_/")[0]
        if bucket_ts.endswith("01000000"):
            snapshot_ts = f"{bucket_ts[:6]}15120000"
            return _redirect(
                f"https://web.archive.org/web/{snapshot_ts}id_/https://www.linkedin.com/company/acme/"
            )
        return _ok(body)

    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-acme",
        canonical_name="Acme",
        canonical_domain=None,
        linkedin_company_url="https://www.linkedin.com/company/acme/",
    )
    obs = WaybackObserver(
        anchor_month=date(2026, 4, 1),
        horizons_months=(6,),
        include_company_web=False,
    )
    async with client:
        signals = await obs.fetch_current_anchor(target, context=context)

    assert len(signals) == 1
    s = signals[0]
    assert s.source_name is SourceName.wayback
    assert s.anchor_type is AnchorType.historical_statement
    assert s.headcount_value_kind is HeadcountValueKind.bucket
    assert s.headcount_value_min == 51
    assert s.headcount_value_max == 200
    assert s.normalized_payload["origin_kind"] == "linkedin_public"


@pytest.mark.asyncio
async def test_wayback_skips_when_snapshot_returns_404(fetch_context) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-missing",
        canonical_name="Missing",
        canonical_domain="missing.example",
        linkedin_company_url=None,
    )
    async with client:
        signals = await WaybackObserver(
            anchor_month=date(2026, 4, 1),
            include_linkedin=False,
            about_paths=("/about",),
        ).fetch_current_anchor(target, context=context)
    assert signals == []


@pytest.mark.asyncio
async def test_wayback_rejects_snapshot_with_excessive_drift(fetch_context) -> None:
    """A 2026 target horizon that only resolves to a 2016 snapshot must
    be discarded: archives older than 12 months mean the headcount
    value doesn't represent the intended anchor month."""

    body = (
        '<script type="application/ld+json">'
        '{"@type":"Organization","numberOfEmployees":50}'
        "</script>"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "web.archive.org/web/" not in url:
            return httpx.Response(404)
        tail = url.split("/web/")[1]
        bucket_ts = tail.split("id_/")[0]
        if bucket_ts.endswith("01000000"):
            # Wayback says "closest I have is from 2016" — way out of
            # drift tolerance.
            return _redirect(
                "https://web.archive.org/web/20160101120000id_/https://acme.example/about"
            )
        return _ok(body)

    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-old",
        canonical_name="Old",
        canonical_domain="acme.example",
        linkedin_company_url=None,
    )
    obs = WaybackObserver(
        anchor_month=date(2026, 4, 1),
        horizons_months=(12,),
        include_linkedin=False,
        about_paths=("/about",),
    )
    async with client:
        signals = await obs.fetch_current_anchor(target, context=context)
    assert signals == []


@pytest.mark.asyncio
async def test_wayback_returns_empty_when_target_has_neither_domain_nor_linkedin(
    fetch_context,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
        raise AssertionError("handler should not run for empty target")

    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-empty",
        canonical_name="Empty",
        canonical_domain=None,
        linkedin_company_url=None,
    )
    async with client:
        signals = await WaybackObserver().fetch_current_anchor(target, context=context)
    assert signals == []


@pytest.mark.asyncio
async def test_wayback_dedups_when_same_snapshot_satisfies_multiple_horizons(
    fetch_context,
) -> None:
    """If Wayback redirects every horizon bucket to the *same* snapshot
    timestamp, only one signal should be emitted: the dedup key is
    ``(origin_url, actual_snapshot_timestamp)``."""

    body = (
        '<script type="application/ld+json">'
        '{"@type":"Organization","numberOfEmployees":315}'
        "</script>"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "web.archive.org/web/" not in url:
            return httpx.Response(404)
        tail = url.split("/web/")[1]
        bucket_ts = tail.split("id_/")[0]
        # Always redirect to exactly the same snapshot, regardless of
        # which bucket we asked for. This is the sparse-coverage case.
        if bucket_ts.endswith("01000000"):
            return _redirect(
                "https://web.archive.org/web/20251015120000id_/https://acme.example/about"
            )
        return _ok(body)

    client, context = fetch_context(handler)
    target = CompanyTarget(
        company_id="c-acme",
        canonical_name="Acme",
        canonical_domain="acme.example",
        linkedin_company_url=None,
    )
    async with client:
        signals = await WaybackObserver(
            anchor_month=date(2026, 4, 1),
            include_linkedin=False,
            about_paths=("/about",),
        ).fetch_current_anchor(target, context=context)

    # Three horizon buckets all redirect to 2025-10-15. The 6m horizon
    # target is 2025-10 (0 drift), the 1y target is 2025-04 (~6m drift),
    # and the 2y target is 2024-04 (~18m drift -> rejected). So two
    # horizons pass the drift gate; dedup collapses them to one.
    assert len(signals) == 1
    s = signals[0]
    assert s.headcount_value_point == 315.0
    assert s.normalized_payload["snapshot_timestamp"] == "20251015120000"
