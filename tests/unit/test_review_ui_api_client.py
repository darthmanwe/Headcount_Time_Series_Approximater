"""Tests for the Streamlit review UI's httpx client.

The client is the *only* thing the UI pages cannot mock away, so we
exercise it end-to-end against an in-process ``httpx.MockTransport``.
This covers: request shape (path, method, query params, body), response
parsing (dict/list/None), and error translation to :class:`ApiError`.
"""

from __future__ import annotations

import json

import httpx
import pytest
from apps.review_ui.api_client import (
    ApiError,
    ClientConfig,
    HeadcountApiClient,
)


def _make_client(handler):  # type: ignore[no-untyped-def]
    transport = httpx.MockTransport(handler)
    return HeadcountApiClient(
        ClientConfig(base_url="http://api.local"),
        transport=transport,
    )


def test_healthz_parses_json() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/healthz"
        return httpx.Response(200, json={"status": "ok", "api_version": "api_v1"})

    with _make_client(handler) as client:
        out = client.healthz()
    assert out["status"] == "ok"


def test_list_companies_sends_limit_offset() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["query"] = request.url.query.decode()
        return httpx.Response(200, json=[{"id": "c1", "canonical_name": "Foo"}])

    with _make_client(handler) as client:
        rows = client.list_companies(limit=25, offset=50)
    assert rows[0]["canonical_name"] == "Foo"
    assert "limit=25" in seen["query"]
    assert "offset=50" in seen["query"]
    assert seen["path"] == "/companies"


def test_get_company_series_passes_month_filters() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["query"] = request.url.query.decode()
        return httpx.Response(
            200,
            json={
                "company": {
                    "id": "c1",
                    "canonical_name": "Foo",
                    "canonical_domain": None,
                    "priority_tier": "P1",
                    "status": "active",
                },
                "estimate_version_id": "v1",
                "months": [],
            },
        )

    with _make_client(handler) as client:
        out = client.get_company_series("c1", start="2024-01", end="2024-06")
    assert captured["path"] == "/companies/c1/series"
    assert "start=2024-01" in captured["query"]
    assert "end=2024-06" in captured["query"]
    assert out["estimate_version_id"] == "v1"


def test_get_company_series_omits_missing_params() -> None:
    seen_query: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_query.append(request.url.query.decode())
        return httpx.Response(
            200,
            json={
                "company": {
                    "id": "c1",
                    "canonical_name": "Foo",
                    "canonical_domain": None,
                    "priority_tier": "P1",
                    "status": "active",
                },
                "estimate_version_id": None,
                "months": [],
            },
        )

    with _make_client(handler) as client:
        client.get_company_series("c1")
    assert seen_query == [""]


def test_transition_review_item_posts_expected_body() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "id": "r1",
                "company_id": "c1",
                "canonical_name": "Foo",
                "review_reason": "low_confidence",
                "priority": 80,
                "status": "assigned",
                "assigned_to": "alice",
            },
        )

    with _make_client(handler) as client:
        out = client.transition_review_item(
            "r1", status="assigned", assigned_to="alice", note="claimed"
        )
    assert captured["method"] == "POST"
    assert captured["path"] == "/review-queue/r1/transition"
    body = captured["body"]
    assert isinstance(body, dict)
    assert body == {"status": "assigned", "assigned_to": "alice", "note": "claimed"}
    assert out["status"] == "assigned"


def test_create_override_posts_payload_verbatim() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        captured["path"] = request.url.path
        return httpx.Response(
            201,
            json={
                "id": "o1",
                "company_id": "c1",
                "field_name": "current_anchor",
                "payload": {"value_point": 1000},
                "reason": "manual",
                "entered_by": "alice",
                "expires_at": None,
                "created_at": "2026-01-01T00:00:00+00:00",
            },
        )

    with _make_client(handler) as client:
        out = client.create_override(
            company_id="c1",
            field_name="current_anchor",
            payload={"value_point": 1000},
            reason="manual",
            entered_by="alice",
        )
    body = captured["body"]
    assert isinstance(body, dict)
    assert body["company_id"] == "c1"
    assert body["payload"] == {"value_point": 1000}
    assert out["id"] == "o1"


def test_benchmark_comparison_repeats_company_id_param() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["query"] = request.url.query.decode()
        return httpx.Response(200, json={"companies": [], "disagreements_total": 0})

    with _make_client(handler) as client:
        client.benchmark_comparison(
            company_ids=["c1", "c2"], threshold=0.3
        )
    # ``repeat`` the company_id key for each ID so FastAPI sees a list.
    assert captured["query"].count("company_id=") == 2
    assert "company_id=c1" in captured["query"]
    assert "company_id=c2" in captured["query"]
    assert "threshold=0.3" in captured["query"]


def test_api_error_carries_detail_and_status() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "company not found: nope"})

    with _make_client(handler) as client:
        with pytest.raises(ApiError) as err:
            client.get_company("nope")
    assert err.value.status_code == 404
    assert "company not found" in err.value.detail


def test_api_error_falls_back_to_text_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="internal oops")

    with _make_client(handler) as client:
        with pytest.raises(ApiError) as err:
            client.healthz()
    assert err.value.status_code == 500
    assert "internal oops" in err.value.detail
