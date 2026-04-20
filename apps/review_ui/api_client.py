"""Thin typed httpx client for the review UI.

The review UI is a pure FastAPI consumer - it never imports SQLAlchemy
models or opens a DB session. Every screen goes through this client,
which:

- keeps a single :class:`httpx.Client` instance per process (reused by
  Streamlit's session state),
- normalizes non-2xx responses into :class:`ApiError` with the server
  detail string,
- parses JSON into plain dicts/lists (no Pydantic on the UI side - the
  server is the contract, and we don't want to double-model the schema).

Kept intentionally dependency-light so it can be unit-tested with
``httpx.MockTransport`` without spinning up the API.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import httpx

DEFAULT_TIMEOUT = 10.0


class ApiError(RuntimeError):
    """Non-2xx response from the headcount API."""

    def __init__(self, status_code: int, detail: str, url: str) -> None:
        super().__init__(f"{status_code} on {url}: {detail}")
        self.status_code = status_code
        self.detail = detail
        self.url = url


@dataclass(frozen=True)
class ClientConfig:
    base_url: str = "http://127.0.0.1:8000"
    timeout: float = DEFAULT_TIMEOUT


class HeadcountApiClient:
    """Small facade over the FastAPI surface.

    Methods intentionally return ``dict[str, Any]`` / ``list[dict]`` so
    the UI layer can render tables and JSON blobs without any
    re-modeling. The server's ``_StrictModel`` responses are the schema.
    """

    def __init__(
        self,
        config: ClientConfig | None = None,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.config = config or ClientConfig()
        self._client = httpx.Client(
            base_url=self.config.base_url,
            timeout=self.config.timeout,
            transport=transport,
            headers={"Accept": "application/json"},
        )

    # ----- lifecycle -----------------------------------------------------

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> HeadcountApiClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # ----- internals -----------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | list[tuple[str, Any]] | None = None,
        json_body: Any | None = None,
    ) -> Any:
        resp = self._client.request(method, path, params=params, json=json_body)
        if resp.status_code >= 400:
            try:
                detail = resp.json().get("detail", resp.text)
            except ValueError:
                detail = resp.text
            if not isinstance(detail, str):
                detail = str(detail)
            raise ApiError(resp.status_code, detail, str(resp.request.url))
        if resp.status_code == 204 or not resp.content:
            return None
        return resp.json()

    # ----- infra ---------------------------------------------------------

    def healthz(self) -> dict[str, Any]:
        return dict(self._request("GET", "/healthz"))

    # ----- companies -----------------------------------------------------

    def list_companies(
        self, *, limit: int = 100, offset: int = 0
    ) -> list[dict[str, Any]]:
        return list(
            self._request(
                "GET",
                "/companies",
                params={"limit": limit, "offset": offset},
            )
        )

    def get_company(self, company_id: str) -> dict[str, Any]:
        return dict(self._request("GET", f"/companies/{company_id}"))

    def get_company_series(
        self,
        company_id: str,
        *,
        start: str | None = None,
        end: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        return dict(
            self._request("GET", f"/companies/{company_id}/series", params=params)
        )

    def get_company_growth(self, company_id: str) -> dict[str, Any]:
        return dict(self._request("GET", f"/companies/{company_id}/growth"))

    def get_company_evidence(
        self,
        company_id: str,
        month: str,
        *,
        version_id: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if version_id:
            params["version_id"] = version_id
        return dict(
            self._request(
                "GET",
                f"/companies/{company_id}/months/{month}/evidence",
                params=params,
            )
        )

    # ----- runs ----------------------------------------------------------

    def list_runs(
        self, *, limit: int = 50, kind: str | None = None
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if kind:
            params["kind"] = kind
        return list(self._request("GET", "/runs", params=params))

    def get_run(self, run_id: str) -> dict[str, Any]:
        return dict(self._request("GET", f"/runs/{run_id}"))

    def status_summary(self) -> dict[str, Any]:
        return dict(self._request("GET", "/status/summary"))

    # ----- review queue --------------------------------------------------

    def list_review_queue(
        self,
        *,
        status: str | None = "open",
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if status:
            params["status"] = status
        return list(self._request("GET", "/review-queue", params=params))

    def transition_review_item(
        self,
        item_id: str,
        *,
        status: str,
        assigned_to: str | None = None,
        note: str | None = None,
        actor_id: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"status": status}
        if assigned_to is not None:
            body["assigned_to"] = assigned_to
        if note is not None:
            body["note"] = note
        if actor_id is not None:
            body["actor_id"] = actor_id
        return dict(
            self._request(
                "POST", f"/review-queue/{item_id}/transition", json_body=body
            )
        )

    # ----- overrides -----------------------------------------------------

    def list_overrides(
        self,
        *,
        company_id: str | None = None,
        active_only: bool = True,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"active_only": active_only}
        if company_id:
            params["company_id"] = company_id
        return list(self._request("GET", "/overrides", params=params))

    def create_override(
        self,
        *,
        company_id: str,
        field_name: str,
        payload: dict[str, Any],
        reason: str | None = None,
        entered_by: str | None = None,
        expires_at: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "company_id": company_id,
            "field_name": field_name,
            "payload": payload,
        }
        if reason is not None:
            body["reason"] = reason
        if entered_by is not None:
            body["entered_by"] = entered_by
        if expires_at is not None:
            body["expires_at"] = expires_at
        return dict(self._request("POST", "/overrides", json_body=body))

    # ----- benchmarks ----------------------------------------------------

    def benchmark_comparison(
        self,
        *,
        company_ids: Iterable[str] | None = None,
        threshold: float = 0.25,
    ) -> dict[str, Any]:
        params: list[tuple[str, Any]] = [("threshold", threshold)]
        if company_ids:
            for cid in company_ids:
                params.append(("company_id", cid))
        return dict(self._request("GET", "/benchmarks/comparison", params=params))

    # ----- audit ---------------------------------------------------------

    def list_audit(
        self,
        *,
        target_type: str | None = None,
        target_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if target_type:
            params["target_type"] = target_type
        if target_id:
            params["target_id"] = target_id
        return list(self._request("GET", "/audit", params=params))


__all__ = ["ApiError", "ClientConfig", "HeadcountApiClient"]
