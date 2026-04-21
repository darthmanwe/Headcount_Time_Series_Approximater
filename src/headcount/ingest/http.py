"""Async HTTP client with persistent file cache.

Wraps ``httpx.AsyncClient`` so every Phase 4+ observer gets the same
behavior for free:

- Conservative default timeouts and retries (retries are off; the
  orchestrator retries via the circuit breaker, not the HTTP layer).
- Polite ``User-Agent`` per source, picked from :class:`HttpClientConfig`.
- File-based cache keyed on ``(method, url, body-hash, headers-that-matter)``.
  Payloads are stored as JSON envelopes with status + headers + body +
  fetched_at so adapters can cheaply replay their last successful fetch.
- Prometheus counters for hit / miss / error and a structured log line
  per outbound call.

We avoid a full HTTP semantic cache (we ignore Vary/Cache-Control) because
our reuse horizon is a single run or a day-scale refresh - the TTL is
short and deterministic. That keeps the cache bytes predictable for
audits.

The fake transport used in tests/fixtures is :class:`httpx.MockTransport`,
so no extra dependency. Fixture recording is deterministic: the cache
filename is the SHA-256 of the normalised request.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from headcount.db.enums import SourceName
from headcount.utils.logging import get_logger
from headcount.utils.metrics import source_fetch_total

if TYPE_CHECKING:
    from headcount.ingest.raw_response_store import RawResponseSink

_log = get_logger("headcount.ingest.http")

_DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=10.0, pool=10.0)
_DEFAULT_UA = "Headcount-Estimator/0.1 (+internal-use; contact@example.com)"


@dataclass(frozen=True, slots=True)
class HttpClientConfig:
    """Per-source HTTP configuration."""

    user_agent: str = _DEFAULT_UA
    timeout: httpx.Timeout = field(default_factory=lambda: _DEFAULT_TIMEOUT)
    max_concurrency: int = 4
    cache_ttl_seconds: int = 24 * 3600
    default_headers: Mapping[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class CachedResponse:
    """Lightweight materialized view of a cached HTTP response."""

    status_code: int
    headers: dict[str, str]
    text: str
    url: str
    fetched_at: float
    from_cache: bool = True

    @classmethod
    def from_httpx(cls, response: httpx.Response, *, from_cache: bool = False) -> CachedResponse:
        return cls(
            status_code=response.status_code,
            headers=dict(response.headers),
            text=response.text,
            url=str(response.request.url),
            fetched_at=time.time(),
            from_cache=from_cache,
        )

    def to_envelope(self) -> dict[str, Any]:
        return {
            "status_code": self.status_code,
            "headers": self.headers,
            "text": self.text,
            "url": self.url,
            "fetched_at": self.fetched_at,
        }

    @classmethod
    def from_envelope(cls, envelope: Mapping[str, Any]) -> CachedResponse:
        return cls(
            status_code=int(envelope["status_code"]),
            headers=dict(envelope.get("headers", {})),
            text=str(envelope.get("text", "")),
            url=str(envelope.get("url", "")),
            fetched_at=float(envelope.get("fetched_at", 0.0)),
            from_cache=True,
        )


class FileCache:
    """Deterministic file cache for HTTP envelopes.

    Keys are SHA-256 over the canonical request tuple so the cache is
    content-addressable and immune to parameter ordering. ``lookup``
    returns ``None`` on miss or when the file has expired.
    """

    def __init__(self, root: Path) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def build_key(
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: bytes | str | None = None,
        headers_included: Mapping[str, str] | None = None,
    ) -> str:
        hasher = hashlib.sha256()
        hasher.update(method.upper().encode())
        hasher.update(b"\0")
        hasher.update(url.encode())
        hasher.update(b"\0")
        if params:
            for k in sorted(params):
                hasher.update(str(k).encode())
                hasher.update(b"=")
                hasher.update(str(params[k]).encode())
                hasher.update(b"&")
        hasher.update(b"\0")
        if body is not None:
            if isinstance(body, str):
                body = body.encode()
            hasher.update(body)
        hasher.update(b"\0")
        if headers_included:
            for k in sorted(headers_included):
                hasher.update(str(k).lower().encode())
                hasher.update(b":")
                hasher.update(str(headers_included[k]).encode())
                hasher.update(b"\n")
        return hasher.hexdigest()

    def path_for(self, source_name: SourceName, key: str) -> Path:
        bucket = self._root / source_name.value
        bucket.mkdir(parents=True, exist_ok=True)
        return bucket / f"{key}.json"

    def lookup(
        self,
        source_name: SourceName,
        key: str,
        *,
        ttl_seconds: int,
    ) -> CachedResponse | None:
        path = self.path_for(source_name, key)
        if not path.exists():
            return None
        try:
            envelope = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        fetched_at = float(envelope.get("fetched_at", 0.0))
        if ttl_seconds > 0 and time.time() - fetched_at > ttl_seconds:
            return None
        return CachedResponse.from_envelope(envelope)

    def store(
        self,
        source_name: SourceName,
        key: str,
        response: CachedResponse,
    ) -> None:
        path = self.path_for(source_name, key)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(response.to_envelope()), encoding="utf-8")
        tmp.replace(path)


class HttpClient:
    """Polite, cached, bounded-concurrency HTTP client.

    One instance per run. ``transport`` is injectable so tests can pass a
    :class:`httpx.MockTransport` that serves canned responses; production
    code leaves it ``None`` and the client builds a real async transport.
    """

    def __init__(
        self,
        *,
        cache: FileCache,
        configs: Mapping[SourceName, HttpClientConfig] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        raw_response_sink: RawResponseSink | None = None,
    ) -> None:
        self._cache = cache
        self._configs = dict(configs or {})
        self._default_config = HttpClientConfig()
        self._semaphores: dict[SourceName, asyncio.Semaphore] = {}
        self._client: httpx.AsyncClient | None = None
        self._transport = transport
        # Optional write-through archive. When set, every live (non
        # cache-hit) response is mirrored into the raw_response table
        # so a later ``reparse_raw_responses`` run can reprocess the
        # bytes without re-fetching the upstream URL. Sink errors are
        # swallowed inside the sink; the fetch path never fails on
        # archival issues.
        self._raw_response_sink = raw_response_sink

    @property
    def cache(self) -> FileCache:
        return self._cache

    async def __aenter__(self) -> HttpClient:
        self._client = httpx.AsyncClient(
            transport=self._transport,
            timeout=_DEFAULT_TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": _DEFAULT_UA},
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _config_for(self, source_name: SourceName) -> HttpClientConfig:
        return self._configs.get(source_name, self._default_config)

    def _semaphore_for(self, source_name: SourceName) -> asyncio.Semaphore:
        sem = self._semaphores.get(source_name)
        if sem is None:
            sem = asyncio.Semaphore(self._config_for(source_name).max_concurrency)
            self._semaphores[source_name] = sem
        return sem

    async def get(
        self,
        source_name: SourceName,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        cache: bool = True,
    ) -> CachedResponse:
        return await self._request(
            "GET",
            source_name,
            url,
            params=params,
            headers=headers,
            cache=cache,
        )

    async def post(
        self,
        source_name: SourceName,
        url: str,
        *,
        data: Mapping[str, Any] | str | None = None,
        headers: Mapping[str, str] | None = None,
        cache: bool = True,
    ) -> CachedResponse:
        return await self._request(
            "POST",
            source_name,
            url,
            data=data,
            headers=headers,
            cache=cache,
        )

    async def _request(
        self,
        method: str,
        source_name: SourceName,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        data: Mapping[str, Any] | str | None = None,
        headers: Mapping[str, str] | None = None,
        cache: bool = True,
    ) -> CachedResponse:
        config = self._config_for(source_name)
        effective_headers: dict[str, str] = {"User-Agent": config.user_agent}
        for key, value in config.default_headers.items():
            effective_headers[key] = value
        if headers:
            for key, value in headers.items():
                effective_headers[key] = value

        body_bytes: bytes | None = None
        if isinstance(data, str):
            body_bytes = data.encode()
        elif isinstance(data, Mapping):
            body_bytes = json.dumps(data, sort_keys=True).encode()

        cache_key = FileCache.build_key(
            method, url, params=params, body=body_bytes, headers_included=None
        )
        if cache and config.cache_ttl_seconds > 0:
            cached = self._cache.lookup(
                source_name, cache_key, ttl_seconds=config.cache_ttl_seconds
            )
            if cached is not None:
                source_fetch_total.labels(source=source_name.value, outcome="cache_hit").inc()
                _log.debug("http_cache_hit", source=source_name.value, url=url)
                return cached

        if self._client is None:
            raise RuntimeError("HttpClient must be used as an async context manager")

        async with self._semaphore_for(source_name):
            request = self._client.build_request(
                method,
                url,
                params=params,
                content=body_bytes if method == "POST" else None,
                headers=effective_headers,
            )
            try:
                response = await self._client.send(request)
            except httpx.HTTPError as exc:
                source_fetch_total.labels(source=source_name.value, outcome="error").inc()
                _log.warning(
                    "http_fetch_error",
                    source=source_name.value,
                    url=url,
                    error=repr(exc),
                )
                raise

        outcome = "ok" if response.status_code < 400 else "error"
        source_fetch_total.labels(source=source_name.value, outcome=outcome).inc()
        if logging.getLogger("headcount.ingest.http").isEnabledFor(logging.DEBUG):
            _log.debug(
                "http_fetch",
                source=source_name.value,
                method=method,
                url=url,
                status=response.status_code,
            )

        cached_response = CachedResponse.from_httpx(response)
        if cache and response.status_code < 400 and config.cache_ttl_seconds > 0:
            self._cache.store(source_name, cache_key, cached_response)
        # Write-through to the raw-response archive. Only mirrors
        # successful live responses - redirects and 5xx pages are
        # already covered by the file cache for intra-run replay but
        # would bloat the long-lived DB without adding parse value.
        if (
            self._raw_response_sink is not None
            and response.status_code < 400
            and cached_response.text
        ):
            self._archive_response(
                source_name=source_name,
                method=method,
                response=cached_response,
            )
        return cached_response

    def _archive_response(
        self,
        *,
        source_name: SourceName,
        method: str,
        response: CachedResponse,
    ) -> None:
        """Best-effort write to the raw-response archive.

        Delegated to the sink which handles its own exceptions. This
        wrapper keeps the import local so a client built without
        ORM access (eg unit tests that stub out the DB) stays fast.
        """

        if self._raw_response_sink is None:
            return
        try:
            from headcount.ingest.raw_response_store import RawResponseWrite

            payload = RawResponseWrite(
                url=response.url,
                method=method,
                status_code=response.status_code,
                body_text=response.text,
                headers=response.headers,
                source_hint=source_name.value,
                fetched_at=datetime.now(tz=UTC),
            )
            self._raw_response_sink.write(payload)
        except Exception as exc:  # pragma: no cover - defensive
            _log.debug(
                "raw_response_archive_skipped",
                source=source_name.value,
                url=response.url,
                error=repr(exc),
            )
