"""Shared observer-test plumbing.

Centralizes the :class:`httpx.MockTransport` wiring so each observer
test can focus on fixture -> signal assertions rather than repeating
HTTP setup.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from headcount.db.enums import SourceName
from headcount.ingest.base import FetchContext
from headcount.ingest.http import FileCache, HttpClient, HttpClientConfig

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "fixtures"


def fixture_path(name: str) -> Path:
    return FIXTURE_DIR / name


def fixture_text(name: str) -> str:
    return fixture_path(name).read_text(encoding="utf-8")


def fixture_json(name: str) -> dict:
    return json.loads(fixture_text(name))


Handler = Callable[[httpx.Request], httpx.Response]


def make_transport(handler: Handler) -> httpx.MockTransport:
    return httpx.MockTransport(handler)


@pytest.fixture()
def cache(tmp_path: Path) -> FileCache:
    return FileCache(tmp_path / "cache")


@pytest.fixture()
def make_client(cache: FileCache):
    def _make(
        handler: Handler, *, configs: dict[SourceName, HttpClientConfig] | None = None
    ) -> HttpClient:
        return HttpClient(
            cache=cache,
            configs=configs or {},
            transport=make_transport(handler),
        )

    return _make


@pytest.fixture()
def fetch_context(make_client):
    """Returns a factory that yields a ready-to-use FetchContext."""

    def _factory(handler: Handler) -> tuple[HttpClient, FetchContext]:
        client = make_client(handler)
        context = FetchContext(run_id="test-run", http=client, live=True)
        return client, context

    return _factory
