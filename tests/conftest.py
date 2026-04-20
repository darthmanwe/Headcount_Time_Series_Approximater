"""Shared test fixtures.

Keeps tests fully offline and deterministic. The ``hc_settings`` fixture
points data directories at a per-test ``tmp_path`` so nothing leaks.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from headcount.config import settings as settings_module
from headcount.config.settings import Settings


@pytest.fixture(autouse=True)
def _isolate_settings_cache() -> Iterator[None]:
    """Clear the cached ``Settings`` between tests."""
    settings_module.get_settings.cache_clear()
    yield
    settings_module.get_settings.cache_clear()


@pytest.fixture()
def hc_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    """Construct a ``Settings`` instance backed by an isolated tmp dir."""
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("LOG_FORMAT", "console")
    monkeypatch.setenv("DB_URL", f"sqlite:///{tmp_path / 'hc.sqlite'}")
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "outputs" / "hc.duckdb"))
    monkeypatch.setenv("CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("RUN_ARTIFACT_DIR", str(tmp_path / "outputs" / "runs"))
    monkeypatch.setenv("SEED_DIR", str(tmp_path / "seeds"))
    monkeypatch.setenv("FIXTURE_DIR", str(tmp_path / "fixtures"))
    settings = Settings()
    settings.ensure_runtime_dirs()
    return settings
