"""Settings contract tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from headcount.config.settings import Settings


def test_settings_defaults_have_required_versions() -> None:
    settings = Settings()
    assert settings.method_version == "hc-v1"
    assert settings.anchor_policy_version == "anchor-v1"
    assert settings.coverage_curve_version == "coverage-v1"


def test_settings_sample_floors_are_monotonic() -> None:
    settings = Settings()
    assert (
        settings.min_current_profile_sample_6m
        <= settings.min_current_profile_sample_1y
        <= settings.min_current_profile_sample_2y
    )


def test_settings_benchmark_thresholds_in_unit_interval() -> None:
    settings = Settings()
    assert 0.0 <= settings.benchmark_disagreement_pct_current <= 1.0
    assert 0.0 <= settings.benchmark_disagreement_pct_2y <= 1.0


def test_log_level_validation_rejects_garbage(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOG_LEVEL", "NOPE")
    with pytest.raises(ValueError):
        Settings()


def test_ensure_runtime_dirs_creates_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CACHE_DIR", str(tmp_path / "c"))
    monkeypatch.setenv("RUN_ARTIFACT_DIR", str(tmp_path / "r"))
    monkeypatch.setenv("SEED_DIR", str(tmp_path / "s"))
    monkeypatch.setenv("FIXTURE_DIR", str(tmp_path / "f"))
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "d" / "x.duckdb"))
    settings = Settings()
    settings.ensure_runtime_dirs()
    for sub in ("c", "r", "s", "f", "d"):
        assert (tmp_path / sub).exists()
