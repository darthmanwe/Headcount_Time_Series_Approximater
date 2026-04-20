"""Phase 0 smoke tests."""

from __future__ import annotations

import importlib
import subprocess
import sys

from apps.api.main import create_app
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from headcount import __version__
from headcount.cli import app as cli_app


def test_package_imports() -> None:
    module = importlib.import_module("headcount")
    assert module.__version__ == __version__


def test_submodule_imports() -> None:
    for name in (
        "headcount.config",
        "headcount.db",
        "headcount.models",
        "headcount.schemas",
        "headcount.clients",
        "headcount.ingest",
        "headcount.parsers",
        "headcount.resolution",
        "headcount.estimation",
        "headcount.review",
        "headcount.serving",
        "headcount.utils",
        "headcount.utils.logging",
        "headcount.utils.metrics",
        "headcount.utils.time",
    ):
        importlib.import_module(name)


def test_cli_help_lists_all_stage_commands() -> None:
    runner = CliRunner()
    result = runner.invoke(cli_app, ["--help"])
    assert result.exit_code == 0, result.output
    for command in (
        "seed-companies",
        "load-benchmarks",
        "canonicalize",
        "collect-anchors",
        "collect-employment",
        "estimate-series",
        "score-confidence",
        "export-growth",
        "compare-benchmark",
        "rerun-company",
        "status",
        "version",
        "config",
    ):
        assert command in result.output


def test_cli_version_command() -> None:
    runner = CliRunner()
    result = runner.invoke(cli_app, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_cli_stub_returns_exit_code_two() -> None:
    runner = CliRunner()
    result = runner.invoke(cli_app, ["estimate-series", "--company-batch", "smoke"])
    assert result.exit_code == 2


def test_api_healthz_and_metrics() -> None:
    app = create_app()
    client = TestClient(app)

    health = client.get("/healthz")
    assert health.status_code == 200
    body = health.json()
    assert body["status"] == "ok"
    assert body["version"] == __version__

    metrics = client.get("/metrics")
    assert metrics.status_code == 200
    assert b"headcount_runs_total" in metrics.content


def test_hc_help_entry_point_runs() -> None:
    """Running ``python -m headcount.cli --help`` must exit cleanly."""
    result = subprocess.run(
        [sys.executable, "-m", "headcount.cli", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "hc" in result.stdout.lower() or "usage" in result.stdout.lower()
