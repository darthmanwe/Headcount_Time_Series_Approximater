"""Structured logging tests."""

from __future__ import annotations

import json

import pytest

from headcount.utils.logging import bind_context, clear_context, configure_logging, get_logger


def test_get_logger_is_structlog_bound() -> None:
    log = get_logger("test")
    assert hasattr(log, "bind")


def test_bound_context_emits_keys(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOG_FORMAT", "json")
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    configure_logging(force=True)
    log = get_logger("headcount.test")
    bind_context(run_id="r1", company_id="c1", stage="canonicalize")
    try:
        log.info("event_fired", extra_field=42)
    finally:
        clear_context()

    out = capsys.readouterr().out.strip().splitlines()
    assert out, "expected at least one JSON log line on stdout"
    payload = json.loads(out[-1])
    assert payload["event"] == "event_fired"
    assert payload["run_id"] == "r1"
    assert payload["company_id"] == "c1"
    assert payload["stage"] == "canonicalize"
    assert payload["extra_field"] == 42
    assert payload["level"] == "info"
    assert "timestamp" in payload


def test_clear_context_removes_bindings(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOG_FORMAT", "json")
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    configure_logging(force=True)
    log = get_logger("headcount.test")

    bind_context(run_id="r1")
    log.info("with_ctx")
    clear_context()
    log.info("without_ctx")

    lines = capsys.readouterr().out.strip().splitlines()
    first = json.loads(lines[-2])
    second = json.loads(lines[-1])
    assert first.get("run_id") == "r1"
    assert "run_id" not in second
