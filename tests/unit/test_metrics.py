"""Prometheus metrics registry tests."""

from __future__ import annotations

from prometheus_client import generate_latest

from headcount.utils.metrics import (
    REGISTRY,
    benchmark_delta_pct,
    company_stage_total,
    estimate_band_total,
    linkedin_gate_total,
    runs_total,
    source_fetch_latency_seconds,
    source_fetch_total,
)


def test_registry_exposes_expected_series() -> None:
    runs_total.labels(kind="full", status="started").inc()
    company_stage_total.labels(stage="canonicalize", status="succeeded").inc()
    source_fetch_total.labels(source="linkedin_public", outcome="gated").inc()
    linkedin_gate_total.labels(reason="auth_wall").inc()
    estimate_band_total.labels(band="medium").inc()
    benchmark_delta_pct.labels(provider="linkedin", window="1y").observe(0.1)
    source_fetch_latency_seconds.labels(source="company_web").observe(0.05)

    payload = generate_latest(REGISTRY).decode("utf-8")
    for series in (
        "headcount_runs_total",
        "headcount_company_stage_total",
        "headcount_source_fetch_total",
        "headcount_linkedin_gate_total",
        "headcount_estimate_band_total",
        "headcount_benchmark_delta_pct",
        "headcount_source_fetch_latency_seconds",
    ):
        assert series in payload
