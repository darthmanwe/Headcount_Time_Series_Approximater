"""Prometheus counters and histograms for pipeline observability.

Metric set is intentionally small and stable; richer breakdowns live in the
per-run artifact parquet written by the orchestrator. The ASGI mount is
added by ``apps/api/main.py`` at ``/metrics``.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Histogram

REGISTRY = CollectorRegistry()

runs_total = Counter(
    "headcount_runs_total",
    "Number of estimator runs started, by kind and status.",
    labelnames=("kind", "status"),
    registry=REGISTRY,
)

company_stage_total = Counter(
    "headcount_company_stage_total",
    "Per-company stage transitions.",
    labelnames=("stage", "status"),
    registry=REGISTRY,
)

source_fetch_total = Counter(
    "headcount_source_fetch_total",
    "Source adapter fetch outcomes.",
    labelnames=("source", "outcome"),
    registry=REGISTRY,
)

linkedin_gate_total = Counter(
    "headcount_linkedin_gate_total",
    "Logged-out public LinkedIn fail-closed gate detections by reason.",
    labelnames=("reason",),
    registry=REGISTRY,
)

estimate_band_total = Counter(
    "headcount_estimate_band_total",
    "Final estimates emitted, by confidence band.",
    labelnames=("band",),
    registry=REGISTRY,
)

benchmark_delta_pct = Histogram(
    "headcount_benchmark_delta_pct",
    "Signed delta percentage between system output and benchmark per provider.",
    labelnames=("provider", "window"),
    buckets=(-1.0, -0.5, -0.25, -0.15, -0.05, 0.0, 0.05, 0.15, 0.25, 0.5, 1.0),
    registry=REGISTRY,
)

source_fetch_latency_seconds = Histogram(
    "headcount_source_fetch_latency_seconds",
    "Source adapter fetch latency in seconds.",
    labelnames=("source",),
    registry=REGISTRY,
)
