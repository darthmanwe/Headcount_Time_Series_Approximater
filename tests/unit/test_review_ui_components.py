"""Tests for the review UI's pure rendering helpers.

These helpers take JSON-shaped API responses and return pandas
DataFrames / Plotly figures, so they can be exercised without
Streamlit running. That's where the real display logic lives; the
pages themselves just glue these into `st.dataframe` / `st.plotly_chart`.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from apps.review_ui.components import (
    benchmark_comparison_to_frame,
    build_series_figure,
    overrides_to_frame,
    review_queue_to_frame,
    series_to_frame,
)


def test_series_to_frame_empty_returns_schema() -> None:
    df = series_to_frame([])
    assert df.empty
    for col in ("month", "value_point", "value_min", "value_max", "confidence_band"):
        assert col in df.columns


def test_series_to_frame_sorts_by_month() -> None:
    rows = [
        {
            "month": "2024-03-01",
            "value_point": 200,
            "value_min": 180,
            "value_max": 220,
            "method": "ratio",
            "confidence_band": "high",
            "confidence_score": 0.9,
            "needs_review": False,
            "public_profile_count": 50,
            "suppression_reason": None,
        },
        {
            "month": "2024-01-01",
            "value_point": 100,
            "value_min": 90,
            "value_max": 110,
            "method": "ratio",
            "confidence_band": "medium",
            "confidence_score": 0.6,
            "needs_review": False,
            "public_profile_count": 20,
            "suppression_reason": None,
        },
    ]
    df = series_to_frame(rows)
    assert list(df["value_point"]) == [100, 200]
    assert df["month"].dtype.kind == "M"


def test_build_series_figure_renders_band_line_and_events() -> None:
    df = series_to_frame(
        [
            {
                "month": "2024-01-01",
                "value_point": 100,
                "value_min": 90,
                "value_max": 110,
                "method": "ratio",
                "confidence_band": "high",
                "confidence_score": 0.9,
                "needs_review": False,
                "public_profile_count": 10,
                "suppression_reason": None,
            },
            {
                "month": "2024-02-01",
                "value_point": 120,
                "value_min": 100,
                "value_max": 140,
                "method": "ratio",
                "confidence_band": "low",
                "confidence_score": 0.3,
                "needs_review": True,
                "public_profile_count": 12,
                "suppression_reason": None,
            },
        ]
    )
    events = [{"event_month": "2024-02-01", "event_type": "layoff"}]
    overrides = [
        {
            "id": "o1",
            "payload": {"anchor_month": "2024-02-01", "value_point": 125},
        }
    ]
    fig = build_series_figure(df, events=events, overrides=overrides)
    assert isinstance(fig, go.Figure)
    # Expect: min, max, estimate line, event marker, override diamond = 5 traces.
    trace_names = [t.name for t in fig.data]
    assert "estimate" in trace_names
    assert "event" in trace_names
    assert "override" in trace_names


def test_build_series_figure_handles_empty_df() -> None:
    fig = build_series_figure(pd.DataFrame())
    assert isinstance(fig, go.Figure)
    assert fig.layout.title.text == "No estimates"


def test_review_queue_to_frame_passthrough() -> None:
    rows = [
        {
            "id": "r1",
            "company_id": "c1",
            "canonical_name": "Foo",
            "review_reason": "low_confidence",
            "priority": 80,
            "status": "open",
            "detail": None,
            "assigned_to": None,
            "updated_at": "2026-01-01T00:00:00+00:00",
        }
    ]
    df = review_queue_to_frame(rows)
    assert len(df) == 1
    assert df["updated_at"].dtype.kind == "M"


def test_overrides_to_frame_flattens_payload() -> None:
    rows = [
        {
            "id": "o1",
            "company_id": "c1",
            "field_name": "current_anchor",
            "payload": {
                "anchor_month": "2024-03-01",
                "value_min": 90,
                "value_point": 100,
                "value_max": 110,
            },
            "reason": "manual",
            "entered_by": "alice",
            "expires_at": None,
            "created_at": "2026-01-01T00:00:00+00:00",
        }
    ]
    df = overrides_to_frame(rows)
    assert df.loc[0, "anchor_month"] == "2024-03-01"
    assert df.loc[0, "value_point"] == 100


def test_benchmark_comparison_to_frame_collects_matches() -> None:
    summary = {
        "companies": [
            {
                "company_id": "c1",
                "canonical_name": "Foo",
                "matches": [
                    {
                        "month": "2024-03-01",
                        "estimate_point": 200,
                        "estimate_min": 180,
                        "estimate_max": 220,
                        "benchmark_point": 300,
                        "provider": "zeeshan",
                        "disagreement": True,
                        "relative_delta": 0.33,
                    },
                    {
                        "month": "2024-06-01",
                        "estimate_point": 250,
                        "estimate_min": 230,
                        "estimate_max": 270,
                        "benchmark_point": 255,
                        "provider": "zeeshan",
                        "disagreement": False,
                        "relative_delta": 0.02,
                    },
                ],
            }
        ]
    }
    df = benchmark_comparison_to_frame(summary)
    assert len(df) == 2
    assert df["disagreement"].tolist() == [True, False]
    assert df.loc[0, "canonical_name"] == "Foo"
