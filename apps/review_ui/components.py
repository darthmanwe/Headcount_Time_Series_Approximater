"""Rendering helpers shared across review UI pages.

Everything here is pure: it takes JSON-shaped dicts/lists from the API
and returns Plotly figures or pandas DataFrames. No Streamlit calls
inside, no httpx calls. That makes the logic testable in isolation and
keeps the pages thin.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.graph_objects as go

CONFIDENCE_COLORS = {
    "high": "#2e8b57",
    "medium": "#daa520",
    "low": "#c0392b",
    "suppressed": "#7f8c8d",
}


def series_to_frame(months: list[dict[str, Any]]) -> pd.DataFrame:
    """Flatten a ``CompanySeriesResponse.months`` list into a DataFrame."""

    if not months:
        return pd.DataFrame(
            columns=[
                "month",
                "value_point",
                "value_min",
                "value_max",
                "method",
                "confidence_band",
                "confidence_score",
                "needs_review",
                "public_profile_count",
                "suppression_reason",
            ]
        )
    df = pd.DataFrame(months)
    df["month"] = pd.to_datetime(df["month"])
    return df.sort_values("month").reset_index(drop=True)


def build_series_figure(
    df: pd.DataFrame,
    *,
    events: list[dict[str, Any]] | None = None,
    overrides: list[dict[str, Any]] | None = None,
) -> go.Figure:
    """Plotly line + interval band + event markers."""

    fig = go.Figure()
    if df.empty:
        fig.update_layout(
            title="No estimates",
            xaxis_title="Month",
            yaxis_title="Headcount",
        )
        return fig

    # Interval band (min/max) as a transparent ribbon.
    fig.add_trace(
        go.Scatter(
            x=df["month"],
            y=df["value_max"],
            mode="lines",
            line={"width": 0},
            showlegend=False,
            hoverinfo="skip",
            name="max",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["month"],
            y=df["value_min"],
            mode="lines",
            line={"width": 0},
            fill="tonexty",
            fillcolor="rgba(100, 149, 237, 0.18)",
            showlegend=False,
            hoverinfo="skip",
            name="min",
        )
    )

    # Point series, colored by confidence band per marker.
    colors = [CONFIDENCE_COLORS.get(str(b), "#4682b4") for b in df["confidence_band"]]
    fig.add_trace(
        go.Scatter(
            x=df["month"],
            y=df["value_point"],
            mode="lines+markers",
            line={"color": "#4682b4", "width": 2},
            marker={"color": colors, "size": 7},
            name="estimate",
            customdata=df[
                ["confidence_band", "method", "public_profile_count", "needs_review"]
            ].values,
            hovertemplate=(
                "<b>%{x|%Y-%m}</b><br>"
                "value: %{y:.0f}<br>"
                "band: %{customdata[0]}<br>"
                "method: %{customdata[1]}<br>"
                "public profiles: %{customdata[2]}<br>"
                "needs_review: %{customdata[3]}"
                "<extra></extra>"
            ),
        )
    )

    # Event markers as a dedicated scatter trace. (We intentionally
    # don't use ``add_vline`` because Plotly's shape math doesn't accept
    # string dates, and coercing dates through their internal
    # ``_mean`` helper crashes on Windows.)
    if events:
        event_xs: list[Any] = []
        event_ys: list[float] = []
        event_labels: list[str] = []
        ymax = float(df["value_max"].max()) if not df.empty else 0.0
        for event in events:
            month = event.get("event_month") or event.get("month")
            if not month:
                continue
            try:
                parsed = pd.to_datetime(month)
            except (ValueError, TypeError):
                continue
            event_xs.append(parsed)
            event_ys.append(ymax)
            event_labels.append(
                str(event.get("event_type") or event.get("label") or "event")
            )
        if event_xs:
            fig.add_trace(
                go.Scatter(
                    x=event_xs,
                    y=event_ys,
                    mode="markers+text",
                    marker={"symbol": "triangle-down", "size": 12, "color": "#8e44ad"},
                    text=event_labels,
                    textposition="top center",
                    name="event",
                    hovertemplate="%{text}<br>%{x|%Y-%m}<extra></extra>",
                    showlegend=False,
                )
            )

    # Override markers as diamond overlay.
    for override in overrides or []:
        month = override.get("anchor_month") or (override.get("payload") or {}).get(
            "anchor_month"
        )
        if not month:
            continue
        payload = override.get("payload") or {}
        y = payload.get("value_point")
        if y is None:
            continue
        fig.add_trace(
            go.Scatter(
                x=[month],
                y=[y],
                mode="markers",
                marker={"symbol": "diamond", "size": 12, "color": "#e67e22"},
                name="override",
                hovertemplate=(
                    f"override: {override.get('id', '')[:8]}<br>"
                    "value: %{y:.0f}"
                    "<extra></extra>"
                ),
                showlegend=False,
            )
        )

    fig.update_layout(
        xaxis_title="Month",
        yaxis_title="Headcount",
        margin={"l": 30, "r": 10, "t": 30, "b": 30},
        hovermode="x unified",
        height=420,
    )
    return fig


def review_queue_to_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"])
    return df


def overrides_to_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    flat = []
    for row in rows:
        payload = row.get("payload") or {}
        flat.append(
            {
                "id": row["id"],
                "company_id": row["company_id"],
                "field_name": row["field_name"],
                "anchor_month": payload.get("anchor_month"),
                "value_point": payload.get("value_point"),
                "value_min": payload.get("value_min"),
                "value_max": payload.get("value_max"),
                "reason": row.get("reason"),
                "entered_by": row.get("entered_by"),
                "expires_at": row.get("expires_at"),
                "created_at": row.get("created_at"),
            }
        )
    return pd.DataFrame(flat)


def benchmark_comparison_to_frame(summary: dict[str, Any]) -> pd.DataFrame:
    """Flatten ComparisonSummary.to_dict() into a reviewer-friendly DataFrame."""

    rows: list[dict[str, Any]] = []
    for company in summary.get("companies", []) or []:
        company_id = company.get("company_id")
        name = company.get("canonical_name") or company_id
        for match in company.get("matches", []) or []:
            rows.append(
                {
                    "company_id": company_id,
                    "canonical_name": name,
                    "month": match.get("month"),
                    "estimate_point": match.get("estimate_point"),
                    "estimate_min": match.get("estimate_min"),
                    "estimate_max": match.get("estimate_max"),
                    "benchmark_point": match.get("benchmark_point"),
                    "provider": match.get("provider"),
                    "disagreement": match.get("disagreement", False),
                    "relative_delta": match.get("relative_delta"),
                }
            )
    return pd.DataFrame(rows)


__all__ = [
    "CONFIDENCE_COLORS",
    "benchmark_comparison_to_frame",
    "build_series_figure",
    "overrides_to_frame",
    "review_queue_to_frame",
    "series_to_frame",
]
