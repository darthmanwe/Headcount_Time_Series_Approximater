"""Evaluation: Phase 11 regression scoreboard and accuracy history."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from apps.review_ui.api_client import ApiError
from apps.review_ui.config import get_client

st.set_page_config(page_title="Evaluation", page_icon=":chart_with_upwards_trend:", layout="wide")
st.title("Evaluation")
st.caption(
    "Phase 11 regression scoreboard. Each row is a snapshot of pipeline output "
    "vs. the benchmark workbooks in `test_source/`. Re-run with `hc evaluate` "
    "to append a new row."
)

client = get_client()


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.2%}"


def _fmt_float(value: float | None, *, digits: int = 4) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


try:
    latest = client.eval_latest()
except ApiError as exc:
    if exc.status_code == 404:
        st.info(
            "No evaluation runs yet. Trigger one with `hc evaluate` "
            "after loading `test_source/` benchmarks."
        )
        latest = None
    else:
        st.error(f"Could not load latest evaluation: {exc.detail}")
        st.stop()

if latest is not None:
    cols = st.columns(5)
    cols[0].metric(
        "Companies evaluated",
        f"{latest.get('companies_evaluated', 0)} / {latest.get('companies_in_scope', 0)}",
    )
    cols[1].metric(
        "Coverage (in-scope)",
        _fmt_pct(latest.get("coverage_in_scope")),
    )
    cols[2].metric(
        "MAPE (current)",
        _fmt_float(latest.get("mape_headcount_current")),
    )
    cols[3].metric(
        "MAE (1y growth)",
        _fmt_float(latest.get("mae_growth_1y_pct")),
    )
    cols[4].metric(
        "High-conf disagreements",
        latest.get("high_confidence_disagreements", 0),
    )
    st.caption(
        f"Run `{latest.get('id', '?')}` @ {latest.get('created_at', '?')} "
        f"(as_of `{latest.get('as_of_month', '?')}`, "
        f"version `{latest.get('evaluation_version', '?')}`)"
    )

    board = latest.get("scoreboard", {}) or {}

    st.divider()
    st.subheader("Per-provider accuracy - headcount metrics")
    accuracy = board.get("accuracy", {}) or {}
    rows: list[dict[str, object]] = []
    for provider, by_metric in accuracy.items():
        for metric, summary in by_metric.items():
            rows.append(
                {
                    "provider": provider,
                    "metric": metric,
                    "n": summary.get("n", 0),
                    "mae": summary.get("mae"),
                    "mape": summary.get("mape"),
                    "median_abs_error": summary.get("median_abs_error"),
                }
            )
    if rows:
        st.dataframe(
            pd.DataFrame(rows).sort_values(["provider", "metric"]),
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.caption("No headcount accuracy data in this run.")

    st.subheader("Per-provider accuracy - growth windows")
    growth = board.get("growth_accuracy", {}) or {}
    growth_rows: list[dict[str, object]] = []
    for provider, by_metric in growth.items():
        for horizon, summary in by_metric.items():
            growth_rows.append(
                {
                    "provider": provider,
                    "horizon": horizon,
                    "n": summary.get("n", 0),
                    "mae": summary.get("mae"),
                    "mape": summary.get("mape"),
                }
            )
    if growth_rows:
        st.dataframe(
            pd.DataFrame(growth_rows).sort_values(["provider", "horizon"]),
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.caption("No growth-window accuracy data in this run.")

    st.subheader("Top disagreements")
    top = board.get("top_disagreements", []) or []
    if top:
        st.dataframe(
            pd.DataFrame(top),
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.caption("No high-gap benchmark disagreements in this run.")

    st.subheader("Confidence band distribution")
    bands = board.get("confidence_bands", {}) or {}
    if bands:
        bands_df = pd.DataFrame(
            [{"band": k, "rows": v} for k, v in bands.items()]
        ).sort_values("rows", ascending=False)
        st.bar_chart(bands_df, x="band", y="rows", use_container_width=True)

st.divider()
st.subheader("Evaluation history")

try:
    history = client.eval_history(limit=50)
except ApiError as exc:
    st.error(f"Could not load evaluation history: {exc.detail}")
    history = []

if history:
    df = pd.DataFrame(history)
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"])
    keep = [
        c
        for c in [
            "created_at",
            "as_of_month",
            "companies_evaluated",
            "companies_in_scope",
            "companies_with_benchmark",
            "coverage_in_scope",
            "mape_headcount_current",
            "mae_growth_1y_pct",
            "high_confidence_disagreements",
            "review_queue_open",
            "evaluation_version",
            "note",
            "id",
        ]
        if c in df.columns
    ]
    st.dataframe(df[keep], hide_index=True, use_container_width=True)

    selected = st.selectbox(
        "Inspect scoreboard",
        options=[r["id"] for r in history],
        format_func=lambda rid: next(
            f"{r['created_at']} - as_of {r['as_of_month']} (mape={_fmt_float(r['mape_headcount_current'])})"
            for r in history
            if r["id"] == rid
        ),
    )
    try:
        detail = client.eval_detail(selected)
    except ApiError as exc:
        st.error(f"Could not load evaluation: {exc.detail}")
    else:
        with st.expander("Full scoreboard JSON", expanded=False):
            st.json(detail.get("scoreboard", {}), expanded=False)
else:
    st.caption("No evaluation history.")
