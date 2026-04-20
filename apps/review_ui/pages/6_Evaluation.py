"""Evaluation: Phase 11 regression scoreboard and accuracy history.

The scoreboard is Harmonic-primary: every headline KPI tracks how
close the pipeline gets to Harmonic.ai's numbers. Supporting providers
(Zeeshan, LinkedIn) are reported in collapsible panels for diagnostic
review but never drive the headline tiles.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from apps.review_ui.api_client import ApiError
from apps.review_ui.config import get_client

st.set_page_config(
    page_title="Evaluation",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
)
st.title("Evaluation")
st.caption(
    "Harmonic-primary regression scoreboard. Each row is a snapshot of "
    "pipeline output vs. the benchmark workbooks in `test_source/`. "
    "Re-run with `hc evaluate` to append a new row."
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


def _fmt_rho(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:+.3f}"


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
    board = latest.get("scoreboard", {}) or {}
    headline = board.get("headline", {}) or {}
    cohort = board.get("harmonic_cohort", {}) or {}

    # ----- Headline row: Harmonic-cohort calibration ----------------------
    st.subheader("Harmonic-cohort calibration (primary KPIs)")
    head_cols = st.columns(5)
    head_cols[0].metric(
        "Harmonic cohort",
        f"{cohort.get('evaluated', 0)} / {cohort.get('size', 0)}",
        help=(
            "Companies in scope with at least one Harmonic benchmark "
            "row (calibration lens) versus how many of them produced "
            "an estimate."
        ),
    )
    head_cols[1].metric(
        "MAPE: headcount (Harmonic)",
        _fmt_float(headline.get("mape_headcount_current")),
        help="Mean absolute percentage error vs Harmonic headcount.",
    )
    head_cols[2].metric(
        "MAE: 1y growth (Harmonic)",
        _fmt_float(headline.get("mae_growth_1y_pct")),
        help=(
            "Mean absolute error in growth ratio vs Harmonic's 365d "
            "rate. 0.05 = 5 percentage points."
        ),
    )
    head_cols[3].metric(
        "Spearman rho: 1y growth",
        _fmt_rho(headline.get("spearman_growth_1y")),
        help=(
            "Rank correlation against Harmonic's 1y growth ordering. "
            "+1.0 = identical sort; 0 = uncorrelated."
        ),
    )
    head_cols[4].metric(
        "High-conf Harmonic disagreements",
        latest.get("high_confidence_disagreements", 0),
        help=(
            "Estimates with high/medium band that miss Harmonic by "
            "more than 2x. Acceptance gate tripwire."
        ),
    )

    # ----- Secondary row: 6m + 2y headlines -------------------------------
    sec_cols = st.columns(3)
    sec_cols[0].metric(
        "MAE: 6m growth (Harmonic)",
        _fmt_float(headline.get("mae_growth_6m_pct")),
    )
    sec_cols[1].metric(
        "MAE: 2y growth (Zeeshan)",
        _fmt_float(headline.get("mae_growth_2y_pct")),
        help="Harmonic does not emit 2y; Zeeshan is the only signal.",
    )
    sec_cols[2].metric(
        "Spearman rho: 6m growth",
        _fmt_rho(headline.get("spearman_growth_6m")),
    )

    st.caption(
        f"Run `{latest.get('id', '?')}` @ {latest.get('created_at', '?')} "
        f"(as_of `{latest.get('as_of_month', '?')}`, "
        f"version `{latest.get('evaluation_version', '?')}`, "
        f"primary `{board.get('primary_provider', 'harmonic')}`)"
    )

    # ----- Full-population coverage panel ---------------------------------
    st.divider()
    st.subheader("Full-population coverage")
    pop_cols = st.columns(4)
    pop_cols[0].metric(
        "Companies evaluated",
        f"{latest.get('companies_evaluated', 0)} / {latest.get('companies_in_scope', 0)}",
    )
    pop_cols[1].metric(
        "Coverage (in-scope)",
        _fmt_pct(latest.get("coverage_in_scope")),
    )
    pop_cols[2].metric(
        "Companies with any benchmark",
        latest.get("companies_with_benchmark", 0),
    )
    declined = (
        board.get("companies", {}).get("declined_to_estimate")
        if isinstance(board.get("companies"), dict)
        else None
    )
    if declined is None:
        declined = latest.get("companies_declined_to_estimate", 0)
    pop_cols[3].metric(
        "Declined to estimate",
        declined,
        help=(
            "Companies whose latest version is entirely placeholder rows "
            "(no real anchor). Excluded from MAPE so thin free-data days "
            "do not drag the headline KPI; surfaced here as a coverage "
            "gap instead."
        ),
    )

    supporting = board.get("review", {}).get("supporting_disagreements", 0)
    st.caption(
        f"Supporting (Zeeshan + LinkedIn) disagreements flagged: {supporting}. "
        "Diagnostic only; never blocks the gate."
    )

    bands = board.get("confidence_bands", {}) or {}
    if bands:
        st.markdown("**Confidence band distribution (full population)**")
        bands_df = pd.DataFrame([{"band": k, "rows": v} for k, v in bands.items()]).sort_values(
            "rows", ascending=False
        )
        st.bar_chart(bands_df, x="band", y="rows", use_container_width=True)

    # ----- Per-provider accuracy ------------------------------------------
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
        df_acc = pd.DataFrame(rows)
        # Sort with primary provider (harmonic) first for readability.
        provider_order = {"harmonic": 0, "zeeshan": 1, "linkedin": 2}
        df_acc["_p"] = df_acc["provider"].map(provider_order).fillna(99)
        df_acc = df_acc.sort_values(["_p", "metric"]).drop(columns=["_p"])
        st.dataframe(df_acc, hide_index=True, use_container_width=True)
    else:
        st.caption("No headcount accuracy data in this run.")

    st.subheader("Per-provider accuracy - growth windows")
    growth = board.get("growth_accuracy", {}) or {}
    growth_rows: list[dict[str, object]] = []
    for provider, by_horizon in growth.items():
        for horizon, summary in by_horizon.items():
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
        df_g = pd.DataFrame(growth_rows)
        df_g["_p"] = df_g["provider"].map({"harmonic": 0, "zeeshan": 1, "linkedin": 2}).fillna(99)
        df_g["_h"] = df_g["horizon"].map({"6m": 0, "1y": 1, "2y": 2}).fillna(99)
        df_g = df_g.sort_values(["_p", "_h"]).drop(columns=["_p", "_h"])
        st.dataframe(df_g, hide_index=True, use_container_width=True)
    else:
        st.caption("No growth-window accuracy data in this run.")

    # ----- Rank correlation table -----------------------------------------
    rank_corr = board.get("rank_correlation", {}) or {}
    if rank_corr:
        st.subheader("Rank correlation (Spearman rho)")
        st.caption(
            "+1.0 = identical ordering as the provider; 0 = uncorrelated; "
            "-1.0 = inverted. Computed only when at least 3 cohort "
            "companies report a value at the horizon."
        )
        rc_rows: list[dict[str, object]] = []
        for provider, by_horizon in rank_corr.items():
            for horizon, rho in by_horizon.items():
                rc_rows.append(
                    {
                        "provider": provider,
                        "horizon": horizon,
                        "spearman_rho": rho,
                    }
                )
        if rc_rows:
            st.dataframe(
                pd.DataFrame(rc_rows).sort_values(["provider", "horizon"]),
                hide_index=True,
                use_container_width=True,
            )

    # ----- Top disagreements ---------------------------------------------
    st.subheader("Top disagreements")
    top = board.get("top_disagreements", []) or []
    if top:
        df_top = pd.DataFrame(top)
        # Highlight Harmonic rows with a leading marker so reviewers
        # can scan the primary signal first.
        if "provider" in df_top.columns:
            df_top.insert(
                0,
                "primary",
                df_top["provider"].apply(lambda p: "★" if p == "harmonic" else ""),
            )
        st.dataframe(df_top, hide_index=True, use_container_width=True)
    else:
        st.caption("No high-gap benchmark disagreements in this run.")

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
            "primary_provider",
            "companies_evaluated",
            "companies_in_scope",
            "harmonic_cohort_size",
            "harmonic_cohort_evaluated",
            "coverage_in_scope",
            "mape_headcount_current",
            "mae_growth_1y_pct",
            "spearman_growth_1y",
            "high_confidence_disagreements",
            "supporting_disagreements",
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
            (
                f"{r['created_at']} - as_of {r['as_of_month']} "
                f"(mape={_fmt_float(r.get('mape_headcount_current'))}, "
                f"rho1y={_fmt_rho(r.get('spearman_growth_1y'))})"
            )
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
