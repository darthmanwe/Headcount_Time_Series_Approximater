"""Status dashboard: runs, stage coverage, review queue roll-up."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from apps.review_ui.api_client import ApiError
from apps.review_ui.config import get_client

st.set_page_config(page_title="Status", page_icon=":satellite:", layout="wide")
st.title("Status")

client = get_client()

try:
    summary = client.status_summary()
except ApiError as exc:
    st.error(f"Could not load status summary: {exc.detail}")
    st.stop()

cols = st.columns(4)
cols[0].metric("Companies total", summary.get("companies_total", 0))
queue = summary.get("review_queue_by_status", {}) or {}
cols[1].metric("Open review", queue.get("open", 0))
cols[2].metric("Assigned", queue.get("assigned", 0))
cols[3].metric("Resolved", queue.get("resolved", 0))

st.divider()
st.subheader("Latest run")
latest = summary.get("latest_run")
if not latest:
    st.info("No runs yet. Trigger one with `hc collect-anchors` or `hc estimate-series`.")
else:
    col1, col2, col3 = st.columns(3)
    col1.metric("Status", latest.get("status", "?"))
    col2.caption(f"Started: `{latest.get('started_at', '?')}`")
    col3.caption(f"Finished: `{latest.get('finished_at') or 'running'}`")
    stage_counts = latest.get("stage_counts") or {}
    if stage_counts:
        df = pd.DataFrame(
            [{"status": k, "count": v} for k, v in stage_counts.items()]
        ).sort_values("count", ascending=False)
        st.bar_chart(df, x="status", y="count", use_container_width=True)

st.divider()
st.subheader("Runs history")
try:
    runs = client.list_runs(limit=50)
except ApiError as exc:
    st.error(f"Could not load runs: {exc.detail}")
    st.stop()

if not runs:
    st.caption("No runs recorded.")
else:
    runs_df = pd.DataFrame(runs)
    if "started_at" in runs_df.columns:
        runs_df["started_at"] = pd.to_datetime(runs_df["started_at"])
    st.dataframe(
        runs_df[
            [c for c in [
                "started_at",
                "kind",
                "status",
                "cutoff_month",
                "method_version",
                "priority_tier",
                "id",
            ] if c in runs_df.columns]
        ],
        hide_index=True,
        use_container_width=True,
    )

    picked = st.selectbox(
        "Inspect run",
        options=[r["id"] for r in runs],
        format_func=lambda rid: next(
            f"{r['started_at']} - {r['kind']} ({r['status']})" for r in runs if r["id"] == rid
        ),
    )
    try:
        detail = client.get_run(picked)
    except ApiError as exc:
        st.error(f"Could not load run: {exc.detail}")
    else:
        stages = detail.get("stages") or []
        if stages:
            expanded_rows = []
            for stage in stages:
                for status, count in (stage.get("counts") or {}).items():
                    expanded_rows.append(
                        {"stage": stage["stage"], "status": status, "count": count}
                    )
            if expanded_rows:
                st.dataframe(
                    pd.DataFrame(expanded_rows),
                    hide_index=True,
                    use_container_width=True,
                )
        with st.expander("Raw run detail", expanded=False):
            st.json(detail, expanded=False)
