"""Company detail page: series chart + evidence inspector.

Workflow:

1. Pick a company from the left-hand selector.
2. The main panel shows the monthly series with an interval ribbon and
   event markers; markers are colored by confidence band.
3. Picking a month from the dropdown (or clicking the chart) loads the
   structured evidence trace on the right.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import streamlit as st

from apps.review_ui.api_client import ApiError
from apps.review_ui.components import build_series_figure, series_to_frame
from apps.review_ui.config import get_client

st.set_page_config(page_title="Company", page_icon=":office:", layout="wide")
st.title("Company detail")

client = get_client()


@st.cache_data(show_spinner=False, ttl=30)
def _load_companies() -> list[dict[str, Any]]:
    return client.list_companies(limit=500)


@st.cache_data(show_spinner=False, ttl=15)
def _load_series(company_id: str) -> dict[str, Any]:
    return client.get_company_series(company_id)


@st.cache_data(show_spinner=False, ttl=15)
def _load_evidence(company_id: str, month: str) -> dict[str, Any]:
    return client.get_company_evidence(company_id, month)


@st.cache_data(show_spinner=False, ttl=30)
def _load_overrides(company_id: str) -> list[dict[str, Any]]:
    return client.list_overrides(company_id=company_id, active_only=True)


try:
    companies = _load_companies()
except ApiError as exc:
    st.error(f"Could not load companies: {exc.detail}")
    st.stop()

if not companies:
    st.info("No companies loaded. Run `hc seed-companies` first.")
    st.stop()

name_by_id = {c["id"]: c["canonical_name"] for c in companies}
ordered_ids = [c["id"] for c in sorted(companies, key=lambda c: c["canonical_name"])]

selected_id = st.selectbox(
    "Company",
    options=ordered_ids,
    format_func=lambda cid: f"{name_by_id[cid]}  ({cid[:8]})",
)

try:
    series = _load_series(selected_id)
except ApiError as exc:
    st.error(f"Could not load series: {exc.detail}")
    st.stop()

months = series.get("months") or []
df = series_to_frame(months)

left, right = st.columns([3, 2], gap="large")

with left:
    st.subheader(name_by_id[selected_id])
    version_id = series.get("estimate_version_id")
    st.caption(
        f"Estimate version: `{version_id[:12] if version_id else 'none'}` - "
        f"{len(df)} months"
    )

    events: list[dict[str, Any]] = []
    overrides: list[dict[str, Any]] = []
    if not df.empty and version_id is not None:
        try:
            overrides = _load_overrides(selected_id)
        except ApiError as exc:
            st.warning(f"Could not load overrides: {exc.detail}")
        try:
            # Pull events + overrides for the first shown month so we at
            # least get all company_event rows once.
            evidence_for_events = _load_evidence(
                selected_id, df["month"].iloc[0].strftime("%Y-%m")
            )
            events = (evidence_for_events.get("inputs") or {}).get("events") or []
        except ApiError:
            events = []

    fig = build_series_figure(df, events=events, overrides=overrides)
    st.plotly_chart(fig, use_container_width=True)

    if not df.empty:
        st.dataframe(
            df[
                [
                    "month",
                    "value_point",
                    "value_min",
                    "value_max",
                    "method",
                    "confidence_band",
                    "confidence_score",
                    "needs_review",
                    "public_profile_count",
                ]
            ].assign(month=lambda x: x["month"].dt.strftime("%Y-%m")),
            hide_index=True,
            use_container_width=True,
        )

with right:
    st.subheader("Evidence")
    if df.empty:
        st.info("No estimates to inspect.")
    else:
        default_month = df["month"].iloc[-1]
        options = [m.strftime("%Y-%m") for m in df["month"]]
        default_idx = options.index(default_month.strftime("%Y-%m"))
        picked = st.selectbox(
            "Month",
            options=options,
            index=default_idx,
            key="evidence_month",
        )
        try:
            evidence = _load_evidence(selected_id, picked)
        except ApiError as exc:
            st.error(f"Could not load evidence: {exc.detail}")
        else:
            estimate = evidence.get("estimate") or {}
            conf = evidence.get("confidence") or {}
            cols = st.columns(3)
            cols[0].metric("Point", f"{estimate.get('value_point', 0):.0f}")
            cols[1].metric(
                "Interval",
                f"{estimate.get('value_min', 0):.0f} - {estimate.get('value_max', 0):.0f}",
            )
            cols[2].metric(
                "Band",
                f"{conf.get('band', '?')}",
                delta=(
                    f"{conf.get('score', 0):.2f}"
                    if isinstance(conf.get("score"), (int, float))
                    else None
                ),
            )

            segment = evidence.get("segment") or {}
            if segment:
                st.markdown(
                    f"**Segment**: {segment.get('start_month', '?')} -> "
                    f"{segment.get('end_month', '?')} "
                    f"(open event: `{segment.get('opening_event_type', 'none')}`)"
                )

            tabs = st.tabs(
                ["Anchors", "Events", "Confidence", "Overrides", "Audit", "Raw"]
            )
            inputs = evidence.get("inputs") or {}

            with tabs[0]:
                anchors = inputs.get("anchors") or []
                if anchors:
                    st.dataframe(
                        pd.DataFrame(anchors),
                        hide_index=True,
                        use_container_width=True,
                    )
                else:
                    st.caption("No anchors contributing to this month.")

            with tabs[1]:
                events_tab = inputs.get("events") or []
                if events_tab:
                    st.dataframe(
                        pd.DataFrame(events_tab),
                        hide_index=True,
                        use_container_width=True,
                    )
                else:
                    st.caption("No events in window.")

            with tabs[2]:
                components = conf.get("components") or {}
                if components:
                    st.dataframe(
                        pd.DataFrame(
                            [
                                {"component": k, "score": v}
                                for k, v in components.items()
                            ]
                        ),
                        hide_index=True,
                        use_container_width=True,
                    )
                else:
                    st.caption("No component breakdown recorded.")

            with tabs[3]:
                applied = evidence.get("overrides_applied") or []
                if applied:
                    st.dataframe(
                        pd.DataFrame(applied),
                        hide_index=True,
                        use_container_width=True,
                    )
                else:
                    st.caption("No overrides applied.")

            with tabs[4]:
                audit = evidence.get("audit") or []
                if audit:
                    st.dataframe(
                        pd.DataFrame(audit),
                        hide_index=True,
                        use_container_width=True,
                    )
                else:
                    st.caption("No audit records for this version.")

            with tabs[5]:
                st.json(evidence, expanded=False)


# Silence Pyflakes when run top-level: ``date`` kept for future
# strongly-typed filters in the header filter row.
_ = date
