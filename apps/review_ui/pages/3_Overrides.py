"""Overrides page: browse active overrides and file new ones.

Payload shapes match :class:`OverrideField`:

- ``current_anchor``: ``{anchor_month, value_min, value_point, value_max, confidence}``
- ``event_segment``: ``{segment_start, segment_end, event_type, ...}``
- others: free-form JSON the analyst provides.
"""

from __future__ import annotations

import json
from datetime import datetime

import streamlit as st

from apps.review_ui.api_client import ApiError
from apps.review_ui.components import overrides_to_frame
from apps.review_ui.config import get_client

st.set_page_config(page_title="Overrides", page_icon=":wrench:", layout="wide")
st.title("Manual overrides")

client = get_client()

OVERRIDE_FIELDS = [
    "canonical_company",
    "current_anchor",
    "event_segment",
    "estimate_suppress_window",
    "company_relation",
    "person_identity_merge",
]

try:
    companies = client.list_companies(limit=500)
except ApiError as exc:
    st.error(f"Could not load companies: {exc.detail}")
    st.stop()

if not companies:
    st.info("No companies loaded.")
    st.stop()

name_by_id = {c["id"]: c["canonical_name"] for c in companies}
ordered_ids = [c["id"] for c in sorted(companies, key=lambda c: c["canonical_name"])]

filter_col, toggle_col = st.columns([3, 1])
company_filter = filter_col.selectbox(
    "Filter by company (optional)",
    options=["<all>", *ordered_ids],
    format_func=lambda cid: "<all>" if cid == "<all>" else f"{name_by_id[cid]}",
)
active_only = toggle_col.toggle("Active only", value=True)

try:
    overrides = client.list_overrides(
        company_id=None if company_filter == "<all>" else company_filter,
        active_only=active_only,
    )
except ApiError as exc:
    st.error(f"Could not load overrides: {exc.detail}")
    st.stop()

df = overrides_to_frame(overrides)
if df.empty:
    st.info("No overrides match that filter.")
else:
    st.dataframe(df, hide_index=True, use_container_width=True)

st.divider()
st.subheader("New override")

with st.form("new_override"):
    target_id = st.selectbox(
        "Company",
        options=ordered_ids,
        format_func=lambda cid: f"{name_by_id[cid]}",
    )
    field_name = st.selectbox("Field", options=OVERRIDE_FIELDS, index=1)
    reason = st.text_input("Reason")
    entered_by = st.text_input("Entered by (username)")
    expires_at_input = st.text_input(
        "Expires at (ISO 8601, optional)",
        placeholder="2026-12-31T00:00:00+00:00",
    )

    st.markdown("**Payload**")
    if field_name == "current_anchor":
        col1, col2 = st.columns(2)
        anchor_month = col1.text_input("Anchor month", value="2026-01")
        confidence = col2.slider("Confidence", 0.0, 1.0, 0.9, 0.05)
        colm, colp, colx = st.columns(3)
        v_min = colm.number_input("value_min", min_value=0, value=0, step=1)
        v_point = colp.number_input("value_point", min_value=0, value=0, step=1)
        v_max = colx.number_input("value_max", min_value=0, value=0, step=1)
        payload_preview = {
            "anchor_month": f"{anchor_month}-01" if len(anchor_month) == 7 else anchor_month,
            "value_min": int(v_min),
            "value_point": int(v_point),
            "value_max": int(v_max),
            "confidence": float(confidence),
        }
    else:
        raw = st.text_area(
            "Payload JSON",
            value="{}",
            height=180,
            help="Free-form JSON; gets stored verbatim in override_value_json.",
        )
        try:
            payload_preview = json.loads(raw) if raw else {}
        except json.JSONDecodeError as exc:
            st.error(f"Invalid JSON: {exc}")
            payload_preview = None

    st.caption("Preview:")
    st.json(payload_preview or {}, expanded=False)

    submitted = st.form_submit_button("Create override")

if submitted:
    if payload_preview is None:
        st.error("Fix the payload JSON before submitting.")
    else:
        expires_iso: str | None = None
        if expires_at_input.strip():
            try:
                # Normalize via datetime round-trip so we reject malformed input
                # before hitting the API.
                expires_iso = datetime.fromisoformat(expires_at_input).isoformat()
            except ValueError:
                st.error("Invalid expires_at; use ISO 8601 (e.g. 2026-12-31T00:00:00+00:00).")
                st.stop()
        try:
            created = client.create_override(
                company_id=target_id,
                field_name=field_name,
                payload=payload_preview,
                reason=reason or None,
                entered_by=entered_by or None,
                expires_at=expires_iso,
            )
        except ApiError as exc:
            st.error(f"{exc.status_code}: {exc.detail}")
        else:
            st.success(f"Created {created['id']}")
            st.cache_data.clear()
            st.rerun()
