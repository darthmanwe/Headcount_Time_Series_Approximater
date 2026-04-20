"""Review queue page: sortable table + claim/resolve/dismiss actions."""

from __future__ import annotations

import streamlit as st

from apps.review_ui.api_client import ApiError
from apps.review_ui.components import review_queue_to_frame
from apps.review_ui.config import get_client

st.set_page_config(page_title="Review Queue", page_icon=":flag:", layout="wide")
st.title("Review queue")

client = get_client()

col_status, col_limit, col_actor = st.columns([1, 1, 2])
status_filter = col_status.selectbox(
    "Status",
    options=["open", "assigned", "resolved", "dismissed", "all"],
    index=0,
)
limit = col_limit.number_input(
    "Limit", min_value=10, max_value=500, value=50, step=10
)
actor = col_actor.text_input(
    "Acting as",
    value=st.session_state.get("review_actor", ""),
    placeholder="analyst username (used for claim/resolve audit)",
    key="review_actor",
)

try:
    rows = client.list_review_queue(
        status=None if status_filter == "all" else status_filter,
        limit=int(limit),
    )
except ApiError as exc:
    st.error(f"Could not load queue: {exc.detail}")
    st.stop()

df = review_queue_to_frame(rows)
if df.empty:
    st.info("Queue empty for this filter.")
    st.stop()

st.caption(f"{len(df)} items (highest priority first)")
st.dataframe(
    df[
        [c for c in [
            "priority",
            "canonical_name",
            "review_reason",
            "status",
            "assigned_to",
            "detail",
            "updated_at",
        ] if c in df.columns]
    ],
    hide_index=True,
    use_container_width=True,
)

st.divider()
st.subheader("Act on an item")

row_labels = {
    row["id"]: f"[{row['priority']}] {row['canonical_name']} - {row['review_reason']}"
    for row in rows
}
selected = st.selectbox(
    "Item",
    options=list(row_labels.keys()),
    format_func=lambda rid: row_labels[rid],
)
selected_row = next(r for r in rows if r["id"] == selected)
st.json({
    "id": selected_row["id"],
    "company_id": selected_row["company_id"],
    "current_status": selected_row["status"],
    "assigned_to": selected_row.get("assigned_to"),
    "detail": selected_row.get("detail"),
}, expanded=False)

note = st.text_input("Note (optional)", key=f"note_{selected}")
act_col1, act_col2, act_col3 = st.columns(3)


def _transition(target_status: str, assigned_to: str | None = None) -> None:
    try:
        result = client.transition_review_item(
            selected,
            status=target_status,
            assigned_to=assigned_to,
            note=note or None,
            actor_id=actor or None,
        )
    except ApiError as exc:
        st.error(f"{exc.status_code}: {exc.detail}")
        return
    st.success(f"-> {result['status']}")
    st.cache_data.clear()
    st.rerun()


with act_col1:
    if st.button("Claim", disabled=not actor, help="Requires 'Acting as' set"):
        _transition("assigned", assigned_to=actor)
with act_col2:
    if st.button("Resolve"):
        _transition("resolved")
with act_col3:
    if st.button("Dismiss"):
        _transition("dismissed")

if not actor:
    st.caption("Set 'Acting as' above to enable Claim.")
