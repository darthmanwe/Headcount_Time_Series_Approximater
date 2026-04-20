"""Benchmark comparison page: estimate-vs-benchmark table with disagreements highlighted."""

from __future__ import annotations

import streamlit as st

from apps.review_ui.api_client import ApiError
from apps.review_ui.components import benchmark_comparison_to_frame
from apps.review_ui.config import get_client

st.set_page_config(page_title="Benchmarks", page_icon=":bar_chart:", layout="wide")
st.title("Benchmark comparison")

client = get_client()

col_threshold, col_refresh = st.columns([3, 1])
threshold = col_threshold.slider(
    "Disagreement threshold (|Δ| / benchmark)",
    min_value=0.05,
    max_value=1.0,
    value=0.25,
    step=0.05,
)
if col_refresh.button("Refresh"):
    st.cache_data.clear()

try:
    summary = client.benchmark_comparison(threshold=threshold)
except ApiError as exc:
    st.error(f"Could not load comparison: {exc.detail}")
    st.stop()

cols = st.columns(4)
cols[0].metric("Companies w/ benchmarks", summary.get("companies_with_benchmarks", 0))
cols[1].metric("Months compared", summary.get("months_compared", 0))
cols[2].metric("Disagreements", summary.get("disagreements_total", 0))
cols[3].metric("Threshold", f"{threshold:.0%}")

df = benchmark_comparison_to_frame(summary)
if df.empty:
    st.info("No benchmark observations to compare against.")
    st.stop()

only_disagreements = st.toggle("Only show disagreements", value=True)
display = df[df["disagreement"]] if only_disagreements else df
display = display.sort_values(
    by=["disagreement", "relative_delta"],
    ascending=[False, False],
    na_position="last",
)

def _style_disagreement(row: object) -> list[str]:
    # row is a pandas Series.
    bad = bool(row["disagreement"])  # type: ignore[index]
    return ["background-color: rgba(192, 57, 43, 0.18)" if bad else "" for _ in row]  # type: ignore[attr-defined]


st.dataframe(
    display.style.apply(_style_disagreement, axis=1),
    hide_index=True,
    use_container_width=True,
)

with st.expander("Raw response", expanded=False):
    st.json(summary, expanded=False)
