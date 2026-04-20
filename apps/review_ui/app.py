"""Streamlit review UI entry point.

Run with::

    hc review-ui [--api-url http://127.0.0.1:8000]

or directly::

    streamlit run apps/review_ui/app.py

The entry page is a lightweight dashboard that renders the status
summary and links out to the five analyst pages (Company, Review Queue,
Overrides, Benchmarks, Status).
"""

from __future__ import annotations

import streamlit as st

from apps.review_ui.api_client import ApiError
from apps.review_ui.config import current_base_url, get_client, set_base_url

st.set_page_config(
    page_title="Headcount Review",
    page_icon=":bar_chart:",
    layout="wide",
)


def _render_sidebar() -> None:
    st.sidebar.header("Configuration")
    current = current_base_url()
    new_url = st.sidebar.text_input("API base URL", value=current, key="_api_url_input")
    if new_url != current and st.sidebar.button("Apply", key="_api_url_apply"):
        set_base_url(new_url.rstrip("/"))
        st.rerun()

    try:
        healthz = get_client().healthz()
    except ApiError as exc:
        st.sidebar.error(f"API unreachable ({exc.status_code}).")
        return
    except Exception as exc:
        st.sidebar.error(f"API unreachable: {exc}")
        return
    st.sidebar.success(f"Connected ({healthz.get('api_version', '?')})")
    st.sidebar.caption(f"Version: {healthz.get('version', '?')}")


def _render_home() -> None:
    st.title("Headcount Review")
    st.caption(
        "Pick a page from the left nav, or use the tiles below. "
        "Every screen goes through the FastAPI surface - if something "
        "looks wrong here, it's wrong in the API."
    )

    try:
        summary = get_client().status_summary()
    except ApiError as exc:
        st.error(f"Could not load status summary: {exc.detail}")
        return
    except Exception as exc:
        st.error(f"API unreachable: {exc}")
        return

    cols = st.columns(4)
    cols[0].metric("Companies", summary.get("companies_total", 0))
    review = summary.get("review_queue_by_status", {}) or {}
    cols[1].metric("Open review items", review.get("open", 0))
    cols[2].metric("Assigned", review.get("assigned", 0))
    latest_run = summary.get("latest_run") or {}
    cols[3].metric("Latest run", latest_run.get("status", "none"))

    st.divider()
    st.subheader("Pages")
    st.markdown(
        "- **Company** - monthly series with intervals, event markers, and evidence.\n"
        "- **Review Queue** - claim, resolve, or dismiss flagged months.\n"
        "- **Overrides** - inspect active overrides and post new ones.\n"
        "- **Benchmarks** - estimate-vs-benchmark disagreement report.\n"
        "- **Status** - runs and coverage dashboard."
    )


def main() -> None:
    _render_sidebar()
    _render_home()


if __name__ == "__main__":
    main()
else:
    # Streamlit runs modules top-to-bottom, so the call below is what
    # actually paints the home page when ``streamlit run app.py`` is used.
    main()
