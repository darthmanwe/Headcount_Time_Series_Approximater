"""Streamlit review UI configuration.

The UI is a pure FastAPI client. The base URL is read from (in order):

1. ``HEADCOUNT_API_URL`` env var (matches ``hc serve``'s host/port).
2. Streamlit ``st.secrets["api_url"]`` if present.
3. Default ``http://127.0.0.1:8000``.

A :class:`HeadcountApiClient` is cached in ``st.session_state`` so we
reuse a single httpx connection pool across reruns.
"""

from __future__ import annotations

import os
from typing import Any

import streamlit as st

from apps.review_ui.api_client import ClientConfig, HeadcountApiClient

_CLIENT_KEY = "_hc_api_client"
_CONFIG_KEY = "_hc_api_config"


def _resolve_base_url() -> str:
    env = os.environ.get("HEADCOUNT_API_URL")
    if env:
        return env
    try:
        secret = st.secrets.get("api_url")  # type: ignore[attr-defined]
    except (FileNotFoundError, AttributeError):
        secret = None
    if isinstance(secret, str) and secret:
        return secret
    return "http://127.0.0.1:8000"


def get_client() -> HeadcountApiClient:
    """Return the per-session API client, constructing on first call."""

    state: Any = st.session_state
    cfg = state.get(_CONFIG_KEY) or ClientConfig(base_url=_resolve_base_url())
    client = state.get(_CLIENT_KEY)
    if client is None:
        client = HeadcountApiClient(cfg)
        state[_CLIENT_KEY] = client
        state[_CONFIG_KEY] = cfg
    return client


def set_base_url(url: str) -> None:
    """Swap the API base URL at runtime (used by the sidebar config)."""

    state: Any = st.session_state
    old = state.get(_CLIENT_KEY)
    if old is not None:
        old.close()
    state[_CONFIG_KEY] = ClientConfig(base_url=url)
    state[_CLIENT_KEY] = HeadcountApiClient(state[_CONFIG_KEY])


def current_base_url() -> str:
    state: Any = st.session_state
    cfg = state.get(_CONFIG_KEY)
    if cfg is None:
        return _resolve_base_url()
    return str(cfg.base_url)


__all__ = ["current_base_url", "get_client", "set_base_url"]
