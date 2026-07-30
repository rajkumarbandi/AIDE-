"""Sidebar status block: Connection Status, Warehouse Status, Current Catalog,
Current Schema — rendered below Streamlit's native page navigation (which
st.navigation places automatically; this component doesn't rebuild nav).
"""

import streamlit as st

from utils.config import DEFAULT_CATALOG, DEFAULT_GOLD_SCHEMA
from utils.databricks import is_connected
from utils.gemini import is_configured


def _status_line(label: str, ok: bool, ok_text: str, fail_text: str) -> None:
    dot_class = "aide-status-success" if ok else "aide-status-error"
    text = ok_text if ok else fail_text
    st.markdown(
        f'<div><span class="aide-status-dot {dot_class}"></span>{label}: {text}</div>',
        unsafe_allow_html=True,
    )


def render_sidebar_status(
    catalog: str = DEFAULT_CATALOG, schema: str = DEFAULT_GOLD_SCHEMA
) -> None:
    """Render the status block. Connectivity checks are best-effort and cheap
    (cached connections/clients) — a missing secrets.toml during local
    development is expected, not an error to alarm over.
    """
    st.markdown('<div class="aide-sidebar-label">Connection Status</div>', unsafe_allow_html=True)
    _status_line("Databricks", is_connected(), "Connected", "Not configured")
    _status_line("Gemini AI", is_configured(), "Connected", "Not configured")

    st.markdown('<div class="aide-sidebar-label">Warehouse Status</div>', unsafe_allow_html=True)
    st.markdown(f"**Current Catalog:** `{catalog}`")
    st.markdown(f"**Current Schema:** `{schema}`")
