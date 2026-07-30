"""SQL editor with syntax highlighting.

New component (not in the original file list) — justified because both
SQL Playground and the AI Assistant ("edit the generated SQL before running
it") need the identical editor; without this, the streamlit-ace integration
and its fallback logic would be duplicated across two pages.

Wraps streamlit-ace, a third-party custom component — the risk with any such
component is a version-compatibility break, so this degrades to a plain
st.text_area (no highlighting, but fully functional) at both import time and
render time. The app must never crash over an optional dependency.
"""

import logging

import streamlit as st

logger = logging.getLogger("aide.sql_editor")

try:
    from streamlit_ace import st_ace

    _ACE_AVAILABLE = True
except ImportError:
    _ACE_AVAILABLE = False
    logger.warning("streamlit-ace not installed; SQL editors will fall back to a plain text area.")


def render_sql_editor(value: str, key: str, height: int = 220) -> str:
    """Render a SQL editor and return its current content.

    Uses streamlit-ace (real SQL syntax highlighting) when available; falls
    back to a plain st.text_area otherwise.
    """
    if _ACE_AVAILABLE:
        try:
            return st_ace(
                value=value,
                language="sql",
                theme="tomorrow_night",
                key=key,
                height=height,
                font_size=14,
                show_gutter=True,
                auto_update=True,
            )
        except Exception as exc:
            logger.warning("streamlit-ace failed to render (%s); falling back to text_area.", exc)

    return st.text_area("SQL query", value=value, height=height, key=f"{key}_fallback")


def set_sql_editor_value(key: str, value: str) -> None:
    """Programmatically overwrite an editor's content (e.g. "Load" a saved
    query/history entry) ahead of its next render.

    Must set both the ace key and its "_fallback" text_area key: whichever
    one is actually live for `key` depends on whether streamlit-ace loaded
    successfully, which the caller shouldn't need to know about.
    """
    st.session_state[key] = value
    st.session_state[f"{key}_fallback"] = value
