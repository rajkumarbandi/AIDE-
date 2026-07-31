"""Theme application — injects assets/style.css once per page render.

Called by components/shell.py::render_app_shell(), so every page applies it
independently (classic multipage runs each page as its own script — see
components/shell.py's docstring for why that matters).
"""

import logging
from pathlib import Path

import streamlit as st

logger = logging.getLogger("aide.styles")

_CSS_PATH = Path(__file__).resolve().parent.parent / "assets" / "style.css"

_LIGHT_OVERRIDES = """
:root {
  --aide-bg: #f7f8fa;
  --aide-surface: #ffffff;
  --aide-surface-hover: #f3f4f6;
  --aide-border: #e5e7eb;
  --aide-text: #111827;
  --aide-text-muted: #6b7280;
  --aide-accent-soft: rgba(37, 99, 235, 0.08);
}
"""


def apply_theme() -> None:
    """Inject the base stylesheet, plus light-mode overrides if the "Theme"
    global filter (session_state["aide_theme"]) is set to "Light". Dark is the
    default, per the app's design brief.
    """
    try:
        css = _CSS_PATH.read_text(encoding="utf-8")
    except OSError as exc:
        logger.warning("Could not load %s: %s — continuing without custom styling.", _CSS_PATH, exc)
        return

    if st.session_state.get("aide_theme", "Dark") == "Light":
        css += _LIGHT_OVERRIDES
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
