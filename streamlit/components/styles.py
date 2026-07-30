"""Theme application — injects assets/style.css once per page render.

Called at the top of app.py, which (via st.navigation) means it applies to
every page automatically. Individual page files don't need to call this
themselves.
"""

from pathlib import Path

import streamlit as st

_CSS_PATH = Path(__file__).parent.parent / "assets" / "style.css"

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
    css = _CSS_PATH.read_text(encoding="utf-8")
    if st.session_state.get("aide_theme", "Dark") == "Light":
        css += _LIGHT_OVERRIDES
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
