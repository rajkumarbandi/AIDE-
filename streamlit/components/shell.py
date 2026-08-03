"""Shared per-page application shell: page config, theme, and the sidebar
(logo, navigation, global filters, connection status).

Classic filename-based multipage (the pages/ directory) runs every page as
an independent script — unlike st.navigation()/.run(), which re-executes
app.py's shared setup code on every navigation, classic multipage does NOT
run app.py again when a user is on pages/02_AI_Data_Catalog.py. Each page
must call render_app_shell() as its very first statement (before any other
st.* call, since st.set_page_config() must run first and only once per
script) to get the same page config/theme/sidebar app.py previously supplied
to every page via st.navigation.

The custom sidebar nav (st.page_link, not st.navigation/st.Page — a separate,
older Streamlit primitive unaffected by the crash this replaces) preserves
the exact icons/titles from NAV_PAGES. Streamlit's own auto-generated nav
list is hidden via CSS (assets/style.css) so only this custom list shows —
if a future Streamlit version ever changes that internal DOM structure, the
only consequence is both lists showing, never a crash.
"""

from pathlib import Path

import streamlit as st

from components.filters import render_global_filters
from components.sidebar import render_sidebar_status
from components.styles import apply_theme
from utils.config import APP_TITLE, NAV_PAGES

_ASSETS_DIR = Path(__file__).resolve().parent.parent / "assets"
_FAVICON_PATH = _ASSETS_DIR / "favicon.png"


def render_app_shell() -> None:
    """Render the shared shell. Call this first, before any other st.* call."""
    st.set_page_config(
        page_title=APP_TITLE,
        # A dedicated flat-design PNG (dark blue -> purple gradient rounded
        # square, white "A"), not an emoji — set_page_config's page_icon
        # accepts a file path directly, so the browser tab favicon doesn't
        # depend on emoji font rendering across OSes.
        page_icon=str(_FAVICON_PATH) if _FAVICON_PATH.exists() else "🧠",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    apply_theme()

    with st.sidebar:
        logo_path = _ASSETS_DIR / "logo.png"
        if logo_path.exists():
            st.image(str(logo_path), width=56)
        st.markdown(f"**{APP_TITLE}**")
        st.divider()

        st.markdown('<div class="aide-sidebar-label">Navigate</div>', unsafe_allow_html=True)
        for nav in NAV_PAGES:
            st.page_link(nav["path"], label=nav["title"], icon=nav["icon"])
        st.divider()

        filters = render_global_filters()
        st.divider()
        render_sidebar_status(catalog=filters["catalog"], schema=filters["schema"])
