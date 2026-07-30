"""AIDE — application entry point.

Sets page config/theme once, defines the 7-page navigation, and renders the
persistent sidebar shell (branding, global filters, connection status) that
appears identically on every page — the actual page content is delegated to
st.navigation()/.run(), per Streamlit's current (st.Page-based) multipage
pattern.
"""

from pathlib import Path

import streamlit as st

from components.filters import render_global_filters
from components.sidebar import render_sidebar_status
from components.styles import apply_theme
from utils.config import APP_ICON, APP_TITLE, NAV_PAGES

_ASSETS_DIR = Path(__file__).parent / "assets"

st.set_page_config(
    page_title=APP_TITLE,
    page_icon=APP_ICON,
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_theme()

pages = [
    st.Page(nav["path"], title=nav["title"], icon=nav["icon"], default=(i == 0))
    for i, nav in enumerate(NAV_PAGES)
]
navigation = st.navigation(pages)

with st.sidebar:
    logo_path = _ASSETS_DIR / "logo.png"
    if logo_path.exists():
        st.image(str(logo_path), width=56)
    st.markdown(f"**{APP_TITLE}**")
    st.divider()
    filters = render_global_filters()
    st.divider()
    render_sidebar_status(catalog=filters["catalog"], schema=filters["schema"])

navigation.run()
