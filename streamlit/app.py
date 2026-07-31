"""AIDE — application entry point.

Classic filename-based multipage (the pages/ directory, auto-discovered by
Streamlit because it sits next to this entry-point script) replaces the
previous st.Page()/st.navigation() implementation, which crashed on Azure
Databricks Apps with `AttributeError: 'StreamlitPage' object has no
attribute '_default'`. Classic multipage has been stable across a much wider
range of Streamlit versions and removes that whole newer-API surface.

Under classic multipage, this file is always the default/first page shown —
it does not automatically become "whichever page was previously the
default" the way st.Page(..., default=True) did. To preserve the original
"Executive Dashboard is the landing page" behavior without duplicating that
page's logic here, this file immediately redirects to it via st.switch_page
(a simple, standalone primitive, unrelated to the st.navigation()/st.Page()
API that crashed).
"""

import streamlit as st

from utils.config import NAV_PAGES

st.switch_page(NAV_PAGES[0]["path"])
