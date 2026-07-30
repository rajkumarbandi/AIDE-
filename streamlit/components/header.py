"""Reusable page header: breadcrumb + title + description.

Every page calls render_page_header() once, near the top, per the "every page
must have a professional header, description, breadcrumb" requirement.
"""

import streamlit as st


def render_page_header(title: str, description: str, breadcrumb: list) -> None:
    """Render the standard page header.

    `breadcrumb` is a list of strings, e.g. ["Home", "AI Data Catalog"] — the
    last item is treated as the current page (not a link, just styled plainly).
    """
    trail = " › ".join(breadcrumb)
    st.markdown(f'<div class="aide-breadcrumb">{trail}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="aide-page-title">{title}</div>', unsafe_allow_html=True)
    if description:
        st.markdown(
            f'<div class="aide-page-description">{description}</div>', unsafe_allow_html=True
        )
