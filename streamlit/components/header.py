"""Reusable page header: breadcrumb + icon + title + description.

Every page calls render_page_header() once, near the top, per the "every page
must have a professional header, description, breadcrumb" requirement.

Each page also passes its own `icon`/`accent` — a distinct color identity per
page (Executive Dashboard=green, AI Data Catalog=blue, AI Data Model
Explorer=purple, Warehouse Explorer/SQL Playground=cyan, AI Assistant=orange,
Data Governance=red, Settings=grey) while keeping the exact same header
layout everywhere — one consistent design language, not a different look per
page. `accent` is a semantic key into _ACCENT_VARS (reusing the app's one
shared color palette in assets/style.css, never a raw hex/new color).
"""

import html

import streamlit as st

# Semantic accent key -> (CSS color variable, CSS soft/tint variable), all
# already defined in assets/style.css — the header never introduces a color
# outside that shared palette.
_ACCENT_VARS = {
    "info": ("--aide-accent", "--aide-accent-soft"),
    "success": ("--aide-success", "--aide-success-soft"),
    "warning": ("--aide-warning", "--aide-warning-soft"),
    "error": ("--aide-error", "--aide-error-soft"),
    "ai": ("--aide-ai", "--aide-ai-soft"),
    "cyan": ("--aide-cyan", "--aide-cyan-soft"),
    "orange": ("--aide-orange", "--aide-orange-soft"),
    "bronze": ("--aide-bronze", "--aide-bronze-soft"),
    "silver": ("--aide-silver", "--aide-silver-soft"),
    "gold": ("--aide-gold", "--aide-gold-soft"),
}


def render_page_header(
    title: str, description: str, breadcrumb: list, icon: str = None, accent: str = None
) -> None:
    """Render the standard page header.

    `breadcrumb` is a list of strings, e.g. ["Home", "AI Data Catalog"] — the
    last item is treated as the current page (not a link, just styled plainly).
    `icon` is a single emoji/glyph shown in a colored chip beside the title;
    `accent` is a key into _ACCENT_VARS (defaults to "info"/blue if omitted or
    unrecognized, never a silent no-op).
    """
    accent_var, accent_soft_var = _ACCENT_VARS.get(accent, _ACCENT_VARS["info"])
    trail = " › ".join(html.escape(str(part)) for part in breadcrumb)

    icon_html = (
        f'<span class="aide-page-icon-chip">{html.escape(icon)}</span>' if icon else ""
    )

    description_html = (
        f'<div class="aide-page-description">{html.escape(description)}</div>'
        if description
        else ""
    )

    st.markdown(
        f"""
        <div class="aide-page-header"
             style="--aide-page-accent: var({accent_var}); --aide-page-accent-soft: var({accent_soft_var});">
          <div class="aide-breadcrumb">{trail}</div>
          <div class="aide-page-title-row">
            {icon_html}
            <div class="aide-page-title">{html.escape(title)}</div>
          </div>
          {description_html}
        </div>
        """,
        unsafe_allow_html=True,
    )
