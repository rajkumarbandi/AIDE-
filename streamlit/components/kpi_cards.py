"""Headline KPI row — the big top-of-page metrics (e.g. Total Revenue, Orders)
— plus small stat badges for secondary callouts (Top Territory, Top Product).

Custom HTML/CSS cards, not st.metric(): st.metric's fixed-size value text
clipped for a long formatted currency string ("$2,758,...") in a 4-column
row — a real reported bug. Uses assets/style.css's .aide-kpi-card classes
(responsive clamp()-based font sizing so the value always fits without
truncation, equal height via min-height) and the existing accent-color CSS
variables (green/blue/purple/amber already defined for badges elsewhere),
so no new colors are introduced anywhere in the app.

Icons are small hand-built inline SVGs using only simple primitives (circle,
rect, line, polyline) rather than intricate bezier paths — deliberately
conservative, since this was authored without a live browser to visually
verify rendering.
"""

import html

import streamlit as st

_ACCENT_CLASSES = {
    "success": "aide-kpi-success",
    "info": "aide-kpi-info",
    "ai": "aide-kpi-ai",
    "warning": "aide-kpi-warning",
}

_SVG_ATTRS = 'viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"'

_ICONS = {
    "money": (
        f'<svg {_SVG_ATTRS}>'
        '<circle cx="12" cy="12" r="9"/>'
        '<text x="12" y="16" text-anchor="middle" font-size="11" font-weight="700" '
        'fill="currentColor" stroke="none">$</text>'
        "</svg>"
    ),
    "orders": (
        f'<svg {_SVG_ATTRS}>'
        '<rect x="3" y="8" width="18" height="12" rx="1"/>'
        '<line x1="3" y1="13" x2="21" y2="13"/>'
        '<polyline points="8,8 8,5 16,5 16,8"/>'
        "</svg>"
    ),
    "customers": (
        f'<svg {_SVG_ATTRS}>'
        '<circle cx="8" cy="9" r="3.2"/>'
        '<circle cx="16" cy="9.5" r="2.6"/>'
        '<path d="M2.5 19.5c0-3.6 2.5-6 5.5-6s5.5 2.4 5.5 6"/>'
        '<path d="M13.2 19.5c0-2.8 1.6-4.8 3.8-5.3"/>'
        "</svg>"
    ),
    "trending_up": (
        f'<svg {_SVG_ATTRS}>'
        '<polyline points="3,17 9,11 13,15 21,5"/>'
        '<polyline points="21,11 21,5 15,5"/>'
        "</svg>"
    ),
}


def render_kpi_row(cards: list) -> None:
    """cards: list of dicts:
      - label: str
      - value: str (already formatted — pass a compact value like "$2.76M"
        for headline KPIs so it reliably fits in a narrow card)
      - help: Optional[str] — full-precision value shown as a native hover
        tooltip (e.g. "$2,758,302.34"), never lost just because the visible
        value is compact
      - icon: Optional[str] — a key into _ICONS ("money"/"orders"/
        "customers"/"trending_up"), or raw emoji/SVG markup
      - accent: Optional[str] — one of "success"/"info"/"ai"/"warning"
    """
    if not cards:
        return
    columns = st.columns(len(cards))
    for column, card in zip(columns, cards):
        with column:
            accent_class = _ACCENT_CLASSES.get(card.get("accent"), "")
            icon_key = card.get("icon")
            icon_markup = _ICONS.get(icon_key, icon_key or "")
            tooltip = f' title="{html.escape(str(card["help"]))}"' if card.get("help") else ""
            st.markdown(
                f"""
                <div class="aide-kpi-card {accent_class}">
                  <div class="aide-kpi-header">
                    <span class="aide-kpi-icon">{icon_markup}</span>
                    <span class="aide-kpi-label">{html.escape(str(card.get('label', '')))}</span>
                  </div>
                  <div class="aide-kpi-value"{tooltip}>{html.escape(str(card.get('value', '—')))}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_stat_badges(badges: list) -> None:
    """badges: list of dicts {"icon" (an emoji), "label", "value"} — compact
    horizontal cards for secondary callouts like Top Territory/Top Product.
    Long values wrap onto a second line rather than being cut off with an
    ellipsis (a real AdventureWorks product name can be fairly long).
    """
    if not badges:
        return
    columns = st.columns(len(badges))
    for column, badge in zip(columns, badges):
        with column:
            st.markdown(
                f"""
                <div class="aide-stat-badge">
                  <span class="aide-stat-badge-icon">{badge.get('icon', '')}</span>
                  <div>
                    <div class="aide-stat-badge-label">{html.escape(str(badge.get('label', '')))}</div>
                    <div class="aide-stat-badge-value">{html.escape(str(badge.get('value', '—')))}</div>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
