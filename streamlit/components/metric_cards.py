"""Generic metric card primitive — a single stat with optional green/amber/red
status coloring, for secondary/detail-panel stats where st.metric's plain
two-tone delta coloring isn't expressive enough (e.g. a null% indicator).

Uses the .aide-card CSS class from assets/style.css, not a from-scratch style
block per call.
"""

import streamlit as st

from utils.config import STATUS_COLORS


def render_metric_card(label: str, value: str, status: str = None, caption: str = None) -> None:
    """`status`: one of "success"/"warning"/"error"/"neutral", or None for a plain card."""
    dot = ""
    if status:
        color = STATUS_COLORS.get(status, STATUS_COLORS["neutral"])
        dot = f'<span class="aide-status-dot" style="background:{color};"></span>'

    caption_html = ""
    if caption:
        caption_html = (
            f'<div style="color:var(--aide-text-muted);font-size:12px;">{caption}</div>'
        )

    st.markdown(
        f"""
        <div class="aide-card">
          <div style="font-size:13px;color:var(--aide-text-muted);">{dot}{label}</div>
          <div style="font-size:22px;font-weight:700;margin-top:4px;">{value}</div>
          {caption_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_metric_grid(metrics: list, columns: int = 3) -> None:
    """`metrics`: list of dicts {"label", "value", "status" (optional), "caption" (optional)}."""
    if not metrics:
        return
    cols = st.columns(columns)
    for i, metric in enumerate(metrics):
        with cols[i % columns]:
            render_metric_card(
                metric.get("label", ""),
                metric.get("value", "—"),
                metric.get("status"),
                metric.get("caption"),
            )
