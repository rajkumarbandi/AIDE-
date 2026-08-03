"""Generic metric card primitive — a single stat with optional green/amber/red
status coloring, for secondary/detail-panel stats where st.metric's plain
two-tone delta coloring isn't expressive enough (e.g. a null% indicator).

Uses the .aide-card CSS class from assets/style.css, not a from-scratch style
block per call.
"""

import html

import streamlit as st

from utils.config import STATUS_COLORS

# Maps a badge "kind" to assets/style.css's .aide-badge-* class — the app-wide
# color convention: blue=info/fact, green=success/dimension, amber=warning,
# red=error, purple=AI/metadata, orange=reviewer, plus bronze/silver/gold for
# medallion layer.
_BADGE_CLASSES = {
    "info": "aide-badge-info",
    "success": "aide-badge-success",
    "warning": "aide-badge-warning",
    "error": "aide-badge-error",
    "ai": "aide-badge-ai",
    "bronze": "aide-badge-bronze",
    "silver": "aide-badge-silver",
    "gold": "aide-badge-gold",
    "fact": "aide-badge-fact",
    "dimension": "aide-badge-dimension",
    "metadata": "aide-badge-metadata",
    "reviewer": "aide-badge-reviewer",
}

# Governance status/priority values map to a badge kind by real business
# meaning (e.g. "Rejected"/"Critical" read as errors, "Resolved"/"Implemented"
# as success), not just alphabetically.
_STATUS_BADGE_KIND = {
    "New": "info",
    "Flagged": "warning",
    "Under Review": "info",
    "Approved": "success",
    "Rejected": "error",
    "Resolved": "success",
    "Implemented": "success",
}
_PRIORITY_BADGE_KIND = {"Low": "info", "Medium": "warning", "High": "warning", "Critical": "error"}

# Metadata AI-analysis processing statuses (02_ai_metadata/03_metadata_analyzer.py's
# incremental workflow) — a distinct vocabulary from governance comment statuses above,
# so it gets its own mapping rather than reusing _STATUS_BADGE_KIND (which has no entry
# for e.g. "SUCCESS"/"FAILED" and would silently fall back to the wrong color).
_PROCESSING_STATUS_BADGE_KIND = {
    "PENDING": "info",
    "PROCESSING": "warning",
    "SUCCESS": "success",
    "FAILED": "error",
    "SKIPPED": "success",  # skipped means "already succeeded", not a failure
}


def render_badge(label: str, kind: str = "info") -> str:
    """Return (not render directly) an inline HTML badge — meant to be
    embedded inside a larger st.markdown(..., unsafe_allow_html=True) call
    alongside other content, e.g. f"{render_badge(status, 'success')} · {author}".
    """
    css_class = _BADGE_CLASSES.get(kind, _BADGE_CLASSES["info"])
    return f'<span class="aide-badge {css_class}">{html.escape(label)}</span>'


def status_badge(status: str) -> str:
    """A governance comment status, badge-colored by real workflow meaning."""
    return render_badge(status, _STATUS_BADGE_KIND.get(status, "info"))


def processing_status_badge(status: str) -> str:
    """An AI-analysis processing status (PENDING/PROCESSING/SUCCESS/FAILED/SKIPPED),
    badge-colored by real workflow meaning — distinct from status_badge() above.
    """
    return render_badge(status, _PROCESSING_STATUS_BADGE_KIND.get(status, "info"))


def priority_badge(priority: str) -> str:
    """A governance comment priority, badge-colored by real urgency."""
    return render_badge(priority, _PRIORITY_BADGE_KIND.get(priority, "info"))


def layer_badge(layer: str) -> str:
    """A medallion layer (bronze/silver/gold), badge-colored to match the
    same convention used by the AI Data Model Explorer's ER diagram.
    """
    return render_badge(layer.title(), layer.lower() if layer.lower() in ("bronze", "silver", "gold") else "info")


def table_kind_badge(kind: str) -> str:
    """A Gold table kind (fact/dimension), badge-colored to match
    utils.config.LAYER_COLORS' fact=blue/dimension=green convention.
    """
    return render_badge(kind.title(), kind.lower() if kind.lower() in ("fact", "dimension") else "info")


def reviewer_badge(name: str) -> str:
    """A reviewer/comment author name, always orange — the app-wide color
    convention for anything reviewer-related (Data Governance, AI Data
    Catalog's per-table comment tab).
    """
    return render_badge(name, "reviewer")


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
