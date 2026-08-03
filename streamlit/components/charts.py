"""Reusable Plotly chart builders, styled to match the app's dark theme.

Plotly chosen for consistency with components/graph.py (same library, same
rendering path via st.plotly_chart) and because it has native pan/zoom/hover
without any extra component dependency.
"""

from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

_PLOT_BGCOLOR = "rgba(0,0,0,0)"
_FONT_COLOR = "#e6e6e6"
_GRID_COLOR = "#2a2f3a"
_ACCENT = "#2563eb"
_SURFACE = "#161b22"

# The same design-token palette as assets/style.css's CSS variables (accent,
# success, ai, warning, cyan, orange, silver, gold) — every multi-category
# chart (currently just the pie) cycles through this instead of Plotly's
# default rainbow qualitative palette, so chart colors stay inside the app's
# one consistent, non-flashy color scheme.
_QUALITATIVE_PALETTE = [
    "#2563eb",  # accent (blue)
    "#16a34a",  # success (green)
    "#7c3aed",  # ai (purple)
    "#d97706",  # warning (amber)
    "#06b6d4",  # cyan
    "#f97316",  # orange
    "#94a3b8",  # silver
    "#eab308",  # gold
]

_HOVERLABEL = dict(bgcolor=_SURFACE, bordercolor=_GRID_COLOR, font=dict(color=_FONT_COLOR))


def _apply_layout(fig: go.Figure, title: Optional[str] = None) -> go.Figure:
    fig.update_layout(
        title=title,
        plot_bgcolor=_PLOT_BGCOLOR,
        paper_bgcolor=_PLOT_BGCOLOR,
        font=dict(color=_FONT_COLOR, family="Segoe UI, -apple-system, sans-serif"),
        margin=dict(l=40, r=20, t=40 if title else 20, b=40),
        xaxis=dict(gridcolor=_GRID_COLOR, zerolinecolor=_GRID_COLOR),
        yaxis=dict(gridcolor=_GRID_COLOR, zerolinecolor=_GRID_COLOR),
        hoverlabel=_HOVERLABEL,
        legend=dict(bgcolor=_PLOT_BGCOLOR),
    )
    return fig


def render_bar_chart(df: pd.DataFrame, x: str, y: str, title: Optional[str] = None) -> None:
    fig = go.Figure(
        go.Bar(x=df[x], y=df[y], marker_color=_ACCENT, marker_line_width=0)
    )
    st.plotly_chart(_apply_layout(fig, title), width="stretch")


def render_line_chart(df: pd.DataFrame, x: str, y: str, title: Optional[str] = None) -> None:
    fig = go.Figure(
        go.Scatter(
            x=df[x], y=df[y], mode="lines+markers",
            line=dict(color=_ACCENT, width=2.5),
            marker=dict(size=6),
        )
    )
    st.plotly_chart(_apply_layout(fig, title), width="stretch")


def render_pie_chart(
    df: pd.DataFrame, names: str, values: str, title: Optional[str] = None
) -> None:
    fig = go.Figure(
        go.Pie(
            labels=df[names],
            values=df[values],
            hole=0.5,
            marker=dict(colors=_QUALITATIVE_PALETTE, line=dict(color=_SURFACE, width=2)),
        )
    )
    st.plotly_chart(_apply_layout(fig, title), width="stretch")
