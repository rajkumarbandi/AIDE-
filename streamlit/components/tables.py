"""Reusable table rendering with a consistent empty state.

Wraps st.dataframe (native, preferred over any custom grid) with the
"show a friendly message instead of a blank grid when there's no data"
behavior every page needs. render_html_table() below is the deliberate
exception — see its docstring for why.
"""

import html
from typing import Callable, Optional

import pandas as pd
import streamlit as st


def render_dataframe(
    df: Optional[pd.DataFrame],
    empty_message: str = "No data available yet.",
    height: Optional[int] = "auto",
    **st_dataframe_kwargs,
) -> None:
    """Render `df` via st.dataframe, or a consistent empty state if it's None/empty.

    `height="auto"` (content-fit) is st.dataframe's real default in the
    installed Streamlit version — verified directly, since plain `None` (the
    natural-looking default) raises StreamlitInvalidHeightError here.
    """
    if df is None or df.empty:
        render_empty_state(empty_message)
        return
    st.dataframe(df, width="stretch", hide_index=True, height=height, **st_dataframe_kwargs)


def render_html_table(
    df: Optional[pd.DataFrame],
    cell_renderers: Optional[dict] = None,
    column_labels: Optional[dict] = None,
    max_height: int = 420,
    empty_message: str = "No data available yet.",
) -> None:
    """A fully custom HTML table for small, high-visibility listings where
    st.dataframe's canvas-rendered grid (glide-data-grid) can't provide sticky
    headers, alternating row colors, or inline badge/icon markup per cell —
    see assets/style.css's [data-testid="stDataFrame"] comment for that
    constraint. Deliberately NOT a general replacement for render_dataframe:
    there's no virtualization, so this is only meant for bounded listings
    (tens of rows, e.g. the AI Data Catalog's table list), never a
    thousand-row query result.

    `cell_renderers`: optional {column_name: callable(value) -> html string}
    to render a cell as something richer than escaped plain text (e.g. a
    layer badge, a status icon + label) — the callable owns its own escaping
    if it embeds untrusted text. Any column without a renderer falls back to
    plain, HTML-escaped text (NaN/None rendered as "—").
    `column_labels`: optional {column_name: display label} override; defaults
    to a title-cased version of the column name.
    """
    if df is None or df.empty:
        render_empty_state(empty_message)
        return

    cell_renderers: dict = cell_renderers or {}
    column_labels: dict = column_labels or {}
    columns = list(df.columns)

    header_html = "".join(
        f"<th>{html.escape(column_labels.get(col, col.replace('_', ' ').title()))}</th>"
        for col in columns
    )

    body_rows = []
    for _, row in df.iterrows():
        cells = []
        for col in columns:
            value = row[col]
            renderer: Optional[Callable] = cell_renderers.get(col)
            if renderer is not None:
                cell_html = renderer(value)
            elif value is None or (isinstance(value, float) and pd.isna(value)):
                cell_html = "—"
            else:
                cell_html = html.escape(str(value))
            cells.append(f"<td>{cell_html}</td>")
        body_rows.append(f"<tr>{''.join(cells)}</tr>")

    st.markdown(
        f"""
        <div class="aide-html-table-wrap" style="max-height:{int(max_height)}px;">
          <table class="aide-html-table">
            <thead><tr>{header_html}</tr></thead>
            <tbody>{''.join(body_rows)}</tbody>
          </table>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_empty_state(message: str, icon: str = "📭") -> None:
    """Consistent "nothing to show" placeholder, reused wherever a table/chart
    has no data (not yet connected, empty query result, etc.).
    """
    st.markdown(
        f"""
        <div class="aide-empty-state">
          <div class="aide-empty-icon">{icon}</div>
          <div>{message}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
