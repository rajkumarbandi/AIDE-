"""Reusable table rendering with a consistent empty state.

Wraps st.dataframe (native, preferred over any custom grid) with the
"show a friendly message instead of a blank grid when there's no data"
behavior every page needs.
"""

from typing import Optional

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
