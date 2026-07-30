"""Headline KPI row — the big top-of-page metrics (e.g. Total Revenue, Orders).

Built on native st.metric() inside st.container(border=True), per "use
Streamlit native components wherever possible" — no hand-rolled HTML card grid
needed for the common case.
"""

import streamlit as st


def render_kpi_row(cards: list) -> None:
    """cards: list of dicts, each {"label", "value", "delta" (optional), "help"
    (optional)}. Renders one bordered container + st.metric per card, in equal
    columns.
    """
    if not cards:
        return
    columns = st.columns(len(cards))
    for column, card in zip(columns, cards):
        with column:
            with st.container(border=True):
                st.metric(
                    label=card.get("label", ""),
                    value=card.get("value", "—"),
                    delta=card.get("delta"),
                    help=card.get("help"),
                )
