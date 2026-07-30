"""Global filters — Catalog, Schema, Date Range, Territory, Customer Type,
Top N, Theme.

Rendered once in the sidebar (called from app.py, so it appears on every
page), persisted to st.session_state so any page can read the current values
via get_filters() without re-rendering the widgets itself.
"""

import datetime

import streamlit as st

from utils.config import DEFAULT_CATALOG, DEFAULT_GOLD_SCHEMA, DEFAULT_TOP_N

# "All" is always valid; the real territory list is populated from dim_territory
# once a page wires up that query — kept as a static placeholder here rather
# than querying the warehouse from a component that renders on every page load.
_TERRITORY_OPTIONS = ["All"]
_CUSTOMER_TYPE_OPTIONS = ["All", "Individual", "Store"]

_DEFAULT_FILTERS = {
    "catalog": DEFAULT_CATALOG,
    "schema": DEFAULT_GOLD_SCHEMA,
    "date_range": None,
    "territory": "All",
    "customer_type": "All",
    "top_n": DEFAULT_TOP_N,
    "theme": "Dark",
}


def render_global_filters() -> dict:
    """Render the global filter widgets in the current container — call this
    inside `with st.sidebar:`. Returns the current filter values as a dict.
    """
    st.markdown('<div class="aide-sidebar-label">Global Filters</div>', unsafe_allow_html=True)

    catalog = st.text_input("Catalog", value=DEFAULT_CATALOG, key="aide_filter_catalog")
    schema = st.text_input("Schema", value=DEFAULT_GOLD_SCHEMA, key="aide_filter_schema")

    today = datetime.date.today()
    date_range = st.date_input(
        "Date Range",
        value=(today - datetime.timedelta(days=365), today),
        key="aide_filter_date_range",
    )

    territory = st.selectbox("Territory", _TERRITORY_OPTIONS, key="aide_filter_territory")
    customer_type = st.selectbox(
        "Customer Type", _CUSTOMER_TYPE_OPTIONS, key="aide_filter_customer_type"
    )
    top_n = st.slider(
        "Top N", min_value=5, max_value=50, value=DEFAULT_TOP_N, step=5, key="aide_filter_top_n"
    )
    theme = st.selectbox("Theme", ["Dark", "Light"], key="aide_theme")

    filters = {
        "catalog": catalog,
        "schema": schema,
        "date_range": date_range,
        "territory": territory,
        "customer_type": customer_type,
        "top_n": top_n,
        "theme": theme,
    }
    st.session_state["aide_filters"] = filters
    return filters


def get_filters() -> dict:
    """Read the current global filter values without re-rendering widgets."""
    return st.session_state.get("aide_filters", _DEFAULT_FILTERS)
