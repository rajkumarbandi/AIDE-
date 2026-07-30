"""Global filters — Catalog, Schema, Date Range, Territory, Customer Type,
Product Category, Salesperson, Year, Top N, Theme.

Rendered once in the sidebar (called from app.py, so it appears on every
page), persisted to st.session_state so any page can read the current values
via get_filters() without re-rendering the widgets itself. Year/Territory/
Category/Salesperson options are loaded live from the warehouse (cached) and
degrade to just "All" if the warehouse isn't reachable — this component
renders on every page, so it must never raise.
"""

import datetime

import streamlit as st

from utils.config import DATA_CACHE_TTL_SECONDS, DEFAULT_CATALOG, DEFAULT_GOLD_SCHEMA, DEFAULT_TOP_N
from utils.databricks import DatabricksConnectionError, DatabricksQueryError, run_query
from utils.queries import (
    sql_distinct_categories,
    sql_distinct_salespersons,
    sql_distinct_territories,
    sql_distinct_years,
)

_CUSTOMER_TYPE_OPTIONS = ["All", "Individual", "Store"]

_DEFAULT_FILTERS = {
    "catalog": DEFAULT_CATALOG,
    "schema": DEFAULT_GOLD_SCHEMA,
    "date_range": None,
    "territory": "All",
    "customer_type": "All",
    "product_category": "All",
    "salesperson": "All",
    "year": "All",
    "top_n": DEFAULT_TOP_N,
    "theme": "Dark",
}


@st.cache_data(ttl=DATA_CACHE_TTL_SECONDS, show_spinner=False)
def _load_filter_options(catalog: str, gold_schema: str) -> dict:
    """Live option lists for Year/Territory/Category/Salesperson. Each list is
    independently best-effort — one failing query doesn't blank out the others.
    """
    options = {"years": [], "territories": [], "categories": [], "salespersons": []}

    def _safe_list(sql: str, column: str) -> list:
        try:
            return run_query(sql)[column].astype(str).tolist()
        except (DatabricksConnectionError, DatabricksQueryError):
            return []

    options["years"] = _safe_list(sql_distinct_years(catalog, gold_schema), "year")
    options["territories"] = _safe_list(
        sql_distinct_territories(catalog, gold_schema), "territory_name"
    )
    options["categories"] = _safe_list(sql_distinct_categories(catalog, gold_schema), "category_name")
    options["salespersons"] = _safe_list(
        sql_distinct_salespersons(catalog, gold_schema), "salesperson_name"
    )
    return options


def render_global_filters() -> dict:
    """Render the global filter widgets in the current container — call this
    inside `with st.sidebar:`. Returns the current filter values as a dict.
    """
    st.markdown('<div class="aide-sidebar-label">Global Filters</div>', unsafe_allow_html=True)

    catalog = st.text_input("Catalog", value=DEFAULT_CATALOG, key="aide_filter_catalog")
    schema = st.text_input("Schema", value=DEFAULT_GOLD_SCHEMA, key="aide_filter_schema")

    options = _load_filter_options(catalog, schema)

    today = datetime.date.today()
    date_range = st.date_input(
        "Date Range",
        value=(today - datetime.timedelta(days=365), today),
        key="aide_filter_date_range",
    )

    year = st.selectbox("Year", ["All"] + options["years"], key="aide_filter_year")
    territory = st.selectbox(
        "Territory", ["All"] + options["territories"], key="aide_filter_territory"
    )
    product_category = st.selectbox(
        "Product Category", ["All"] + options["categories"], key="aide_filter_product_category"
    )
    salesperson = st.selectbox(
        "Salesperson", ["All"] + options["salespersons"], key="aide_filter_salesperson"
    )
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
        "product_category": product_category,
        "salesperson": salesperson,
        "year": year,
        "top_n": top_n,
        "theme": theme,
    }
    st.session_state["aide_filters"] = filters
    return filters


def get_filters() -> dict:
    """Read the current global filter values without re-rendering widgets."""
    return st.session_state.get("aide_filters", _DEFAULT_FILTERS)
