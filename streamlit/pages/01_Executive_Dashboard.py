"""Executive Dashboard — headline KPIs from the Gold layer's sales_kpi_summary
snapshot (written by notebooks/03_gold/07_sales_dashboard.py).

Foundation scope: this page is wired to a real query (it's a single-row
lookup, not a business-logic-heavy computation) — trend charts, territory/
product breakdowns, and narrative insights are planned for the next phase.
"""

import streamlit as st

from components.filters import get_filters
from components.header import render_page_header
from components.kpi_cards import render_kpi_row
from components.tables import render_empty_state
from utils.databricks import DatabricksConnectionError, DatabricksQueryError, run_query
from utils.helpers import format_currency, format_number
from utils.queries import sql_kpi_summary

render_page_header(
    title="Executive Dashboard",
    description="Headline sales KPIs from the Gold warehouse layer.",
    breadcrumb=["Home", "Executive Dashboard"],
)

filters = get_filters()

df = None
with st.spinner("Loading KPI summary..."):
    try:
        df = run_query(sql_kpi_summary(catalog=filters["catalog"], gold_schema=filters["schema"]))
    except (DatabricksConnectionError, DatabricksQueryError) as exc:
        st.error(f"Could not load the Gold KPI summary: {exc}")

if df is None:
    render_empty_state(
        "Connect to Databricks (see ⚙ Settings) to load live KPIs from "
        f"`{filters['catalog']}.{filters['schema']}.sales_kpi_summary`.",
        icon="🔌",
    )
elif df.empty:
    render_empty_state(
        "sales_kpi_summary has no rows yet — run 03_gold/07_sales_dashboard.py first.",
        icon="📭",
    )
else:
    row = df.iloc[0]
    render_kpi_row(
        [
            {"label": "Total Revenue", "value": format_currency(row.get("total_revenue"))},
            {"label": "Total Orders", "value": format_number(row.get("total_orders"))},
            {"label": "Total Customers", "value": format_number(row.get("total_customers"))},
            {"label": "Avg Order Value", "value": format_currency(row.get("avg_order_value"))},
        ]
    )
    st.caption(f"Top territory: {row.get('top_territory_name', '—')} · Top product: "
               f"{row.get('top_product_name', '—')}")

st.divider()
st.info(
    "This is the application foundation. Trend charts, territory/product "
    "breakdowns, and narrative business insights are planned for the next "
    "implementation phase."
)
