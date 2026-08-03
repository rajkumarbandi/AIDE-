"""Executive Dashboard — headline KPIs plus a full set of interactive,
filter-aware trend/breakdown charts and an AI-generated executive summary.

Every number on this page — the KPI row included — is computed live against
fact_sales joined to its dimensions (utils.queries._fact_joins), filtered by
the same global Year/Territory/Product Category/Salesperson `filters` dict
from components.filters.get_filters(). The KPI row previously read the
precomputed, all-time sales_kpi_summary table instead, which has no
Year/Territory/Category/Salesperson dimensionality at all and could never
respond to the sidebar filters — a real reported bug, fixed by switching it
to utils.queries.sql_kpi_summary_live() (see that function's docstring).

Top Territory/Top Product are derived from the SAME territory_df/
top_products_df DataFrames the charts/tables further down the page already
fetch — computed once, near the top, and reused everywhere, rather than
querying twice for the same filtered breakdown.
"""

import streamlit as st

from components.charts import render_bar_chart, render_line_chart, render_pie_chart
from components.filters import get_filters
from components.header import render_page_header
from components.kpi_cards import render_kpi_row
from components.shell import render_app_shell
from components.tables import render_dataframe, render_empty_state
from utils.databricks import DatabricksConnectionError, DatabricksQueryError, run_query
from utils.gemini import GeminiClientError, GeminiConfigurationError, generate_executive_summary
from utils.helpers import format_currency, format_number
from utils.queries import (
    sql_customer_growth,
    sql_kpi_summary_live,
    sql_monthly_revenue_trend,
    sql_order_trend,
    sql_quarterly_revenue,
    sql_revenue_by_category,
    sql_revenue_by_territory,
    sql_top_customers,
    sql_top_products,
)

render_app_shell()

render_page_header(
    title="Executive Dashboard",
    description="Headline KPIs, trends, and an AI-generated summary of the "
    "currently filtered sales data.",
    breadcrumb=["Home", "Executive Dashboard"],
)

filters = get_filters()
catalog, schema = filters["catalog"], filters["schema"]


def _safe_query(sql: str):
    """Run a query, returning None (not raising) on failure — every chart
    below degrades to its own empty state rather than taking the whole page
    down with it.
    """
    try:
        return run_query(sql)
    except (DatabricksConnectionError, DatabricksQueryError) as exc:
        st.session_state.setdefault("_dashboard_errors", []).append(str(exc))
        return None


# --- Shared fetches: computed once here, reused both by the KPI row below and
# --- by their own chart/table sections further down the page (no duplicate
# --- queries for the same filtered breakdown).
territory_df = _safe_query(sql_revenue_by_territory(catalog, schema, filters))
top_products_df = _safe_query(sql_top_products(catalog, schema, filters, filters["top_n"]))

# --- Headline KPI row — live, filter-aware (see sql_kpi_summary_live's docstring
# --- for why this replaced the precomputed sales_kpi_summary table) ---------
kpi_df = _safe_query(sql_kpi_summary_live(catalog, schema, filters))

if kpi_df is None:
    render_empty_state(
        "Connect to Databricks (see ⚙ Settings) to load live KPIs from "
        f"`{catalog}.{schema}.fact_sales`.",
        icon="🔌",
    )
elif kpi_df.empty or kpi_df.iloc[0].get("total_orders") in (None, 0):
    render_empty_state("No sales data for the current filters.", icon="📭")
else:
    row = kpi_df.iloc[0]
    top_territory_name = (
        territory_df.iloc[0]["territory_name"]
        if territory_df is not None and not territory_df.empty
        else "—"
    )
    top_product_name = (
        top_products_df.iloc[0]["product_name"]
        if top_products_df is not None and not top_products_df.empty
        else "—"
    )
    render_kpi_row(
        [
            {"label": "Total Revenue", "value": format_currency(row.get("total_revenue"))},
            {"label": "Total Orders", "value": format_number(row.get("total_orders"))},
            {"label": "Total Customers", "value": format_number(row.get("total_customers"))},
            {"label": "Avg Order Value", "value": format_currency(row.get("avg_order_value"))},
        ]
    )
    st.caption(f"Top territory: {top_territory_name} · Top product: {top_product_name}")

st.divider()
st.caption(
    "Everything on this page reflects the current Year / Territory / Product Category / "
    "Salesperson filters (sidebar)."
)

# --- Trends ------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("📈 Monthly Sales Trend")
    monthly_df = _safe_query(sql_monthly_revenue_trend(catalog, schema, filters))
    if monthly_df is not None and not monthly_df.empty:
        monthly_df["period"] = monthly_df["month_name"] + " " + monthly_df["year"].astype(str)
        render_line_chart(monthly_df, x="period", y="revenue")
    else:
        render_empty_state("No monthly revenue data for the current filters.")

with col2:
    st.subheader("📊 Quarterly Revenue")
    quarterly_df = _safe_query(sql_quarterly_revenue(catalog, schema, filters))
    if quarterly_df is not None and not quarterly_df.empty:
        quarterly_df["period"] = "Q" + quarterly_df["quarter"].astype(str) + " " + quarterly_df[
            "year"
        ].astype(str)
        render_bar_chart(quarterly_df, x="period", y="revenue")
    else:
        render_empty_state("No quarterly revenue data for the current filters.")

col3, col4 = st.columns(2)

with col3:
    st.subheader("🌍 Sales by Territory")
    if territory_df is not None and not territory_df.empty:
        render_bar_chart(territory_df, x="territory_name", y="revenue")
    else:
        render_empty_state("No territory data for the current filters.")

with col4:
    st.subheader("🛒 Sales by Product Category")
    category_df = _safe_query(sql_revenue_by_category(catalog, schema, filters))
    if category_df is not None and not category_df.empty:
        render_pie_chart(category_df, names="category_name", values="revenue")
    else:
        render_empty_state("No product category data for the current filters.")

col5, col6 = st.columns(2)

with col5:
    st.subheader("📦 Order Trends")
    order_trend_df = _safe_query(sql_order_trend(catalog, schema, filters))
    if order_trend_df is not None and not order_trend_df.empty:
        order_trend_df["period"] = order_trend_df["month_name"] + " " + order_trend_df[
            "year"
        ].astype(str)
        render_line_chart(order_trend_df, x="period", y="orders")
    else:
        render_empty_state("No order trend data for the current filters.")

with col6:
    st.subheader("👥 Customer Growth")
    growth_df = _safe_query(sql_customer_growth(catalog, schema, filters))
    if growth_df is not None and not growth_df.empty:
        growth_df["period"] = growth_df["year"].astype(str) + "-" + growth_df["month"].astype(
            str
        ).str.zfill(2)
        render_line_chart(growth_df, x="period", y="new_customers")
    else:
        render_empty_state("No customer growth data for the current filters.")

st.divider()

# --- Top N tables ------------------------------------------------------------
col7, col8 = st.columns(2)

with col7:
    st.subheader(f"🏆 Top {filters['top_n']} Products")
    render_dataframe(top_products_df, empty_message="No product data for the current filters.")

with col8:
    st.subheader(f"⭐ Top {filters['top_n']} Customers")
    top_customers_df = _safe_query(sql_top_customers(catalog, schema, filters, filters["top_n"]))
    render_dataframe(top_customers_df, empty_message="No customer data for the current filters.")

st.divider()

# --- AI Executive Summary -----------------------------------------------------
st.subheader("🤖 AI Executive Summary")
st.caption("Generated by Gemini from the real query results above — never fabricated figures.")

if st.button("Generate AI Executive Summary"):
    context_parts = []
    if monthly_df is not None and not monthly_df.empty:
        context_parts.append("Monthly revenue:\n" + monthly_df.to_string(index=False))
    if territory_df is not None and not territory_df.empty:
        context_parts.append("Revenue by territory:\n" + territory_df.to_string(index=False))
    if category_df is not None and not category_df.empty:
        context_parts.append("Revenue by category:\n" + category_df.to_string(index=False))
    if top_products_df is not None and not top_products_df.empty:
        context_parts.append("Top products:\n" + top_products_df.to_string(index=False))

    if not context_parts:
        st.warning("No data available yet to summarize — connect to Databricks first.")
    else:
        with st.spinner("Asking Gemini for an executive summary..."):
            try:
                summary = generate_executive_summary("\n\n".join(context_parts))
                st.success(summary)
            except GeminiConfigurationError:
                st.error("Gemini isn't configured yet — see ⚙ Settings.")
            except GeminiClientError as exc:
                st.error(f"Could not generate a summary: {exc}")
