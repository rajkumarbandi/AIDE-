# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Sales Dashboard
# MAGIC
# MAGIC The executive reporting layer. Reads **Gold only** (`fact_sales` + all five
# MAGIC dimensions) — never Silver/Bronze directly, the same boundary the future
# MAGIC Streamlit app will respect.
# MAGIC
# MAGIC Computes the full KPI set, writes a one-row snapshot to
# MAGIC `gold.sales_kpi_summary` (a small, cheap-to-query cache for downstream
# MAGIC consumers instead of re-aggregating `fact_sales` on every page load), and
# MAGIC renders an executive HTML dashboard.
# MAGIC
# MAGIC **Note on "Customers"/"Products Sold"/"Territories"/"Sales Persons" KPIs:** all
# MAGIC four are counted from `fact_sales` (i.e. "customers who bought something,
# MAGIC products that sold, territories that transacted, reps who sold") rather than
# MAGIC the static dimension catalog size — a consistent, "did it actually happen"
# MAGIC framing across all four. Territory/salesperson counts exclude the -1 Unknown
# MAGIC member.

# COMMAND ----------

# MAGIC %md ## Import shared utilities

# COMMAND ----------

# MAGIC %run "./00_gold_utils"

# COMMAND ----------

# MAGIC %run "../02_silver/00_common_utils"

# COMMAND ----------

# MAGIC %md ## Widgets (job/notebook parameters)

# COMMAND ----------

dbutils.widgets.text("catalog", DEFAULT_CATALOG, "Catalog")
dbutils.widgets.text("gold_schema", DEFAULT_GOLD_SCHEMA, "Gold Schema")
dbutils.widgets.text("top_n", "10", "Top N (products/customers)")

# COMMAND ----------

# MAGIC %md ## Imports & logging

# COMMAND ----------

import logging

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("aide.gold_sales_dashboard")

# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

SUMMARY_TABLE_NAME = "sales_kpi_summary"
TOP_TERRITORIES_LIMIT = 10
LARGE_DISCOUNT_THRESHOLD = 0.5

# COMMAND ----------

# MAGIC %md ## KPI computations

# COMMAND ----------


def compute_core_kpis(
    fact_df: DataFrame, dim_territory_df: DataFrame, dim_product_df: DataFrame
) -> dict:
    real_territory_fact = fact_df.filter(F.col("territory_key") != UNKNOWN_MEMBER_KEY)
    real_salesperson_fact = fact_df.filter(F.col("salesperson_key") != UNKNOWN_MEMBER_KEY)

    total_revenue = float(fact_df.agg(F.sum("line_total")).first()[0] or 0.0)
    total_orders = fact_df.select("sales_order_id").distinct().count()
    total_customers = fact_df.select("customer_key").distinct().count()
    total_products_sold = fact_df.select("product_key").distinct().count()
    total_territories = real_territory_fact.select("territory_key").distinct().count()
    total_salespersons = real_salesperson_fact.select("salesperson_key").distinct().count()

    avg_order_value = total_revenue / total_orders if total_orders else 0.0
    revenue_per_customer = total_revenue / total_customers if total_customers else 0.0
    revenue_per_territory = total_revenue / total_territories if total_territories else 0.0

    top_territory_row = (
        real_territory_fact.groupBy("territory_key")
        .agg(F.sum("line_total").alias("revenue"))
        .join(dim_territory_df.select("territory_key", "territory_name"), "territory_key")
        .orderBy(F.col("revenue").desc())
        .first()
    )
    top_product_row = (
        fact_df.groupBy("product_key")
        .agg(F.sum("line_total").alias("revenue"))
        .join(dim_product_df.select("product_key", "product_name"), "product_key")
        .orderBy(F.col("revenue").desc())
        .first()
    )

    return {
        "total_revenue": total_revenue,
        "total_orders": total_orders,
        "total_customers": total_customers,
        "total_products_sold": total_products_sold,
        "total_territories": total_territories,
        "total_salespersons": total_salespersons,
        "avg_order_value": avg_order_value,
        "revenue_per_customer": revenue_per_customer,
        "revenue_per_territory": revenue_per_territory,
        "top_territory_name": top_territory_row["territory_name"] if top_territory_row else "N/A",
        "top_territory_revenue": float(top_territory_row["revenue"]) if top_territory_row else 0.0,
        "top_product_name": top_product_row["product_name"] if top_product_row else "N/A",
        "top_product_revenue": float(top_product_row["revenue"]) if top_product_row else 0.0,
    }


def compute_top_products(fact_df: DataFrame, dim_product_df: DataFrame, top_n: int) -> list:
    return (
        fact_df.groupBy("product_key")
        .agg(F.sum("line_total").alias("revenue"), F.sum("order_qty").alias("units_sold"))
        .join(
            dim_product_df.select("product_key", "product_name", "category_name"), "product_key"
        )
        .orderBy(F.col("revenue").desc())
        .limit(top_n)
        .collect()
    )


def compute_top_customers(fact_df: DataFrame, dim_customer_df: DataFrame, top_n: int) -> list:
    return (
        fact_df.groupBy("customer_key")
        .agg(
            F.sum("line_total").alias("revenue"),
            F.countDistinct("sales_order_id").alias("orders"),
        )
        .join(
            dim_customer_df.select("customer_key", "customer_name", "customer_type"),
            "customer_key",
        )
        .orderBy(F.col("revenue").desc())
        .limit(top_n)
        .collect()
    )


def compute_territory_breakdown(
    fact_df: DataFrame, dim_territory_df: DataFrame, total_revenue: float
) -> list:
    rows = (
        fact_df.filter(F.col("territory_key") != UNKNOWN_MEMBER_KEY)
        .groupBy("territory_key")
        .agg(
            F.sum("line_total").alias("revenue"),
            F.countDistinct("sales_order_id").alias("orders"),
        )
        .join(dim_territory_df.select("territory_key", "territory_name"), "territory_key")
        .orderBy(F.col("revenue").desc())
        .limit(TOP_TERRITORIES_LIMIT)
        .collect()
    )
    return [
        {
            "territory_name": r["territory_name"],
            "revenue": float(r["revenue"]),
            "orders": r["orders"],
            "contribution_pct": round(100.0 * float(r["revenue"]) / total_revenue, 2)
            if total_revenue
            else 0.0,
        }
        for r in rows
    ]


def compute_recent_orders(fact_df: DataFrame, dim_customer_df: DataFrame, top_n: int) -> list:
    return (
        fact_df.groupBy("sales_order_id", "customer_key", "order_date_key")
        .agg(F.sum("line_total").alias("order_revenue"))
        .join(dim_customer_df.select("customer_key", "customer_name"), "customer_key")
        .withColumn("order_date", F.to_date(F.col("order_date_key").cast("string"), "yyyyMMdd"))
        .orderBy(F.col("order_date_key").desc(), F.col("sales_order_id").desc())
        .limit(top_n)
        .collect()
    )


def compute_customer_distribution(dim_customer_df: DataFrame) -> list:
    return (
        dim_customer_df.groupBy("customer_type")
        .agg(F.count("*").alias("customer_count"))
        .orderBy(F.col("customer_count").desc())
        .collect()
    )


def compute_product_distribution(fact_df: DataFrame, dim_product_df: DataFrame) -> list:
    return (
        fact_df.groupBy("product_key")
        .agg(F.sum("line_total").alias("revenue"))
        .join(dim_product_df.select("product_key", "category_name"), "product_key")
        .groupBy("category_name")
        .agg(F.sum("revenue").alias("revenue"))
        .orderBy(F.col("revenue").desc())
        .collect()
    )


def compute_top_salesperson(fact_df: DataFrame, dim_salesperson_df: DataFrame):
    return (
        fact_df.filter(F.col("salesperson_key") != UNKNOWN_MEMBER_KEY)
        .groupBy("salesperson_key")
        .agg(F.sum("line_total").alias("revenue"))
        .join(
            dim_salesperson_df.select("salesperson_key", "salesperson_name"), "salesperson_key"
        )
        .orderBy(F.col("revenue").desc())
        .first()
    )


# COMMAND ----------

# MAGIC %md ## Business insights
# MAGIC
# MAGIC Turns the raw numbers into plain-language observations, per the requirement
# MAGIC that this notebook "provide meaningful observations", not just display numbers.

# COMMAND ----------


def compute_business_insights(
    fact_df: DataFrame,
    kpis: dict,
    territory_breakdown: list,
    product_distribution: list,
    customer_distribution: list,
    top_salesperson,
) -> list:
    insights = []

    if territory_breakdown:
        top = territory_breakdown[0]
        insights.append(
            f"Highest revenue territory: {top['territory_name']} (${top['revenue']:,.2f}, "
            f"{top['contribution_pct']:.1f}% of total revenue)."
        )

    if product_distribution:
        top_category = product_distribution[0]
        insights.append(
            f"'{top_category['category_name']}' is the top revenue-contributing product "
            f"category (${top_category['revenue']:,.2f})."
        )

    products_per_order = (
        fact_df.groupBy("sales_order_id")
        .agg(F.countDistinct("product_key").alias("distinct_products"))
        .agg(F.avg("distinct_products"))
        .first()[0]
        or 0
    )
    insights.append(f"Average of {products_per_order:.1f} distinct products per order.")

    if customer_distribution:
        segments = ", ".join(
            f"{r['customer_type']}: {r['customer_count']:,}" for r in customer_distribution
        )
        insights.append(f"Customer segmentation — {segments}.")

    if top_salesperson:
        insights.append(
            f"Top-performing salesperson: {top_salesperson['salesperson_name']} "
            f"(${float(top_salesperson['revenue']):,.2f})."
        )

    yearly_revenue = (
        fact_df.withColumn("order_year", (F.col("order_date_key") / 10000).cast("int"))
        .groupBy("order_year")
        .agg(F.sum("line_total").alias("revenue"))
        .orderBy("order_year")
        .collect()
    )
    if len(yearly_revenue) >= 2:
        latest, previous = yearly_revenue[-1], yearly_revenue[-2]
        latest_revenue = float(latest["revenue"])
        previous_revenue = float(previous["revenue"])
        change_pct = (
            round(100.0 * (latest_revenue - previous_revenue) / previous_revenue, 1)
            if previous_revenue
            else 0.0
        )
        direction = "up" if change_pct >= 0 else "down"
        insights.append(
            f"Revenue trend: {latest['order_year']} is {direction} {abs(change_pct):.1f}% vs. "
            f"{previous['order_year']} (${previous_revenue:,.2f} -> ${latest_revenue:,.2f})."
        )
    else:
        insights.append("Not enough multi-year data yet to establish a revenue trend.")

    largest_discount = float(fact_df.agg(F.max("unit_price_discount")).first()[0] or 0.0)
    discount_note = f"Largest single-line discount applied: {largest_discount * 100:.1f}%."
    if largest_discount > LARGE_DISCOUNT_THRESHOLD:
        discount_note += " ⚠️ Unusually large — worth a closer look."
    insights.append(discount_note)

    return insights


# COMMAND ----------

# MAGIC %md ## HTML dashboard rendering

# COMMAND ----------


def _kpi_card(icon: str, value: str, label: str) -> str:
    return (
        '<div style="background:#ffffff;border:1px solid #e5e7eb;border-radius:12px;'
        'box-shadow:0 1px 3px rgba(0,0,0,0.08);padding:16px 20px;min-width:160px;">'
        f'<div style="font-size:22px;">{icon}</div>'
        f'<div style="font-size:22px;font-weight:700;color:#111827;margin-top:4px;">{value}</div>'
        f'<div style="color:#6b7280;font-size:12px;">{label}</div></div>'
    )


def _section_title(text: str) -> str:
    return f'<h3 style="margin:24px 0 8px 0;color:#111827;">{text}</h3>'


def _table(headers: list, rows_html: str) -> str:
    header_html = "".join(f'<th style="padding:8px 6px;">{h}</th>' for h in headers)
    return f"""
    <table style="width:100%;border-collapse:collapse;font-size:13px;background:#ffffff;
                  border-radius:10px;overflow:hidden;">
      <thead><tr style="text-align:left;border-bottom:2px solid #e5e7eb;color:#6b7280;">
        {header_html}
      </tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
    """


def build_dashboard_html(
    kpis: dict,
    insights: list,
    top_products: list,
    top_customers: list,
    territory_breakdown: list,
    recent_orders: list,
    customer_distribution: list,
    product_distribution: list,
) -> str:
    kpi_cards = "".join(
        [
            _kpi_card("💰", f"${kpis['total_revenue']:,.2f}", "Total Revenue"),
            _kpi_card("📦", f"{kpis['total_orders']:,}", "Orders"),
            _kpi_card("👥", f"{kpis['total_customers']:,}", "Customers"),
            _kpi_card("🛒", f"{kpis['total_products_sold']:,}", "Products Sold"),
            _kpi_card("🌍", f"{kpis['total_territories']:,}", "Territories"),
            _kpi_card("👨‍💼", f"{kpis['total_salespersons']:,}", "Sales Persons"),
            _kpi_card("📊", f"${kpis['avg_order_value']:,.2f}", "Average Order Value"),
            _kpi_card("📈", f"${kpis['revenue_per_customer']:,.2f}", "Revenue / Customer"),
            _kpi_card("🗺️", f"${kpis['revenue_per_territory']:,.2f}", "Revenue / Territory"),
            _kpi_card("🏆", kpis["top_territory_name"], "Top Territory"),
            _kpi_card("⭐", kpis["top_product_name"], "Top Product"),
        ]
    )

    insights_html = "".join(f"<li style='margin-bottom:6px;'>{i}</li>" for i in insights)

    products_rows = "".join(
        f"<tr><td>{r['product_name']}</td><td>{r['category_name']}</td>"
        f"<td style='text-align:right;'>${r['revenue']:,.2f}</td>"
        f"<td style='text-align:right;'>{r['units_sold']:,}</td></tr>"
        for r in top_products
    )
    customers_rows = "".join(
        f"<tr><td>{r['customer_name']}</td><td>{r['customer_type']}</td>"
        f"<td style='text-align:right;'>${r['revenue']:,.2f}</td>"
        f"<td style='text-align:right;'>{r['orders']:,}</td></tr>"
        for r in top_customers
    )
    territory_rows = "".join(
        f"<tr><td>{t['territory_name']}</td>"
        f"<td style='text-align:right;'>${t['revenue']:,.2f}</td>"
        f"<td style='text-align:right;'>{t['orders']:,}</td>"
        f"<td style='text-align:right;'>{t['contribution_pct']:.1f}%</td></tr>"
        for t in territory_breakdown
    )
    recent_orders_rows = "".join(
        f"<tr><td>{r['sales_order_id']}</td><td>{r['order_date']}</td>"
        f"<td>{r['customer_name']}</td>"
        f"<td style='text-align:right;'>${r['order_revenue']:,.2f}</td></tr>"
        for r in recent_orders
    )
    customer_dist_rows = "".join(
        f"<tr><td>{r['customer_type']}</td>"
        f"<td style='text-align:right;'>{r['customer_count']:,}</td></tr>"
        for r in customer_distribution
    )
    product_dist_rows = "".join(
        f"<tr><td>{r['category_name']}</td>"
        f"<td style='text-align:right;'>${r['revenue']:,.2f}</td></tr>"
        for r in product_distribution
    )

    return f"""
<div style="font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
            color:#111827;max-width:1100px;">
  <h2 style="margin-bottom:4px;">Sales Executive Dashboard</h2>
  <p style="color:#6b7280;margin-top:0;">AdventureWorks — Gold Layer Reporting</p>

  <div style="display:flex;gap:14px;flex-wrap:wrap;margin:18px 0 8px 0;">{kpi_cards}</div>

  <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:12px;padding:16px 20px;
              margin-top:16px;">
    <div style="font-weight:600;margin-bottom:8px;">Business Insights</div>
    <ul style="margin:0;padding-left:20px;font-size:13px;color:#374151;">{insights_html}</ul>
  </div>

  {_section_title(f"Top {len(top_products)} Products")}
  {_table(["Product", "Category", "Revenue", "Units Sold"], products_rows)}

  {_section_title(f"Top {len(top_customers)} Customers")}
  {_table(["Customer", "Type", "Revenue", "Orders"], customers_rows)}

  {_section_title("Top Territories")}
  {_table(["Territory", "Revenue", "Orders", "Contribution %"], territory_rows)}

  {_section_title(f"Recent Orders (Top {len(recent_orders)})")}
  {_table(["Order ID", "Order Date", "Customer", "Revenue"], recent_orders_rows)}

  <div style="display:flex;gap:24px;flex-wrap:wrap;">
    <div style="flex:1;min-width:260px;">
      {_section_title("Customer Distribution")}
      {_table(["Customer Type", "Count"], customer_dist_rows)}
    </div>
    <div style="flex:1;min-width:260px;">
      {_section_title("Product Distribution (by Category Revenue)")}
      {_table(["Category", "Revenue"], product_dist_rows)}
    </div>
  </div>
</div>
"""


# COMMAND ----------

# MAGIC %md ## main()

# COMMAND ----------


def main() -> None:
    start_time = start_execution_timer()

    catalog = dbutils.widgets.get("catalog")
    gold_schema = dbutils.widgets.get("gold_schema")
    top_n = int(dbutils.widgets.get("top_n"))

    logger.info(
        "Starting sales dashboard | catalog=%s gold_schema=%s top_n=%d",
        catalog,
        gold_schema,
        top_n,
    )

    try:
        fact_df = spark.table(f"{catalog}.{gold_schema}.fact_sales")
        dim_customer_df = spark.table(f"{catalog}.{gold_schema}.dim_customer")
        dim_product_df = spark.table(f"{catalog}.{gold_schema}.dim_product")
        dim_territory_df = spark.table(f"{catalog}.{gold_schema}.dim_territory")
        dim_salesperson_df = spark.table(f"{catalog}.{gold_schema}.dim_salesperson")

        # Validate inputs before computing KPIs from them — a dashboard built on an
        # empty table would be actively misleading, not just incomplete.
        for label, df in [
            ("fact_sales", fact_df),
            ("dim_customer", dim_customer_df),
            ("dim_product", dim_product_df),
            ("dim_territory", dim_territory_df),
            ("dim_salesperson", dim_salesperson_df),
        ]:
            check = check_row_count_positive(df)
            if not check["passed"]:
                raise GoldTransformationError(f"Gold source table '{label}' is empty; aborting.")

        kpis = compute_core_kpis(fact_df, dim_territory_df, dim_product_df)
        top_products = compute_top_products(fact_df, dim_product_df, top_n)
        top_customers = compute_top_customers(fact_df, dim_customer_df, top_n)
        territory_breakdown = compute_territory_breakdown(
            fact_df, dim_territory_df, kpis["total_revenue"]
        )
        recent_orders = compute_recent_orders(fact_df, dim_customer_df, top_n)
        customer_distribution = compute_customer_distribution(dim_customer_df)
        product_distribution = compute_product_distribution(fact_df, dim_product_df)
        top_salesperson = compute_top_salesperson(fact_df, dim_salesperson_df)

        insights = compute_business_insights(
            fact_df, kpis, territory_breakdown, product_distribution, customer_distribution,
            top_salesperson,
        )

        summary_row = spark.createDataFrame([Row(**kpis)])
        source_tables = "fact_sales,dim_customer,dim_product,dim_territory,dim_salesperson"
        summary_row = add_gold_audit_columns(summary_row, source_tables)
        full_summary_table_name = write_gold_table(
            spark, summary_row, catalog, gold_schema, SUMMARY_TABLE_NAME
        )

        elapsed_seconds = get_elapsed_seconds(start_time)
        logger.info(
            "Sales dashboard computed and '%s' refreshed in %.2fs.",
            full_summary_table_name,
            elapsed_seconds,
        )

        displayHTML(
            build_dashboard_html(
                kpis,
                insights,
                top_products,
                top_customers,
                territory_breakdown,
                recent_orders,
                customer_distribution,
                product_distribution,
            )
        )

    except Exception:
        logger.exception("Sales dashboard generation failed.")
        raise


# COMMAND ----------

main()
