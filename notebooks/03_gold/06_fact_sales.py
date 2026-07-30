# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — fact_sales
# MAGIC
# MAGIC Builds the sales fact table — the single source of truth for revenue, order,
# MAGIC and units-sold KPIs. Grain: **one row per sales order line item**.
# MAGIC
# MAGIC **Business Transformations:**
# MAGIC - `order_date_key`/`due_date_key`/`ship_date_key` derived as `YYYYMMDD` ints to
# MAGIC   join `dim_date`. `ship_date_key` stays null when `ship_date` is null (the
# MAGIC   order hasn't shipped yet — a real business state, not a missing dimension
# MAGIC   member, so no sentinel here).
# MAGIC - `salesperson_key`/`territory_key` resolve a null source FK to `-1` (Unknown
# MAGIC   member), so this fact table never has a dangling reference.
# MAGIC - `tax_amt`/`freight` are deliberately **not** carried in — they exist at
# MAGIC   order (header) grain, and allocating them to line grain would need a
# MAGIC   fabricated pro-rata rule nobody asked for.
# MAGIC
# MAGIC Built from Silver only — validated against Silver directly (not the Gold
# MAGIC dimensions) so this notebook has no build-order dependency on 01-05. The Gold
# MAGIC dimensions are read only afterward, for the reporting section, with a fallback
# MAGIC if they aren't there yet.

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
dbutils.widgets.text("silver_schema", DEFAULT_SILVER_SCHEMA, "Silver Schema")
dbutils.widgets.text("gold_schema", DEFAULT_GOLD_SCHEMA, "Gold Schema")

# COMMAND ----------

# MAGIC %md ## Imports & logging

# COMMAND ----------

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("aide.gold_fact_sales")

# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

TABLE_NAME = "fact_sales"
TOP_N = 5

# COMMAND ----------

# MAGIC %md ## Business transformations

# COMMAND ----------


def build_fact_sales(order_header_df: DataFrame, order_detail_df: DataFrame) -> DataFrame:
    """Join header context onto every detail line and resolve FKs."""
    header_flat = order_header_df.select(
        "sales_order_id",
        "order_date",
        "due_date",
        "ship_date",
        "customer_id",
        "sales_person_id",
        "territory_id",
        F.col("order_status_description").alias("order_status"),
        F.col("online_order_flag").alias("is_online_order"),
    )

    joined = order_detail_df.join(header_flat, "sales_order_id", "inner")

    return joined.select(
        F.col("sales_order_detail_id"),
        F.col("sales_order_id"),
        F.date_format(F.col("order_date"), "yyyyMMdd").cast("int").alias("order_date_key"),
        F.date_format(F.col("due_date"), "yyyyMMdd").cast("int").alias("due_date_key"),
        F.when(
            F.col("ship_date").isNotNull(),
            F.date_format(F.col("ship_date"), "yyyyMMdd").cast("int"),
        ).alias("ship_date_key"),
        F.col("customer_id").alias("customer_key"),
        F.col("product_id").alias("product_key"),
        F.coalesce(F.col("sales_person_id"), F.lit(UNKNOWN_MEMBER_KEY)).alias("salesperson_key"),
        F.coalesce(F.col("territory_id"), F.lit(UNKNOWN_MEMBER_KEY)).alias("territory_key"),
        F.col("order_qty"),
        F.col("unit_price"),
        F.col("unit_price_discount"),
        F.col("line_total"),
        F.col("order_status"),
        F.col("is_online_order"),
    )


# COMMAND ----------

# MAGIC %md ## Data quality & reconciliation checks

# COMMAND ----------


def check_row_reconciliation(order_detail_df: DataFrame, fact_df: DataFrame) -> dict:
    """Every source detail row must produce exactly one fact row — a mismatch means
    the header join silently dropped or duplicated rows.
    """
    detail_count = order_detail_df.count()
    fact_count = fact_df.count()
    passed = detail_count == fact_count
    detail = f"source={detail_count:,} fact={fact_count:,}"
    if not passed:
        logger.warning("Data quality check failed — row_reconciliation: %s", detail)
    return {"check_name": "row_reconciliation", "passed": passed, "detail": detail}


def check_revenue_reconciliation(order_detail_df: DataFrame, fact_df: DataFrame) -> dict:
    """Total line_total in the fact must match the source exactly (line_total is a
    direct passthrough, never recomputed).
    """
    source_revenue = order_detail_df.agg(F.sum("line_total")).first()[0] or 0
    fact_revenue = fact_df.agg(F.sum("line_total")).first()[0] or 0
    passed = abs(float(source_revenue) - float(fact_revenue)) < 0.01
    detail = f"source={source_revenue:,.2f} fact={fact_revenue:,.2f}"
    if not passed:
        logger.warning("Data quality check failed — revenue_reconciliation: %s", detail)
    return {"check_name": "revenue_reconciliation", "passed": passed, "detail": detail}


# COMMAND ----------

# MAGIC %md ## Reporting helpers
# MAGIC
# MAGIC Reads the just-built fact table back from Unity Catalog and the Gold
# MAGIC dimensions (for display names only) to compute the business KPIs for the
# MAGIC executive HTML summary. Falls back to showing raw keys if a dimension isn't
# MAGIC available yet, rather than failing the whole notebook over a display detail.

# COMMAND ----------


def load_dimension_name_lookup(
    spark: SparkSession,
    catalog: str,
    gold_schema: str,
    table_name: str,
    key_col: str,
    name_col: str,
) -> dict:
    try:
        full_table_name = f"{catalog}.{gold_schema}.{table_name}"
        rows = spark.table(full_table_name).select(key_col, name_col).collect()
        return {row[key_col]: row[name_col] for row in rows}
    except Exception:
        logger.warning(
            "Could not load '%s' for display enrichment; showing keys only.", table_name
        )
        return {}


def build_fact_sales_html(
    fact_df: DataFrame,
    full_table_name: str,
    territory_names: dict,
    product_names: dict,
    dq_results: list,
    elapsed_seconds: float,
) -> str:
    total_revenue = fact_df.agg(F.sum("line_total")).first()[0] or 0
    total_orders = fact_df.select("sales_order_id").distinct().count()
    avg_order_value = (total_revenue / total_orders) if total_orders else 0
    unique_customers = fact_df.select("customer_key").distinct().count()
    unique_products = fact_df.select("product_key").distinct().count()

    order_totals = fact_df.groupBy("sales_order_id").agg(F.sum("line_total").alias("order_total"))
    largest_order_row = order_totals.orderBy(F.col("order_total").desc()).first()
    largest_order_value = largest_order_row["order_total"] if largest_order_row else 0
    largest_order_id = largest_order_row["sales_order_id"] if largest_order_row else "N/A"

    territory_rows = (
        fact_df.groupBy("territory_key")
        .agg(F.sum("line_total").alias("revenue"))
        .orderBy(F.col("revenue").desc())
        .limit(TOP_N)
        .collect()
    )
    territory_table_rows = "".join(
        f"<tr><td>{territory_names.get(row['territory_key'], row['territory_key'])}</td>"
        f"<td style='text-align:right;'>${row['revenue']:,.2f}</td></tr>"
        for row in territory_rows
    )

    product_rows = (
        fact_df.groupBy("product_key")
        .agg(F.sum("line_total").alias("revenue"), F.sum("order_qty").alias("units"))
        .orderBy(F.col("revenue").desc())
        .limit(TOP_N)
        .collect()
    )
    product_table_rows = "".join(
        f"<tr><td>{product_names.get(row['product_key'], row['product_key'])}</td>"
        f"<td style='text-align:right;'>${row['revenue']:,.2f}</td>"
        f"<td style='text-align:right;'>{row['units']:,}</td></tr>"
        for row in product_rows
    )

    failed_checks = sum(1 for r in dq_results if not r["passed"])
    dq_color = "#16a34a" if failed_checks == 0 else "#dc2626"
    dq_rows = "".join(
        f"<tr><td>{r['check_name']}</td>"
        f"<td style='color:{'#16a34a' if r['passed'] else '#dc2626'};'>"
        f"{'PASS' if r['passed'] else 'FAIL'}</td><td>{r['detail']}</td></tr>"
        for r in dq_results
    )

    def _card(value: str, label: str) -> str:
        card_style = "background:#f9fafb;border-radius:10px;padding:16px 22px;min-width:150px;"
        return (
            f'<div style="{card_style}">'
            f'<div style="font-size:24px;font-weight:700;color:#111827;">{value}</div>'
            f'<div style="color:#6b7280;font-size:12px;">{label}</div></div>'
        )

    cards = "".join(
        [
            _card(f"${total_revenue:,.2f}", "Total Revenue"),
            _card(f"{total_orders:,}", "Total Orders"),
            _card(f"${avg_order_value:,.2f}", "Average Order Value"),
            _card(f"{unique_customers:,}", "Unique Customers"),
            _card(f"{unique_products:,}", "Unique Products"),
            _card(f"${largest_order_value:,.2f}", f"Largest Order (#{largest_order_id})"),
        ]
    )

    return f"""
<div style="font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
            max-width:1000px;color:#111827;">
  <h2 style="margin-bottom:4px;">fact_sales — Load Summary</h2>
  <p style="color:#6b7280;margin-top:0;">{full_table_name}</p>

  <div style="display:flex;gap:14px;flex-wrap:wrap;margin:18px 0;">{cards}</div>

  <div style="display:flex;gap:24px;flex-wrap:wrap;margin-bottom:20px;">
    <div style="flex:1;min-width:280px;">
      <h3 style="margin-bottom:8px;">Revenue by Territory (Top {TOP_N})</h3>
      <table style="width:100%;border-collapse:collapse;font-size:13px;">
        <thead><tr style="text-align:left;border-bottom:2px solid #e5e7eb;color:#6b7280;">
          <th style="padding:6px;">Territory</th>
          <th style="padding:6px;text-align:right;">Revenue</th>
        </tr></thead>
        <tbody>{territory_table_rows}</tbody>
      </table>
    </div>
    <div style="flex:1;min-width:320px;">
      <h3 style="margin-bottom:8px;">Top Selling Products (Top {TOP_N})</h3>
      <table style="width:100%;border-collapse:collapse;font-size:13px;">
        <thead><tr style="text-align:left;border-bottom:2px solid #e5e7eb;color:#6b7280;">
          <th style="padding:6px;">Product</th>
          <th style="padding:6px;text-align:right;">Revenue</th>
          <th style="padding:6px;text-align:right;">Units</th>
        </tr></thead>
        <tbody>{product_table_rows}</tbody>
      </table>
    </div>
  </div>

  <h3 style="margin-bottom:8px;">Execution Statistics</h3>
  <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;">
    <span style="background:{dq_color};width:10px;height:10px;border-radius:50%;
                 display:inline-block;"></span>
    <span style="color:#6b7280;font-size:13px;">
      {len(dq_results) - failed_checks}/{len(dq_results)} checks passed &middot;
      {elapsed_seconds:.2f}s elapsed
    </span>
  </div>
  <table style="width:100%;border-collapse:collapse;font-size:13px;">
    <thead><tr style="text-align:left;border-bottom:2px solid #e5e7eb;color:#6b7280;">
      <th style="padding:6px;">Check</th>
      <th style="padding:6px;">Status</th>
      <th style="padding:6px;">Detail</th>
    </tr></thead>
    <tbody>{dq_rows}</tbody>
  </table>
</div>
"""


# COMMAND ----------

# MAGIC %md ## main()

# COMMAND ----------


def main() -> None:
    start_time = start_execution_timer()

    catalog = dbutils.widgets.get("catalog")
    silver_schema = dbutils.widgets.get("silver_schema")
    gold_schema = dbutils.widgets.get("gold_schema")

    logger.info(
        "Starting Gold transformation | table=%s catalog=%s silver_schema=%s gold_schema=%s",
        TABLE_NAME,
        catalog,
        silver_schema,
        gold_schema,
    )

    try:
        order_header_df = read_silver_table(spark, catalog, silver_schema, "sales_order_header")
        order_detail_df = read_silver_table(spark, catalog, silver_schema, "sales_order_detail")
        customer_df = read_silver_table(spark, catalog, silver_schema, "customer")
        product_df = read_silver_table(spark, catalog, silver_schema, "product")
        territory_df = read_silver_table(spark, catalog, silver_schema, "sales_territory")
        sales_person_df = read_silver_table(spark, catalog, silver_schema, "sales_person")

        gold_df = build_fact_sales(order_header_df, order_detail_df)
        gold_df = add_gold_audit_columns(gold_df, "sales_order_header,sales_order_detail")

        dq_results = [
            check_row_count_positive(gold_df),
            check_no_nulls(gold_df, ["sales_order_detail_id", "customer_key", "product_key"]),
            check_no_duplicate_keys(gold_df, ["sales_order_detail_id"]),
            check_referential_integrity(
                order_header_df, "customer_id", customer_df, "customer_id"
            ),
            check_referential_integrity(order_detail_df, "product_id", product_df, "product_id"),
            check_referential_integrity(
                order_header_df, "territory_id", territory_df, "territory_id"
            ),
            check_referential_integrity(
                order_header_df, "sales_person_id", sales_person_df, "business_entity_id"
            ),
            check_row_reconciliation(order_detail_df, gold_df),
            check_revenue_reconciliation(order_detail_df, gold_df),
        ]

        full_table_name = write_gold_table(spark, gold_df, catalog, gold_schema, TABLE_NAME)
        written_df = spark.table(full_table_name)
        rows_written = written_df.count()

        territory_names = load_dimension_name_lookup(
            spark, catalog, gold_schema, "dim_territory", "territory_key", "territory_name"
        )
        product_names = load_dimension_name_lookup(
            spark, catalog, gold_schema, "dim_product", "product_key", "product_name"
        )

        elapsed_seconds = get_elapsed_seconds(start_time)
        print_gold_execution_summary(
            TABLE_NAME, full_table_name, rows_written, dq_results, elapsed_seconds
        )
        displayHTML(
            build_fact_sales_html(
                written_df,
                full_table_name,
                territory_names,
                product_names,
                dq_results,
                elapsed_seconds,
            )
        )

    except Exception:
        logger.exception("Gold transformation failed for table '%s'.", TABLE_NAME)
        raise


# COMMAND ----------

main()
