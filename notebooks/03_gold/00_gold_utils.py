# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Common Utilities
# MAGIC
# MAGIC Reusable helpers shared by every Gold notebook: reading Silver, writing Gold,
# MAGIC Gold audit columns, the plain-text execution summary (same style as
# MAGIC Bronze/Silver), and one shared HTML "dimension loaded" card renderer used by
# MAGIC every dimension notebook (fact_sales and sales_dashboard have their own
# MAGIC one-off HTML, since their KPI shape is genuinely different, not reused
# MAGIC anywhere else).
# MAGIC
# MAGIC Import this notebook from any Gold notebook with:
# MAGIC
# MAGIC ```
# MAGIC %run "./00_gold_utils"
# MAGIC ```
# MAGIC
# MAGIC **Design:** mirrors `02_silver/00_common_utils.py` exactly — same shape, same
# MAGIC reasoning, just named for this layer. Business/reporting logic stays local to
# MAGIC each notebook; only genuinely generic, table-agnostic primitives live here.
# MAGIC Execution-timer and generic cleaning/DQ primitives (`start_execution_timer`,
# MAGIC `trim_string_columns`, `check_no_nulls`, etc.) are layer-agnostic already, so
# MAGIC Gold notebooks `%run` the Silver common_utils directly for those rather than
# MAGIC redefining them here.

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
logger = logging.getLogger("aide.gold_common")

# COMMAND ----------

# MAGIC %md ## Exceptions

# COMMAND ----------


class GoldTransformationError(Exception):
    """Base error for failures in the Gold transformation framework."""


class SilverSourceNotFoundError(GoldTransformationError):
    """Raised when an expected Silver source table does not exist."""


# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

DEFAULT_CATALOG = "aide"
DEFAULT_SILVER_SCHEMA = "silver"
DEFAULT_GOLD_SCHEMA = "gold"

# Surrogate key used for every dimension's manually-inserted "Unknown" member row,
# so a fact row with an unresolvable FK (e.g. no assigned salesperson) never has a
# dangling reference.
UNKNOWN_MEMBER_KEY = -1

# COMMAND ----------

# MAGIC %md ## Helper functions — read, write, audit

# COMMAND ----------


def read_silver_table(
    spark: SparkSession, catalog: str, silver_schema: str, table_name: str
) -> DataFrame:
    """Read a Silver table, failing loudly with a clear message if it doesn't exist."""
    full_table_name = f"{catalog}.{silver_schema}.{table_name}"
    try:
        return spark.table(full_table_name)
    except Exception as exc:
        raise SilverSourceNotFoundError(
            f"Silver source table '{full_table_name}' not found or not readable. "
            f"Has the corresponding Silver notebook been run for this table?"
        ) from exc


def write_gold_table(
    spark: SparkSession, df: DataFrame, catalog: str, gold_schema: str, table_name: str
) -> str:
    """Write the DataFrame as a managed Delta table and register it in Unity Catalog.

    Full overwrite, matching Bronze/Silver: every Gold object is rebuilt fresh from
    Silver each run (Type-1, no history tracking in this phase).
    """
    full_table_name = f"{catalog}.{gold_schema}.{table_name}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )
    logger.info("Wrote and registered Unity Catalog table '%s'.", full_table_name)
    return full_table_name


def add_gold_audit_columns(df: DataFrame, source_silver_tables: str) -> DataFrame:
    """Attach standard Gold audit columns. `source_silver_tables` is a comma-joined
    list, since a Gold object typically combines more than one Silver table.
    """
    return df.withColumn("gold_load_timestamp", F.current_timestamp()).withColumn(
        "source_silver_tables", F.lit(source_silver_tables)
    )


def print_gold_execution_summary(
    table_name: str,
    full_table_name: str,
    rows_written: int,
    dq_results: list,
    elapsed_seconds: float,
) -> None:
    """Print a clean, human-readable summary — same dashed-box style as Bronze/Silver."""
    print("=" * 80)
    print("GOLD TRANSFORMATION SUMMARY")
    print("=" * 80)
    print(f"Target Table         : {full_table_name}")
    print(f"Rows Written         : {rows_written:,}")
    print(f"Elapsed Time (sec)   : {elapsed_seconds:.2f}")
    print("-" * 80)
    print("Data Quality Checks  :")
    for result in dq_results:
        status = "PASS" if result["passed"] else "FAIL"
        print(f"  [{status}] {result['check_name']} — {result['detail']}")
    failed_checks = sum(1 for r in dq_results if not r["passed"])
    print("-" * 80)
    print(f"Status               : {'SUCCESS' if failed_checks == 0 else 'SUCCESS_WITH_WARNINGS'}")
    print("=" * 80)


# COMMAND ----------

# MAGIC %md ## Helper functions — shared HTML (dimension notebooks only)
# MAGIC
# MAGIC fact_sales and sales_dashboard render their own HTML directly — their KPI shape
# MAGIC is one-off, not reused anywhere, so a shared function would add a parameter list
# MAGIC longer than the HTML itself. This one *is* reused by all five dimension
# MAGIC notebooks, which is what justifies it living here.

# COMMAND ----------


def get_status_color(pct: float) -> str:
    """Green at 0%, amber for a small amount, red beyond 1% — used for null%/dup% cards."""
    if pct == 0:
        return "#16a34a"
    if pct <= 1.0:
        return "#d97706"
    return "#dc2626"


def render_dimension_summary_html(
    table_name: str,
    full_table_name: str,
    rows_written: int,
    business_key: str,
    null_pct: float,
    duplicate_pct: float,
    elapsed_seconds: float,
) -> str:
    """Render the 'Dimension Loaded Successfully' card for displayHTML()."""
    null_color = get_status_color(null_pct)
    dup_color = get_status_color(duplicate_pct)
    overall_color = "#16a34a" if null_pct == 0 and duplicate_pct == 0 else "#d97706"

    return f"""
<div style="font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
            max-width:900px;">
  <div style="background:#ffffff;border:1px solid #e5e7eb;border-radius:12px;
              box-shadow:0 1px 3px rgba(0,0,0,0.08);padding:20px 24px;">
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:16px;">
      <span style="background:{overall_color};width:10px;height:10px;border-radius:50%;
                   display:inline-block;"></span>
      <h3 style="margin:0;color:#111827;font-weight:600;">
        Dimension Loaded Successfully — {table_name}
      </h3>
    </div>
    <div style="display:flex;gap:16px;flex-wrap:wrap;">
      <div style="background:#f9fafb;border-radius:10px;padding:14px 20px;min-width:130px;">
        <div style="font-size:22px;font-weight:700;color:#111827;">{rows_written:,}</div>
        <div style="color:#6b7280;font-size:12px;">Rows Written</div>
      </div>
      <div style="background:#f9fafb;border-radius:10px;padding:14px 20px;min-width:130px;">
        <div style="font-size:22px;font-weight:700;color:#111827;">{business_key}</div>
        <div style="color:#6b7280;font-size:12px;">Business Key</div>
      </div>
      <div style="background:#f9fafb;border-radius:10px;padding:14px 20px;min-width:130px;">
        <div style="font-size:22px;font-weight:700;color:{null_color};">{null_pct:.2f}%</div>
        <div style="color:#6b7280;font-size:12px;">Null %</div>
      </div>
      <div style="background:#f9fafb;border-radius:10px;padding:14px 20px;min-width:130px;">
        <div style="font-size:22px;font-weight:700;color:{dup_color};">{duplicate_pct:.2f}%</div>
        <div style="color:#6b7280;font-size:12px;">Duplicate %</div>
      </div>
      <div style="background:#f9fafb;border-radius:10px;padding:14px 20px;min-width:130px;">
        <div style="font-size:22px;font-weight:700;color:#111827;">{elapsed_seconds:.2f}s</div>
        <div style="color:#6b7280;font-size:12px;">Execution Time</div>
      </div>
    </div>
    <div style="color:#9ca3af;font-size:11px;margin-top:14px;">{full_table_name}</div>
  </div>
</div>
"""
