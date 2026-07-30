# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Sales Order Detail
# MAGIC
# MAGIC Cleans `bronze.sales_order_detail` into `silver.sales_order_detail`.
# MAGIC
# MAGIC **Business Transformations:**
# MAGIC - Recompute `line_total_calculated = ROUND(unit_price * (1 - unit_price_discount)
# MAGIC   * order_qty), 4)` and derive `line_total_variance` against the source's own
# MAGIC   `line_total` — validating a source-computed column rather than trusting it
# MAGIC   blindly. A data quality check below flags rows where the variance exceeds a
# MAGIC   small tolerance.
# MAGIC - Validate `sales_order_id` and `product_id` resolve to real rows.
# MAGIC
# MAGIC Runs independently — no dependency on any other Silver notebook (reads
# MAGIC `bronze.sales_order_header`/`bronze.product` directly for the referential checks).

# COMMAND ----------

# MAGIC %md ## Import shared Silver utilities

# COMMAND ----------

# MAGIC %run "./00_common_utils"

# COMMAND ----------

# MAGIC %md ## Widgets (job/notebook parameters)

# COMMAND ----------

dbutils.widgets.text("catalog", DEFAULT_CATALOG, "Catalog")
dbutils.widgets.text("bronze_schema", DEFAULT_BRONZE_SCHEMA, "Bronze Schema")
dbutils.widgets.text("silver_schema", DEFAULT_SILVER_SCHEMA, "Silver Schema")

# COMMAND ----------

# MAGIC %md ## Imports & logging

# COMMAND ----------

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("aide.silver_sales_order_detail")

# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

TABLE_NAME = "sales_order_detail"
LINE_TOTAL_VARIANCE_TOLERANCE = 0.01

# COMMAND ----------

# MAGIC %md ## Business transformations

# COMMAND ----------


def recompute_line_total(df: DataFrame) -> DataFrame:
    """Recompute line_total from its source formula and derive the variance against
    the source's own stored line_total — a validation column, not a replacement.
    """
    return df.withColumn(
        "line_total_calculated",
        F.round(
            F.col("unit_price") * (F.lit(1) - F.col("unit_price_discount")) * F.col("order_qty"),
            4,
        ),
    ).withColumn(
        "line_total_variance",
        F.round(F.abs(F.col("line_total") - F.col("line_total_calculated")), 4),
    )


# COMMAND ----------

# MAGIC %md ## Table-specific data quality checks

# COMMAND ----------


def check_line_total_accuracy(df: DataFrame) -> dict:
    """Flag rows where the recomputed line_total disagrees with the source's stored
    value by more than a small tolerance (rounding is expected; large variance is not).
    """
    variance_filter = F.col("line_total_variance") > LINE_TOTAL_VARIANCE_TOLERANCE
    violation_count = df.filter(variance_filter).count()
    passed = violation_count == 0
    detail = (
        "no violations"
        if passed
        else f"{violation_count} row(s) exceed the {LINE_TOTAL_VARIANCE_TOLERANCE} tolerance"
    )
    if not passed:
        logger.warning("Data quality check failed — line_total_accuracy: %s", detail)
    return {"check_name": "line_total_accuracy", "passed": passed, "detail": detail}


# COMMAND ----------

# MAGIC %md ## main()

# COMMAND ----------


def main() -> None:
    start_time = start_execution_timer()

    catalog = dbutils.widgets.get("catalog")
    bronze_schema = dbutils.widgets.get("bronze_schema")
    silver_schema = dbutils.widgets.get("silver_schema")

    logger.info(
        "Starting Silver transformation | table=%s catalog=%s bronze_schema=%s silver_schema=%s",
        TABLE_NAME,
        catalog,
        bronze_schema,
        silver_schema,
    )

    try:
        bronze_df = read_bronze_table(spark, catalog, bronze_schema, TABLE_NAME)
        rows_read = bronze_df.count()

        order_header_df = read_bronze_table(spark, catalog, bronze_schema, "sales_order_header")
        product_df = read_bronze_table(spark, catalog, bronze_schema, "product")

        key_columns = ["sales_order_id", "sales_order_detail_id"]
        cleaned_df = drop_technical_columns(bronze_df)
        cleaned_df = trim_string_columns(cleaned_df)
        cleaned_df = deduplicate_by_key(cleaned_df, key_columns)
        transformed_df = recompute_line_total(cleaned_df)

        dq_results = [
            check_row_count_positive(bronze_df),
            check_no_nulls(bronze_df, key_columns),
            check_no_duplicate_keys(bronze_df, key_columns),
            check_line_total_accuracy(transformed_df),
            check_referential_integrity(
                bronze_df, "sales_order_id", order_header_df, "sales_order_id"
            ),
            check_referential_integrity(bronze_df, "product_id", product_df, "product_id"),
        ]

        silver_df = add_silver_audit_columns(
            transformed_df, f"{catalog}.{bronze_schema}.{TABLE_NAME}"
        )

        full_table_name = write_silver_table(spark, silver_df, catalog, silver_schema, TABLE_NAME)
        rows_written = spark.table(full_table_name).count()

        elapsed_seconds = get_elapsed_seconds(start_time)
        print_silver_execution_summary(
            TABLE_NAME, full_table_name, rows_read, rows_written, dq_results, elapsed_seconds
        )

    except Exception:
        logger.exception("Silver transformation failed for table '%s'.", TABLE_NAME)
        raise


# COMMAND ----------

main()
