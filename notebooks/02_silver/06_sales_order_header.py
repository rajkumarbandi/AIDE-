# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Sales Order Header
# MAGIC
# MAGIC Cleans `bronze.sales_order_header` into `silver.sales_order_header`.
# MAGIC
# MAGIC **Business Transformations:**
# MAGIC - Cast `online_order_flag` to boolean.
# MAGIC - Decode `status` into `order_status_description`, using the AdventureWorks-
# MAGIC   documented status codes (verified from `instawdb.sql`'s own extended-property
# MAGIC   comment: 1=In Process, 2=Approved, 3=Backordered, 4=Rejected, 5=Shipped,
# MAGIC   6=Cancelled). The CHECK constraint technically allows 0-8, so anything outside
# MAGIC   1-6 decodes to 'Unknown' rather than assuming only the six documented values
# MAGIC   ever appear.
# MAGIC - Validate the source's own CHECK-constraint business rules
# MAGIC   (`due_date >= order_date`, `ship_date IS NULL OR ship_date >= order_date`) as a
# MAGIC   Silver-layer data quality gate — re-checking for drift, not trusting blindly.
# MAGIC - Drop `credit_card_approval_code` — low business value; not carrying
# MAGIC   payment-adjacent data further than necessary. `comment` is kept (small,
# MAGIC   potential future input to AI-driven analysis).
# MAGIC - Validate `customer_id` resolves to a real customer.
# MAGIC
# MAGIC Runs independently — no dependency on any other Silver notebook (reads
# MAGIC `bronze.customer` directly for the referential check, not `silver.customer`).

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
logger = logging.getLogger("aide.silver_sales_order_header")

# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

TABLE_NAME = "sales_order_header"

# COMMAND ----------

# MAGIC %md ## Business transformations

# COMMAND ----------


def decode_order_status(df: DataFrame) -> DataFrame:
    """Decode status into a readable order_status_description."""
    return df.withColumn(
        "order_status_description",
        F.when(F.col("status") == 1, F.lit("In Process"))
        .when(F.col("status") == 2, F.lit("Approved"))
        .when(F.col("status") == 3, F.lit("Backordered"))
        .when(F.col("status") == 4, F.lit("Rejected"))
        .when(F.col("status") == 5, F.lit("Shipped"))
        .when(F.col("status") == 6, F.lit("Cancelled"))
        .otherwise(F.lit("Unknown")),
    )


# COMMAND ----------

# MAGIC %md ## Table-specific data quality checks

# COMMAND ----------


def check_valid_date_sequence(df: DataFrame) -> dict:
    """Re-validate the source's own CHECK constraints: due_date >= order_date, and
    ship_date (when set) >= order_date.
    """
    violation_count = df.filter(
        (F.col("due_date") < F.col("order_date"))
        | (F.col("ship_date").isNotNull() & (F.col("ship_date") < F.col("order_date")))
    ).count()
    passed = violation_count == 0
    detail = (
        "no violations"
        if passed
        else f"{violation_count} row(s) with an invalid date sequence"
    )
    if not passed:
        logger.warning("Data quality check failed — valid_date_sequence: %s", detail)
    return {"check_name": "valid_date_sequence", "passed": passed, "detail": detail}


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

        customer_df = read_bronze_table(spark, catalog, bronze_schema, "customer")

        dq_results = [
            check_row_count_positive(bronze_df),
            check_no_nulls(bronze_df, ["sales_order_id", "customer_id", "order_date"]),
            check_no_duplicate_keys(bronze_df, ["sales_order_id"]),
            check_valid_date_sequence(bronze_df),
            check_referential_integrity(bronze_df, "customer_id", customer_df, "customer_id"),
        ]

        cleaned_df = drop_technical_columns(
            bronze_df, extra_columns=["credit_card_approval_code"]
        )
        cleaned_df = trim_string_columns(cleaned_df)
        cleaned_df = flags_to_boolean(cleaned_df, ["online_order_flag"])
        cleaned_df = deduplicate_by_key(cleaned_df, ["sales_order_id"])
        transformed_df = decode_order_status(cleaned_df)
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
