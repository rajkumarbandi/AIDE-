# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Currency
# MAGIC
# MAGIC Cleans `bronze.currency` into `silver.currency`.
# MAGIC
# MAGIC **Business Transformations:**
# MAGIC - Rename `name` -> `currency_name` (clarity — a bare `name` is ambiguous once
# MAGIC   joined with other entities at Gold).
# MAGIC
# MAGIC This is a reference-table addition beyond the original 15-table Silver design
# MAGIC (see `docs/silver_gold_warehouse_design.md`) — a small, low-risk extension, not
# MAGIC an architecture change: `sales_order_header.currency_rate_id` references
# MAGIC currency data, so a clean currency reference table is a reasonable thing to have
# MAGIC available even though no current Gold KPI consumes it yet.
# MAGIC
# MAGIC Runs independently — no dependency on any other Silver notebook.

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("aide.silver_currency")

# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

TABLE_NAME = "currency"

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

        dq_results = [
            check_row_count_positive(bronze_df),
            check_no_nulls(bronze_df, ["currency_code"]),
            check_no_duplicate_keys(bronze_df, ["currency_code"]),
        ]

        cleaned_df = drop_technical_columns(bronze_df)
        cleaned_df = trim_string_columns(cleaned_df)
        cleaned_df = deduplicate_by_key(cleaned_df, ["currency_code"])
        transformed_df = cleaned_df.withColumnRenamed("name", "currency_name")
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
