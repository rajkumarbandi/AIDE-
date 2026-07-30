# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Customer
# MAGIC
# MAGIC Cleans `bronze.customer` into `silver.customer`.
# MAGIC
# MAGIC **Business rule:** a customer is either a person or a store, never both — per
# MAGIC AdventureWorks' design, exactly one of `person_id`/`store_id` is non-null. This
# MAGIC notebook derives `customer_type` from that rule, feeding the Gold "Customer Type"
# MAGIC KPI directly (see `docs/silver_gold_warehouse_design.md` §1).
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

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("aide.silver_customer")

# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

TABLE_NAME = "customer"

# COMMAND ----------

# MAGIC %md ## Business transformations
# MAGIC
# MAGIC Table-specific logic lives here, not in 00_common_utils.py — only this notebook
# MAGIC knows what "customer_type" means for the customer table.

# COMMAND ----------


def derive_customer_type(df: DataFrame) -> DataFrame:
    """Derive customer_type from person_id/store_id nullability.

    person_id is checked first, so a (invalid) row with both set is classified as
    'Individual' — check_customer_type_exclusivity below reports any such row as a
    data quality finding rather than letting it pass silently.
    """
    return df.withColumn(
        "customer_type",
        F.when(F.col("person_id").isNotNull(), F.lit("Individual"))
        .when(F.col("store_id").isNotNull(), F.lit("Store"))
        .otherwise(F.lit("Unknown")),
    )


# COMMAND ----------

# MAGIC %md ## Table-specific data quality checks
# MAGIC
# MAGIC Returns the same `{check_name, passed, detail}` shape as the generic checks in
# MAGIC 00_common_utils.py, so it drops into the same results list.

# COMMAND ----------


def check_customer_type_exclusivity(df: DataFrame) -> dict:
    """Validate that no row has both person_id and store_id set."""
    violation_count = df.filter(
        F.col("person_id").isNotNull() & F.col("store_id").isNotNull()
    ).count()
    passed = violation_count == 0
    detail = (
        "no violations"
        if passed
        else f"{violation_count} row(s) with both person_id and store_id set"
    )
    if not passed:
        logger.warning("Data quality check failed — customer_type_exclusivity: %s", detail)
    return {"check_name": "customer_type_exclusivity", "passed": passed, "detail": detail}


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
            check_no_nulls(bronze_df, ["customer_id"]),
            check_no_duplicate_keys(bronze_df, ["customer_id"]),
            check_customer_type_exclusivity(bronze_df),
        ]

        cleaned_df = drop_technical_columns(bronze_df)
        cleaned_df = trim_string_columns(cleaned_df)
        cleaned_df = deduplicate_by_key(cleaned_df, ["customer_id"])
        transformed_df = derive_customer_type(cleaned_df)
        silver_df = add_silver_audit_columns(
            transformed_df, f"{catalog}.{bronze_schema}.{TABLE_NAME}"
        )

        full_table_name = write_silver_table(spark, silver_df, catalog, silver_schema, TABLE_NAME)
        rows_written = spark.table(full_table_name).count()

        elapsed_seconds = get_elapsed_seconds(start_time)
        print_silver_execution_summary(
            TABLE_NAME, full_table_name, rows_read, rows_written, dq_results, elapsed_seconds
        )
        display(spark.table(full_table_name).limit(10))

    except Exception:
        logger.exception("Silver transformation failed for table '%s'.", TABLE_NAME)
        raise


# COMMAND ----------

main()
