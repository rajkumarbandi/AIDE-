# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Salesperson Domain
# MAGIC
# MAGIC Cleans two related Bronze tables into their Silver equivalents: `sales_person`,
# MAGIC `employee`. Grouped together as the "salesperson" domain since a salesperson is
# MAGIC always also an employee record in AdventureWorks.
# MAGIC
# MAGIC **Business Transformations:**
# MAGIC - Derive `quota_attainment_pct = sales_ytd / sales_quota * 100`, guarded against
# MAGIC   a null/zero `sales_quota` (new hires legitimately have no quota yet).
# MAGIC - Cast `salaried_flag`/`current_flag` on `employee` to boolean.
# MAGIC - Validate `sales_person.business_entity_id` resolves to a real employee.
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

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("aide.silver_salesperson")

# COMMAND ----------

# MAGIC %md ## Business transformations

# COMMAND ----------


def derive_quota_attainment_pct(df: DataFrame) -> DataFrame:
    """sales_ytd / sales_quota * 100, guarded against a null/zero sales_quota."""
    return df.withColumn(
        "quota_attainment_pct",
        F.when(
            F.col("sales_quota").isNotNull() & (F.col("sales_quota") > 0),
            F.round(F.col("sales_ytd") / F.col("sales_quota") * 100, 2),
        ),
    )


# COMMAND ----------

# MAGIC %md ## Per-table processing

# COMMAND ----------


def process_employee(
    spark: SparkSession, catalog: str, bronze_schema: str, silver_schema: str
) -> None:
    start_time = start_execution_timer()
    bronze_table = silver_table = "employee"

    bronze_df = read_bronze_table(spark, catalog, bronze_schema, bronze_table)
    rows_read = bronze_df.count()

    dq_results = [
        check_row_count_positive(bronze_df),
        check_no_nulls(bronze_df, ["business_entity_id"]),
        check_no_duplicate_keys(bronze_df, ["business_entity_id"]),
    ]

    cleaned_df = drop_technical_columns(bronze_df)
    cleaned_df = trim_string_columns(cleaned_df)
    cleaned_df = flags_to_boolean(cleaned_df, ["salaried_flag", "current_flag"])
    cleaned_df = deduplicate_by_key(cleaned_df, ["business_entity_id"])
    silver_df = add_silver_audit_columns(
        cleaned_df, f"{catalog}.{bronze_schema}.{bronze_table}"
    )

    full_table_name = write_silver_table(spark, silver_df, catalog, silver_schema, silver_table)
    rows_written = spark.table(full_table_name).count()

    elapsed_seconds = get_elapsed_seconds(start_time)
    print_silver_execution_summary(
        silver_table, full_table_name, rows_read, rows_written, dq_results, elapsed_seconds
    )


def process_sales_person(
    spark: SparkSession, catalog: str, bronze_schema: str, silver_schema: str
) -> None:
    start_time = start_execution_timer()
    bronze_table, silver_table = "salesperson", "sales_person"

    bronze_df = read_bronze_table(spark, catalog, bronze_schema, bronze_table)
    rows_read = bronze_df.count()

    employee_df = read_bronze_table(spark, catalog, bronze_schema, "employee")

    dq_results = [
        check_row_count_positive(bronze_df),
        check_no_nulls(bronze_df, ["business_entity_id"]),
        check_no_duplicate_keys(bronze_df, ["business_entity_id"]),
        check_referential_integrity(
            bronze_df, "business_entity_id", employee_df, "business_entity_id"
        ),
    ]

    cleaned_df = drop_technical_columns(bronze_df)
    cleaned_df = trim_string_columns(cleaned_df)
    cleaned_df = deduplicate_by_key(cleaned_df, ["business_entity_id"])
    transformed_df = derive_quota_attainment_pct(cleaned_df)
    silver_df = add_silver_audit_columns(
        transformed_df, f"{catalog}.{bronze_schema}.{bronze_table}"
    )

    full_table_name = write_silver_table(spark, silver_df, catalog, silver_schema, silver_table)
    rows_written = spark.table(full_table_name).count()

    elapsed_seconds = get_elapsed_seconds(start_time)
    print_silver_execution_summary(
        silver_table, full_table_name, rows_read, rows_written, dq_results, elapsed_seconds
    )


# COMMAND ----------

# MAGIC %md ## main()

# COMMAND ----------


def main() -> None:
    start_time = start_execution_timer()

    catalog = dbutils.widgets.get("catalog")
    bronze_schema = dbutils.widgets.get("bronze_schema")
    silver_schema = dbutils.widgets.get("silver_schema")

    logger.info(
        "Starting Silver transformation | domain=salesperson catalog=%s bronze_schema=%s "
        "silver_schema=%s",
        catalog,
        bronze_schema,
        silver_schema,
    )

    try:
        process_employee(spark, catalog, bronze_schema, silver_schema)
        process_sales_person(spark, catalog, bronze_schema, silver_schema)

        elapsed_seconds = get_elapsed_seconds(start_time)
        print(f"Salesperson domain completed in {elapsed_seconds:.2f} sec (2 tables).")

    except Exception:
        logger.exception("Silver transformation failed for the salesperson domain.")
        raise


# COMMAND ----------

main()
