# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Address Domain
# MAGIC
# MAGIC Cleans four small, related Bronze tables into their Silver equivalents:
# MAGIC `country_region`, `state_province`, `address`, `business_entity_address`.
# MAGIC Grouped in one notebook because they're all part of the same "location" domain —
# MAGIC each is still cleaned independently at its own source grain (no cross-table joins
# MAGIC here; denormalization happens at Gold, per the approved design).
# MAGIC
# MAGIC **Business Transformations:**
# MAGIC - Rename generic `name` columns to entity-specific names (`country_region_name`,
# MAGIC   `state_province_name`) — a bare `name` is ambiguous once tables are joined at Gold.
# MAGIC - Cast `is_only_state_province_flag` to boolean.
# MAGIC - Drop `spatial_location` from `address` — a WKB-hex geography blob, unusable
# MAGIC   without a geospatial library; no current KPI needs it.
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

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("aide.silver_address")

# COMMAND ----------

# MAGIC %md ## Business transformations & per-table processing
# MAGIC
# MAGIC One function per table — each is fully self-contained (read, DQ checks, clean,
# MAGIC write, summary), matching the pattern established in 01_customer.py.

# COMMAND ----------


def process_country_region(
    spark: SparkSession, catalog: str, bronze_schema: str, silver_schema: str
) -> None:
    start_time = start_execution_timer()
    bronze_table, silver_table = "countryregion", "country_region"

    bronze_df = read_bronze_table(spark, catalog, bronze_schema, bronze_table)
    rows_read = bronze_df.count()

    dq_results = [
        check_row_count_positive(bronze_df),
        check_no_nulls(bronze_df, ["country_region_code"]),
        check_no_duplicate_keys(bronze_df, ["country_region_code"]),
    ]

    cleaned_df = drop_technical_columns(bronze_df)
    cleaned_df = trim_string_columns(cleaned_df)
    cleaned_df = deduplicate_by_key(cleaned_df, ["country_region_code"])
    transformed_df = cleaned_df.withColumnRenamed("name", "country_region_name")
    silver_df = add_silver_audit_columns(
        transformed_df, f"{catalog}.{bronze_schema}.{bronze_table}"
    )

    full_table_name = write_silver_table(spark, silver_df, catalog, silver_schema, silver_table)
    rows_written = spark.table(full_table_name).count()

    elapsed_seconds = get_elapsed_seconds(start_time)
    print_silver_execution_summary(
        silver_table, full_table_name, rows_read, rows_written, dq_results, elapsed_seconds
    )


def process_state_province(
    spark: SparkSession, catalog: str, bronze_schema: str, silver_schema: str
) -> None:
    start_time = start_execution_timer()
    bronze_table, silver_table = "stateprovince", "state_province"

    bronze_df = read_bronze_table(spark, catalog, bronze_schema, bronze_table)
    rows_read = bronze_df.count()

    dq_results = [
        check_row_count_positive(bronze_df),
        check_no_nulls(bronze_df, ["state_province_id"]),
        check_no_duplicate_keys(bronze_df, ["state_province_id"]),
    ]

    cleaned_df = drop_technical_columns(bronze_df)
    cleaned_df = trim_string_columns(cleaned_df)
    cleaned_df = flags_to_boolean(cleaned_df, ["is_only_state_province_flag"])
    cleaned_df = deduplicate_by_key(cleaned_df, ["state_province_id"])
    transformed_df = cleaned_df.withColumnRenamed("name", "state_province_name")
    silver_df = add_silver_audit_columns(
        transformed_df, f"{catalog}.{bronze_schema}.{bronze_table}"
    )

    full_table_name = write_silver_table(spark, silver_df, catalog, silver_schema, silver_table)
    rows_written = spark.table(full_table_name).count()

    elapsed_seconds = get_elapsed_seconds(start_time)
    print_silver_execution_summary(
        silver_table, full_table_name, rows_read, rows_written, dq_results, elapsed_seconds
    )


def process_address(
    spark: SparkSession, catalog: str, bronze_schema: str, silver_schema: str
) -> None:
    start_time = start_execution_timer()
    bronze_table = silver_table = "address"

    bronze_df = read_bronze_table(spark, catalog, bronze_schema, bronze_table)
    rows_read = bronze_df.count()

    dq_results = [
        check_row_count_positive(bronze_df),
        check_no_nulls(bronze_df, ["address_id"]),
        check_no_duplicate_keys(bronze_df, ["address_id"]),
    ]

    cleaned_df = drop_technical_columns(bronze_df, extra_columns=["spatial_location"])
    cleaned_df = trim_string_columns(cleaned_df)
    cleaned_df = deduplicate_by_key(cleaned_df, ["address_id"])
    silver_df = add_silver_audit_columns(cleaned_df, f"{catalog}.{bronze_schema}.{bronze_table}")

    full_table_name = write_silver_table(spark, silver_df, catalog, silver_schema, silver_table)
    rows_written = spark.table(full_table_name).count()

    elapsed_seconds = get_elapsed_seconds(start_time)
    print_silver_execution_summary(
        silver_table, full_table_name, rows_read, rows_written, dq_results, elapsed_seconds
    )


def process_business_entity_address(
    spark: SparkSession, catalog: str, bronze_schema: str, silver_schema: str
) -> None:
    start_time = start_execution_timer()
    bronze_table, silver_table = "businessentityaddress", "business_entity_address"
    key_columns = ["business_entity_id", "address_id", "address_type_id"]

    bronze_df = read_bronze_table(spark, catalog, bronze_schema, bronze_table)
    rows_read = bronze_df.count()

    dq_results = [
        check_row_count_positive(bronze_df),
        check_no_nulls(bronze_df, key_columns),
        check_no_duplicate_keys(bronze_df, key_columns),
    ]

    cleaned_df = drop_technical_columns(bronze_df)
    cleaned_df = trim_string_columns(cleaned_df)
    cleaned_df = deduplicate_by_key(cleaned_df, key_columns)
    silver_df = add_silver_audit_columns(cleaned_df, f"{catalog}.{bronze_schema}.{bronze_table}")

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
        "Starting Silver transformation | domain=address catalog=%s bronze_schema=%s "
        "silver_schema=%s",
        catalog,
        bronze_schema,
        silver_schema,
    )

    try:
        process_country_region(spark, catalog, bronze_schema, silver_schema)
        process_state_province(spark, catalog, bronze_schema, silver_schema)
        process_address(spark, catalog, bronze_schema, silver_schema)
        process_business_entity_address(spark, catalog, bronze_schema, silver_schema)

        elapsed_seconds = get_elapsed_seconds(start_time)
        print(f"Address domain completed in {elapsed_seconds:.2f} sec (4 tables).")

    except Exception:
        logger.exception("Silver transformation failed for the address domain.")
        raise


# COMMAND ----------

main()
