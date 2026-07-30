# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Sales Territory
# MAGIC
# MAGIC Cleans `bronze.salesterritory` into `silver.sales_territory`.
# MAGIC
# MAGIC **Business Transformations:**
# MAGIC - Rename `name`->`territory_name`, `group`->`territory_group` (clarity, and
# MAGIC   avoids the friction of a reserved-word-ish column name).
# MAGIC - `sales_ytd`/`sales_last_year`/`cost_ytd`/`cost_last_year` are carried through
# MAGIC   unchanged for reference/audit only — they are the *source system's* own
# MAGIC   point-in-time snapshot, not something this pipeline computed. Gold's territory
# MAGIC   revenue KPIs are always computed fresh from `fact_sales`, never from these
# MAGIC   columns, to avoid two disagreeing "territory revenue" numbers.
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("aide.silver_territory")

# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

BRONZE_TABLE_NAME = "salesterritory"
SILVER_TABLE_NAME = "sales_territory"

# COMMAND ----------

# MAGIC %md ## Business transformations

# COMMAND ----------


def rename_territory_columns(df: DataFrame) -> DataFrame:
    """Rename the ambiguous/reserved-word-ish source column names for clarity."""
    return df.withColumnRenamed("name", "territory_name").withColumnRenamed(
        "group", "territory_group"
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
        "Starting Silver transformation | table=%s catalog=%s bronze_schema=%s silver_schema=%s",
        SILVER_TABLE_NAME,
        catalog,
        bronze_schema,
        silver_schema,
    )

    try:
        bronze_df = read_bronze_table(spark, catalog, bronze_schema, BRONZE_TABLE_NAME)
        rows_read = bronze_df.count()

        dq_results = [
            check_row_count_positive(bronze_df),
            check_no_nulls(bronze_df, ["territory_id"]),
            check_no_duplicate_keys(bronze_df, ["territory_id"]),
        ]

        cleaned_df = drop_technical_columns(bronze_df)
        cleaned_df = trim_string_columns(cleaned_df)
        cleaned_df = deduplicate_by_key(cleaned_df, ["territory_id"])
        transformed_df = rename_territory_columns(cleaned_df)
        silver_df = add_silver_audit_columns(
            transformed_df, f"{catalog}.{bronze_schema}.{BRONZE_TABLE_NAME}"
        )

        full_table_name = write_silver_table(
            spark, silver_df, catalog, silver_schema, SILVER_TABLE_NAME
        )
        rows_written = spark.table(full_table_name).count()

        elapsed_seconds = get_elapsed_seconds(start_time)
        print_silver_execution_summary(
            SILVER_TABLE_NAME,
            full_table_name,
            rows_read,
            rows_written,
            dq_results,
            elapsed_seconds,
        )

    except Exception:
        logger.exception("Silver transformation failed for table '%s'.", SILVER_TABLE_NAME)
        raise


# COMMAND ----------

main()
