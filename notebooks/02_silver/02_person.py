# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Person
# MAGIC
# MAGIC Cleans `bronze.person` into `silver.person`.
# MAGIC
# MAGIC **Business Transformations:**
# MAGIC - Decode `person_type` into a readable `person_type_description`, using the
# MAGIC   AdventureWorks-documented code meanings (verified from `instawdb.sql`'s own
# MAGIC   extended-property comment, not assumed).
# MAGIC - Derive `full_name` from `first_name`/`middle_name`/`last_name`, correctly
# MAGIC   handling a null `middle_name` (no double-spacing).
# MAGIC - Drop `additional_contact_info`/`demographics` — raw XML with no clean tabular
# MAGIC   structure; no current KPI needs them (parsing `demographics` for customer
# MAGIC   segmentation is noted as future scope in the design doc, not built here).
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
logger = logging.getLogger("aide.silver_person")

# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

TABLE_NAME = "person"

# Verified from instawdb.sql's own extended-property comment on Person.PersonType,
# not recalled from general AdventureWorks familiarity.
VALID_PERSON_TYPES = ["SC", "IN", "SP", "EM", "VC", "GC"]

# COMMAND ----------

# MAGIC %md ## Business transformations

# COMMAND ----------


def decode_person_type(df: DataFrame) -> DataFrame:
    """Decode person_type into a readable person_type_description."""
    return df.withColumn(
        "person_type_description",
        F.when(F.col("person_type") == "SC", F.lit("Store Contact"))
        .when(F.col("person_type") == "IN", F.lit("Individual (Retail) Customer"))
        .when(F.col("person_type") == "SP", F.lit("Sales Person"))
        .when(F.col("person_type") == "EM", F.lit("Employee (Non-Sales)"))
        .when(F.col("person_type") == "VC", F.lit("Vendor Contact"))
        .when(F.col("person_type") == "GC", F.lit("General Contact"))
        .otherwise(F.lit("Unknown")),
    )


def derive_full_name(df: DataFrame) -> DataFrame:
    """Concatenate first/middle/last name. concat_ws skips nulls automatically, so a
    null middle_name never produces a double space.
    """
    name_parts = [F.col("first_name"), F.col("middle_name"), F.col("last_name")]
    full_name_expr = F.concat_ws(" ", *name_parts)
    return df.withColumn("full_name", F.trim(full_name_expr))


# COMMAND ----------

# MAGIC %md ## Table-specific data quality checks

# COMMAND ----------


def check_valid_person_type(df: DataFrame) -> dict:
    """Validate person_type is one of the documented codes."""
    invalid_count = df.filter(~F.col("person_type").isin(VALID_PERSON_TYPES)).count()
    passed = invalid_count == 0
    detail = (
        "no violations" if passed else f"{invalid_count} row(s) with an unrecognized person_type"
    )
    if not passed:
        logger.warning("Data quality check failed — valid_person_type: %s", detail)
    return {"check_name": "valid_person_type", "passed": passed, "detail": detail}


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
            check_no_nulls(bronze_df, ["business_entity_id"]),
            check_no_duplicate_keys(bronze_df, ["business_entity_id"]),
            check_valid_person_type(bronze_df),
        ]

        cleaned_df = drop_technical_columns(
            bronze_df, extra_columns=["additional_contact_info", "demographics"]
        )
        cleaned_df = trim_string_columns(cleaned_df)
        cleaned_df = deduplicate_by_key(cleaned_df, ["business_entity_id"])
        transformed_df = decode_person_type(cleaned_df)
        transformed_df = derive_full_name(transformed_df)
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
