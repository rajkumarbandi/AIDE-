# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Product Domain
# MAGIC
# MAGIC Cleans three related Bronze tables into their Silver equivalents:
# MAGIC `product_category`, `product_subcategory`, `product`. Grouped in one notebook as
# MAGIC the "product" domain — each cleaned independently at its own source grain;
# MAGIC category/subcategory are only denormalized onto `dim_product` at Gold.
# MAGIC
# MAGIC **Business Transformations:**
# MAGIC - Rename `name` -> `category_name` / `subcategory_name` / `product_name`, and
# MAGIC   `class`/`style` -> `product_class`/`product_style` (clarity; avoids
# MAGIC   reserved-word-ish column names).
# MAGIC - Cast `make_flag`/`finished_goods_flag` to boolean.
# MAGIC - Derive `product_status` ('Discontinued'/'Inactive'/'Active') — needed to filter
# MAGIC   "Product Performance" KPIs to currently-sellable products.
# MAGIC - Derive `margin_pct`, guarded against `list_price = 0` — raw-material/component
# MAGIC   products that are never sold directly genuinely have `list_price = 0` in this
# MAGIC   data, not a hypothetical edge case.
# MAGIC - Validate `product.product_subcategory_id` resolves to a real subcategory
# MAGIC   (when non-null — components/raw materials legitimately have none).
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
logger = logging.getLogger("aide.silver_product")

# COMMAND ----------

# MAGIC %md ## Business transformations

# COMMAND ----------


def derive_product_status(df: DataFrame) -> DataFrame:
    """'Discontinued' if discontinued_date is set, 'Inactive' if sell_end_date has
    passed, else 'Active'.
    """
    return df.withColumn(
        "product_status",
        F.when(F.col("discontinued_date").isNotNull(), F.lit("Discontinued"))
        .when(
            F.col("sell_end_date").isNotNull() & (F.col("sell_end_date") < F.current_date()),
            F.lit("Inactive"),
        )
        .otherwise(F.lit("Active")),
    )


def derive_margin_pct(df: DataFrame) -> DataFrame:
    """(list_price - standard_cost) / list_price * 100, guarded against
    list_price = 0 (raw-material/component products with no direct sale price).
    """
    return df.withColumn(
        "margin_pct",
        F.when(
            F.col("list_price") > 0,
            F.round((F.col("list_price") - F.col("standard_cost")) / F.col("list_price") * 100, 2),
        ),
    )


# COMMAND ----------

# MAGIC %md ## Per-table processing
# MAGIC
# MAGIC One function per table — each self-contained (read, DQ checks, clean, write,
# MAGIC summary), matching the pattern established in 01_customer.py.

# COMMAND ----------


def process_product_category(
    spark: SparkSession, catalog: str, bronze_schema: str, silver_schema: str
) -> None:
    start_time = start_execution_timer()
    bronze_table, silver_table = "productcategory", "product_category"

    bronze_df = read_bronze_table(spark, catalog, bronze_schema, bronze_table)
    rows_read = bronze_df.count()

    dq_results = [
        check_row_count_positive(bronze_df),
        check_no_nulls(bronze_df, ["product_category_id"]),
        check_no_duplicate_keys(bronze_df, ["product_category_id"]),
    ]

    cleaned_df = drop_technical_columns(bronze_df)
    cleaned_df = trim_string_columns(cleaned_df)
    cleaned_df = deduplicate_by_key(cleaned_df, ["product_category_id"])
    transformed_df = cleaned_df.withColumnRenamed("name", "category_name")
    silver_df = add_silver_audit_columns(
        transformed_df, f"{catalog}.{bronze_schema}.{bronze_table}"
    )

    full_table_name = write_silver_table(spark, silver_df, catalog, silver_schema, silver_table)
    rows_written = spark.table(full_table_name).count()

    elapsed_seconds = get_elapsed_seconds(start_time)
    print_silver_execution_summary(
        silver_table, full_table_name, rows_read, rows_written, dq_results, elapsed_seconds
    )


def process_product_subcategory(
    spark: SparkSession, catalog: str, bronze_schema: str, silver_schema: str
) -> None:
    start_time = start_execution_timer()
    bronze_table, silver_table = "productsubcategory", "product_subcategory"

    bronze_df = read_bronze_table(spark, catalog, bronze_schema, bronze_table)
    rows_read = bronze_df.count()

    dq_results = [
        check_row_count_positive(bronze_df),
        check_no_nulls(bronze_df, ["product_subcategory_id"]),
        check_no_duplicate_keys(bronze_df, ["product_subcategory_id"]),
    ]

    cleaned_df = drop_technical_columns(bronze_df)
    cleaned_df = trim_string_columns(cleaned_df)
    cleaned_df = deduplicate_by_key(cleaned_df, ["product_subcategory_id"])
    transformed_df = cleaned_df.withColumnRenamed("name", "subcategory_name")
    silver_df = add_silver_audit_columns(
        transformed_df, f"{catalog}.{bronze_schema}.{bronze_table}"
    )

    full_table_name = write_silver_table(spark, silver_df, catalog, silver_schema, silver_table)
    rows_written = spark.table(full_table_name).count()

    elapsed_seconds = get_elapsed_seconds(start_time)
    print_silver_execution_summary(
        silver_table, full_table_name, rows_read, rows_written, dq_results, elapsed_seconds
    )


def process_product(
    spark: SparkSession, catalog: str, bronze_schema: str, silver_schema: str
) -> None:
    start_time = start_execution_timer()
    bronze_table = silver_table = "product"

    bronze_df = read_bronze_table(spark, catalog, bronze_schema, bronze_table)
    rows_read = bronze_df.count()

    subcategory_df = read_bronze_table(spark, catalog, bronze_schema, "productsubcategory")

    dq_results = [
        check_row_count_positive(bronze_df),
        check_no_nulls(bronze_df, ["product_id"]),
        check_no_duplicate_keys(bronze_df, ["product_id"]),
        check_referential_integrity(
            bronze_df, "product_subcategory_id", subcategory_df, "product_subcategory_id"
        ),
    ]

    cleaned_df = drop_technical_columns(bronze_df)
    cleaned_df = trim_string_columns(cleaned_df)
    cleaned_df = flags_to_boolean(cleaned_df, ["make_flag", "finished_goods_flag"])
    cleaned_df = deduplicate_by_key(cleaned_df, ["product_id"])
    transformed_df = (
        cleaned_df.withColumnRenamed("name", "product_name")
        .withColumnRenamed("class", "product_class")
        .withColumnRenamed("style", "product_style")
    )
    transformed_df = derive_product_status(transformed_df)
    transformed_df = derive_margin_pct(transformed_df)
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
        "Starting Silver transformation | domain=product catalog=%s bronze_schema=%s "
        "silver_schema=%s",
        catalog,
        bronze_schema,
        silver_schema,
    )

    try:
        process_product_category(spark, catalog, bronze_schema, silver_schema)
        process_product_subcategory(spark, catalog, bronze_schema, silver_schema)
        process_product(spark, catalog, bronze_schema, silver_schema)

        elapsed_seconds = get_elapsed_seconds(start_time)
        print(f"Product domain completed in {elapsed_seconds:.2f} sec (3 tables).")

    except Exception:
        logger.exception("Silver transformation failed for the product domain.")
        raise


# COMMAND ----------

main()
