# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — dim_product
# MAGIC
# MAGIC Builds the product dimension for "Product Performance" KPIs (top products,
# MAGIC revenue by product, units sold). Pure star: category/subcategory are
# MAGIC denormalized flat onto this table — no separate `dim_product_category`.
# MAGIC
# MAGIC **Business Transformations:**
# MAGIC - Flatten `product_subcategory.subcategory_name` and
# MAGIC   `product_category.category_name` onto the product row via the
# MAGIC   subcategory -> category chain.
# MAGIC - `product_status`/`margin_pct` are carried through as-is from Silver, where
# MAGIC   they were already derived.
# MAGIC
# MAGIC Reads Silver only — no dependency on any other Gold notebook.

# COMMAND ----------

# MAGIC %md ## Import shared utilities

# COMMAND ----------

# MAGIC %run "./00_gold_utils"

# COMMAND ----------

# MAGIC %run "../02_silver/00_common_utils"

# COMMAND ----------

# MAGIC %md ## Widgets (job/notebook parameters)

# COMMAND ----------

dbutils.widgets.text("catalog", DEFAULT_CATALOG, "Catalog")
dbutils.widgets.text("silver_schema", DEFAULT_SILVER_SCHEMA, "Silver Schema")
dbutils.widgets.text("gold_schema", DEFAULT_GOLD_SCHEMA, "Gold Schema")

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
logger = logging.getLogger("aide.gold_dim_product")

# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

TABLE_NAME = "dim_product"

# COMMAND ----------

# MAGIC %md ## Business transformations

# COMMAND ----------


def build_dim_product(
    product_df: DataFrame, subcategory_df: DataFrame, category_df: DataFrame
) -> DataFrame:
    """Flatten category/subcategory names onto the product row (pure star, no
    separate category/subcategory Gold tables).
    """
    subcategory_flat = subcategory_df.select(
        F.col("product_subcategory_id").alias("_subcat_key"),
        F.col("subcategory_name"),
        F.col("product_category_id").alias("_subcat_category_id"),
    )
    category_flat = category_df.select(
        F.col("product_category_id").alias("_cat_key"), F.col("category_name")
    )

    joined = product_df.join(
        subcategory_flat,
        product_df["product_subcategory_id"] == subcategory_flat["_subcat_key"],
        "left",
    ).join(
        category_flat, subcategory_flat["_subcat_category_id"] == category_flat["_cat_key"], "left"
    )

    return joined.select(
        F.col("product_id").alias("product_key"),
        F.col("product_id"),
        F.col("product_name"),
        F.col("product_number"),
        F.col("color"),
        F.col("size"),
        F.col("weight"),
        F.col("product_line"),
        F.col("product_class"),
        F.col("product_style"),
        F.col("subcategory_name"),
        F.col("category_name"),
        F.col("standard_cost"),
        F.col("list_price"),
        F.col("margin_pct"),
        F.col("product_status"),
        F.col("make_flag"),
        F.col("finished_goods_flag"),
        F.col("sell_start_date"),
        F.col("sell_end_date"),
        F.col("discontinued_date"),
    )


# COMMAND ----------

# MAGIC %md ## main()

# COMMAND ----------


def main() -> None:
    start_time = start_execution_timer()

    catalog = dbutils.widgets.get("catalog")
    silver_schema = dbutils.widgets.get("silver_schema")
    gold_schema = dbutils.widgets.get("gold_schema")

    logger.info(
        "Starting Gold transformation | table=%s catalog=%s silver_schema=%s gold_schema=%s",
        TABLE_NAME,
        catalog,
        silver_schema,
        gold_schema,
    )

    try:
        product_df = read_silver_table(spark, catalog, silver_schema, "product")
        subcategory_df = read_silver_table(spark, catalog, silver_schema, "product_subcategory")
        category_df = read_silver_table(spark, catalog, silver_schema, "product_category")

        gold_df = build_dim_product(product_df, subcategory_df, category_df)
        gold_df = add_gold_audit_columns(gold_df, "product,product_subcategory,product_category")

        dq_results = [
            check_row_count_positive(gold_df),
            check_no_nulls(gold_df, ["product_key"]),
            check_no_duplicate_keys(gold_df, ["product_key"]),
        ]

        full_table_name = write_gold_table(spark, gold_df, catalog, gold_schema, TABLE_NAME)
        rows_written = spark.table(full_table_name).count()

        null_count = gold_df.filter(F.col("product_key").isNull()).count()
        distinct_keys = gold_df.select("product_key").distinct().count()
        null_pct = round(100.0 * null_count / rows_written, 2) if rows_written else 0.0
        duplicate_count = rows_written - distinct_keys
        duplicate_pct = (
            round(100.0 * duplicate_count / rows_written, 2) if rows_written else 0.0
        )

        elapsed_seconds = get_elapsed_seconds(start_time)
        print_gold_execution_summary(
            TABLE_NAME, full_table_name, rows_written, dq_results, elapsed_seconds
        )
        displayHTML(
            render_dimension_summary_html(
                TABLE_NAME,
                full_table_name,
                rows_written,
                "product_key",
                null_pct,
                duplicate_pct,
                elapsed_seconds,
            )
        )

    except Exception:
        logger.exception("Gold transformation failed for table '%s'.", TABLE_NAME)
        raise


# COMMAND ----------

main()
