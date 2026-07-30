# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — dim_territory
# MAGIC
# MAGIC Builds the sales territory dimension for "Sales by Territory" KPIs. Pure star:
# MAGIC country name is denormalized flat — no separate `dim_country`.
# MAGIC
# MAGIC **Business Transformations:**
# MAGIC - Flatten `country_region.country_region_name` onto the territory row.
# MAGIC - Add a manually-inserted **"Unknown Territory"** member
# MAGIC   (`territory_key = -1`) so `fact_sales` never has a dangling FK for an order
# MAGIC   with no assigned territory. `sales_ytd`/`sales_last_year`/`cost_ytd`/
# MAGIC   `cost_last_year` from the source are deliberately **not** carried forward —
# MAGIC   territory revenue is always computed fresh from `fact_sales`.
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
logger = logging.getLogger("aide.gold_dim_territory")

# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

TABLE_NAME = "dim_territory"

# COMMAND ----------

# MAGIC %md ## Business transformations

# COMMAND ----------


def build_dim_territory(territory_df: DataFrame, country_region_df: DataFrame) -> DataFrame:
    """Flatten country name onto the territory row and append the Unknown member.

    The Unknown row uses -1 for both territory_key and territory_id (rather than a
    null territory_id) to sidestep any question of whether the source's non-nullable
    territory_id schema would reject a null literal here — -1/-1 is a standard,
    unambiguous sentinel convention.
    """
    real_rows = (
        territory_df.join(country_region_df, "country_region_code", "left")
        .select(
            F.col("territory_id").alias("territory_key"),
            F.col("territory_id"),
            F.col("territory_name"),
            F.col("territory_group"),
            F.col("country_region_code"),
            F.col("country_region_name"),
        )
    )
    unknown_row = spark.createDataFrame(
        [
            (
                UNKNOWN_MEMBER_KEY,
                UNKNOWN_MEMBER_KEY,
                "Unknown Territory",
                "Unknown",
                "N/A",
                "Unknown",
            )
        ],
        schema=real_rows.schema,
    )
    return real_rows.unionByName(unknown_row)


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
        territory_df = read_silver_table(spark, catalog, silver_schema, "sales_territory")
        country_region_df = read_silver_table(spark, catalog, silver_schema, "country_region")

        gold_df = build_dim_territory(territory_df, country_region_df)
        gold_df = add_gold_audit_columns(gold_df, "sales_territory,country_region")

        dq_results = [
            check_row_count_positive(gold_df),
            check_no_nulls(gold_df, ["territory_key"]),
            check_no_duplicate_keys(gold_df, ["territory_key"]),
        ]

        full_table_name = write_gold_table(spark, gold_df, catalog, gold_schema, TABLE_NAME)
        rows_written = spark.table(full_table_name).count()

        null_count = gold_df.filter(F.col("territory_key").isNull()).count()
        distinct_keys = gold_df.select("territory_key").distinct().count()
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
                "territory_key",
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
