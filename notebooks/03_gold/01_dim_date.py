# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — dim_date
# MAGIC
# MAGIC Generated calendar dimension — the only Gold object with no Silver source.
# MAGIC Spans a fixed wide date range (not `MIN`/`MAX` of order dates) so future orders
# MAGIC never require regenerating the dimension.
# MAGIC
# MAGIC **Business Transformations:**
# MAGIC - `date_key` is a `YYYYMMDD` "smart key" — sortable, human-readable, and the one
# MAGIC   dimension where reusing it as both surrogate and business key is standard
# MAGIC   Kimball practice.
# MAGIC - Fiscal year/quarter follow AdventureWorks' own convention: fiscal year starts
# MAGIC   **July 1** and is named after the calendar year it ends in (e.g. July 2025
# MAGIC   through June 2026 is fiscal year 2026).
# MAGIC
# MAGIC Runs independently — no dependency on any other Gold notebook.

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
dbutils.widgets.text("gold_schema", DEFAULT_GOLD_SCHEMA, "Gold Schema")
dbutils.widgets.text("start_date", "2005-01-01", "Start Date")
dbutils.widgets.text("end_date", "2030-12-31", "End Date")

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
logger = logging.getLogger("aide.gold_dim_date")

# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

TABLE_NAME = "dim_date"

# COMMAND ----------

# MAGIC %md ## Business transformations

# COMMAND ----------


def build_dim_date(start_date: str, end_date: str) -> DataFrame:
    """Generate one row per calendar date between start_date and end_date inclusive."""
    date_seq_df = spark.sql(
        f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), "
        f"interval 1 day)) AS full_date"
    )
    date_key_expr = F.date_format(F.col("full_date"), "yyyyMMdd").cast("int")
    return (
        date_seq_df.withColumn("date_key", date_key_expr)
        .withColumn("year", F.year(F.col("full_date")))
        .withColumn("quarter", F.quarter(F.col("full_date")))
        .withColumn("month", F.month(F.col("full_date")))
        .withColumn("month_name", F.date_format(F.col("full_date"), "MMMM"))
        .withColumn("day_of_month", F.dayofmonth(F.col("full_date")))
        .withColumn("day_of_week", F.dayofweek(F.col("full_date")))
        .withColumn("day_name", F.date_format(F.col("full_date"), "EEEE"))
        .withColumn("week_of_year", F.weekofyear(F.col("full_date")))
        .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))
        .withColumn(
            "fiscal_year",
            F.when(F.col("month") >= 7, F.col("year") + 1).otherwise(F.col("year")),
        )
        .withColumn(
            "fiscal_quarter",
            F.when(F.col("month").isin([7, 8, 9]), F.lit(1))
            .when(F.col("month").isin([10, 11, 12]), F.lit(2))
            .when(F.col("month").isin([1, 2, 3]), F.lit(3))
            .otherwise(F.lit(4)),
        )
    )


# COMMAND ----------

# MAGIC %md ## main()

# COMMAND ----------


def main() -> None:
    start_time = start_execution_timer()

    catalog = dbutils.widgets.get("catalog")
    gold_schema = dbutils.widgets.get("gold_schema")
    start_date = dbutils.widgets.get("start_date")
    end_date = dbutils.widgets.get("end_date")

    logger.info(
        "Starting Gold transformation | table=%s catalog=%s gold_schema=%s range=%s..%s",
        TABLE_NAME,
        catalog,
        gold_schema,
        start_date,
        end_date,
    )

    try:
        gold_df = build_dim_date(start_date, end_date)
        gold_df = add_gold_audit_columns(gold_df, "generated")

        dq_results = [
            check_row_count_positive(gold_df),
            check_no_nulls(gold_df, ["date_key", "full_date"]),
            check_no_duplicate_keys(gold_df, ["date_key"]),
        ]

        full_table_name = write_gold_table(spark, gold_df, catalog, gold_schema, TABLE_NAME)
        rows_written = spark.table(full_table_name).count()

        null_count = gold_df.filter(F.col("date_key").isNull()).count()
        distinct_keys = gold_df.select("date_key").distinct().count()
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
                "date_key",
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
