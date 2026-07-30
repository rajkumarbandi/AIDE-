# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — dim_salesperson
# MAGIC
# MAGIC Builds the salesperson dimension. Pure star: assigned territory is
# MAGIC denormalized flat (`territory_name`) — no FK to `dim_territory`.
# MAGIC
# MAGIC **Business Transformations:**
# MAGIC - Flatten `employee.job_title`/`hire_date`/`current_flag` and
# MAGIC   `person.full_name` onto the salesperson row.
# MAGIC - Add a manually-inserted **"Unknown Salesperson"** member
# MAGIC   (`salesperson_key = -1`) so `fact_sales` never has a dangling FK for an order
# MAGIC   with no assigned rep (e.g. an online order).
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
logger = logging.getLogger("aide.gold_dim_salesperson")

# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

TABLE_NAME = "dim_salesperson"

# COMMAND ----------

# MAGIC %md ## Business transformations

# COMMAND ----------


def build_dim_salesperson(
    sales_person_df: DataFrame,
    employee_df: DataFrame,
    person_df: DataFrame,
    territory_df: DataFrame,
) -> DataFrame:
    """Flatten employee/person/territory attributes onto the salesperson row and
    append the Unknown member.
    """
    employee_flat = employee_df.select(
        F.col("business_entity_id").alias("_emp_key"),
        F.col("job_title"),
        F.col("hire_date"),
        F.col("current_flag").alias("is_current_employee"),
    )
    person_flat = person_df.select(
        F.col("business_entity_id").alias("_person_key"),
        F.col("full_name").alias("salesperson_name"),
    )
    territory_flat = territory_df.select(
        F.col("territory_id").alias("_territory_key"), F.col("territory_name")
    )

    joined = (
        sales_person_df.join(
            employee_flat,
            sales_person_df["business_entity_id"] == employee_flat["_emp_key"],
            "left",
        )
        .join(
            person_flat,
            sales_person_df["business_entity_id"] == person_flat["_person_key"],
            "left",
        )
        .join(
            territory_flat,
            sales_person_df["territory_id"] == territory_flat["_territory_key"],
            "left",
        )
    )

    real_rows = joined.select(
        F.col("business_entity_id").alias("salesperson_key"),
        F.col("business_entity_id").alias("salesperson_id"),
        F.col("salesperson_name"),
        F.col("job_title"),
        F.col("hire_date"),
        F.col("is_current_employee"),
        F.col("territory_name"),
        F.col("sales_quota"),
        F.col("sales_ytd"),
        F.col("sales_last_year"),
        F.col("commission_pct"),
        F.col("quota_attainment_pct"),
    )

    unknown_row = spark.createDataFrame(
        [
            (
                UNKNOWN_MEMBER_KEY,
                UNKNOWN_MEMBER_KEY,
                "Unknown Salesperson",
                None,
                None,
                False,
                "Unknown",
                None,
                None,
                None,
                None,
                None,
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
        sales_person_df = read_silver_table(spark, catalog, silver_schema, "sales_person")
        employee_df = read_silver_table(spark, catalog, silver_schema, "employee")
        person_df = read_silver_table(spark, catalog, silver_schema, "person")
        territory_df = read_silver_table(spark, catalog, silver_schema, "sales_territory")

        gold_df = build_dim_salesperson(sales_person_df, employee_df, person_df, territory_df)
        gold_df = add_gold_audit_columns(gold_df, "sales_person,employee,person,sales_territory")

        dq_results = [
            check_row_count_positive(gold_df),
            check_no_nulls(gold_df, ["salesperson_key"]),
            check_no_duplicate_keys(gold_df, ["salesperson_key"]),
        ]

        full_table_name = write_gold_table(spark, gold_df, catalog, gold_schema, TABLE_NAME)
        rows_written = spark.table(full_table_name).count()

        null_count = gold_df.filter(F.col("salesperson_key").isNull()).count()
        distinct_keys = gold_df.select("salesperson_key").distinct().count()
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
                "salesperson_key",
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
