# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — dim_customer
# MAGIC
# MAGIC Builds the customer dimension for "Customer Analytics" KPIs (total/active
# MAGIC customers, customer type, customers by territory). Pure star: no dimension-to-
# MAGIC dimension FKs — territory context is flattened onto this table directly.
# MAGIC
# MAGIC **Business Transformations:**
# MAGIC - `customer_name` = the person's `full_name` for individual customers, or the
# MAGIC   store's `store_name` for store customers (via `COALESCE`).
# MAGIC - Primary address: a customer can have multiple addresses in
# MAGIC   `business_entity_address` (billing, shipping, etc.); this picks the one with
# MAGIC   the lowest `address_id` per customer as a deterministic tiebreaker — a
# MAGIC   simplification (no `address_type` name lookup exists in Silver to prefer a
# MAGIC   specific type by name).
# MAGIC - `customer_territory_name`/`_group` = the customer's *registered* territory —
# MAGIC   a distinct concept from `fact_sales.territory_key` (the *transaction's*
# MAGIC   territory), by design (see `docs/silver_gold_warehouse_design.md`).
# MAGIC - `is_active_customer` = placed an order within the trailing 12 months **of the
# MAGIC   most recent order date in the data**, not real-world `current_date()` —
# MAGIC   AdventureWorks is a static historical dataset, not a live feed. Customers
# MAGIC   with zero orders are explicitly `False`, not null.
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
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("aide.gold_dim_customer")

# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

TABLE_NAME = "dim_customer"
ACTIVITY_WINDOW_DAYS = 365

# COMMAND ----------

# MAGIC %md ## Business transformations

# COMMAND ----------


def build_customer_name(
    customer_df: DataFrame, person_df: DataFrame, store_df: DataFrame
) -> DataFrame:
    """customer_name = person's full_name (individual) or store's store_name (store)."""
    person_names = person_df.select(
        F.col("business_entity_id").alias("_person_key"), F.col("full_name")
    )
    store_names = store_df.select(
        F.col("business_entity_id").alias("_store_key"), F.col("store_name")
    )
    joined = customer_df.join(
        person_names, customer_df["person_id"] == person_names["_person_key"], "left"
    ).join(store_names, customer_df["store_id"] == store_names["_store_key"], "left")

    return joined.withColumn(
        "customer_name", F.coalesce(F.col("full_name"), F.col("store_name"))
    ).drop("_person_key", "_store_key", "full_name", "store_name")


def build_primary_address(
    business_entity_address_df: DataFrame,
    address_df: DataFrame,
    state_province_df: DataFrame,
    country_region_df: DataFrame,
) -> DataFrame:
    """One address per business_entity_id: the lowest address_id (deterministic
    tiebreaker), enriched with city/state/country.
    """
    address_rank_window = Window.partitionBy("business_entity_id").orderBy(
        F.col("address_id").asc()
    )
    primary_link = (
        business_entity_address_df.withColumn("_rank", F.row_number().over(address_rank_window))
        .filter(F.col("_rank") == 1)
        .select("business_entity_id", "address_id")
    )
    address_enriched = (
        address_df.join(state_province_df, "state_province_id", "left")
        .join(country_region_df, "country_region_code", "left")
        .select(
            "address_id",
            "address_line1",
            "city",
            "postal_code",
            "state_province_name",
            "country_region_name",
        )
    )
    return primary_link.join(address_enriched, "address_id", "left").drop("address_id")


def build_customer_activity(order_header_df: DataFrame) -> DataFrame:
    """first/most-recent order date and is_active_customer per customer_id, relative
    to the data's own most recent order date (not real-world current_date()).
    """
    reference_date = order_header_df.agg(F.max("order_date")).first()[0]
    activity_threshold = F.date_sub(F.lit(reference_date), ACTIVITY_WINDOW_DAYS)

    return (
        order_header_df.groupBy("customer_id")
        .agg(
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("most_recent_order_date"),
        )
        .withColumn("is_active_customer", F.col("most_recent_order_date") >= activity_threshold)
    )


def build_dim_customer(
    customer_df: DataFrame,
    person_df: DataFrame,
    store_df: DataFrame,
    business_entity_address_df: DataFrame,
    address_df: DataFrame,
    state_province_df: DataFrame,
    country_region_df: DataFrame,
    territory_df: DataFrame,
    order_header_df: DataFrame,
) -> DataFrame:
    named_df = build_customer_name(customer_df, person_df, store_df)
    named_df = named_df.withColumn(
        "_customer_business_entity_id", F.coalesce(F.col("person_id"), F.col("store_id"))
    )

    primary_address_df = build_primary_address(
        business_entity_address_df, address_df, state_province_df, country_region_df
    )
    activity_df = build_customer_activity(order_header_df)

    territory_flat = territory_df.select(
        F.col("territory_id").alias("_territory_key"),
        F.col("territory_name").alias("customer_territory_name"),
        F.col("territory_group").alias("customer_territory_group"),
    )

    with_address = named_df.join(
        primary_address_df,
        named_df["_customer_business_entity_id"] == primary_address_df["business_entity_id"],
        "left",
    ).drop("_customer_business_entity_id", "business_entity_id")

    with_territory = with_address.join(
        territory_flat, with_address["territory_id"] == territory_flat["_territory_key"], "left"
    ).drop("_territory_key")

    with_activity = with_territory.join(activity_df, "customer_id", "left")

    return with_activity.select(
        F.col("customer_id").alias("customer_key"),
        F.col("customer_id"),
        F.col("customer_type"),
        F.col("customer_name"),
        F.col("account_number"),
        F.col("address_line1"),
        F.col("city"),
        F.col("state_province_name"),
        F.col("country_region_name"),
        F.col("postal_code"),
        F.col("customer_territory_name"),
        F.col("customer_territory_group"),
        F.coalesce(F.col("is_active_customer"), F.lit(False)).alias("is_active_customer"),
        F.col("first_order_date"),
        F.col("most_recent_order_date"),
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
        customer_df = read_silver_table(spark, catalog, silver_schema, "customer")
        person_df = read_silver_table(spark, catalog, silver_schema, "person")
        store_df = read_silver_table(spark, catalog, silver_schema, "store")
        business_entity_address_df = read_silver_table(
            spark, catalog, silver_schema, "business_entity_address"
        )
        address_df = read_silver_table(spark, catalog, silver_schema, "address")
        state_province_df = read_silver_table(spark, catalog, silver_schema, "state_province")
        country_region_df = read_silver_table(spark, catalog, silver_schema, "country_region")
        territory_df = read_silver_table(spark, catalog, silver_schema, "sales_territory")
        order_header_df = read_silver_table(spark, catalog, silver_schema, "sales_order_header")

        gold_df = build_dim_customer(
            customer_df,
            person_df,
            store_df,
            business_entity_address_df,
            address_df,
            state_province_df,
            country_region_df,
            territory_df,
            order_header_df,
        )
        gold_df = add_gold_audit_columns(
            gold_df,
            "customer,person,store,business_entity_address,address,state_province,"
            "country_region,sales_territory,sales_order_header",
        )

        dq_results = [
            check_row_count_positive(gold_df),
            check_no_nulls(gold_df, ["customer_key"]),
            check_no_nulls(gold_df, ["customer_name"]),
            check_no_duplicate_keys(gold_df, ["customer_key"]),
        ]

        full_table_name = write_gold_table(spark, gold_df, catalog, gold_schema, TABLE_NAME)
        rows_written = spark.table(full_table_name).count()

        null_count = gold_df.filter(F.col("customer_key").isNull()).count()
        distinct_keys = gold_df.select("customer_key").distinct().count()
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
                "customer_key",
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
