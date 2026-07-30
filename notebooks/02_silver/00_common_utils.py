# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Common Utilities
# MAGIC
# MAGIC Reusable helpers shared by every Silver notebook: reading Bronze, generic cleaning
# MAGIC (technical-column drop, string trimming, flag-to-boolean casting, dedup-by-key),
# MAGIC fail-soft data quality checks, audit columns, and the write + execution-summary
# MAGIC pattern used consistently across this repository.
# MAGIC
# MAGIC Import this notebook from any Silver notebook with:
# MAGIC
# MAGIC ```
# MAGIC %run "./00_common_utils"
# MAGIC ```
# MAGIC
# MAGIC **Design:** these are generic, table-agnostic primitives only. Table-specific business
# MAGIC rules (e.g. deriving `customer_type`, decoding `person_type`) belong in each domain
# MAGIC notebook, not here — a shared "business rules engine" would just be a harder-to-read
# MAGIC second copy of what's already clearly expressed as code per table (see
# MAGIC `docs/silver_gold_warehouse_design.md` §6 for the same reasoning applied to config).
# MAGIC
# MAGIC **No `%pip install` here** — this notebook has no external dependencies, but even if it
# MAGIC did, `%pip install` restarts the Python interpreter and would wipe the state of whatever
# MAGIC notebook `%run`s this one (same reasoning as `00_shared/01_gemini_client.py`).

# COMMAND ----------

# MAGIC %md ## Imports & logging

# COMMAND ----------

import logging
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("aide.silver_common")

# COMMAND ----------

# MAGIC %md ## Exceptions

# COMMAND ----------


class SilverTransformationError(Exception):
    """Base error for failures in the Silver transformation framework."""


class BronzeSourceNotFoundError(SilverTransformationError):
    """Raised when the expected Bronze source table does not exist."""


# COMMAND ----------

# MAGIC %md ## Constants
# MAGIC
# MAGIC Centralized here so every Silver notebook's widgets default to the same values
# MAGIC instead of each one hardcoding "aide"/"bronze"/"silver" independently.

# COMMAND ----------

DEFAULT_CATALOG = "aide"
DEFAULT_BRONZE_SCHEMA = "bronze"
DEFAULT_SILVER_SCHEMA = "silver"

# Dropped from every Silver table: a SQL Server replication artifact with zero
# analytical value, present on nearly every AdventureWorks Bronze table.
TECHNICAL_COLUMNS_TO_DROP = ["rowguid"]

# COMMAND ----------

# MAGIC %md ## Helper functions — execution timing
# MAGIC
# MAGIC Two plain functions, not a decorator or context manager — every notebook already
# MAGIC has a `main()` with a clear start/end, so `start = start_execution_timer()` /
# MAGIC `get_elapsed_seconds(start)` at the two ends of it is all that's needed.

# COMMAND ----------


def start_execution_timer() -> float:
    """Start a lightweight execution timer. Pair with get_elapsed_seconds()."""
    return time.perf_counter()


def get_elapsed_seconds(start_time: float) -> float:
    """Return elapsed seconds since `start_time` (from start_execution_timer())."""
    return time.perf_counter() - start_time


# COMMAND ----------

# MAGIC %md ## Helper functions — read

# COMMAND ----------


def read_bronze_table(
    spark: SparkSession, catalog: str, bronze_schema: str, table_name: str
) -> DataFrame:
    """Read a Bronze table, failing loudly with a clear message if it doesn't exist
    rather than letting Spark's own AnalysisException surface unexplained.
    """
    full_table_name = f"{catalog}.{bronze_schema}.{table_name}"
    try:
        return spark.table(full_table_name)
    except Exception as exc:
        raise BronzeSourceNotFoundError(
            f"Bronze source table '{full_table_name}' not found or not readable. "
            f"Has 01_bronze_ingestion.py been run for this table?"
        ) from exc


# COMMAND ----------

# MAGIC %md ## Helper functions — generic cleaning
# MAGIC
# MAGIC Table-agnostic; every Silver notebook composes these with its own business rules.

# COMMAND ----------


def drop_technical_columns(df: DataFrame, extra_columns: list = None) -> DataFrame:
    """Drop rowguid (always) plus any table-specific columns with no analytical value
    (e.g. a geography blob, a raw XML column) passed via `extra_columns`.

    Silently ignores names that aren't present, so this is safe to call even if a
    column was already removed upstream.
    """
    columns_to_drop = TECHNICAL_COLUMNS_TO_DROP + (extra_columns or [])
    present = [c for c in columns_to_drop if c in df.columns]
    return df.drop(*present) if present else df


def trim_string_columns(df: DataFrame) -> DataFrame:
    """Trim leading/trailing whitespace on every StringType column.

    Introspects the schema rather than requiring each notebook to list its own string
    columns — whitespace hygiene is universal, not table-specific.
    """
    string_columns = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for column in string_columns:
        df = df.withColumn(column, F.trim(F.col(column)))
    return df


def flags_to_boolean(df: DataFrame, flag_columns: list) -> DataFrame:
    """Cast the given INT(0/1) flag columns to BooleanType.

    Safe here in a way it wasn't at Bronze: these columns are already clean, typed
    ints (Bronze already resolved the raw-string-vs-ANSI-cast risk during ingestion),
    so casting int -> boolean carries no parsing ambiguity.
    """
    for column in flag_columns:
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast("boolean"))
    return df


def deduplicate_by_key(
    df: DataFrame, key_columns: list, order_by_column: str = "modified_date"
) -> DataFrame:
    """Keep exactly one row per `key_columns` combination — the most recent by
    `order_by_column` if duplicates exist.

    Bronze's full-overwrite load shouldn't produce duplicates, but Silver should never
    assume that blindly; this is defensive, not a no-op.
    """
    window = Window.partitionBy(*key_columns).orderBy(F.col(order_by_column).desc())
    return (
        df.withColumn("_row_rank", F.row_number().over(window))
        .filter(F.col("_row_rank") == 1)
        .drop("_row_rank")
    )


# COMMAND ----------

# MAGIC %md ## Helper functions — data quality checks
# MAGIC
# MAGIC Every check returns the same shape — `{"check_name", "passed", "detail"}` — so a
# MAGIC notebook can collect them into one list for its execution summary. None of these
# MAGIC raise: a data-quality finding is logged and reported, never a reason to fail the
# MAGIC whole run (the same fail-soft philosophy used throughout this project's Bronze and
# MAGIC AI-analysis notebooks).

# COMMAND ----------


def check_no_nulls(df: DataFrame, columns: list) -> dict:
    """Verify the given columns (typically a primary key or another required field)
    contain zero nulls.
    """
    null_counts = df.select(
        [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in columns]
    ).first()
    offending = {c: null_counts[c] for c in columns if null_counts[c] > 0}
    passed = not offending
    detail = "no nulls found" if passed else f"null counts: {offending}"
    if not passed:
        logger.warning("Data quality check failed — no_nulls(%s): %s", columns, detail)
    return {"check_name": f"no_nulls:{','.join(columns)}", "passed": passed, "detail": detail}


def check_no_duplicate_keys(df: DataFrame, key_columns: list) -> dict:
    """Verify `key_columns` uniquely identifies each row (no duplicate key groups)."""
    total_rows = df.count()
    distinct_keys = df.select(*key_columns).distinct().count()
    duplicate_count = total_rows - distinct_keys
    passed = duplicate_count == 0
    detail = "no duplicate keys" if passed else f"{duplicate_count} duplicate key row(s)"
    if not passed:
        logger.warning(
            "Data quality check failed — no_duplicate_keys(%s): %s", key_columns, detail
        )
    return {
        "check_name": f"no_duplicate_keys:{','.join(key_columns)}",
        "passed": passed,
        "detail": detail,
    }


def check_referential_integrity(
    df: DataFrame, fk_column: str, valid_keys_df: DataFrame, valid_key_column: str
) -> dict:
    """Verify every non-null value of `fk_column` exists in `valid_keys_df`.

    Nulls are excluded deliberately — a nullable FK (e.g. an order with no assigned
    salesperson) is a legitimate business state, not a referential integrity failure.
    """
    valid_keys = valid_keys_df.select(F.col(valid_key_column).alias("_valid_key")).distinct()
    orphan_count = (
        df.filter(F.col(fk_column).isNotNull())
        .join(valid_keys, F.col(fk_column) == F.col("_valid_key"), "left_anti")
        .count()
    )
    passed = orphan_count == 0
    detail = "no orphaned references" if passed else f"{orphan_count} orphaned row(s)"
    if not passed:
        logger.warning(
            "Data quality check failed — referential_integrity(%s): %s", fk_column, detail
        )
    return {
        "check_name": f"referential_integrity:{fk_column}",
        "passed": passed,
        "detail": detail,
    }


def check_row_count_positive(df: DataFrame) -> dict:
    """Sanity check that the table isn't unexpectedly empty."""
    row_count = df.count()
    passed = row_count > 0
    detail = f"{row_count:,} row(s)" if passed else "table is empty"
    if not passed:
        logger.warning("Data quality check failed — row_count_positive: %s", detail)
    return {"check_name": "row_count_positive", "passed": passed, "detail": detail}


# COMMAND ----------

# MAGIC %md ## Helper functions — audit columns, write, summary

# COMMAND ----------


def add_silver_audit_columns(df: DataFrame, source_bronze_table: str) -> DataFrame:
    """Attach standard Silver audit columns for lineage back to the Bronze source."""
    return df.withColumn("silver_load_timestamp", F.current_timestamp()).withColumn(
        "source_bronze_table", F.lit(source_bronze_table)
    )


def write_silver_table(
    spark: SparkSession, df: DataFrame, catalog: str, silver_schema: str, table_name: str
) -> str:
    """Write the DataFrame as a managed Delta table and register it in Unity Catalog.

    Full overwrite, matching Bronze: AdventureWorks is a static, full-refresh source
    with no CDC feed, so there is nothing to MERGE against. A production source with
    real change data capture would use MERGE INTO on the natural key instead — the
    documented upgrade path, not something to build speculatively now.
    """
    full_table_name = f"{catalog}.{silver_schema}.{table_name}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )
    logger.info("Wrote and registered Unity Catalog table '%s'.", full_table_name)
    return full_table_name


def print_silver_execution_summary(
    table_name: str,
    full_table_name: str,
    rows_read: int,
    rows_written: int,
    dq_results: list,
    elapsed_seconds: float,
) -> None:
    """Print a clean, human-readable summary of a Silver transformation run — same
    dashed-box style used by print_execution_summary in 01_bronze_ingestion.py.
    """
    print("=" * 80)
    print("SILVER TRANSFORMATION SUMMARY")
    print("=" * 80)
    print(f"Source Table         : {table_name}")
    print(f"Target Table         : {full_table_name}")
    print(f"Rows Read            : {rows_read:,}")
    print(f"Rows Written         : {rows_written:,}")
    print(f"Elapsed Time (sec)   : {elapsed_seconds:.2f}")
    print("-" * 80)
    print("Data Quality Checks  :")
    for result in dq_results:
        status = "PASS" if result["passed"] else "FAIL"
        print(f"  [{status}] {result['check_name']} — {result['detail']}")
    failed_checks = sum(1 for r in dq_results if not r["passed"])
    print("-" * 80)
    print(f"Status               : {'SUCCESS' if failed_checks == 0 else 'SUCCESS_WITH_WARNINGS'}")
    print("=" * 80)
