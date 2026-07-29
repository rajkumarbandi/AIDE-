# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — AI Metadata Collector
# MAGIC
# MAGIC Scans every Bronze table in a given catalog/schema and builds a structured metadata
# MAGIC repository in `<catalog>.metadata.table_metadata`.
# MAGIC
# MAGIC **Scope (Phase 1 — metadata collection only):** deterministic PySpark/Unity Catalog
# MAGIC metadata collection. No LLM calls, no AI-generated descriptions, no data quality
# MAGIC analysis, no Silver/Gold layers, no business rules — those come later, once this
# MAGIC metadata repository exists for them to read from.
# MAGIC
# MAGIC **Design:** tables are auto-discovered from `information_schema` — no hardcoded table
# MAGIC names. Onboarding a new Bronze table requires no change here; it's picked up on the
# MAGIC next run automatically. A failure on one table is logged and skipped, never stopping
# MAGIC the rest of the scan.

# COMMAND ----------

# MAGIC %md ## Widgets (job/notebook parameters)

# COMMAND ----------

dbutils.widgets.text("catalog", "aide", "Catalog")
dbutils.widgets.text("schema", "bronze", "Schema")

# COMMAND ----------

# MAGIC %md ## Imports & logging

# COMMAND ----------

import logging
import time
from typing import Optional

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("aide.metadata_collector")

# COMMAND ----------

# MAGIC %md ## Constants & output schema
# MAGIC
# MAGIC The metadata repository's catalog follows the `catalog` widget (so dev/staging/prod
# MAGIC runs each write to their own catalog's metadata schema); the schema and table name are
# MAGIC fixed — this is meant to be *the* one well-known metadata repository, not something
# MAGIC that varies per run.

# COMMAND ----------

METADATA_SCHEMA = "metadata"
METADATA_TABLE = "table_metadata"
SAMPLE_SCAN_SIZE = 200
SAMPLE_VALUES_PER_COLUMN = 5

COLUMN_METADATA_SCHEMA = StructType(
    [
        StructField("ordinal_position", IntegerType(), nullable=False),
        StructField("column_name", StringType(), nullable=False),
        StructField("data_type", StringType(), nullable=False),
        StructField("is_nullable", BooleanType(), nullable=False),
        StructField("distinct_count", LongType(), nullable=True),
        StructField("null_count", LongType(), nullable=True),
        StructField("null_percentage", DoubleType(), nullable=True),
        StructField("sample_values", ArrayType(StringType()), nullable=True),
    ]
)

TABLE_METADATA_SCHEMA = StructType(
    [
        StructField("catalog_name", StringType(), nullable=False),
        StructField("schema_name", StringType(), nullable=False),
        StructField("table_name", StringType(), nullable=False),
        StructField("full_table_name", StringType(), nullable=False),
        StructField("column_count", IntegerType(), nullable=False),
        StructField("row_count", LongType(), nullable=True),
        StructField("table_size_bytes", LongType(), nullable=True),
        StructField("partition_columns", ArrayType(StringType()), nullable=True),
        StructField("primary_key_candidates", ArrayType(StringType()), nullable=True),
        StructField("table_properties", MapType(StringType(), StringType()), nullable=True),
        StructField("delta_properties", MapType(StringType(), StringType()), nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("last_modified_at", TimestampType(), nullable=True),
        StructField("columns", ArrayType(COLUMN_METADATA_SCHEMA), nullable=False),
        # Seeded as None in collect_table_metadata and overwritten via current_timestamp()
        # in write_metadata_table — must be nullable=True so createDataFrame's schema
        # validation accepts that placeholder before the overwrite happens.
        StructField("metadata_collected_at", TimestampType(), nullable=True),
    ]
)

# COMMAND ----------

# MAGIC %md ## Helper functions — table discovery

# COMMAND ----------


def discover_bronze_tables(spark: SparkSession, catalog: str, schema: str) -> list:
    """Auto-discover every table (excluding views) in catalog.schema via
    information_schema — no hardcoded table names, and onboarding a new Bronze table
    requires no change here.
    """
    query = f"""
        SELECT table_name
        FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}' AND table_type != 'VIEW'
        ORDER BY table_name
    """
    return [row.table_name for row in spark.sql(query).collect()]


# COMMAND ----------

# MAGIC %md ## Helper functions — per-table metadata collection

# COMMAND ----------


def get_column_base_metadata(df: DataFrame) -> list:
    """Column name, order, data type, and nullability, straight from the DataFrame schema."""
    return [
        {
            "ordinal_position": position,
            "column_name": field_.name,
            "data_type": field_.dataType.simpleString(),
            "is_nullable": field_.nullable,
        }
        for position, field_ in enumerate(df.schema.fields, start=1)
    ]


def compute_column_statistics(df: DataFrame, columns: list) -> dict:
    """Null count and approximate distinct count for every column, in a single
    aggregation pass. `approx_count_distinct` (HyperLogLog) is used instead of an exact
    countDistinct per column — the standard choice for profiling, since running many
    exact distinct aggregates together in one pass scales poorly, while the approximate
    version stays single-pass regardless of column count, at negligible accuracy cost.
    """
    agg_exprs = []
    for column in columns:
        agg_exprs.append(F.approx_count_distinct(F.col(column)).alias(f"distinct__{column}"))
        agg_exprs.append(
            F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(f"nulls__{column}")
        )
    result: Row = df.agg(*agg_exprs).collect()[0]

    stats = {}
    for column in columns:
        stats[column] = {
            "distinct_count": result[f"distinct__{column}"],
            "null_count": result[f"nulls__{column}"],
        }
    return stats


def compute_sample_values(df: DataFrame, columns: list) -> dict:
    """Up to SAMPLE_VALUES_PER_COLUMN distinct, non-null example values per column,
    drawn from one bounded sample of the table rather than a separate scan per column.
    These are illustrative examples, not the most-frequent values — a true top-N-by-
    frequency would require a groupBy/count per column, which isn't worth the extra
    scans for a "give me some example values" use case.
    """
    sample_rows = df.limit(SAMPLE_SCAN_SIZE).collect()
    sample_values = {column: [] for column in columns}

    for row in sample_rows:
        row_dict = row.asDict()
        for column in columns:
            value = row_dict.get(column)
            if value is None:
                continue
            existing = sample_values[column]
            str_value = str(value)
            if str_value not in existing and len(existing) < SAMPLE_VALUES_PER_COLUMN:
                existing.append(str_value)

    return sample_values


def get_delta_detail(spark: SparkSession, full_table_name: str) -> dict:
    """Table size, partition columns, timestamps, and Delta protocol info, all from a
    single DESCRIBE DETAIL call. Missing keys default to None rather than raising —
    DESCRIBE DETAIL's column set has grown across Delta versions, and a table shouldn't
    fail metadata collection just because one optional field isn't present.
    """
    detail = spark.sql(f"DESCRIBE DETAIL {full_table_name}").collect()[0].asDict()
    return {
        "size_in_bytes": detail.get("sizeInBytes"),
        "partition_columns": list(detail.get("partitionColumns") or []),
        "created_at": detail.get("createdAt"),
        "last_modified": detail.get("lastModified"),
        "properties": dict(detail.get("properties") or {}),
        "format": detail.get("format"),
        "min_reader_version": detail.get("minReaderVersion"),
        "min_writer_version": detail.get("minWriterVersion"),
    }


def identify_primary_key_candidates(
    column_base: list, column_stats: dict, row_count: Optional[int]
) -> list:
    """A column is a primary key candidate if every value is non-null and distinct
    across the whole table. Approximate distinct counts make this a heuristic, not a
    guarantee — good enough to shortlist candidates for human/AI review later, not a
    substitute for an actual constraint check.
    """
    if not row_count:
        return []

    candidates = []
    for column in column_base:
        stats = column_stats.get(column["column_name"], {})
        if stats.get("null_count") == 0 and stats.get("distinct_count") == row_count:
            candidates.append(column["column_name"])
    return candidates


def collect_table_metadata(
    spark: SparkSession, catalog: str, schema: str, table_name: str
) -> dict:
    """Run the full metadata collection pipeline for exactly one table and return a
    dict matching TABLE_METADATA_SCHEMA. Raises on failure — the caller (main loop)
    decides whether to log and skip.
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"
    df = spark.table(full_table_name)
    columns = df.columns

    row_count = df.count()
    column_base = get_column_base_metadata(df)
    column_stats = compute_column_statistics(df, columns)
    sample_values = compute_sample_values(df, columns)
    delta_detail = get_delta_detail(spark, full_table_name)

    column_records = []
    for col_meta in column_base:
        name = col_meta["column_name"]
        stats = column_stats.get(name, {})
        null_count = stats.get("null_count")
        has_null_count = null_count is not None
        null_percentage = (
            round(null_count / row_count * 100, 2) if row_count and has_null_count else None
        )
        column_records.append(
            Row(
                **{
                    **col_meta,
                    "distinct_count": stats.get("distinct_count"),
                    "null_count": null_count,
                    "null_percentage": null_percentage,
                    "sample_values": sample_values.get(name, []),
                }
            )
        )

    delta_properties = {
        "format": str(delta_detail.get("format")),
        "min_reader_version": str(delta_detail.get("min_reader_version")),
        "min_writer_version": str(delta_detail.get("min_writer_version")),
    }

    return {
        "catalog_name": catalog,
        "schema_name": schema,
        "table_name": table_name,
        "full_table_name": full_table_name,
        "column_count": len(column_base),
        "row_count": row_count,
        "table_size_bytes": delta_detail.get("size_in_bytes"),
        "partition_columns": delta_detail.get("partition_columns", []),
        "primary_key_candidates": identify_primary_key_candidates(
            column_base, column_stats, row_count
        ),
        "table_properties": delta_detail.get("properties", {}),
        "delta_properties": delta_properties,
        "created_at": delta_detail.get("created_at"),
        "last_modified_at": delta_detail.get("last_modified"),
        "columns": column_records,
        "metadata_collected_at": None,  # filled in at write time, see write_metadata_table
    }


# COMMAND ----------

# MAGIC %md ## Helper functions — write & summarize

# COMMAND ----------


def write_metadata_table(spark: SparkSession, rows: list, catalog: str) -> str:
    """Create the metadata schema if needed and overwrite the metadata repository table.

    Overwrite semantics: this notebook always reflects the *current* state of the
    Bronze layer on each run, not an append-only history — a table dropped from Bronze
    should disappear from the metadata repository too, not linger as a stale row.
    """
    full_table_name = f"{catalog}.{METADATA_SCHEMA}.{METADATA_TABLE}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{METADATA_SCHEMA}")

    # Rows are wrapped in Row(**dict) rather than passed as plain dicts so field-name
    # matching against TABLE_METADATA_SCHEMA doesn't depend on createDataFrame's
    # (version-dependent) raw-dict handling. metadata_collected_at=None is a valid
    # literal here; the real value is stamped in afterward via current_timestamp().
    df = spark.createDataFrame([Row(**row) for row in rows], schema=TABLE_METADATA_SCHEMA)
    df = df.withColumn("metadata_collected_at", F.current_timestamp())
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )
    logger.info("Wrote and registered Unity Catalog table '%s'.", full_table_name)
    return full_table_name


def print_execution_summary(
    results: list, full_table_name: Optional[str], elapsed_seconds: float
) -> None:
    """Print a per-table status table, aggregate counts, and failure details — the
    same reporting pattern used by the Bronze ingestion framework's ALL mode.
    """
    succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
    failed = len(results) - succeeded

    print("-" * 60)
    print("AI Metadata Collection Summary")
    print("-" * 60)
    for result in results:
        print(f"{result['table_name']:<28}{result['status']}")
    print("-" * 60)
    print(f"Metadata Table      : {full_table_name}")
    print(f"Total Tables        : {len(results)}")
    print(f"Succeeded           : {succeeded}")
    print(f"Failed              : {failed}")
    print(f"Execution Time      : {elapsed_seconds:.2f} sec")
    print("-" * 60)

    failures = [r for r in results if r["status"] == "FAILED"]
    if failures:
        print("Failure Details")
        print("-" * 60)
        for result in failures:
            print(f"Table Name         : {result['table_name']}")
            print(f"Exception Message  : {result['error']}")
            print("-" * 60)


# COMMAND ----------

# MAGIC %md ## main()

# COMMAND ----------


def main() -> None:
    start_time = time.perf_counter()

    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")

    logger.info("Starting AI metadata collection | catalog=%s schema=%s", catalog, schema)

    try:
        table_names = discover_bronze_tables(spark, catalog, schema)
        if not table_names:
            logger.warning("No tables found in '%s.%s'.", catalog, schema)

        collected_rows = []
        results = []
        for table_name in table_names:
            try:
                metadata_row = collect_table_metadata(spark, catalog, schema, table_name)
                collected_rows.append(metadata_row)
                results.append({"table_name": table_name, "status": "SUCCESS", "error": None})
                logger.info("Collected metadata for '%s'.", table_name)
            except Exception as exc:
                results.append({"table_name": table_name, "status": "FAILED", "error": str(exc)})
                logger.error("Metadata collection failed for '%s': %s", table_name, exc)

        full_table_name = (
            write_metadata_table(spark, collected_rows, catalog) if collected_rows else None
        )

        elapsed_seconds = time.perf_counter() - start_time
        print_execution_summary(results, full_table_name, elapsed_seconds)

    except Exception:
        logger.exception("Unexpected error during AI metadata collection.")
        raise


# COMMAND ----------

main()
