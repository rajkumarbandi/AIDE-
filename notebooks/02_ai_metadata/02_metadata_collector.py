# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — AI Metadata Collector
# MAGIC
# MAGIC Scans every table in one or more layers (Bronze, Silver, Gold) of a given catalog and
# MAGIC builds a structured metadata repository in `<catalog>.metadata.table_metadata`.
# MAGIC
# MAGIC **Scope:** deterministic PySpark/Unity Catalog metadata collection. No LLM calls, no
# MAGIC AI-generated descriptions, no data quality judgment calls — that's
# MAGIC `03_metadata_analyzer.py`'s job, once this metadata repository exists for it to
# MAGIC read from. This notebook now covers all three medallion layers (previously Bronze
# MAGIC only) — every layer gets the same metadata enrichment capability.
# MAGIC
# MAGIC **Design:** tables are auto-discovered from `information_schema` — no hardcoded table
# MAGIC names. Onboarding a new table in any layer requires no change here; it's picked up on
# MAGIC the next run automatically. A failure on one table is logged and skipped, never
# MAGIC stopping the rest of the scan.
# MAGIC
# MAGIC **Per-layer overwrite:** a single run only ever replaces the row(s) for the layer(s)
# MAGIC it actually processed (via Delta's `replaceWhere`) — running with `schema=bronze`
# MAGIC does not touch previously-collected Silver/Gold rows, and vice versa.

# COMMAND ----------

# MAGIC %md ## Widgets (job/notebook parameters)

# COMMAND ----------

dbutils.widgets.text("catalog", "aide", "Catalog")
dbutils.widgets.text("schema", "ALL", "Schema (bronze, silver, gold, or ALL)")

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
    DataType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    NumericType,
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
SAMPLE_VALUES_PER_COLUMN = 5
ALL_LAYERS = ["bronze", "silver", "gold"]

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
        # New profiling fields (additive — existing fields/order above are unchanged).
        StructField("min_value", StringType(), nullable=True),  # numeric/date/timestamp only
        StructField("max_value", StringType(), nullable=True),  # numeric/date/timestamp only
        StructField("min_length", IntegerType(), nullable=True),  # string columns only
        StructField("max_length", IntegerType(), nullable=True),  # string columns only
        StructField("average_length", DoubleType(), nullable=True),  # string columns only
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


def resolve_target_schemas(schema_widget_value: str) -> list:
    """Resolve the `schema` widget to a concrete list of layers to scan —
    `ALL` (case-insensitive) expands to all three medallion layers; anything
    else is treated as one specific schema name (not necessarily one of the
    three, so a custom schema name still works for a non-standard layout).
    """
    if schema_widget_value.strip().upper() == "ALL":
        return list(ALL_LAYERS)
    return [schema_widget_value.strip()]


def discover_tables_in_schema(spark: SparkSession, catalog: str, schema: str) -> list:
    """Auto-discover every table (excluding views) in catalog.schema via
    information_schema — no hardcoded table names, and onboarding a new table
    in any layer requires no change here.
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


def is_numeric_or_temporal(data_type: DataType) -> bool:
    """True for column types where min/max bounds are a meaningful statistic."""
    return isinstance(data_type, (NumericType, DateType, TimestampType))


def is_string_type(data_type: DataType) -> bool:
    """True for column types where length statistics are a meaningful statistic."""
    return isinstance(data_type, StringType)


def build_profiling_expressions(schema_fields: list) -> list:
    """Build every aggregate expression needed to profile a table, for use in one
    single `.agg(...)` call (one Spark job, one physical scan of the table).

    Distinct counts use `collect_set` (exact) rather than `countDistinct`/
    `approx_count_distinct`:
      - `approx_count_distinct` was the previous implementation and is the root cause
        of distinct_count exceeding row_count (HyperLogLog is an estimate, not a
        guarantee) — unacceptable once distinct_count feeds primary-key detection.
      - Multiple exact `F.countDistinct(col)` calls over *different* columns in one
        query trigger Spark's "expand" aggregation strategy, which duplicates rows per
        distinct-column combination — expensive for a wide table. `collect_set` is a
        plain (non DISTINCT-marked) aggregate, so many of them combine into a single
        pass without that penalty.
      - `size(collect_set(col))` can never exceed the row count by construction (it is
        the exact count of distinct non-null values), which is the correctness
        guarantee this whole change exists to provide.

    Sample values are taken from the same pass (a second, string-cast `collect_set`
    per column) rather than a separate bounded-row scan — removing what was previously
    an extra full-table read per profiling run.

    Trade-off: `collect_set` ships the full distinct value set back to the driver,
    unlike `approx_count_distinct`'s tiny sketch. Fine at AdventureWorks Bronze scale
    (tens of thousands of rows); a table with a very high-cardinality column (millions
    of near-unique values) would need a hybrid approach — e.g. approximate first, exact
    only when the approximate count is below a safe threshold.
    """
    exprs = [F.count(F.lit(1)).alias("__row_count__")]

    for field_ in schema_fields:
        column = field_.name
        col_expr = F.col(column)

        exprs.append(F.count(col_expr).alias(f"nonnull__{column}"))
        exprs.append(F.size(F.collect_set(col_expr)).alias(f"distinct__{column}"))
        exprs.append(
            F.sort_array(F.collect_set(col_expr.cast("string"))).alias(f"samples__{column}")
        )

        if is_numeric_or_temporal(field_.dataType):
            exprs.append(F.min(col_expr).alias(f"min__{column}"))
            exprs.append(F.max(col_expr).alias(f"max__{column}"))

        if is_string_type(field_.dataType):
            length_expr = F.length(col_expr)
            exprs.append(F.min(length_expr).alias(f"minlen__{column}"))
            exprs.append(F.max(length_expr).alias(f"maxlen__{column}"))
            exprs.append(F.avg(length_expr).alias(f"avglen__{column}"))

    return exprs


def profile_table(df: DataFrame, schema_fields: list) -> dict:
    """Profile every column of a table in exactly one aggregation pass.

    Replaces what was previously three separate full-table operations (row count,
    null/distinct stats, sample-value scan) with a single Spark job.
    """
    agg_row: Row = df.agg(*build_profiling_expressions(schema_fields)).collect()[0]
    row_count = agg_row["__row_count__"]

    columns_stats = {}
    for field_ in schema_fields:
        column = field_.name
        non_null_count = agg_row[f"nonnull__{column}"]
        samples = list(agg_row[f"samples__{column}"] or [])

        stats = {
            "distinct_count": agg_row[f"distinct__{column}"],
            "null_count": row_count - non_null_count,
            "sample_values": samples[:SAMPLE_VALUES_PER_COLUMN],
            "min_value": None,
            "max_value": None,
            "min_length": None,
            "max_length": None,
            "average_length": None,
        }

        if is_numeric_or_temporal(field_.dataType):
            stats["min_value"] = _stringify(agg_row[f"min__{column}"])
            stats["max_value"] = _stringify(agg_row[f"max__{column}"])

        if is_string_type(field_.dataType):
            stats["min_length"] = agg_row[f"minlen__{column}"]
            stats["max_length"] = agg_row[f"maxlen__{column}"]
            avg_length = agg_row[f"avglen__{column}"]
            stats["average_length"] = round(avg_length, 2) if avg_length is not None else None

        columns_stats[column] = stats

    return {"row_count": row_count, "columns": columns_stats}


def _stringify(value) -> Optional[str]:
    """Render a min/max value (of whatever the column's native type is) as a string,
    so heterogeneous column types can share one StringType min_value/max_value field.
    """
    return None if value is None else str(value)


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
    across the whole table (both counts are now exact — see profile_table). Uniqueness
    alone doesn't prove a column is *the* intended key, only that it could be one, so
    this remains a shortlist for human/AI review, not a substitute for an actual
    constraint check — but it is no longer subject to approximation error.
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

    column_base = get_column_base_metadata(df)
    profile = profile_table(df, df.schema.fields)
    row_count = profile["row_count"]
    column_stats = profile["columns"]
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
                    "sample_values": stats.get("sample_values", []),
                    "min_value": stats.get("min_value"),
                    "max_value": stats.get("max_value"),
                    "min_length": stats.get("min_length"),
                    "max_length": stats.get("max_length"),
                    "average_length": stats.get("average_length"),
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


def write_metadata_table(
    spark: SparkSession, rows: list, catalog: str, processed_schemas: list
) -> str:
    """Create the metadata schema if needed and write the metadata repository table.

    Overwrite semantics are now per-layer, not whole-table: this run always reflects
    the *current* state of whichever layer(s) it actually scanned (`processed_schemas`)
    — a table dropped from Bronze should disappear from the metadata repository, but a
    Bronze-only run (schema=bronze) must never wipe out previously-collected Silver/Gold
    rows. Delta's `replaceWhere` gives exactly this: an atomic, selective overwrite
    scoped to `schema_name IN (...)`, leaving every other layer's rows untouched. The
    very first run (table doesn't exist yet) has nothing to selectively preserve, so it
    does a plain full overwrite instead — `replaceWhere` requires the table to already
    exist.
    """
    full_table_name = f"{catalog}.{METADATA_SCHEMA}.{METADATA_TABLE}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{METADATA_SCHEMA}")

    # Rows are wrapped in Row(**dict) rather than passed as plain dicts so field-name
    # matching against TABLE_METADATA_SCHEMA doesn't depend on createDataFrame's
    # (version-dependent) raw-dict handling. metadata_collected_at=None is a valid
    # literal here; the real value is stamped in afterward via current_timestamp().
    df = spark.createDataFrame([Row(**row) for row in rows], schema=TABLE_METADATA_SCHEMA)
    df = df.withColumn("metadata_collected_at", F.current_timestamp())

    writer = df.write.format("delta").mode("overwrite")
    if spark.catalog.tableExists(full_table_name):
        quoted_schemas = ", ".join(f"'{s}'" for s in processed_schemas)
        writer = writer.option("replaceWhere", f"schema_name IN ({quoted_schemas})")
    else:
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(full_table_name)

    logger.info(
        "Wrote and registered Unity Catalog table '%s' (layers: %s).",
        full_table_name,
        ", ".join(processed_schemas),
    )
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
        qualified_name = f"{result['schema_name']}.{result['table_name']}"
        print(f"{qualified_name:<36}{result['status']}")
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
    schema_widget_value = dbutils.widgets.get("schema")
    target_schemas = resolve_target_schemas(schema_widget_value)

    logger.info(
        "Starting AI metadata collection | catalog=%s schemas=%s", catalog, target_schemas
    )

    try:
        collected_rows = []
        results = []
        for schema in target_schemas:
            table_names = discover_tables_in_schema(spark, catalog, schema)
            if not table_names:
                logger.warning("No tables found in '%s.%s'.", catalog, schema)

            for table_name in table_names:
                try:
                    metadata_row = collect_table_metadata(spark, catalog, schema, table_name)
                    collected_rows.append(metadata_row)
                    results.append(
                        {
                            "schema_name": schema,
                            "table_name": table_name,
                            "status": "SUCCESS",
                            "error": None,
                        }
                    )
                    logger.info("Collected metadata for '%s.%s'.", schema, table_name)
                except Exception as exc:
                    results.append(
                        {
                            "schema_name": schema,
                            "table_name": table_name,
                            "status": "FAILED",
                            "error": str(exc),
                        }
                    )
                    logger.error(
                        "Metadata collection failed for '%s.%s': %s", schema, table_name, exc
                    )

        full_table_name = (
            write_metadata_table(spark, collected_rows, catalog, target_schemas)
            if collected_rows
            else None
        )

        elapsed_seconds = time.perf_counter() - start_time
        print_execution_summary(results, full_table_name, elapsed_seconds)

    except Exception:
        logger.exception("Unexpected error during AI metadata collection.")
        raise


# COMMAND ----------

main()
