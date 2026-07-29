# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingestion Framework
# MAGIC
# MAGIC Generic, metadata-driven ingestion of a single AdventureWorks source file into a
# MAGIC Unity Catalog Bronze Delta table.
# MAGIC
# MAGIC **Scope (Sprint 1):** Bronze ingestion only. No AI, no metadata generation, no data
# MAGIC quality analysis, no release impact analysis — those are later sprints.
# MAGIC
# MAGIC **Design:** table-specific knowledge (delimiter, column names/types) lives in
# MAGIC `config/adventureworks_tables.yaml`, not in this notebook. Onboarding a new table is a
# MAGIC config change; this notebook's code does not change. Tables with no registry entry are
# MAGIC still ingested, via a generic tab-delimited + automatic-schema-inference fallback.
# MAGIC
# MAGIC **Validated for:** Customer, Person, Address, Product, SalesOrderHeader, SalesOrderDetail.

# COMMAND ----------

# MAGIC %md ## Widgets (job/notebook parameters)

# COMMAND ----------

dbutils.widgets.text("catalog", "aide_dev", "Catalog")
dbutils.widgets.text("schema", "bronze", "Schema")
dbutils.widgets.text("dataset_folder", "/Volumes/aide_dev/raw/adventureworks", "Dataset Folder")
dbutils.widgets.text("file_name", "Customer.csv", "File Name")

# COMMAND ----------

# MAGIC %md ## Imports & logging

# COMMAND ----------

import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Optional

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, DecimalType, IntegerType, StringType, TimestampType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("aide.bronze_ingestion")

# COMMAND ----------

# MAGIC %md ## Exceptions

# COMMAND ----------


class BronzeIngestionError(Exception):
    """Base error for failures in the Bronze ingestion framework."""


class TableRegistryError(BronzeIngestionError):
    """Raised when the table registry config cannot be loaded or parsed."""


class SourceFileNotFoundError(BronzeIngestionError):
    """Raised when the requested source file does not exist at the resolved path."""


# COMMAND ----------

# MAGIC %md ## Configuration model

# COMMAND ----------


@dataclass(frozen=True)
class ColumnSpec:
    """A single column's target name and logical type, as declared in the registry."""

    name: str
    type: str


@dataclass(frozen=True)
class TableConfig:
    """Resolved ingestion configuration for one source file.

    `columns` is None for files with no registry entry — the generic fallback path
    (tab-delimited, automatic schema inference, positional column names) is used instead.
    """

    source_file: str
    target_table: str
    delimiter: str = "\t"
    row_terminator_marker: Optional[str] = None
    columns: Optional[list] = field(default=None)


# COMMAND ----------

# MAGIC %md ## Helper functions — registry loading & path resolution

# COMMAND ----------


def get_repo_root() -> str:
    """Resolve the repo root directory so config files can be located regardless of
    which workspace path this notebook is checked out under.

    Databricks Repos sets the notebook's working directory to its own directory in
    the repo checkout, so the repo root is reliably two levels up from
    notebooks/01_development/.
    """
    return os.path.abspath(os.path.join(os.getcwd(), "..", ".."))


def load_table_registry(config_path: str) -> dict:
    """Load the AdventureWorks table registry from YAML.

    Raises TableRegistryError if the file is missing or malformed — ingestion should
    fail loudly rather than silently falling back when the registry itself is broken.
    """
    try:
        with open(config_path, "r", encoding="utf-8") as handle:
            registry = yaml.safe_load(handle)
    except FileNotFoundError as exc:
        raise TableRegistryError(f"Table registry not found at '{config_path}'.") from exc
    except yaml.YAMLError as exc:
        raise TableRegistryError(f"Table registry at '{config_path}' is not valid YAML.") from exc

    if not registry or "tables" not in registry:
        raise TableRegistryError(f"Table registry at '{config_path}' has no 'tables' section.")
    return registry["tables"]


def resolve_table_config(registry: dict, file_name: str) -> TableConfig:
    """Match a source file name to a registry entry (case-insensitive), or fall back
    to a generic config for source files that haven't been onboarded into the
    registry yet.
    """
    stem = os.path.splitext(file_name)[0].lower()
    for entry in registry.values():
        if entry["source_file"].lower() == file_name.lower():
            columns = [ColumnSpec(**col) for col in entry["columns"]]
            return TableConfig(
                source_file=entry["source_file"],
                target_table=entry["target_table"],
                delimiter=entry["delimiter"],
                row_terminator_marker=entry.get("row_terminator_marker"),
                columns=columns,
            )

    logger.warning(
        "No registry entry for '%s'. Falling back to generic tab-delimited ingestion "
        "with automatic schema inference and positional column names.",
        file_name,
    )
    return TableConfig(
        source_file=file_name,
        target_table=standardize_column_name(stem),
        delimiter="\t",
        row_terminator_marker=None,
        columns=None,
    )


# COMMAND ----------

# MAGIC %md ## Helper functions — column naming & type mapping

# COMMAND ----------


def standardize_column_name(raw_name: str) -> str:
    """Clean an arbitrary column name into a safe, consistent snake_case identifier.

    Applies: trim whitespace, CamelCase -> snake_case, spaces/special characters -> "_",
    collapse repeated underscores, lowercase, and guard against a leading digit or an
    empty result. Applied uniformly regardless of whether the name came from a config
    file, a real CSV header, or a generated positional placeholder.
    """
    name = raw_name.strip()
    name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)  # camelCase / PascalCase boundary
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)  # non-alphanumeric -> underscore
    name = re.sub(r"_+", "_", name).strip("_").lower()
    if not name:
        name = "unnamed_column"
    if name[0].isdigit():
        name = f"col_{name}"
    return name


def resolve_spark_type(logical_type: str) -> DataType:
    """Map a registry logical type string to a Spark SQL DataType.

    Supports "int", "string", "timestamp", and parameterized "decimal(p,s)". Unknown
    type strings default to StringType — the safest, non-lossy fallback for a Bronze
    layer, where raw fidelity matters more than early type strictness.
    """
    logical_type = logical_type.strip().lower()
    decimal_match = re.fullmatch(r"decimal\((\d+),\s*(\d+)\)", logical_type)
    if decimal_match:
        precision, scale = (int(g) for g in decimal_match.groups())
        return DecimalType(precision, scale)

    type_map = {
        "int": IntegerType(),
        "string": StringType(),
        "timestamp": TimestampType(),
    }
    if logical_type not in type_map:
        logger.warning("Unknown logical type '%s'; defaulting to string.", logical_type)
    return type_map.get(logical_type, StringType())


# COMMAND ----------

# MAGIC %md ## Helper functions — reading source files

# COMMAND ----------


def read_delimited_source(
    spark: SparkSession, file_path: str, delimiter: str, infer_types: bool
) -> DataFrame:
    """Read a single-character-delimited source file.

    `infer_types` controls whether Spark's automatic type inference runs:
      - False (registry-backed tables): read as strings; the registry's verified
        column types take precedence over guesswork — AdventureWorks' export quirks
        (money columns, embedded computed columns, GUIDs) make blind inference an
        unnecessary risk for tables we already know the schema of.
      - True (tables with no registry entry): let Spark infer real types, since no
        verified schema exists to apply instead — this is the "generic, works for any
        table" fallback path.
    Quoting/escaping and multiline records are handled defensively even though the
    six validated AdventureWorks files don't currently require either.
    """
    return (
        spark.read.format("csv")
        .option("header", "false")
        .option("sep", delimiter)
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .option("inferSchema", str(infer_types).lower())
        .load(file_path)
    )


def read_custom_delimited_source(
    spark: SparkSession, file_path: str, delimiter: str, row_terminator_marker: Optional[str]
) -> DataFrame:
    """Read a source file whose delimiter is more than one character.

    Spark's CSV reader only supports single-character separators, so this path reads
    each physical line as raw text, strips the row-terminator marker if one precedes
    the newline (as AdventureWorks' Person.csv does), and splits on the literal
    (regex-escaped) delimiter. Needed for Person.csv, whose BULK INSERT definition
    uses FIELDTERMINATOR='+|' / ROWTERMINATOR='&|\\n'.
    """
    lines = spark.read.text(file_path)
    if row_terminator_marker:
        marker_pattern = re.escape(row_terminator_marker) + "$"
        lines = lines.withColumn("value", F.regexp_replace(F.col("value"), marker_pattern, ""))

    split_pattern = re.escape(delimiter)
    parts = F.split(F.col("value"), split_pattern)
    field_count = lines.select(F.max(F.size(parts))).first()[0] or 0

    select_exprs = [parts.getItem(i).alias(f"_c{i}") for i in range(field_count)]
    return lines.select(*select_exprs)


def read_source_file(spark: SparkSession, file_path: str, table_config: TableConfig) -> DataFrame:
    """Dispatch to the appropriate reader based on the resolved delimiter's length."""
    if len(table_config.delimiter) == 1:
        infer_types = table_config.columns is None
        return read_delimited_source(spark, file_path, table_config.delimiter, infer_types)
    return read_custom_delimited_source(
        spark, file_path, table_config.delimiter, table_config.row_terminator_marker
    )


# COMMAND ----------

# MAGIC %md ## Helper functions — applying names, types & audit columns

# COMMAND ----------


def apply_schema(raw_df: DataFrame, table_config: TableConfig) -> DataFrame:
    """Rename positional columns (_c0.._cN) to their registry names and cast to their
    registry types. Falls back to generic positional naming + left-as-string typing
    when the table has no registry entry, or when the raw file's column count doesn't
    match what the registry expects (logged loudly rather than silently truncating —
    a mismatch here usually signals upstream schema drift, which is exactly the kind
    of thing the future Release Impact Analyzer will care about).
    """
    raw_column_count = len(raw_df.columns)

    if table_config.columns is None:
        logger.info("Applying generic schema inference for '%s'.", table_config.source_file)
        renamed = [
            F.col(f"_c{i}").alias(standardize_column_name(f"col_{i + 1}"))
            for i in range(raw_column_count)
        ]
        return raw_df.select(*renamed)

    expected_count = len(table_config.columns)
    if raw_column_count != expected_count:
        logger.warning(
            "Column count mismatch for '%s': registry expects %d, source file has %d. "
            "Falling back to generic positional naming for this run.",
            table_config.source_file,
            expected_count,
            raw_column_count,
        )
        renamed = [
            F.col(f"_c{i}").alias(standardize_column_name(f"col_{i + 1}"))
            for i in range(raw_column_count)
        ]
        return raw_df.select(*renamed)

    select_exprs = []
    for i, col_spec in enumerate(table_config.columns):
        raw_value = F.col(f"_c{i}")
        # Nullify blank fields before casting: AdventureWorks exports NULL as an empty
        # string, and casting "" directly to a numeric/timestamp type is not something
        # to rely on across Spark ANSI-mode settings (it can raise instead of returning
        # NULL) — normalize explicitly rather than depending on that behavior.
        null_safe_value = F.when(F.trim(raw_value) == "", F.lit(None)).otherwise(raw_value)
        select_exprs.append(
            null_safe_value.cast(resolve_spark_type(col_spec.type)).alias(
                standardize_column_name(col_spec.name)
            )
        )
    return raw_df.select(*select_exprs)


def add_audit_columns(df: DataFrame, source_file_path: str) -> DataFrame:
    """Attach standard Bronze audit columns for lineage and re-ingestion tracking."""
    return (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("load_date", F.current_date())
        .withColumn("source_file", F.lit(source_file_path))
    )


# COMMAND ----------

# MAGIC %md ## Helper functions — write, register, validate

# COMMAND ----------


def write_bronze_table(df: DataFrame, catalog: str, schema: str, table_name: str) -> str:
    """Write the DataFrame as a managed Delta table and register it in Unity Catalog.

    Uses overwrite semantics: AdventureWorks source tables are static snapshots, so a
    full-overwrite reload is the correct, idempotent default for repeated runs — this
    is not an append/incremental pipeline. `overwriteSchema` is enabled so a corrected
    registry entry (e.g. a fixed column type) is reflected on the next run rather than
    failing on a schema mismatch against a previous run.
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )
    logger.info("Wrote and registered Unity Catalog table '%s'.", full_table_name)
    return full_table_name


def validate_ingestion(spark: SparkSession, full_table_name: str) -> dict:
    """Re-read the table from Unity Catalog (not the in-memory DataFrame) to confirm
    it was actually written and is queryable, then print the required validation facts.
    """
    table_df = spark.table(full_table_name)
    row_count = table_df.count()
    column_count = len(table_df.columns)

    print(f"Table Name    : {full_table_name}")
    print(f"Row Count     : {row_count:,}")
    print(f"Column Count  : {column_count}")
    print("Schema        :")
    table_df.printSchema()

    return {"row_count": row_count, "column_count": column_count}


def print_execution_summary(
    file_name: str, full_table_name: str, stats: dict, elapsed_seconds: float
) -> None:
    """Print a clean, human-readable summary of the ingestion run."""
    print("=" * 80)
    print("BRONZE INGESTION SUMMARY")
    print("=" * 80)
    print(f"Source File          : {file_name}")
    print(f"Target Table         : {full_table_name}")
    print(f"Row Count            : {stats['row_count']:,}")
    print(f"Column Count         : {stats['column_count']}")
    print(f"Elapsed Time (sec)   : {elapsed_seconds:.2f}")
    print("Status               : SUCCESS")
    print("=" * 80)


# COMMAND ----------

# MAGIC %md ## main()

# COMMAND ----------


def main() -> None:
    start_time = time.perf_counter()

    catalog = dbutils.widgets.get("catalog")
    schema_name = dbutils.widgets.get("schema")
    dataset_folder = dbutils.widgets.get("dataset_folder")
    file_name = dbutils.widgets.get("file_name")

    logger.info(
        "Starting Bronze ingestion | catalog=%s schema=%s dataset_folder=%s file_name=%s",
        catalog,
        schema_name,
        dataset_folder,
        file_name,
    )

    try:
        repo_root = get_repo_root()
        registry_path = os.path.join(repo_root, "config", "adventureworks_tables.yaml")
        registry = load_table_registry(registry_path)
        table_config = resolve_table_config(registry, file_name)

        source_path = f"{dataset_folder.rstrip('/')}/{file_name}"
        if not _path_exists(source_path):
            raise SourceFileNotFoundError(f"Source file not found: '{source_path}'.")

        raw_df = read_source_file(spark, source_path, table_config)
        typed_df = apply_schema(raw_df, table_config)
        bronze_df = add_audit_columns(typed_df, source_path)

        full_table_name = write_bronze_table(
            bronze_df, catalog, schema_name, table_config.target_table
        )

        stats = validate_ingestion(spark, full_table_name)
        display(spark.table(full_table_name).limit(10))

        elapsed_seconds = time.perf_counter() - start_time
        print_execution_summary(file_name, full_table_name, stats, elapsed_seconds)

    except BronzeIngestionError:
        logger.exception("Bronze ingestion failed for '%s'.", file_name)
        raise
    except Exception:
        logger.exception("Unexpected error during Bronze ingestion for '%s'.", file_name)
        raise


def _path_exists(path: str) -> bool:
    """Check source file existence via dbutils.fs, which works across DBFS, Unity
    Catalog Volumes, and mounted object storage alike.
    """
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


# COMMAND ----------

main()
