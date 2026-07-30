# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — AI Metadata Analyzer (Proof of Concept)
# MAGIC
# MAGIC Validates the end-to-end flow:
# MAGIC
# MAGIC ```
# MAGIC Metadata Table  ->  Prompt Generation  ->  Gemini  ->  AI Analysis
# MAGIC ```
# MAGIC
# MAGIC Reads one table's row from `<metadata_catalog>.metadata.table_metadata` (written by
# MAGIC `02_metadata_collector.py`), converts it into a prompt, sends it to Gemini via the
# MAGIC shared client, and prints the AI's analysis.
# MAGIC
# MAGIC **Scope: proof of concept only.** No persistence, no analysis tables, no retries/
# MAGIC evaluation — just validating that the pieces connect correctly end-to-end.
# MAGIC
# MAGIC **Note:** the metadata sent to Gemini includes real column sample values. That's
# MAGIC acceptable here because AdventureWorks is synthetic demo data with no real PII —
# MAGIC this would need a redaction/guardrail step before pointing this flow at real data.

# COMMAND ----------

# MAGIC %md ## Import the shared Gemini client
# MAGIC
# MAGIC Brings `generate_content(...)` into scope from `notebooks/00_shared/01_gemini_client.py`.

# COMMAND ----------

# MAGIC %run "../00_shared/01_gemini_client"

# COMMAND ----------

# MAGIC %md ## Widgets (job/notebook parameters)

# COMMAND ----------

dbutils.widgets.text("metadata_catalog", "aide", "Metadata Catalog")
dbutils.widgets.text("target_table_name", "customer", "Target Table Name")

# COMMAND ----------

# MAGIC %md ## Imports & logging

# COMMAND ----------

import json
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("aide.metadata_analyzer_poc")

# COMMAND ----------

# MAGIC %md ## Exceptions

# COMMAND ----------


class MetadataNotFoundError(Exception):
    """Raised when no metadata row exists for the requested table."""


# COMMAND ----------

# MAGIC %md ## Helper functions

# COMMAND ----------


def fetch_table_metadata(
    spark: SparkSession, metadata_catalog: str, target_table_name: str
) -> dict:
    """Read the single metadata row for `target_table_name` from the metadata
    repository and return it as a plain (JSON-serializable-ready) nested dict.
    """
    full_table_name = f"{metadata_catalog}.metadata.table_metadata"
    matches = (
        spark.table(full_table_name)
        .filter(F.col("table_name") == target_table_name)
        .collect()
    )

    if not matches:
        raise MetadataNotFoundError(
            f"No metadata found for table_name='{target_table_name}' in '{full_table_name}'. "
            f"Has 02_metadata_collector.py been run for this table?"
        )
    if len(matches) > 1:
        logger.warning(
            "Found %d metadata rows for table_name='%s'; using the first.",
            len(matches),
            target_table_name,
        )

    return matches[0].asDict(recursive=True)


def build_prompt(metadata: dict) -> str:
    """Render the metadata dict as indented JSON and embed it in the analysis prompt.

    `default=str` covers the handful of non-JSON-native values in the metadata
    (e.g. created_at/last_modified_at timestamps).
    """
    metadata_json = json.dumps(metadata, indent=2, default=str)

    return f"""You are a Senior Data Architect.

Analyze the following table metadata.

Provide:
1. Business Purpose
2. Table Description
3. Column Descriptions
4. Candidate Primary Keys
5. Data Quality Observations
6. Possible Relationships
7. Suggested Improvements

Only use the supplied metadata. Do not hallucinate.

Metadata:
{metadata_json}
"""


# COMMAND ----------

# MAGIC %md ## main()

# COMMAND ----------


def main() -> None:
    metadata_catalog = dbutils.widgets.get("metadata_catalog")
    target_table_name = dbutils.widgets.get("target_table_name")

    logger.info(
        "Starting AI metadata analysis POC | catalog=%s table=%s",
        metadata_catalog,
        target_table_name,
    )

    try:
        metadata = fetch_table_metadata(spark, metadata_catalog, target_table_name)
        prompt = build_prompt(metadata)
        print(f"--- Generated prompt ({len(prompt):,} characters) ---")
        print(prompt)

        response_text = generate_content(prompt)

        print("--- Gemini AI Analysis ---")
        print(response_text)

    except Exception:
        logger.exception("AI metadata analysis POC failed for table '%s'.", target_table_name)
        raise


# COMMAND ----------

main()
