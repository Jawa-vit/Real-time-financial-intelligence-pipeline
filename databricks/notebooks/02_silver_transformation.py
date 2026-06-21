# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Transformation
# MAGIC Reads the bronze Delta table, applies data-quality rules, dedupes, and
# MAGIC casts/cleans into a typed, trustworthy "silver" table. This is the layer
# MAGIC analysts and the gold-layer job should treat as ground truth.

# COMMAND ----------

dbutils.widgets.text("storage_account", "<your-storage-account>", "ADLS Storage Account")

storage_account = dbutils.widgets.get("storage_account")
bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/finance_ticks"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/finance_ticks"

# COMMAND ----------

from pyspark.sql.functions import col

bronze_df = spark.read.format("delta").load(bronze_path)

silver_df = (
    bronze_df.filter(col("price") > 0)
    .filter(col("volume") >= 0)
    .filter(col("symbol").isNotNull())
    # de-duplicate on (symbol, event_time, source) in case the producer or
    # Kafka redelivers a message (at-least-once delivery semantics)
    .dropDuplicates(["symbol", "event_time", "source"])
    .select(
        "symbol",
        "price",
        "open",
        "day_high",
        "day_low",
        "prev_close",
        "volume",
        "currency",
        "event_time",
        "source",
        "_ingested_at",
    )
)

# COMMAND ----------

(
    silver_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("symbol")
    .save(silver_path)
)

display(silver_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS finance.silver_ticks
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@<account>.dfs.core.windows.net/finance_ticks'
# MAGIC ```
# MAGIC
# MAGIC In production this notebook would run as a **streaming** job (readStream
# MAGIC from bronze with `foreachBatch` for the dedup/merge logic via `MERGE INTO`)
# MAGIC rather than a full overwrite batch read -- the batch version here is kept
# MAGIC simple and is what the nightly Databricks Job in this repo actually calls.
