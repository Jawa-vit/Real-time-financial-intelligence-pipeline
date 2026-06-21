# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Aggregation
# MAGIC Reads the silver table, computes daily OHLCV + technical indicators
# MAGIC (RSI-14, volatility, day-over-day % change) and writes the gold Delta
# MAGIC table. ADF then copies this table into PostgreSQL for Power BI.
# MAGIC
# MAGIC This notebook re-uses `spark_processing/batch_aggregator.py` logic so the
# MAGIC local-Spark version and the Databricks version never drift apart.

# COMMAND ----------

dbutils.widgets.text("storage_account", "<your-storage-account>", "ADLS Storage Account")
storage_account = dbutils.widgets.get("storage_account")

silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/finance_ticks"
gold_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/finance_daily"

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/finance-pipeline")
from spark_processing.batch_aggregator import compute_rsi  # reuse the RSI window logic

from pyspark.sql import Window
from pyspark.sql.functions import (
    col, to_date, first, last, max as smax, min as smin,
    avg, stddev, sum as ssum, lag, when, lit,
)

silver_df = spark.read.format("delta").load(silver_path)
silver_df = silver_df.withColumn("trade_date", to_date(col("event_time")))

daily = (
    silver_df.groupBy("symbol", "trade_date")
    .agg(
        first("price").alias("open_price"),
        smax("price").alias("high_price"),
        smin("price").alias("low_price"),
        last("price").alias("close_price"),
        ssum("volume").alias("total_volume"),
        avg("price").alias("avg_price"),
        stddev("price").alias("volatility"),
    )
)

sym_w = Window.partitionBy("symbol").orderBy("trade_date")
daily = daily.withColumn("prev_close", lag("close_price", 1).over(sym_w))
daily = daily.withColumn(
    "pct_change",
    when(col("prev_close").isNotNull() & (col("prev_close") != 0),
         ((col("close_price") - col("prev_close")) / col("prev_close")) * 100),
)

gold_df = compute_rsi(daily)

# COMMAND ----------

(
    gold_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("symbol")
    .save(gold_path)
)

display(gold_df.orderBy(col("trade_date").desc()).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS finance.gold_daily_metrics
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@<account>.dfs.core.windows.net/finance_daily'
# MAGIC ```
# MAGIC Next stop: **Azure Data Factory** copies this table into PostgreSQL
# MAGIC (`azure/adf/pipeline_finance_etl.json`), which Power BI then connects to.
