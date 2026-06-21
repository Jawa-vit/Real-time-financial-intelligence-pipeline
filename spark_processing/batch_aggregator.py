"""
spark_processing/batch_aggregator.py
---------------------------------------
Batch job (run nightly via Databricks Job / ADF trigger) that reads the
silver layer (cleaned tick data) and produces the "gold" layer: daily
OHLCV summaries plus technical indicators per symbol, ready to be loaded
into PostgreSQL and visualized in Power BI.

This is intentionally the script that ADF's Databricks-notebook activity
(see azure/adf/pipeline_finance_etl.json) triggers each day.

Run locally:
    spark-submit spark_processing/batch_aggregator.py \
        --input-path ./data/silver \
        --output-path ./data/gold \
        --storage-format parquet

Run on Databricks: identical, with abfss:// paths and --storage-format delta.
"""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    to_date,
    first,
    last,
    max as spark_max,
    min as spark_min,
    avg,
    stddev,
    sum as spark_sum,
    lag,
    when,
    lit,
)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input-path", default="./data/silver")
    p.add_argument("--output-path", default="./data/gold")
    p.add_argument("--storage-format", default="parquet", choices=["parquet", "delta"])
    return p.parse_args()


def compute_rsi(df, price_col="close_price", period=14):
    """
    Wilder's RSI computed with Spark window functions, partitioned by symbol
    and ordered by date. Mirrors common.indicators.relative_strength_index,
    but vectorized across the whole gold dataset instead of looping in Python.
    """
    w = Window.partitionBy("symbol").orderBy("trade_date")

    delta = col(price_col) - lag(price_col, 1).over(w)
    gain = when(delta > 0, delta).otherwise(lit(0.0))
    loss = when(delta < 0, -delta).otherwise(lit(0.0))

    df = df.withColumn("_gain", gain).withColumn("_loss", loss)

    roll_w = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-(period - 1), 0)
    df = df.withColumn("_avg_gain", avg("_gain").over(roll_w))
    df = df.withColumn("_avg_loss", avg("_loss").over(roll_w))

    df = df.withColumn(
        "rsi_14",
        when(col("_avg_loss") == 0, lit(100.0)).otherwise(
            100 - (100 / (1 + (col("_avg_gain") / col("_avg_loss"))))
        ),
    )
    return df.drop("_gain", "_loss", "_avg_gain", "_avg_loss")


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("FinanceBatchAggregator").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.format(args.storage_format).load(args.input_path)
    silver = silver.withColumn("trade_date", to_date(col("event_time")))

    daily_w = Window.partitionBy("symbol", "trade_date").orderBy("event_time")

    daily = (
        silver.groupBy("symbol", "trade_date")
        .agg(
            first("price").alias("open_price"),
            spark_max("price").alias("high_price"),
            spark_min("price").alias("low_price"),
            last("price").alias("close_price"),
            spark_sum("volume").alias("total_volume"),
            avg("price").alias("avg_price"),
            stddev("price").alias("volatility"),
        )
    )

    # Day-over-day percent change
    sym_w = Window.partitionBy("symbol").orderBy("trade_date")
    daily = daily.withColumn("prev_close", lag("close_price", 1).over(sym_w))
    daily = daily.withColumn(
        "pct_change",
        when(col("prev_close").isNotNull() & (col("prev_close") != 0),
             ((col("close_price") - col("prev_close")) / col("prev_close")) * 100),
    )

    gold = compute_rsi(daily)

    (
        gold.write.format(args.storage_format)
        .mode("overwrite")
        .partitionBy("symbol")
        .save(args.output_path)
    )

    print(f"Wrote gold layer to {args.output_path} ({args.storage_format})")


if __name__ == "__main__":
    main()
