"""
spark_processing/stream_processor.py
--------------------------------------
PySpark Structured Streaming job.

    Kafka (finance-raw-stream)
        -> parse JSON + enforce schema
        -> data-quality filtering (drop nulls / nonsense prices)
        -> compute rolling indicators per symbol (windowed)
        -> write to:
             a) Delta/parquet "silver" path on ADLS (source of truth for Databricks)
             b) Kafka topic finance-processed-stream (for any downstream consumer
                that wants low-latency access, e.g. a live dashboard)

This same script runs identically on a local Spark install (pointed at a
local filesystem path for testing) or inside Databricks (pointed at an
`abfss://` ADLS path) -- only the `--output-path` and `--storage-format`
args change. That portability is the point of using PySpark here instead
of writing Kafka-consumer-specific code twice.

Run locally:
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        spark_processing/stream_processor.py \
        --kafka-broker localhost:9092 \
        --output-path ./data/silver \
        --checkpoint-path ./data/_checkpoints/silver \
        --storage-format parquet

Run on Databricks (cluster already has the Kafka connector + Delta):
    same script, just point --output-path at abfss://silver@<storageacct>.dfs.core.windows.net/finance
    and --storage-format delta
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_json,
    struct,
    window,
    avg,
    stddev,
    max as spark_max,
    min as spark_min,
    count,
)

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from common.schema import TICK_SPARK_SCHEMA, KAFKA_TOPIC_RAW, KAFKA_TOPIC_PROCESSED  # noqa: E402


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--kafka-broker", default="localhost:9092")
    p.add_argument("--output-path", default="./data/silver")
    p.add_argument("--checkpoint-path", default="./data/_checkpoints/silver")
    p.add_argument("--storage-format", default="parquet", choices=["parquet", "delta"])
    p.add_argument("--trigger-seconds", type=int, default=15)
    return p.parse_args()


def build_spark_session(app_name: str = "FinanceStreamProcessor") -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    return builder.getOrCreate()


def main():
    args = parse_args()
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Read raw bytes from Kafka
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_broker)
        .option("subscribe", KAFKA_TOPIC_RAW)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parse JSON value column against our shared schema
    parsed = (
        raw_stream.selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp")
        .select(from_json(col("json_value"), TICK_SPARK_SCHEMA).alias("data"), col("kafka_timestamp"))
        .select("data.*", "kafka_timestamp")
    )

    # 3. Data quality filter: drop ticks with non-positive price/volume or missing symbol
    clean = parsed.filter(
        (col("symbol").isNotNull())
        & (col("price") > 0)
        & (col("volume") >= 0)
    ).withWatermark("event_time", "5 minutes")

    # 4a. Sink 1: write cleaned, enriched events to the silver layer (Delta/parquet on ADLS)
    silver_query = (
        clean.writeStream.format(args.storage_format)
        .option("path", args.output_path)
        .option("checkpointLocation", args.checkpoint_path)
        .partitionBy("symbol")
        .outputMode("append")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    # 4b. Sink 2: rolling 1-minute window stats per symbol, republished to Kafka
    #     for any low-latency dashboard consumer.
    windowed_stats = (
        clean.groupBy(window(col("event_time"), "1 minute"), col("symbol"))
        .agg(
            avg("price").alias("avg_price"),
            spark_max("price").alias("max_price"),
            spark_min("price").alias("min_price"),
            stddev("price").alias("volatility"),
            count("price").alias("tick_count"),
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "avg_price",
            "max_price",
            "min_price",
            "volatility",
            "tick_count",
        )
    )

    processed_query = (
        windowed_stats.select(
            col("symbol").alias("key"),
            to_json(
                struct(
                    "symbol",
                    "window_start",
                    "window_end",
                    "avg_price",
                    "max_price",
                    "min_price",
                    "volatility",
                    "tick_count",
                )
            ).alias("value"),
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_broker)
        .option("topic", KAFKA_TOPIC_PROCESSED)
        .option("checkpointLocation", args.checkpoint_path + "_kafka")
        .outputMode("update")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
