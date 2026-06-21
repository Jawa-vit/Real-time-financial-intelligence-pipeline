"""
common/schema.py
-----------------
Single source of truth for the message schema flowing through the pipeline.

Used by:
  - data_ingestion/finance_api_producer.py  (validates outgoing JSON)
  - spark_processing/stream_processor.py    (parses incoming Kafka JSON)
  - databricks/notebooks/*.py               (bronze ingestion schema)

Keeping one schema definition avoids the classic "producer and consumer
silently drift apart" bug in streaming pipelines.
"""

# The PySpark import is optional: this module is shared by the lightweight
# producer/tests (no PySpark dependency) as well as the Spark jobs (which
# do have it). Guard the import so `from common.schema import
# TICK_JSON_SCHEMA` works even where PySpark isn't installed.
try:
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        DoubleType,
        LongType,
        TimestampType,
    )

    _PYSPARK_AVAILABLE = True
except ImportError:
    _PYSPARK_AVAILABLE = False

# JSON Schema (plain dict) -- used for lightweight validation in the
# Python producer, where we don't want a PySpark dependency.
TICK_JSON_SCHEMA = {
    "type": "object",
    "required": ["symbol", "price", "volume", "event_time", "source"],
    "properties": {
        "symbol": {"type": "string"},
        "price": {"type": "number"},
        "open": {"type": "number"},
        "day_high": {"type": "number"},
        "day_low": {"type": "number"},
        "prev_close": {"type": "number"},
        "volume": {"type": "number"},
        "currency": {"type": "string"},
        "event_time": {"type": "string"},  # ISO-8601
        "source": {"type": "string"},
    },
}

# PySpark StructType -- used by Structured Streaming to parse the Kafka
# message value (which arrives as JSON-encoded bytes). Only constructed if
# PySpark is actually installed in this environment.
if _PYSPARK_AVAILABLE:
    TICK_SPARK_SCHEMA = StructType(
        [
            StructField("symbol", StringType(), nullable=False),
            StructField("price", DoubleType(), nullable=False),
            StructField("open", DoubleType(), nullable=True),
            StructField("day_high", DoubleType(), nullable=True),
            StructField("day_low", DoubleType(), nullable=True),
            StructField("prev_close", DoubleType(), nullable=True),
            StructField("volume", LongType(), nullable=True),
            StructField("currency", StringType(), nullable=True),
            StructField("event_time", TimestampType(), nullable=False),
            StructField("source", StringType(), nullable=False),
        ]
    )
else:
    TICK_SPARK_SCHEMA = None  # raises clearly only if accidentally used without PySpark installed

KAFKA_TOPIC_RAW = "finance-raw-stream"
KAFKA_TOPIC_PROCESSED = "finance-processed-stream"
