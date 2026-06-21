# Architecture

## Data flow

```
Finance APIs (yfinance)
        │  poll every N seconds
        ▼
   Kafka topic: finance-raw-stream
        │
        ▼
PySpark Structured Streaming (stream_processor.py)
        │  parse JSON, validate, windowed stats
        ├──────────────► Kafka topic: finance-processed-stream  (live dashboard feed)
        ▼
ADLS Gen2 "silver" container  (Delta/parquet, partitioned by symbol)
        │
        ▼
Databricks notebooks (medallion architecture)
   01_bronze_ingestion   -- raw Kafka -> Delta, untouched
   02_silver_transformation -- dedup, type-cast, quality filters
   03_gold_aggregation   -- daily OHLCV + RSI-14 + volatility + % change
        │
        ▼
Azure Data Factory (pipeline_finance_etl.json)
   1. DatabricksNotebook activity -> runs 03_gold_aggregation on a schedule
   2. Copy activity -> upserts gold Delta table into PostgreSQL
        │
        ▼
PostgreSQL (public.gold_daily_metrics)
        │
        ▼
Power BI  (scheduled refresh, DirectQuery or Import)
```

## Why this shape

**Kafka decouples ingestion from processing.** The producer doesn't know or
care who's consuming -- today it's one Spark job, tomorrow it could also feed
a websocket server for a live front-end, with zero changes to the producer.

**Medallion architecture (bronze/silver/gold) on Delta Lake** gives a clear
contract at each layer: bronze is "exactly what we received," silver is
"clean and deduplicated," gold is "business-ready aggregates." This is the
standard Databricks lakehouse pattern and is what most data engineering job
descriptions are actually asking for.

**ADF as the orchestrator, not the processor.** ADF doesn't transform data
itself here -- it triggers Databricks (which does the heavy lifting) and
moves the final result into Postgres. This mirrors how most real Azure data
platforms split orchestration (ADF) from compute (Databricks/Spark).

**PostgreSQL as the serving layer**, not the lake. Power BI could query
Delta/ADLS directly, but a relational warehouse with proper indexes/views is
usually what BI tools and downstream consumers expect, and it's a cheap,
familiar sink for ADF's Copy activity.

## Local vs. cloud parity

The same `stream_processor.py` and `batch_aggregator.py` scripts run in both
environments:

| Concern | Local (docker-compose) | Azure |
|---|---|---|
| Kafka | confluentinc/cp-kafka container | Event Hubs (Kafka-compatible) or self-hosted Kafka |
| Spark | bitnami/spark container, spark-submit | Databricks job clusters |
| Storage | local filesystem (`./data/silver`, `./data/gold`) | ADLS Gen2 (`abfss://...`) |
| Storage format | parquet | Delta |
| Warehouse load | `postgres/load_to_postgres.py` | ADF Copy activity |
| Orchestration | manual / cron | ADF triggers + Databricks Jobs schedule |

Only the `--output-path` / `--storage-format` CLI args and the linked-service
configs change between the two -- the transformation logic is identical,
which is the point of building it this way: you can develop and demo
everything locally for free, then "lift" the same code into the paid Azure
services for the resume-worthy version.

## Scaling considerations (good talking points for interviews)

- **Kafka partitioning** by symbol allows parallel consumption; this repo
  uses 3 partitions per topic as a starting point.
- **Watermarking** (`withWatermark("event_time", "5 minutes")`) bounds late
  data and prevents unbounded state growth in the windowed aggregation.
- **Checkpointing** on every streaming sink makes the job resumable after a
  restart without reprocessing or dropping data (exactly-once-ish semantics
  combined with idempotent upserts downstream).
- **Partitioning the Delta tables by `symbol`** keeps daily-aggregate scans
  for a single ticker cheap, which is the dominant access pattern for a
  Power BI "symbol deep dive" page.
- **Upsert (not append) into Postgres**, keyed on `(symbol, trade_date)`,
  makes the nightly ADF load idempotent -- safe to re-run after a failure.
