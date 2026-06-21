# Real-Time Financial Intelligence Data Pipeline

An end-to-end data engineering pipeline that streams live stock quotes through
Kafka, processes them with PySpark, lands them in a medallion-architecture
lakehouse on Azure Data Lake Storage via Databricks, orchestrates the nightly
batch with Azure Data Factory, serves the results from PostgreSQL, and
visualizes everything in Power BI.

```
Finance APIs → Kafka → PySpark → Databricks (ADLS, bronze/silver/gold)
                                       → Azure Data Factory → PostgreSQL → Power BI
```

See [`docs/architecture.md`](docs/architecture.md) for the full data-flow
diagram and design rationale.

## Repo layout

```
common/                Shared schema + indicator math (used by producer, Spark, Databricks)
data_ingestion/         Kafka producer that polls live market data
kafka/                  Topic creation script
spark_processing/      PySpark streaming + batch jobs (runs locally OR on Databricks)
databricks/             Databricks notebooks (bronze/silver/gold) + job config
azure/                  ADF pipeline/dataset/linked-service JSON + infra provisioning script
postgres/               Schema + loader script
powerbi/                Dashboard setup guide
tests/                  Unit tests for indicator math + schema validation
.github/workflows/      CI/CD: lint, test, build/push Docker image, trigger Databricks, deploy ADF
docker-compose.yml      Full local stack: Kafka, Postgres, producer, Spark
```

## Option A: Run it entirely locally (free, no Azure account needed)

This proves the pipeline end-to-end on your laptop using the real Kafka +
Spark + Postgres stack, just pointed at local disk instead of ADLS.

```bash
git clone <this-repo>
cd financial-intelligence-pipeline
cp .env.example .env          # defaults work as-is

# 1. Start Kafka, Postgres, the producer, and a Spark container
docker compose up -d

# 2. Confirm topics were auto-created (or run kafka/create_topics.sh)
docker exec finance-kafka kafka-topics --list --bootstrap-server localhost:9092

# 3. Watch the producer publishing live quotes
docker logs -f finance-producer

# 4. Run the streaming job (parses Kafka -> writes silver parquet locally)
docker exec -it finance-spark spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark_processing/stream_processor.py \
  --kafka-broker kafka:29092 \
  --output-path /app/data/silver \
  --checkpoint-path /app/data/_checkpoints/silver \
  --storage-format parquet
# Let this run for a few minutes (or a few market-hours) to accumulate ticks,
# then stop it with Ctrl+C.

# 5. Run the batch aggregator (silver -> gold daily metrics)
docker exec -it finance-spark spark-submit \
  spark_processing/batch_aggregator.py \
  --input-path /app/data/silver \
  --output-path /app/data/gold \
  --storage-format parquet

# 6. Load gold metrics into Postgres
pip install -r postgres/requirements.txt --break-system-packages
python postgres/load_to_postgres.py --input-path ./data/gold

# 7. Check it landed
docker exec -it finance-postgres psql -U finance_user -d finance \
  -c "SELECT * FROM public.gold_daily_metrics ORDER BY trade_date DESC LIMIT 10;"
```

Then point Power BI Desktop at `localhost:5432` / database `finance` (see
[`powerbi/README_powerbi_setup.md`](powerbi/README_powerbi_setup.md)).

> Note: with only a few minutes of tick data the daily aggregates will be
> thin (one partial "day"). For a meaningful demo, let the producer run
> during market hours for a day or two, or seed `data/silver` with a backfill
> script using `yfinance.download()` for historical data — happy to add that
> if you want a richer demo dataset.

## Option B: Deploy the full Azure version

```bash
# 1. Provision everything (ADLS, Postgres Flexible Server, ADF, Databricks, ACR, Key Vault)
az login
chmod +x azure/deploy_infra.sh
./azure/deploy_infra.sh
# Note the printed resource names -- you'll need them below.

# 2. Run the Postgres schema against the new Azure Postgres server
psql "host=<server>.postgres.database.azure.com dbname=finance user=<admin> sslmode=require" \
  -f postgres/schema.sql

# 3. In the Databricks workspace UI:
#    - Connect a Databricks Repo to this git repo
#    - Generate a Personal Access Token, store it in Key Vault as 'databricks-pat-token'
#    - Import databricks/databricks_job_config.json as a new Job
#      (Workflows -> Jobs -> Create Job -> Edit as JSON / use Databricks CLI:
#         databricks jobs create --json @databricks/databricks_job_config.json)

# 4. In Azure Data Factory Studio:
#    - Import the linked services from azure/adf/linked_services.json
#    - Import the datasets from azure/adf/datasets.json
#    - Import the pipeline from azure/adf/pipeline_finance_etl.json
#    - Add a Schedule trigger (e.g. daily 02:00 UTC)

# 5. Configure GitHub Actions secrets (Settings -> Secrets -> Actions):
#    ACR_LOGIN_SERVER, ACR_USERNAME, ACR_PASSWORD,
#    DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_JOB_ID,
#    AZURE_CREDENTIALS, AZURE_RESOURCE_GROUP, ADF_NAME
#    Push to main -> CI/CD builds the producer image, pushes to ACR,
#    triggers the Databricks job, and deploys the ADF pipeline.

# 6. Deploy the producer container (Azure Container Instances, AKS, or
#    Container Apps -- pick whichever matches what you want on your resume)
#    pointed at an Event Hub's Kafka-compatible endpoint as KAFKA_BROKER.
```

## Running tests

```bash
pip install -r data_ingestion/requirements.txt -r postgres/requirements.txt pytest --break-system-packages
pytest tests/ -v
```

16 unit tests cover the technical-indicator math (SMA, volatility, % change,
RSI-14) and the Kafka message schema validation -- the parts of the pipeline
that are pure logic and worth regression-testing independent of any
Kafka/Spark/Postgres infrastructure being up.

## Talking points for your resume / interviews

- Built a **decoupled streaming architecture**: Kafka producer/consumer
  separation means ingestion and processing scale and fail independently.
- Implemented a **medallion lakehouse** (bronze/silver/gold) on Delta Lake,
  the standard pattern for Databricks-based data platforms.
- Wrote **portable PySpark jobs** that run identically against local
  filesystem/parquet for dev and ADLS/Delta for production -- only config
  changes, not code.
- Used **Structured Streaming watermarking and checkpointing** for
  fault-tolerant, resumable stream processing.
- Orchestrated cross-service workflows with **Azure Data Factory**
  (Databricks-notebook + Copy activities, parameterized, with failure alerting).
- Built **idempotent upserts** into PostgreSQL keyed on natural business
  keys, so the nightly load is safely re-runnable.
- Set up **CI/CD with GitHub Actions**: lint/test gate, Docker image build
  and push to ACR, automated Databricks job trigger and ADF pipeline deploy.
- Designed the **Power BI semantic layer** (views, DAX measures) for
  RSI-based overbought/oversold flags and top-movers tracking.

## Extending this project

A few directions that make good "v2" additions if you want to keep building:
a historical backfill script using `yfinance.download()` so gold metrics have
real depth on day one; an Airflow or Databricks Workflows-based alerting rule
when RSI crosses 70/30; a FastAPI service reading `finance-processed-stream`
for a live (sub-minute) dashboard instead of Power BI's nightly refresh; or
swapping yfinance for a paid websocket feed (Polygon.io, IEX Cloud) for true
tick-level real-time data.
