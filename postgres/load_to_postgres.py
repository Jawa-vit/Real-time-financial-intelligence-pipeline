"""
postgres/load_to_postgres.py
-------------------------------
Loads the gold-layer parquet output (produced locally by
spark_processing/batch_aggregator.py) into PostgreSQL.

In the full Azure deployment, Azure Data Factory's Copy activity does this
job directly from the Databricks Delta table (see
azure/adf/pipeline_finance_etl.json) -- no Python script needed there.

This script exists so you can run and demo the **entire pipeline locally**
end-to-end with `docker compose up`, without needing an actual ADF/Databricks
subscription, while keeping the exact same target schema.

Usage:
    python postgres/load_to_postgres.py --input-path ./data/gold
"""

import argparse
import os
import sys
import logging

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("postgres-loader")

UPSERT_SQL = """
INSERT INTO public.gold_daily_metrics (
    symbol, trade_date, open_price, high_price, low_price, close_price,
    total_volume, avg_price, volatility, pct_change, rsi_14
) VALUES (
    :symbol, :trade_date, :open_price, :high_price, :low_price, :close_price,
    :total_volume, :avg_price, :volatility, :pct_change, :rsi_14
)
ON CONFLICT (symbol, trade_date) DO UPDATE SET
    open_price   = EXCLUDED.open_price,
    high_price   = EXCLUDED.high_price,
    low_price    = EXCLUDED.low_price,
    close_price  = EXCLUDED.close_price,
    total_volume = EXCLUDED.total_volume,
    avg_price    = EXCLUDED.avg_price,
    volatility   = EXCLUDED.volatility,
    pct_change   = EXCLUDED.pct_change,
    rsi_14       = EXCLUDED.rsi_14,
    loaded_at    = now();
"""


def get_engine():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "finance")
    user = os.getenv("POSTGRES_USER", "finance_user")
    password = os.getenv("POSTGRES_PASSWORD", "finance_pass")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


def load(input_path: str):
    if not os.path.exists(input_path):
        logger.error("Input path %s does not exist. Run spark_processing/batch_aggregator.py first.", input_path)
        sys.exit(1)

    df = pd.read_parquet(input_path)
    logger.info("Read %d gold rows from %s", len(df), input_path)

    # Normalize column subset/order to match the SQL statement exactly.
    cols = [
        "symbol", "trade_date", "open_price", "high_price", "low_price",
        "close_price", "total_volume", "avg_price", "volatility", "pct_change", "rsi_14",
    ]
    df = df[cols]

    engine = get_engine()
    records = df.to_dict(orient="records")

    with engine.begin() as conn:
        for record in records:
            conn.execute(text(UPSERT_SQL), record)

    logger.info("Upserted %d rows into public.gold_daily_metrics", len(records))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", default="./data/gold")
    args = parser.parse_args()
    load(args.input_path)
