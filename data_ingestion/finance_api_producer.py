"""
data_ingestion/finance_api_producer.py
----------------------------------------
Polls live market data and publishes each tick as a JSON message to Kafka.
"""

import os
import sys
import json
import time
import logging
import random

from datetime import datetime, timezone

import yfinance as yf
from jsonschema import validate, ValidationError
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from common.schema import TICK_JSON_SCHEMA, KAFKA_TOPIC_RAW  # noqa: E402

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("finance-producer")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
SYMBOLS = [
    s.strip().upper()
    for s in os.getenv(
        "STOCK_SYMBOLS",
        "AAPL,MSFT,GOOGL,TSLA,AMZN"
    ).split(",")
]
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "15"))


def build_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=50,
            )
            logger.info(
                "Connected to Kafka broker at %s",
                KAFKA_BROKER
            )
            return producer

        except NoBrokersAvailable:
            logger.warning(
                "Kafka not ready (attempt %d/%d). Retrying in %ds...",
                attempt,
                retries,
                delay,
            )
            time.sleep(delay)

    raise RuntimeError(
        f"Could not connect to Kafka broker at {KAFKA_BROKER}"
    )


def fetch_quotes(symbols):
    ticks = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for symbol in symbols:
        price = round(random.uniform(100, 500), 2)

        tick = {
            "symbol": symbol,
            "price": price,
            "open": round(price - random.uniform(0, 5), 2),
            "day_high": round(price + random.uniform(0, 10), 2),
            "day_low": round(price - random.uniform(0, 10), 2),
            "prev_close": round(price - random.uniform(-5, 5), 2),
            "volume": random.randint(1000, 100000),
            "currency": "USD",
            "event_time": now_iso,
            "source": "simulator",
        }

        ticks.append(tick)

    return ticks


def run():
    producer = build_producer()

    logger.info(
        "Starting producer loop | symbols=%s | interval=%ss | topic=%s",
        SYMBOLS,
        POLL_INTERVAL,
        KAFKA_TOPIC_RAW,
    )

    try:
        while True:
            cycle_start = time.time()

            ticks = fetch_quotes(SYMBOLS)

            for tick in ticks:
                try:
                    validate(
                        instance=tick,
                        schema=TICK_JSON_SCHEMA,
                    )

                except ValidationError as ve:
                    logger.error(
                        "Schema validation failed for %s: %s",
                        tick.get("symbol"),
                        ve.message,
                    )
                    continue

                producer.send(
                    KAFKA_TOPIC_RAW,
                    key=tick["symbol"],
                    value=tick,
                )

                logger.info(
                    "Published %-6s price=%.2f vol=%s",
                    tick["symbol"],
                    tick["price"],
                    tick["volume"],
                )

            producer.flush()

            elapsed = time.time() - cycle_start

            time.sleep(
                max(
                    0,
                    POLL_INTERVAL - elapsed
                )
            )

    except KeyboardInterrupt:
        logger.info("Shutting down producer...")

    finally:
        producer.close()


if __name__ == "__main__":
    run()