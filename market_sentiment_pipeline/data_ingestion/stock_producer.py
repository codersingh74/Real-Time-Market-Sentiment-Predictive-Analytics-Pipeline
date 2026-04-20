"""
data_ingestion/stock_producer.py
----------------------------------
Fetches OHLCV stock quotes from Alpha Vantage (or simulator)
and publishes them to the Kafka raw_stocks topic.
"""

import json
import time
import logging
import argparse
import requests
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
from config.settings import settings
from data_ingestion.simulator import DataSimulator

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("stock_producer")

AV_BASE = "https://www.alphavantage.co/query"


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


def fetch_quote_live(symbol: str) -> dict | None:
    """Fetch a real-time quote from Alpha Vantage."""
    try:
        resp = requests.get(
            AV_BASE,
            params={"function": "GLOBAL_QUOTE", "symbol": symbol,
                    "apikey": settings.ALPHA_VANTAGE_KEY},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json().get("Global Quote", {})
        if not data or not data.get("05. price"):
            return None

        return {
            "source":      "stocks",
            "symbol":      symbol,
            "price":       float(data.get("05. price", 0)),
            "open":        float(data.get("02. open", 0)),
            "high":        float(data.get("03. high", 0)),
            "low":         float(data.get("04. low", 0)),
            "volume":      int(data.get("06. volume", 0)),
            "prev_close":  float(data.get("08. previous close", 0)),
            "change_pct":  float(
                data.get("10. change percent", "0%").replace("%", "")
            ),
            "trade_date":  data.get("07. latest trading day", ""),
            "ingested_at": datetime.utcnow().isoformat(),
            "simulated":   False,
        }
    except Exception as e:
        logger.warning("Alpha Vantage error for %s (%s) — using simulator", symbol, e)
        return None


def run(once: bool = False):
    """Main stock producer loop."""
    simulator = DataSimulator(settings.STOCK_SYMBOLS)
    use_live  = settings.ALPHA_VANTAGE_KEY not in ("", "demo", "your_alpha_vantage_key_here")

    logger.info(
        "Stock producer starting. Mode: %s",
        "LIVE Alpha Vantage" if use_live else "SIMULATOR",
    )

    producer = create_producer()

    try:
        while True:
            for symbol in settings.STOCK_SYMBOLS:
                record = fetch_quote_live(symbol) if use_live else None
                if not record:
                    record = simulator.generate_stock_quote(symbol)

                producer.send(settings.TOPIC_STOCKS, value=record)
                logger.info("Stock quote sent: %s @ $%.2f", symbol, record["price"])

                if use_live:
                    time.sleep(15)   # respect Alpha Vantage free-tier rate limit

            producer.flush()
            logger.info("Stock producer cycle complete.")

            if once:
                break
            time.sleep(300)   # 5-minute interval

    except KeyboardInterrupt:
        logger.info("Stock producer stopped.")
    except KafkaError as e:
        logger.error("Kafka error: %s", e)
        raise
    finally:
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    run(once=args.once)
