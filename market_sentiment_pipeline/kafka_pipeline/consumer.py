"""
kafka_pipeline/consumer.py
----------------------------
Kafka consumer — reads from all three topics, cleans each record,
runs VADER sentiment analysis, and writes enriched rows to PostgreSQL.
"""

import json
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from config.settings import settings
from sentiment_analysis.cleaner import clean_tweet, clean_reddit_post, clean_stock_quote
from sentiment_analysis.analyzer import enrich_social_record
from database.db import init_db, upsert_social_post, upsert_stock_quote

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("kafka_consumer")

TOPICS = [settings.TOPIC_TWEETS, settings.TOPIC_REDDIT, settings.TOPIC_STOCKS]


def create_consumer() -> KafkaConsumer:
    """Build and return a configured Kafka consumer."""
    return KafkaConsumer(
        *TOPICS,
        bootstrap_servers=settings.KAFKA_BROKER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="market-sentiment-consumer-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=10_000,   # exit iterator after 10s of silence
    )


def process_message(topic: str, raw: dict) -> bool:
    """
    Route a raw Kafka message to the appropriate cleaner + writer.
    Returns True on success, False on skip/error.
    """
    try:
        if topic in (settings.TOPIC_TWEETS, settings.TOPIC_REDDIT):
            if topic == settings.TOPIC_TWEETS:
                cleaned = clean_tweet(raw)
            else:
                cleaned = clean_reddit_post(raw)

            if cleaned is None:
                return False

            enriched = enrich_social_record(cleaned)
            upsert_social_post(enriched)

        elif topic == settings.TOPIC_STOCKS:
            cleaned = clean_stock_quote(raw)
            if cleaned is None:
                return False
            upsert_stock_quote(cleaned)

        return True

    except Exception as e:
        logger.warning("Error processing message from %s: %s | raw: %.100s", topic, e, str(raw))
        return False


def run():
    """Main consumer loop — runs until keyboard interrupt or timeout."""
    logger.info("Initialising database...")
    init_db()

    logger.info("Starting Kafka consumer on topics: %s", TOPICS)
    consumer = create_consumer()

    processed = 0
    errors    = 0

    try:
        for message in consumer:
            success = process_message(message.topic, message.value)
            if success:
                processed += 1
            else:
                errors += 1

            if processed % 50 == 0 and processed > 0:
                logger.info(
                    "Consumer progress: %d processed, %d errors",
                    processed, errors,
                )
    except KafkaError as e:
        logger.error("Kafka error: %s", e)
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    finally:
        consumer.close()
        logger.info(
            "Consumer shut down. Total: %d processed, %d errors",
            processed, errors,
        )


if __name__ == "__main__":
    run()
