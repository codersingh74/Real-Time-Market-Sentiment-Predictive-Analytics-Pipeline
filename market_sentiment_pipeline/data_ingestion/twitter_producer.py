"""
data_ingestion/twitter_producer.py
------------------------------------
Pulls tweets from the Twitter v2 API (or falls back to the simulator)
and publishes them to the Kafka raw_tweets topic.
"""

import json
import time
import logging
import argparse
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
logger = logging.getLogger("twitter_producer")


def create_producer() -> KafkaProducer:
    """Create and return a configured Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=settings.KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        retry_backoff_ms=500,
    )


def fetch_tweets_live(query: str, max_results: int = 10) -> list[dict]:
    """Fetch real tweets from the Twitter v2 API."""
    try:
        import tweepy
        client = tweepy.Client(
            bearer_token=settings.TWITTER_BEARER_TOKEN,
            wait_on_rate_limit=True,
        )
        full_query = f"{query} lang:en -is:retweet"
        response   = client.search_recent_tweets(
            query=full_query,
            max_results=max_results,
            tweet_fields=["created_at", "author_id", "public_metrics", "text"],
        )
        if not response.data:
            return []

        records = []
        for tweet in response.data:
            records.append({
                "source":     "twitter",
                "id":         str(tweet.id),
                "text":       tweet.text,
                "author_id":  str(tweet.author_id),
                "created_at": str(tweet.created_at),
                "likes":      tweet.public_metrics.get("like_count", 0),
                "retweets":   tweet.public_metrics.get("retweet_count", 0),
                "query_term": query,
                "subreddit":  "",
                "ingested_at": datetime.utcnow().isoformat(),
                "simulated":  False,
            })
        return records
    except Exception as e:
        logger.warning("Twitter API error (%s) — using simulator", e)
        return []


def run(once: bool = False):
    """
    Main producer loop.
    Attempts live Twitter API first; falls back to simulator automatically.
    """
    simulator = DataSimulator(settings.STOCK_SYMBOLS)
    use_live  = bool(settings.TWITTER_BEARER_TOKEN)

    logger.info(
        "Twitter producer starting. Mode: %s",
        "LIVE API" if use_live else "SIMULATOR",
    )

    producer = create_producer()

    try:
        while True:
            total = 0
            for term in settings.SEARCH_TERMS:
                records = fetch_tweets_live(term) if use_live else []

                if not records:
                    # Fall back to 5 simulated tweets per term
                    records = [simulator.generate_tweet() for _ in range(5)]

                for record in records:
                    producer.send(settings.TOPIC_TWEETS, value=record)
                    total += 1

            producer.flush()
            logger.info("Twitter producer: sent %d messages", total)

            if once:
                break
            time.sleep(settings.INGESTION_INTERVAL)

    except KeyboardInterrupt:
        logger.info("Twitter producer stopped by user.")
    except KafkaError as e:
        logger.error("Kafka error: %s", e)
        raise
    finally:
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Twitter/Simulator Kafka Producer")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    args = parser.parse_args()
    run(once=args.once)
