"""
data_ingestion/reddit_producer.py
-----------------------------------
Pulls posts from Reddit PRAW (or falls back to the simulator)
and publishes them to the Kafka raw_reddit topic.
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
logger = logging.getLogger("reddit_producer")


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


def fetch_reddit_live(subreddit: str, limit: int = 25) -> list[dict]:
    """Fetch posts from a real Reddit subreddit via PRAW."""
    try:
        import praw
        reddit = praw.Reddit(
            client_id=settings.REDDIT_CLIENT_ID,
            client_secret=settings.REDDIT_CLIENT_SECRET,
            user_agent=settings.REDDIT_USER_AGENT,
            read_only=True,
        )
        records = []
        for post in reddit.subreddit(subreddit).new(limit=limit):
            records.append({
                "source":       "reddit",
                "id":           post.id,
                "title":        post.title,
                "selftext":     post.selftext[:500],
                "subreddit":    subreddit,
                "score":        post.score,
                "upvote_ratio": post.upvote_ratio,
                "num_comments": post.num_comments,
                "created_at":   datetime.utcfromtimestamp(post.created_utc).isoformat(),
                "url":          post.url,
                "query_term":   "",
                "ingested_at":  datetime.utcnow().isoformat(),
                "simulated":    False,
            })
        return records
    except Exception as e:
        logger.warning("Reddit API error (%s) — using simulator", e)
        return []


def run(once: bool = False):
    """Main Reddit producer loop."""
    simulator = DataSimulator(settings.STOCK_SYMBOLS)
    use_live  = bool(settings.REDDIT_CLIENT_ID and settings.REDDIT_CLIENT_SECRET)

    logger.info(
        "Reddit producer starting. Mode: %s",
        "LIVE API" if use_live else "SIMULATOR",
    )

    producer = create_producer()

    try:
        while True:
            total = 0
            for sub in settings.REDDIT_SUBREDDITS:
                records = fetch_reddit_live(sub) if use_live else []

                if not records:
                    records = [simulator.generate_reddit_post() for _ in range(5)]

                for record in records:
                    producer.send(settings.TOPIC_REDDIT, value=record)
                    total += 1

            producer.flush()
            logger.info("Reddit producer: sent %d messages", total)

            if once:
                break
            time.sleep(settings.INGESTION_INTERVAL * 2)

    except KeyboardInterrupt:
        logger.info("Reddit producer stopped.")
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
