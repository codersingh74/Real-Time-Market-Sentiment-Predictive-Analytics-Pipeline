"""
main.py
--------
Single entry-point to run any part of the pipeline from the command line.

Usage examples:
    python main.py ingest          # run all producers once
    python main.py consume         # start the Kafka consumer
    python main.py dashboard       # launch Streamlit dashboard
    python main.py demo            # full demo: simulate data → consume → show summary
    python main.py setup           # initialise DB schema only
"""

import argparse
import logging
import sys
import os

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import settings

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("main")


def cmd_setup():
    """Initialise the PostgreSQL schema."""
    from database.db import init_db
    logger.info("Initialising database schema...")
    init_db()
    logger.info("Done.")


def cmd_ingest(once: bool = True):
    """Run all three producers for one cycle."""
    import subprocess, time

    scripts = [
        "data_ingestion/twitter_producer.py",
        "data_ingestion/reddit_producer.py",
        "data_ingestion/stock_producer.py",
    ]
    args = ["--once"] if once else []

    for script in scripts:
        logger.info("Running %s...", script)
        result = subprocess.run(
            [sys.executable, script] + args,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        if result.returncode != 0:
            logger.warning("%s exited with code %d", script, result.returncode)
        time.sleep(1)


def cmd_consume():
    """Start the Kafka consumer loop."""
    from kafka_pipeline.consumer import run
    logger.info("Starting Kafka consumer...")
    run()


def cmd_dashboard():
    """Launch the Streamlit dashboard."""
    import subprocess
    dashboard_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "dashboard", "app.py"
    )
    logger.info("Launching Streamlit dashboard at http://localhost:8501")
    subprocess.run([sys.executable, "-m", "streamlit", "run", dashboard_path])


def cmd_demo():
    """
    Full demo workflow:
    1. Simulate data directly into Kafka (no API keys needed)
    2. Consume Kafka → clean → NLP → PostgreSQL
    3. Print a sentiment summary
    """
    import json, time
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
    from data_ingestion.simulator import DataSimulator
    from database.db import init_db, fetch_sentiment_summary

    logger.info("=== DEMO MODE ===")
    logger.info("Step 1: Initialise database")
    init_db()

    logger.info("Step 2: Simulate %d records → Kafka", len(settings.STOCK_SYMBOLS) * 7)
    try:
        producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        sim = DataSimulator(settings.STOCK_SYMBOLS)
        total = 0
        for record in sim.stream_mixed():
            if record["source"].startswith("twitter"):
                topic = settings.TOPIC_TWEETS
            elif record["source"].startswith("reddit"):
                topic = settings.TOPIC_REDDIT
            else:
                topic = settings.TOPIC_STOCKS
            producer.send(topic, value=record)
            total += 1
        producer.flush()
        producer.close()
        logger.info("Sent %d simulated records to Kafka", total)
    except NoBrokersAvailable:
        logger.warning("Kafka not reachable — skipping Kafka step. Inserting directly to DB.")
        # Insert directly for demo without Kafka
        from sentiment_analysis.cleaner import clean_tweet, clean_reddit_post, clean_stock_quote
        from sentiment_analysis.analyzer import enrich_social_record
        from database.db import upsert_social_post, upsert_stock_quote
        sim = DataSimulator(settings.STOCK_SYMBOLS)
        for record in sim.stream_mixed():
            if record["source"].startswith("twitter"):
                cleaned = clean_tweet(record)
                if cleaned:
                    upsert_social_post(enrich_social_record(cleaned))
            elif record["source"].startswith("reddit"):
                cleaned = clean_reddit_post(record)
                if cleaned:
                    upsert_social_post(enrich_social_record(cleaned))
            else:
                cleaned = clean_stock_quote(record)
                if cleaned:
                    upsert_stock_quote(cleaned)
        logger.info("Demo data inserted directly to PostgreSQL")

    logger.info("Step 3: Sentiment summary")
    try:
        time.sleep(2)
        summary = fetch_sentiment_summary(hours=1)
        if summary:
            logger.info("\n{'='*50}")
            logger.info("Sentiment summary (last 1h):")
            for row in summary:
                logger.info(
                    "  %s: avg=%+.3f  posts=%d  pos=%d  neg=%d",
                    row["symbol"], float(row["avg_compound"] or 0),
                    row["post_count"], row["positive_count"], row["negative_count"],
                )
        else:
            logger.info("No summary data yet (consumer may still be processing).")
    except Exception as e:
        logger.warning("Could not fetch summary: %s", e)

    logger.info("Demo complete. Run `python main.py dashboard` to open the UI.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Market Sentiment Pipeline — command-line runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  setup      Initialise PostgreSQL schema
  ingest     Run all producers for one cycle (simulator if no API keys)
  consume    Start Kafka consumer loop (Ctrl-C to stop)
  dashboard  Launch Streamlit dashboard
  demo       Full demo without API keys (simulate → DB → summary)
        """,
    )
    parser.add_argument(
        "command",
        choices=["setup", "ingest", "consume", "dashboard", "demo"],
        help="Pipeline command to run",
    )
    args = parser.parse_args()

    dispatch = {
        "setup":     cmd_setup,
        "ingest":    cmd_ingest,
        "consume":   cmd_consume,
        "dashboard": cmd_dashboard,
        "demo":      cmd_demo,
    }
    dispatch[args.command]()


if __name__ == "__main__":
    main()
