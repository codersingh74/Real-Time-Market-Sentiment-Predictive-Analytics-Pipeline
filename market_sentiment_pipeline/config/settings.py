"""
config/settings.py
------------------
Centralised configuration loader. Reads from environment variables
(populated via .env file or Docker environment).
"""

import os
from dotenv import load_dotenv

# Load .env from project root or config directory
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env.example"))
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"),
            override=True)


class Config:
    """Central configuration object — import this everywhere."""

    # ── Twitter ──────────────────────────────────────────────────
    TWITTER_BEARER_TOKEN: str = os.getenv("TWITTER_BEARER_TOKEN", "")

    # ── Reddit ───────────────────────────────────────────────────
    REDDIT_CLIENT_ID: str     = os.getenv("REDDIT_CLIENT_ID", "")
    REDDIT_CLIENT_SECRET: str = os.getenv("REDDIT_CLIENT_SECRET", "")
    REDDIT_USER_AGENT: str    = os.getenv("REDDIT_USER_AGENT", "MarketSentimentBot/1.0")

    # ── Alpha Vantage ────────────────────────────────────────────
    ALPHA_VANTAGE_KEY: str    = os.getenv("ALPHA_VANTAGE_KEY", "demo")

    # ── Kafka ────────────────────────────────────────────────────
    KAFKA_BROKER: str         = os.getenv("KAFKA_BROKER", "localhost:9092")
    TOPIC_TWEETS: str         = os.getenv("KAFKA_TOPIC_TWEETS", "raw_tweets")
    TOPIC_REDDIT: str         = os.getenv("KAFKA_TOPIC_REDDIT", "raw_reddit")
    TOPIC_STOCKS: str         = os.getenv("KAFKA_TOPIC_STOCKS", "raw_stocks")

    # ── PostgreSQL ───────────────────────────────────────────────
    DB_HOST: str              = os.getenv("DB_HOST", "localhost")
    DB_PORT: int              = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str              = os.getenv("DB_NAME", "market_sentiment")
    DB_USER: str              = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str          = os.getenv("DB_PASSWORD", "postgres123")

    @property
    def DB_URL(self) -> str:
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    # ── Tracking targets ─────────────────────────────────────────
    STOCK_SYMBOLS: list = os.getenv(
        "STOCK_SYMBOLS", "AAPL,TSLA,NVDA,MSFT,AMZN"
    ).split(",")

    REDDIT_SUBREDDITS: list = os.getenv(
        "REDDIT_SUBREDDITS", "wallstreetbets,stocks,investing"
    ).split(",")

    SEARCH_TERMS: list = os.getenv(
        "SEARCH_TERMS", "AAPL,Tesla,NVDA,stock market"
    ).split(",")

    # ── Pipeline ─────────────────────────────────────────────────
    INGESTION_INTERVAL: int   = int(os.getenv("INGESTION_INTERVAL_SECONDS", "60"))
    LOG_LEVEL: str            = os.getenv("LOG_LEVEL", "INFO")


settings = Config()
