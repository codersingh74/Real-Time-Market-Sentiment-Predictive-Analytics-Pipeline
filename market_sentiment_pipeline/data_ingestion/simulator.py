"""
data_ingestion/simulator.py
----------------------------
Realistic data simulator for development/demo when live API keys
are not available. Generates plausible social-media posts and
stock quotes for the configured symbols.
"""

import random
import time
import logging
from datetime import datetime, timedelta
from typing import Generator

logger = logging.getLogger(__name__)

# ── Sentiment-biased tweet templates ────────────────────────────────────────

BULLISH_TEMPLATES = [
    "{sym} is absolutely crushing it right now! Bought more at the dip. 🚀",
    "Just loaded up on {sym}. The fundamentals are insanely strong.",
    "{sym} to the moon! Revenue growth is unbelievable this quarter.",
    "Incredible earnings from {sym}. This stock is going places.",
    "Everyone sleeping on {sym} is going to regret it. Mark my words.",
    "{sym} just broke out of resistance. Next stop: all-time high.",
    "Bought {sym} calls. The chart setup is perfect for a breakout.",
    "Long {sym} here. Institutional buyers are piling in.",
    "{sym} just landed a massive partnership deal. Huge news!",
    "The AI tailwinds for {sym} are just getting started. Long term hold.",
]

BEARISH_TEMPLATES = [
    "{sym} is overvalued. I'm shorting this garbage into earnings.",
    "Sold all my {sym} today. The growth story is broken.",
    "{sym} is going to crash hard. The valuation makes no sense.",
    "Stay away from {sym}. Revenue growth is slowing fast.",
    "{sym} insiders are dumping shares. That's a huge red flag.",
    "Terrible guidance from {sym}. This is not going to end well.",
    "I lost a lot on {sym} today. Lesson learned. Never again.",
    "{sym} is a value trap. Avoid at all costs.",
    "The bear case for {sym} is getting stronger every day.",
    "Cutting my losses on {sym}. The macro environment is brutal.",
]

NEUTRAL_TEMPLATES = [
    "What does everyone think about {sym}? Earnings next week.",
    "{sym} had an interesting day. Watching closely.",
    "Holding {sym} but not adding here. Waiting for more clarity.",
    "Can someone explain the {sym} move today? Confused.",
    "{sym} report comes out Thursday. Anyone have predictions?",
    "Mixed signals on {sym}. Not sure which way it breaks.",
    "Just did a deep dive on {sym}. Interesting company.",
    "{sym} volume spike today. Something happening?",
    "Reading the {sym} 10-K. Lots to digest.",
    "Watching {sym} for a clear setup. Not pulling the trigger yet.",
]

REDDIT_BULLISH = [
    "**{sym} DD**: Why I think this is the most undervalued stock in the market right now. Long thesis inside.",
    "Bought 1000 shares of {sym} today. Here's my 12-month price target and why.",
    "Technical analysis: {sym} is forming a perfect cup-and-handle. Breakout imminent.",
    "{sym} earnings preview: why I think they'll crush estimates by 20%+",
    "The {sym} bull case nobody is talking about. This is a generational opportunity.",
]

REDDIT_BEARISH = [
    "Why {sym} is massively overvalued — a detailed bear case with DCF analysis",
    "I've been short {sym} for 6 months. Here's why I'm staying short.",
    "{sym} red flags: insider selling, slowing growth, and margin compression.",
    "The {sym} bubble is about to pop. Get out before it's too late.",
    "Unpopular opinion: {sym} is worth 40% less than its current price.",
]


class DataSimulator:
    """
    Generates realistic synthetic market sentiment data.
    Used when live API credentials are unavailable.
    """

    def __init__(self, symbols: list[str]):
        self.symbols = symbols
        self._post_counter = 0

    def _random_timestamp(self, max_minutes_ago: int = 120) -> str:
        """Generate a recent random timestamp."""
        delta = random.randint(0, max_minutes_ago * 60)
        ts    = datetime.utcnow() - timedelta(seconds=delta)
        return ts.isoformat()

    def generate_tweet(self, symbol: str | None = None) -> dict:
        """Generate one realistic synthetic tweet."""
        sym    = symbol or random.choice(self.symbols)
        bias   = random.random()

        if bias > 0.55:
            text = random.choice(BULLISH_TEMPLATES).format(sym=sym)
        elif bias < 0.35:
            text = random.choice(BEARISH_TEMPLATES).format(sym=sym)
        else:
            text = random.choice(NEUTRAL_TEMPLATES).format(sym=sym)

        self._post_counter += 1
        return {
            "source":      "twitter_sim",
            "id":          f"tw_{int(time.time())}_{self._post_counter}",
            "text":        text,
            "author_id":   f"user_{random.randint(10000, 99999)}",
            "created_at":  self._random_timestamp(),
            "likes":       random.randint(0, 5000),
            "retweets":    random.randint(0, 1000),
            "query_term":  sym,
            "subreddit":   "",
            "ingested_at": datetime.utcnow().isoformat(),
            "simulated":   True,
        }

    def generate_reddit_post(self, symbol: str | None = None) -> dict:
        """Generate one realistic synthetic Reddit post."""
        sym   = symbol or random.choice(self.symbols)
        sub   = random.choice(["wallstreetbets", "stocks", "investing"])
        bias  = random.random()

        if bias > 0.5:
            title = random.choice(REDDIT_BULLISH).format(sym=sym)
        else:
            title = random.choice(REDDIT_BEARISH).format(sym=sym)

        self._post_counter += 1
        return {
            "source":      "reddit_sim",
            "id":          f"rd_{int(time.time())}_{self._post_counter}",
            "title":       title,
            "selftext":    f"This is a detailed analysis of {sym}. " * random.randint(1, 5),
            "subreddit":   sub,
            "score":       random.randint(-50, 10000),
            "upvote_ratio": round(random.uniform(0.5, 0.98), 2),
            "num_comments": random.randint(0, 2000),
            "created_at":  self._random_timestamp(),
            "url":         f"https://reddit.com/r/{sub}/comments/sim_{self._post_counter}",
            "ingested_at": datetime.utcnow().isoformat(),
            "simulated":   True,
        }

    def generate_stock_quote(self, symbol: str | None = None) -> dict:
        """Generate one realistic synthetic stock quote."""
        sym = symbol or random.choice(self.symbols)

        # Realistic base prices per symbol
        base_prices = {
            "AAPL": 185.0, "TSLA": 245.0, "NVDA": 875.0,
            "MSFT": 415.0, "AMZN": 185.0,
        }
        base   = base_prices.get(sym, 100.0)
        price  = round(base * random.uniform(0.97, 1.03), 2)
        change = round(price - base, 2)
        pct    = round((change / base) * 100, 2)

        return {
            "source":      "stocks_sim",
            "symbol":      sym,
            "price":       price,
            "open":        round(base * random.uniform(0.99, 1.01), 2),
            "high":        round(price * random.uniform(1.0, 1.02), 2),
            "low":         round(price * random.uniform(0.98, 1.0), 2),
            "volume":      random.randint(10_000_000, 150_000_000),
            "prev_close":  base,
            "change_pct":  pct,
            "trade_date":  datetime.utcnow().strftime("%Y-%m-%d"),
            "ingested_at": datetime.utcnow().isoformat(),
            "simulated":   True,
        }

    def stream_mixed(self, batch_size: int = 10) -> Generator[dict, None, None]:
        """Yield a mixed batch of tweets, reddit posts, and stock quotes."""
        for sym in self.symbols:
            # 4 tweets per symbol
            for _ in range(4):
                yield self.generate_tweet(sym)
            # 2 reddit posts per symbol
            for _ in range(2):
                yield self.generate_reddit_post(sym)
            # 1 stock quote per symbol
            yield self.generate_stock_quote(sym)
