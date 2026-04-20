"""
sentiment_analysis/cleaner.py
-------------------------------
Data cleaning and normalisation for raw social and stock records.
"""

import re
import html
import math
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def normalize_text(text: str) -> str:
    """Clean raw social text: unescape HTML, strip URLs, normalise whitespace."""
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"http\S+|www\.\S+", "", text)
    text = re.sub(r"@\w+", "", text)
    text = re.sub(r"#(\w+)", r"\1", text)
    text = re.sub(r"[^\w\s,.!?'\"$%-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.lower()


def safe_float(value, default: float = 0.0) -> float:
    try:
        v = float(value)
        return default if math.isnan(v) else v
    except (TypeError, ValueError):
        return default


def safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_dt(value) -> datetime:
    if isinstance(value, datetime):
        return value
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S+00:00"):
        try:
            return datetime.strptime(str(value)[:19], fmt)
        except (ValueError, TypeError):
            continue
    return datetime.utcnow()


def clean_tweet(raw: dict) -> dict | None:
    """Validate and normalise a raw tweet. Returns None if should be discarded."""
    tweet_id = raw.get("id")
    text     = raw.get("text", "")
    if not tweet_id or not text:
        return None

    cleaned = normalize_text(text)
    if len(cleaned) < 5:
        return None

    return {
        "source":      raw.get("source", "twitter"),
        "record_id":   str(tweet_id),
        "text":        cleaned,
        "raw_text":    text[:500],
        "author_id":   str(raw.get("author_id", "")),
        "created_at":  parse_dt(raw.get("created_at")),
        "likes":       safe_int(raw.get("likes")),
        "retweets":    safe_int(raw.get("retweets")),
        "query_term":  raw.get("query_term", ""),
        "subreddit":   raw.get("subreddit", ""),
        "ingested_at": parse_dt(raw.get("ingested_at")),
        "simulated":   bool(raw.get("simulated", False)),
    }


def clean_reddit_post(raw: dict) -> dict | None:
    """Validate and normalise a raw Reddit post."""
    post_id = raw.get("id")
    title   = raw.get("title", "")
    if not post_id or not title:
        return None

    combined = normalize_text(f"{title} {raw.get('selftext', '')}")
    if len(combined) < 5:
        return None

    return {
        "source":      "reddit",
        "record_id":   str(post_id),
        "text":        combined,
        "raw_text":    f"{title} {raw.get('selftext', '')}".strip()[:500],
        "author_id":   "",
        "created_at":  parse_dt(raw.get("created_at")),
        "likes":       safe_int(raw.get("score")),
        "retweets":    safe_int(raw.get("num_comments")),
        "query_term":  raw.get("query_term", ""),
        "subreddit":   raw.get("subreddit", ""),
        "ingested_at": parse_dt(raw.get("ingested_at")),
        "simulated":   bool(raw.get("simulated", False)),
    }


def clean_stock_quote(raw: dict) -> dict | None:
    """Validate and normalise a raw stock quote."""
    symbol = raw.get("symbol")
    price  = safe_float(raw.get("price"))
    if not symbol or price <= 0:
        return None

    change_str = str(raw.get("change_pct", "0")).replace("%", "")
    change_pct = safe_float(change_str)

    if change_pct > 1.0:      direction = "strong_up"
    elif change_pct > 0:       direction = "up"
    elif change_pct < -1.0:    direction = "strong_down"
    elif change_pct < 0:       direction = "down"
    else:                      direction = "flat"

    high   = safe_float(raw.get("high", price))
    low    = safe_float(raw.get("low", price))
    intr   = round(((high - low) / price) * 100, 4) if price > 0 else 0.0
    vol    = safe_int(raw.get("volume"))

    import math as _math
    return {
        "symbol":         symbol.upper(),
        "price":          price,
        "open":           safe_float(raw.get("open")),
        "high":           high,
        "low":            low,
        "volume":         vol,
        "prev_close":     safe_float(raw.get("prev_close")),
        "change_pct":     change_pct,
        "direction":      direction,
        "intraday_range": intr,
        "volume_norm":    round(_math.log1p(vol), 4) if vol > 0 else 0.0,
        "trade_date":     raw.get("trade_date", datetime.utcnow().strftime("%Y-%m-%d")),
        "ingested_at":    parse_dt(raw.get("ingested_at")),
        "simulated":      bool(raw.get("simulated", False)),
    }
