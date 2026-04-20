"""
sentiment_analysis/analyzer.py
--------------------------------
Sentiment analysis engine using VADER.
Provides compound scores, labels, and engagement-weighted sentiment.
"""

import math
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Lazy-load VADER to avoid import errors if vaderSentiment not yet installed
_analyzer = None


def _get_analyzer():
    global _analyzer
    if _analyzer is None:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        _analyzer = SentimentIntensityAnalyzer()
    return _analyzer


# ── Thresholds (standard VADER values) ──────────────────────────────────────

POSITIVE_THRESHOLD =  0.05
NEGATIVE_THRESHOLD = -0.05


def score_text(text: str) -> dict:
    """
    Run VADER sentiment analysis on a piece of text.

    Returns
    -------
    dict with keys:
        compound  : float  [-1.0, +1.0]
        positive  : float  [0, 1]
        neutral   : float  [0, 1]
        negative  : float  [0, 1]
        label     : str    'positive' | 'neutral' | 'negative'
    """
    if not text or not text.strip():
        return {
            "compound": 0.0, "positive": 0.0,
            "neutral": 1.0,  "negative": 0.0,
            "label": "neutral",
        }

    try:
        scores   = _get_analyzer().polarity_scores(text)
        compound = scores["compound"]

        if compound >= POSITIVE_THRESHOLD:
            label = "positive"
        elif compound <= NEGATIVE_THRESHOLD:
            label = "negative"
        else:
            label = "neutral"

        return {
            "compound": round(compound, 4),
            "positive": round(scores["pos"], 4),
            "neutral":  round(scores["neu"], 4),
            "negative": round(scores["neg"], 4),
            "label":    label,
        }
    except Exception as e:
        logger.error("Sentiment scoring failed: %s", e)
        return {
            "compound": 0.0, "positive": 0.0,
            "neutral": 1.0,  "negative": 0.0,
            "label": "neutral",
        }


def enrich_social_record(record: dict) -> dict:
    """
    Add sentiment fields to a cleaned social-media record.
    Works for both tweets and Reddit posts.
    """
    text      = record.get("text", "") or record.get("title", "")
    sentiment = score_text(text)

    likes    = int(record.get("likes", 0) or record.get("score", 0))
    retweets = int(record.get("retweets", 0) or record.get("num_comments", 0))

    # Engagement score: log-scale so viral posts don't dominate
    engagement = round(math.log1p(likes + retweets * 2), 4)

    return {
        **record,
        "sentiment_compound":  sentiment["compound"],
        "sentiment_positive":  sentiment["positive"],
        "sentiment_neutral":   sentiment["neutral"],
        "sentiment_negative":  sentiment["negative"],
        "sentiment_label":     sentiment["label"],
        "engagement_score":    engagement,
        "weighted_sentiment":  round(sentiment["compound"] * engagement, 4),
        "processed_at":        datetime.utcnow().isoformat(),
    }
