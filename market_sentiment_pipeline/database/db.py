"""
database/db.py
---------------
PostgreSQL connection pool + all upsert / query helpers.
"""

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timedelta, date

import psycopg2
import psycopg2.extras
from psycopg2 import pool

logger = logging.getLogger(__name__)

# ── Lazy connection pool ──────────────────────────────────────────────────────

_pool = None


def _get_db_config() -> dict:
    """Read DB credentials from environment (works inside & outside Docker)."""
    from config.settings import settings
    return {
        "host":     settings.DB_HOST,
        "port":     settings.DB_PORT,
        "dbname":   settings.DB_NAME,
        "user":     settings.DB_USER,
        "password": settings.DB_PASSWORD,
    }


def get_pool() -> pool.SimpleConnectionPool:
    global _pool
    if _pool is None:
        cfg  = _get_db_config()
        _pool = pool.SimpleConnectionPool(minconn=1, maxconn=10, **cfg)
        logger.info("PostgreSQL connection pool created (host=%s db=%s)", cfg["host"], cfg["dbname"])
    return _pool


@contextmanager
def get_conn():
    """Borrow a connection from the pool, commit on success, rollback on error."""
    conn = get_pool().getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        get_pool().putconn(conn)


# ── Schema initialisation ─────────────────────────────────────────────────────

def init_db():
    """Create tables and indexes if they don't exist. Safe to call repeatedly."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path) as f:
        sql = f.read()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    logger.info("Database schema initialised.")


# ── Upserts ───────────────────────────────────────────────────────────────────

def upsert_social_post(record: dict) -> bool:
    """Insert or update a social post. Returns True if inserted (new row)."""
    sql = """
        INSERT INTO social_posts (
            source, record_id, text, raw_text, author_id,
            query_term, subreddit,
            sentiment_compound, sentiment_positive,
            sentiment_neutral, sentiment_negative, sentiment_label,
            likes, retweets, engagement_score, weighted_sentiment,
            simulated, created_at, ingested_at, processed_at
        ) VALUES (
            %(source)s, %(record_id)s, %(text)s, %(raw_text)s, %(author_id)s,
            %(query_term)s, %(subreddit)s,
            %(sentiment_compound)s, %(sentiment_positive)s,
            %(sentiment_neutral)s, %(sentiment_negative)s, %(sentiment_label)s,
            %(likes)s, %(retweets)s, %(engagement_score)s, %(weighted_sentiment)s,
            %(simulated)s, %(created_at)s, %(ingested_at)s, %(processed_at)s
        )
        ON CONFLICT (source, record_id) DO UPDATE SET
            sentiment_compound = EXCLUDED.sentiment_compound,
            sentiment_label    = EXCLUDED.sentiment_label,
            engagement_score   = EXCLUDED.engagement_score,
            weighted_sentiment = EXCLUDED.weighted_sentiment,
            processed_at       = EXCLUDED.processed_at
        RETURNING (xmax = 0) AS inserted;
    """
    # Ensure datetime objects are used (not ISO strings)
    for dt_field in ("created_at", "ingested_at", "processed_at"):
        val = record.get(dt_field)
        if isinstance(val, str):
            try:
                from datetime import datetime as dt
                record[dt_field] = dt.fromisoformat(val[:19])
            except Exception:
                record[dt_field] = datetime.utcnow()
        elif val is None:
            record[dt_field] = datetime.utcnow()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, record)
            row = cur.fetchone()
            return bool(row[0]) if row else False


def upsert_stock_quote(record: dict) -> bool:
    """Insert or update a stock quote keyed on (symbol, trade_date)."""
    sql = """
        INSERT INTO stock_quotes (
            symbol, trade_date, price, open, high, low, volume,
            prev_close, change_pct, direction, intraday_range,
            volume_norm, simulated, ingested_at, processed_at
        ) VALUES (
            %(symbol)s, %(trade_date)s, %(price)s, %(open)s, %(high)s,
            %(low)s, %(volume)s, %(prev_close)s, %(change_pct)s,
            %(direction)s, %(intraday_range)s,
            %(volume_norm)s, %(simulated)s, %(ingested_at)s, NOW()
        )
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
            price          = EXCLUDED.price,
            change_pct     = EXCLUDED.change_pct,
            direction      = EXCLUDED.direction,
            volume_norm    = EXCLUDED.volume_norm,
            processed_at   = NOW()
        RETURNING (xmax = 0) AS inserted;
    """
    # Coerce trade_date
    td = record.get("trade_date", datetime.utcnow().strftime("%Y-%m-%d"))
    if isinstance(td, datetime):
        td = td.date()
    record["trade_date"] = td

    # Coerce ingested_at
    ia = record.get("ingested_at")
    if isinstance(ia, str):
        try:
            record["ingested_at"] = datetime.fromisoformat(ia[:19])
        except Exception:
            record["ingested_at"] = datetime.utcnow()
    elif ia is None:
        record["ingested_at"] = datetime.utcnow()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, record)
            row = cur.fetchone()
            return bool(row[0]) if row else False


def insert_prediction(record: dict) -> int:
    """Insert a new prediction row. Returns the new id."""
    if isinstance(record.get("features_json"), dict):
        record = {**record, "features_json": json.dumps(record["features_json"])}

    sql = """
        INSERT INTO predictions (
            symbol, predicted_direction, confidence_score,
            avg_sentiment, post_count, model_version,
            features_json, predicted_at
        ) VALUES (
            %(symbol)s, %(predicted_direction)s, %(confidence_score)s,
            %(avg_sentiment)s, %(post_count)s, %(model_version)s,
            %(features_json)s, %(predicted_at)s
        )
        ON CONFLICT (symbol, DATE(predicted_at), model_version)
        DO UPDATE SET
            predicted_direction = EXCLUDED.predicted_direction,
            confidence_score    = EXCLUDED.confidence_score,
            features_json       = EXCLUDED.features_json
        RETURNING id;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, record)
            return cur.fetchone()[0]


def backfill_prediction(prediction_id: int, actual_direction: str):
    """Set actual_direction and was_correct on a prediction row."""
    sql = """
        UPDATE predictions
        SET actual_direction = %(actual)s,
            was_correct      = (predicted_direction = %(actual)s)
        WHERE id = %(id)s;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"actual": actual_direction, "id": prediction_id})


# ── Query helpers (dashboard + ML) ────────────────────────────────────────────

def fetch_sentiment_summary(hours: int = 24) -> list[dict]:
    sql = """
        SELECT query_term AS symbol, COUNT(*) AS post_count,
               ROUND(AVG(sentiment_compound)::NUMERIC, 4) AS avg_compound,
               ROUND(AVG(weighted_sentiment)::NUMERIC, 4) AS avg_weighted,
               SUM(CASE WHEN sentiment_label='positive' THEN 1 ELSE 0 END) AS positive_count,
               SUM(CASE WHEN sentiment_label='negative' THEN 1 ELSE 0 END) AS negative_count,
               SUM(CASE WHEN sentiment_label='neutral'  THEN 1 ELSE 0 END) AS neutral_count,
               MAX(created_at) AS latest_post_at
        FROM social_posts
        WHERE created_at >= NOW() - INTERVAL %(interval)s
          AND query_term  != ''
        GROUP BY query_term
        ORDER BY avg_compound DESC;
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, {"interval": f"{hours} hours"})
            return [dict(r) for r in cur.fetchall()]


def fetch_sentiment_timeseries(symbol: str, hours: int = 48) -> list[dict]:
    sql = """
        SELECT DATE_TRUNC('hour', created_at) AS hour,
               ROUND(AVG(sentiment_compound)::NUMERIC, 4) AS avg_compound,
               COUNT(*) AS post_count
        FROM social_posts
        WHERE query_term = %(symbol)s
          AND created_at >= NOW() - INTERVAL %(interval)s
        GROUP BY DATE_TRUNC('hour', created_at)
        ORDER BY hour ASC;
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, {"symbol": symbol, "interval": f"{hours} hours"})
            return [dict(r) for r in cur.fetchall()]


def fetch_recent_posts(symbol: str | None = None, limit: int = 20) -> list[dict]:
    where = "AND query_term = %(symbol)s" if symbol and symbol != "All" else ""
    sql   = f"""
        SELECT source, text, sentiment_label, sentiment_compound,
               query_term, engagement_score, created_at
        FROM social_posts
        WHERE 1=1 {where}
        ORDER BY created_at DESC
        LIMIT %(limit)s;
    """
    params = {"limit": limit, "symbol": symbol}
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]


def fetch_latest_predictions(limit: int = 20) -> list[dict]:
    sql = """
        SELECT p.id, p.symbol, p.predicted_direction, p.confidence_score,
               p.avg_sentiment, p.post_count, p.model_version,
               p.predicted_at, p.was_correct,
               q.price, q.change_pct
        FROM predictions p
        LEFT JOIN stock_quotes q
               ON q.symbol = p.symbol AND q.trade_date = p.predicted_at::DATE
        ORDER BY p.predicted_at DESC
        LIMIT %(limit)s;
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, {"limit": limit})
            return [dict(r) for r in cur.fetchall()]


def fetch_stock_history(symbol: str, days: int = 30) -> list[dict]:
    sql = """
        SELECT * FROM stock_quotes
        WHERE symbol = %(symbol)s
          AND trade_date >= NOW() - INTERVAL %(interval)s
        ORDER BY trade_date ASC;
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, {"symbol": symbol, "interval": f"{days} days"})
            return [dict(r) for r in cur.fetchall()]


def fetch_model_accuracy() -> list[dict]:
    sql = "SELECT * FROM v_prediction_accuracy;"
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return [dict(r) for r in cur.fetchall()]


def fetch_recent_social_posts(symbol: str | None, hours: int = 24) -> list[dict]:
    where = "AND query_term = %(symbol)s" if symbol and symbol != "All" else ""
    sql   = f"""
        SELECT sentiment_compound, weighted_sentiment, engagement_score, sentiment_label
        FROM social_posts
        WHERE created_at >= NOW() - INTERVAL %(interval)s {where};
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, {"interval": f"{hours} hours", "symbol": symbol})
            return [dict(r) for r in cur.fetchall()]
