"""
airflow_dags/market_sentiment_daily.py
----------------------------------------
Daily pipeline DAG — runs every 4 hours.

Tasks:
  1. check_kafka_health       — broker reachability check
  2. ingest_all_sources       — runs all three producers (one cycle)
  3. run_processing_pipeline  — Kafka consumer → clean → NLP → Postgres
  4. generate_predictions     — simple rule-based + sentiment scoring
  5. backfill_accuracy        — fills actual_direction on yesterday's preds
  6. send_summary_alert       — logs summary (Slack if configured)
"""

import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ROOT = "/opt/airflow/project"
sys.path.insert(0, PROJECT_ROOT)

SYMBOLS = ["AAPL", "TSLA", "NVDA", "MSFT", "AMZN"]

DEFAULT_ARGS = {
    "owner":            "market_sentiment",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}


# ── Task callables ────────────────────────────────────────────────────────────

def _check_kafka_health(**ctx):
    """Verify Kafka broker is reachable."""
    from kafka import KafkaAdminClient
    from kafka.errors import KafkaError
    from config.settings import settings

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=settings.KAFKA_BROKER,
            request_timeout_ms=8000,
        )
        topics = admin.list_topics()
        admin.close()
        print(f"[Kafka] Healthy — topics: {topics}")
    except KafkaError as e:
        raise RuntimeError(f"Kafka unreachable: {e}")


def _ingest_all_sources(**ctx):
    """Run one ingestion cycle for all three data sources."""
    import subprocess, time, json

    project = PROJECT_ROOT
    results = {}

    for script in [
        "data_ingestion/twitter_producer.py",
        "data_ingestion/reddit_producer.py",
        "data_ingestion/stock_producer.py",
    ]:
        path   = os.path.join(project, script)
        result = subprocess.run(
            ["python", path, "--once"],
            capture_output=True, text=True, timeout=180,
            cwd=project,
        )
        name = os.path.basename(script)
        results[name] = {"rc": result.returncode, "stdout": result.stdout[-200:]}
        if result.returncode != 0:
            print(f"[Ingest] WARN {name}: {result.stderr[:200]}")
        else:
            print(f"[Ingest] OK {name}")
        time.sleep(2)

    ctx["ti"].xcom_push(key="ingest_results", value=results)
    return results


def _run_processing_pipeline(**ctx):
    """Consume Kafka messages → clean → sentiment → write to DB."""
    from kafka_pipeline.consumer import run as run_consumer
    print("[Processing] Starting consumer pipeline...")
    run_consumer()
    print("[Processing] Consumer cycle complete.")


def _generate_predictions(**ctx):
    """
    Rule-based prediction: combines sentiment score + price direction
    to generate a simple directional prediction for each symbol.
    """
    from database.db import (
        fetch_recent_social_posts, fetch_stock_history, insert_prediction
    )
    from datetime import datetime

    results = []
    for symbol in SYMBOLS:
        posts  = fetch_recent_social_posts(symbol, hours=24)
        stocks = fetch_stock_history(symbol, days=3)

        if not posts:
            print(f"[Predict] No posts for {symbol}, skipping.")
            continue

        avg_sent   = sum(p["sentiment_compound"] for p in posts) / len(posts)
        post_count = len(posts)

        # Simple rule: sentiment + momentum
        if stocks:
            last_dir = stocks[-1].get("direction", "flat")
        else:
            last_dir = "flat"

        if avg_sent > 0.1 or last_dir in ("up", "strong_up"):
            direction = "up"
            confidence = min(0.5 + abs(avg_sent) * 0.5, 0.95)
        elif avg_sent < -0.1 or last_dir in ("down", "strong_down"):
            direction = "down"
            confidence = min(0.5 + abs(avg_sent) * 0.5, 0.95)
        else:
            direction  = "flat"
            confidence = 0.40

        rec = {
            "symbol":               symbol,
            "predicted_direction":  direction,
            "confidence_score":     round(confidence, 4),
            "avg_sentiment":        round(avg_sent, 4),
            "post_count":           post_count,
            "model_version":        "rule_v1",
            "features_json":        {
                "avg_sentiment": avg_sent,
                "post_count":    post_count,
                "last_direction": last_dir,
            },
            "predicted_at":         datetime.utcnow(),
        }
        pred_id = insert_prediction(rec)
        results.append({"id": pred_id, "symbol": symbol, "direction": direction})
        print(f"[Predict] {symbol}: {direction.upper()} (conf {confidence:.0%})")

    ctx["ti"].xcom_push(key="predictions", value=results)
    return results


def _backfill_accuracy(**ctx):
    """Fill actual_direction + was_correct for yesterday's predictions."""
    from database.db import get_conn, fetch_stock_history, backfill_prediction
    from datetime import date, timedelta
    import psycopg2.extras

    yesterday = date.today() - timedelta(days=1)

    for symbol in SYMBOLS:
        sql = """
            SELECT id, predicted_direction FROM predictions
            WHERE symbol = %(sym)s
              AND predicted_at::DATE = %(date)s
              AND actual_direction IS NULL;
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, {"sym": symbol, "date": yesterday})
                pending = cur.fetchall()

        if not pending:
            continue

        history = fetch_stock_history(symbol, days=3)
        actual_quote = next(
            (q for q in history if str(q.get("trade_date")) == str(yesterday)),
            None,
        )
        if not actual_quote:
            continue

        direction = actual_quote.get("direction", "flat")
        if "up" in direction:    actual = "up"
        elif "down" in direction: actual = "down"
        else:                     actual = "flat"

        for pred in pending:
            backfill_prediction(pred["id"], actual)
            print(f"[Backfill] {symbol} id={pred['id']}: actual={actual}")


def _send_summary_alert(**ctx):
    """Log a pipeline summary (extend to Slack/email as needed)."""
    ti          = ctx["ti"]
    predictions = ti.xcom_pull(task_ids="generate_predictions", key="predictions") or []
    run_date    = ctx["ds"]

    lines = "\n".join(
        f"  {p['symbol']}: {p['direction'].upper()}"
        for p in predictions
    )
    print(
        f"\n{'='*50}\n"
        f"Market Sentiment Pipeline — {run_date}\n"
        f"Predictions:\n{lines or '  (none)'}\n"
        f"{'='*50}"
    )


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="market_sentiment_daily",
    description="Ingest → process → predict → backfill (every 4 hours)",
    schedule_interval="0 */4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["market_sentiment", "production"],
    default_args=DEFAULT_ARGS,
) as dag:

    start = EmptyOperator(task_id="start")

    check_kafka = PythonOperator(
        task_id="check_kafka_health",
        python_callable=_check_kafka_health,
        execution_timeout=timedelta(minutes=2),
    )

    ingest = PythonOperator(
        task_id="ingest_all_sources",
        python_callable=_ingest_all_sources,
        execution_timeout=timedelta(minutes=15),
    )

    process = PythonOperator(
        task_id="run_processing_pipeline",
        python_callable=_run_processing_pipeline,
        execution_timeout=timedelta(minutes=20),
    )

    predict = PythonOperator(
        task_id="generate_predictions",
        python_callable=_generate_predictions,
        execution_timeout=timedelta(minutes=10),
    )

    backfill = PythonOperator(
        task_id="backfill_accuracy",
        python_callable=_backfill_accuracy,
        execution_timeout=timedelta(minutes=5),
    )

    alert = PythonOperator(
        task_id="send_summary_alert",
        python_callable=_send_summary_alert,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    start >> check_kafka >> ingest >> process >> predict >> backfill >> alert >> end
