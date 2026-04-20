"""
airflow_dags/market_sentiment_healthcheck.py
---------------------------------------------
Lightweight health-check DAG — runs every 30 minutes.
Alerts if no new social posts in the last hour.
"""

import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/opt/airflow/project"
sys.path.insert(0, PROJECT_ROOT)

DEFAULT_ARGS = {
    "owner":   "market_sentiment",
    "retries": 0,
    "email_on_failure": False,
}


def _run_healthcheck(**ctx):
    """Check pipeline health — alert if data is stale."""
    from database.db import get_conn
    import psycopg2.extras

    checks = {}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM social_posts "
                    "WHERE created_at >= NOW() - INTERVAL '60 minutes'"
                )
                checks["recent_posts"] = cur.fetchone()[0]

                cur.execute(
                    "SELECT COUNT(*) FROM predictions "
                    "WHERE predicted_at >= NOW() - INTERVAL '6 hours'"
                )
                checks["recent_predictions"] = cur.fetchone()[0]

                cur.execute("SELECT MAX(processed_at) FROM stock_quotes")
                last_stock = cur.fetchone()[0]
                checks["last_stock"] = str(last_stock)

        status = "HEALTHY" if checks["recent_posts"] > 0 else "STALE"
        print(f"[HealthCheck] Status: {status} | {checks}")

        if checks["recent_posts"] == 0:
            print("[HealthCheck] WARNING: No new posts in last 60 minutes!")

    except Exception as e:
        print(f"[HealthCheck] ERROR: {e}")
        raise


with DAG(
    dag_id="market_sentiment_healthcheck",
    description="Pipeline health monitor — every 30 min",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["market_sentiment", "monitoring"],
    default_args=DEFAULT_ARGS,
) as dag:

    PythonOperator(
        task_id="run_healthcheck",
        python_callable=_run_healthcheck,
    )
