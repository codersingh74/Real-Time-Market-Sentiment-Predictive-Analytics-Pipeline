# Real-Time Market Sentiment & Predictive Analytics Pipeline

A production-ready, end-to-end data engineering and ML project that collects social media and stock data in real time, performs sentiment analysis, stores enriched results in PostgreSQL, orchestrates everything with Apache Airflow, and visualises insights in a live Streamlit dashboard.

---

## Architecture Overview

```
Twitter API ─┐
Reddit API  ─┼──► Kafka Producer ──► Kafka Topics ──► Kafka Consumer
Alpha Vantage┘        (3 topics)                         │
   (or Simulator)                                        ▼
                                               Clean + VADER NLP
                                                        │
                                                        ▼
                                                  PostgreSQL DB
                                               (social_posts, stock_quotes,
                                                predictions)
                                                        │
                                    ┌───────────────────┤
                                    ▼                   ▼
                              Airflow DAGs        Streamlit Dashboard
                         (orchestrate every 4h)  (live charts + preds)
```

## Tech Stack

| Layer | Technology |
|---|---|
| Data ingestion | Python, Tweepy, PRAW, Requests |
| Message broker | Apache Kafka + Zookeeper |
| Stream processing | kafka-python consumer |
| Sentiment analysis | VADER (vaderSentiment) |
| Storage | PostgreSQL 15 |
| Orchestration | Apache Airflow 2.8 |
| Dashboard | Streamlit + Plotly |
| Containerisation | Docker + Docker Compose |

---

## Folder Structure

```
market_sentiment_pipeline/
│
├── data_ingestion/
│   ├── twitter_producer.py    # Twitter API → Kafka (simulator fallback)
│   ├── reddit_producer.py     # Reddit API → Kafka (simulator fallback)
│   ├── stock_producer.py      # Alpha Vantage → Kafka (simulator fallback)
│   └── simulator.py           # Realistic synthetic data generator
│
├── kafka_pipeline/
│   └── consumer.py            # Kafka consumer → clean → NLP → PostgreSQL
│
├── sentiment_analysis/
│   ├── analyzer.py            # VADER sentiment scoring engine
│   └── cleaner.py             # Data normalisation + validation
│
├── database/
│   ├── schema.sql             # PostgreSQL DDL (tables + views)
│   └── db.py                  # Connection pool + all CRUD helpers
│
├── airflow_dags/
│   ├── market_sentiment_daily.py       # Main pipeline (every 4h)
│   └── market_sentiment_healthcheck.py # Health monitor (every 30min)
│
├── dashboard/
│   └── app.py                 # Full Streamlit dashboard
│
├── config/
│   ├── settings.py            # Centralised config from env vars
│   └── .env.example           # Environment variable template
│
├── main.py                    # CLI entry point
├── requirements.txt           # Python dependencies
├── docker-compose.yml         # All services in one file
├── Dockerfile.pipeline        # Image for producers + consumer
├── Dockerfile.dashboard       # Image for Streamlit
└── README.md
```

---

## Quick Start

### Option A — Docker (recommended, no setup required)

**Prerequisites:** Docker Desktop installed and running.

```bash
# 1. Clone or unzip the project
cd market_sentiment_pipeline

# 2. Copy environment file
cp config/.env.example .env
# (Optional) Edit .env to add real API keys

# 3. Start everything with one command
docker compose up -d

# 4. Wait ~60 seconds for services to initialise, then open:
#    Streamlit Dashboard  → http://localhost:8501
#    Airflow UI           → http://localhost:8080  (admin / admin)
#    Kafka UI             → http://localhost:8090
```

**View logs:**
```bash
docker compose logs -f kafka-consumer    # watch data flowing in
docker compose logs -f twitter-producer  # watch ingestion
docker compose logs -f airflow-scheduler # watch DAG runs
```

**Stop everything:**
```bash
docker compose down          # keep volumes (data preserved)
docker compose down -v       # also delete all data volumes
```

---

### Option B — Local Python (no Docker)

**Prerequisites:** Python 3.11+, PostgreSQL 15, Kafka running locally.

```bash
# 1. Create virtual environment
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp config/.env.example .env
# Edit .env with your DB credentials and API keys

# 4. Start Kafka (or use Docker for just Kafka)
docker compose up -d zookeeper kafka kafka-ui

# 5. Initialise the database
python main.py setup

# 6. Run the full demo (works without any API keys)
python main.py demo

# 7. Or run each component separately (in separate terminals):
python data_ingestion/twitter_producer.py
python data_ingestion/reddit_producer.py
python data_ingestion/stock_producer.py
python kafka_pipeline/consumer.py
streamlit run dashboard/app.py
```

---

## API Keys (all optional — simulator runs without them)

| Service | Where to get | .env variable |
|---|---|---|
| Twitter v2 | developer.twitter.com | `TWITTER_BEARER_TOKEN` |
| Reddit | reddit.com/prefs/apps | `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET` |
| Alpha Vantage | alphavantage.co/support/#api-key | `ALPHA_VANTAGE_KEY` |

The pipeline automatically falls back to the built-in data simulator when API keys are missing or invalid.

---

## Dashboard Sections

| Section | Description |
|---|---|
| KPI row | Total posts, avg sentiment, prediction count, model accuracy |
| Sentiment by symbol | Horizontal bar chart — green=bullish, red=bearish |
| Sentiment trend | 48h line chart per symbol with configurable window |
| Latest predictions | Table with direction, confidence, back-fill status |
| Accuracy chart | Per-symbol accuracy vs 33% random baseline |
| Price history | 30-day price with daily change % overlay |
| Volume chart | 30-day trade volume bars |
| Distribution donut | Positive / neutral / negative split |
| Raw posts feed | Live stream of most recent processed posts |

---

## Airflow DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `market_sentiment_daily` | Every 4 hours | Ingest → process → predict → backfill |
| `market_sentiment_healthcheck` | Every 30 minutes | Stale data detection |

Access the Airflow UI at `http://localhost:8080` (admin / admin).
Enable DAGs by clicking the toggle on the DAGs list page.

---

## Database Schema

```sql
social_posts    -- cleaned tweets & Reddit posts with sentiment scores
stock_quotes    -- OHLCV quotes with direction labels
predictions     -- directional predictions + back-filled actuals

-- Convenience views:
v_sentiment_summary_24h  -- aggregated sentiment by symbol
v_prediction_accuracy    -- accuracy metrics per model/symbol
```

---

## Expected Output (without API keys)

After running `docker compose up -d` and waiting ~2 minutes:

```
Streamlit dashboard → sentiment bars show AAPL +0.31, NVDA +0.54, TSLA -0.09
Kafka UI → raw_tweets topic shows ~35 messages/minute
PostgreSQL → social_posts grows by ~50 rows every 60 seconds
Airflow → market_sentiment_daily DAG runs every 4 hours
```

---

## Project Highlights (for Portfolio)

- **Production patterns**: connection pooling, retry logic, dead-letter handling, health checks
- **Fault tolerance**: API fallback to simulator, Kafka consumer retries, Airflow task retries
- **Clean architecture**: separation of ingestion / processing / storage / serving layers
- **Observability**: structured logging, Airflow monitoring DAG, Kafka UI
- **12-factor config**: all secrets via environment variables, never hard-coded

---

## Future Improvements

- **ML upgrade**: replace rule-based predictions with a trained RandomForest or LSTM model
- **More sources**: add news article scraping (NewsAPI, RSS feeds)
- **Alerts**: integrate Slack / email notifications for significant sentiment shifts
- **Scaling**: add Kafka partitions and multiple consumer instances for higher throughput
- **CI/CD**: add GitHub Actions workflow for linting, testing, and Docker image builds
- **Testing**: add pytest unit tests for cleaner, analyzer, and DB layer
- **More indicators**: RSI, MACD, Bollinger Bands from historical data

---

## License

MIT License — free to use, modify, and distribute for personal and commercial projects.
