-- ============================================================
--  Market Sentiment Pipeline — PostgreSQL Schema
--  Run: psql -U postgres -d market_sentiment -f schema.sql
--  Or called automatically by init_db() on startup.
-- ============================================================

-- ── social_posts ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS social_posts (
    id                   SERIAL PRIMARY KEY,
    source               VARCHAR(30)    NOT NULL,
    record_id            VARCHAR(80)    NOT NULL,
    text                 TEXT           NOT NULL,
    raw_text             TEXT,
    author_id            VARCHAR(80),
    query_term           VARCHAR(120),
    subreddit            VARCHAR(120),

    -- Sentiment (VADER)
    sentiment_compound   FLOAT          NOT NULL DEFAULT 0,
    sentiment_positive   FLOAT          NOT NULL DEFAULT 0,
    sentiment_neutral    FLOAT          NOT NULL DEFAULT 0,
    sentiment_negative   FLOAT          NOT NULL DEFAULT 0,
    sentiment_label      VARCHAR(15)    NOT NULL DEFAULT 'neutral',

    -- Engagement
    likes                INTEGER        NOT NULL DEFAULT 0,
    retweets             INTEGER        NOT NULL DEFAULT 0,
    engagement_score     FLOAT          NOT NULL DEFAULT 0,
    weighted_sentiment   FLOAT          NOT NULL DEFAULT 0,

    -- Meta
    simulated            BOOLEAN        NOT NULL DEFAULT FALSE,
    created_at           TIMESTAMP      NOT NULL DEFAULT NOW(),
    ingested_at          TIMESTAMP,
    processed_at         TIMESTAMP      NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_social_record UNIQUE (source, record_id)
);

CREATE INDEX IF NOT EXISTS idx_social_created   ON social_posts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_social_label     ON social_posts (sentiment_label);
CREATE INDEX IF NOT EXISTS idx_social_query     ON social_posts (query_term);
CREATE INDEX IF NOT EXISTS idx_social_source    ON social_posts (source);


-- ── stock_quotes ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stock_quotes (
    id               SERIAL PRIMARY KEY,
    symbol           VARCHAR(15)    NOT NULL,
    trade_date       DATE           NOT NULL,
    price            FLOAT          NOT NULL,
    open             FLOAT,
    high             FLOAT,
    low              FLOAT,
    volume           BIGINT,
    prev_close       FLOAT,
    change_pct       FLOAT,
    direction        VARCHAR(15),
    intraday_range   FLOAT,
    volume_norm      FLOAT,
    simulated        BOOLEAN        NOT NULL DEFAULT FALSE,
    ingested_at      TIMESTAMP,
    processed_at     TIMESTAMP      NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_stock_day UNIQUE (symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_stock_symbol     ON stock_quotes (symbol);
CREATE INDEX IF NOT EXISTS idx_stock_date       ON stock_quotes (trade_date DESC);


-- ── predictions ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS predictions (
    id                   SERIAL PRIMARY KEY,
    symbol               VARCHAR(15)    NOT NULL,
    predicted_direction  VARCHAR(15)    NOT NULL,
    confidence_score     FLOAT          NOT NULL,
    avg_sentiment        FLOAT,
    post_count           INTEGER,
    model_version        VARCHAR(30)    NOT NULL DEFAULT 'v1',
    features_json        JSONB,
    predicted_at         TIMESTAMP      NOT NULL DEFAULT NOW(),
    actual_direction     VARCHAR(15),
    was_correct          BOOLEAN,

    CONSTRAINT uq_prediction UNIQUE (symbol, DATE(predicted_at), model_version)
);

CREATE INDEX IF NOT EXISTS idx_pred_symbol      ON predictions (symbol);
CREATE INDEX IF NOT EXISTS idx_pred_at          ON predictions (predicted_at DESC);


-- ── Convenience views ─────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_sentiment_summary_24h AS
SELECT
    query_term                                     AS symbol,
    COUNT(*)                                       AS post_count,
    ROUND(AVG(sentiment_compound)::NUMERIC, 4)     AS avg_compound,
    ROUND(AVG(weighted_sentiment)::NUMERIC, 4)     AS avg_weighted,
    SUM(CASE WHEN sentiment_label='positive' THEN 1 ELSE 0 END) AS positive_count,
    SUM(CASE WHEN sentiment_label='negative' THEN 1 ELSE 0 END) AS negative_count,
    SUM(CASE WHEN sentiment_label='neutral'  THEN 1 ELSE 0 END) AS neutral_count,
    MAX(created_at)                                AS latest_post_at
FROM social_posts
WHERE created_at >= NOW() - INTERVAL '24 hours'
  AND query_term  != ''
GROUP BY query_term
ORDER BY avg_compound DESC;


CREATE OR REPLACE VIEW v_prediction_accuracy AS
SELECT
    symbol,
    model_version,
    COUNT(*)                                                           AS total,
    SUM(CASE WHEN was_correct = TRUE THEN 1 ELSE 0 END)               AS correct,
    ROUND(
        100.0 * SUM(CASE WHEN was_correct = TRUE THEN 1 ELSE 0 END)
              / NULLIF(COUNT(CASE WHEN was_correct IS NOT NULL THEN 1 END), 0),
    2)                                                                 AS accuracy_pct
FROM predictions
GROUP BY symbol, model_version
ORDER BY accuracy_pct DESC NULLS LAST;
