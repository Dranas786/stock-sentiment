-- infra/init-db.sql

-- Raw posts (optional, if you want long-term storage)
CREATE TABLE IF NOT EXISTS raw_posts (
    source TEXT NOT NULL,
    post_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    text TEXT,
    author TEXT,
    url TEXT,
    meta JSONB,
    PRIMARY KEY (source, post_id)
);

-- Enriched posts (optional persistence layer)
CREATE TABLE IF NOT EXISTS enriched_posts (
    source TEXT NOT NULL,
    post_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    tickers TEXT[],
    finbert_score DOUBLE PRECISION,
    stance_label TEXT,
    conviction DOUBLE PRECISION,
    emotions JSONB,
    source_weight DOUBLE PRECISION,
    PRIMARY KEY (source, post_id)
);

-- Core aggregation table (5-minute windows)
CREATE TABLE IF NOT EXISTS ticker_agg_5m (
    bucket_start TIMESTAMPTZ NOT NULL,
    ticker TEXT NOT NULL,

    -- Volume
    mention_count INT NOT NULL DEFAULT 0,
    weighted_mentions DOUBLE PRECISION NOT NULL DEFAULT 0,
    unique_authors INT NOT NULL DEFAULT 0,

    -- Source mix
    news_count INT NOT NULL DEFAULT 0,
    reddit_count INT NOT NULL DEFAULT 0,

    -- Sentiment
    sentiment_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    sentiment_count INT NOT NULL DEFAULT 0,

    -- Stance
    bullish_count INT NOT NULL DEFAULT 0,
    bearish_count INT NOT NULL DEFAULT 0,
    neutral_count INT NOT NULL DEFAULT 0,

    -- Conviction
    conviction_sum DOUBLE PRECISION NOT NULL DEFAULT 0,

    -- Emotions (store sums for later mean calc)
    emotion_sums JSONB NOT NULL DEFAULT '{}'::jsonb,

    PRIMARY KEY (bucket_start, ticker)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_ticker_agg_5m_ticker
    ON ticker_agg_5m (ticker);

CREATE INDEX IF NOT EXISTS idx_ticker_agg_5m_bucket
    ON ticker_agg_5m (bucket_start);