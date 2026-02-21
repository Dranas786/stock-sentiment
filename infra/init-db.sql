-- infra/init-db.sql

CREATE TABLE IF NOT EXISTS ticker_agg_5m (
    bucket_start TIMESTAMPTZ NOT NULL,
    ticker TEXT NOT NULL,
    mention_count INT NOT NULL,
    sentiment_sum DOUBLE PRECISION NOT NULL,
    sentiment_count INT NOT NULL,
    PRIMARY KEY (bucket_start, ticker)
);

-- Helpful index for API queries
CREATE INDEX IF NOT EXISTS idx_ticker_bucket
ON ticker_agg_5m (ticker, bucket_start DESC);
