# services/api/main.py

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Optional

import psycopg
from fastapi import FastAPI, Query

from shared.settings import settings


app = FastAPI(title="Stock Sentiment API", version="0.1.0")


def get_conn() -> psycopg.Connection:
    """
    Create a short-lived Postgres connection.
    For MVP simplicity: open per request.
    In production: use a connection pool.
    """
    return psycopg.connect(settings.DATABASE_URL_SYNC)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/tickers/trending")
def trending(
    window_minutes: int = Query(60, ge=5, le=1440),
    limit: int = Query(20, ge=1, le=200),
) -> List[dict]:
    """
    Returns top tickers by mention_count in the last N minutes.
    """
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=window_minutes)

    sql = """
        SELECT
            ticker,
            SUM(mention_count) AS mentions,
            SUM(sentiment_sum) / NULLIF(SUM(sentiment_count), 0) AS sentiment_mean
        FROM ticker_agg_5m
        WHERE bucket_start >= %s
        GROUP BY ticker
        ORDER BY mentions DESC
        LIMIT %s;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (start, limit))
        rows = cur.fetchall()

    return [
        {"ticker": ticker, "mentions": int(mentions), "sentiment_mean": float(sent_mean) if sent_mean is not None else 0.0}
        for (ticker, mentions, sent_mean) in rows
    ]


@app.get("/tickers/{ticker}/timeseries")
def timeseries(
    ticker: str,
    hours: int = Query(24, ge=1, le=168),
) -> List[dict]:
    """
    Returns 5-minute bucket time series for a ticker over last N hours.
    """
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)

    sql = """
        SELECT
            bucket_start,
            mention_count,
            sentiment_sum / NULLIF(sentiment_count, 0) AS sentiment_mean
        FROM ticker_agg_5m
        WHERE ticker = %s AND bucket_start >= %s
        ORDER BY bucket_start ASC;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (ticker.upper(), start))
        rows = cur.fetchall()

    return [
        {
            "bucket_start": bucket_start.isoformat(),
            "mentions": int(mentions),
            "sentiment_mean": float(sent_mean) if sent_mean is not None else 0.0,
        }
        for (bucket_start, mentions, sent_mean) in rows
    ]
