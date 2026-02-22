# services/api/main.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import psycopg
from fastapi import FastAPI, Query
from psycopg.rows import dict_row

from shared.settings import settings

app = FastAPI(title="Stock Emotional Analysis API")


def get_conn() -> psycopg.Connection:
    # Use dict_row so SQL results come back as dicts
    return psycopg.connect(settings.DATABASE_URL_SYNC, row_factory=dict_row)


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/tickers/trending")
def trending(
    minutes: int = Query(60, ge=5, le=24 * 60),
    limit: int = Query(25, ge=1, le=200),
) -> List[Dict[str, Any]]:
    """
    Simple trending list: highest mention_count in the last N minutes.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=minutes)

    sql = """
    SELECT
      ticker,
      SUM(mention_count) AS mentions,
      (SUM(sentiment_sum) / NULLIF(SUM(sentiment_count), 0)) AS sentiment_mean
    FROM ticker_agg_5m
    WHERE bucket_start >= %s
    GROUP BY ticker
    ORDER BY mentions DESC
    LIMIT %s;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (start, limit))
        rows = cur.fetchall()

    # Convert None sentiment_mean to 0.0 for easy UI usage
    for r in rows:
        r["sentiment_mean"] = float(r["sentiment_mean"] or 0.0)
        r["mentions"] = int(r["mentions"] or 0)

    return rows


@app.get("/tickers/movers")
def movers(
    window_minutes: int = Query(60, ge=5, le=6 * 60),
    baseline_hours: int = Query(24, ge=1, le=14 * 24),
    limit: int = Query(25, ge=1, le=200),
    require_cross_source: bool = Query(False),
) -> List[Dict[str, Any]]:
    """
    Higher-quality "hyped movers" ranking.

    window_minutes: recent window to score (e.g. 60)
    baseline_hours: lookback for baseline rate (e.g. 24)
    require_cross_source: if True, only show tickers with both news+reddit activity in window
    """
    now = datetime.now(timezone.utc)
    win_start = now - timedelta(minutes=window_minutes)

    # Baseline uses the period BEFORE the window:
    # [now - baseline_hours, win_start)
    base_start = now - timedelta(hours=baseline_hours)
    base_end = win_start

    # NOTE: emotion_sums is JSONB of numeric sums.
    # We safely cast missing keys to 0.
    # Cardiff emotion model commonly includes: joy, optimism, anger, sadness
    # Some variants include fear. We handle both.
    sql = """
    WITH win AS (
      SELECT
        ticker,
        SUM(mention_count) AS mentions,
        SUM(weighted_mentions) AS w_mentions,
        SUM(news_count) AS news_mentions,
        SUM(reddit_count) AS reddit_mentions,

        SUM(sentiment_sum) AS sent_sum,
        SUM(sentiment_count) AS sent_cnt,

        SUM(bullish_count) AS bull,
        SUM(bearish_count) AS bear,
        SUM(neutral_count) AS neu,

        SUM(conviction_sum) AS conv_sum,

        -- Emotion sums across JSONB
        SUM(COALESCE((emotion_sums->>'joy')::double precision, 0)) AS joy_sum,
        SUM(COALESCE((emotion_sums->>'optimism')::double precision, 0)) AS optimism_sum,
        SUM(COALESCE((emotion_sums->>'fear')::double precision, 0)) AS fear_sum,
        SUM(COALESCE((emotion_sums->>'sadness')::double precision, 0)) AS sadness_sum,
        SUM(COALESCE((emotion_sums->>'anger')::double precision, 0)) AS anger_sum
      FROM ticker_agg_5m
      WHERE bucket_start >= %s
      GROUP BY ticker
    ),
    base AS (
      SELECT
        ticker,
        SUM(mention_count) AS base_mentions,
        COUNT(*) AS base_buckets
      FROM ticker_agg_5m
      WHERE bucket_start >= %s
        AND bucket_start < %s
      GROUP BY ticker
    ),
    joined AS (
      SELECT
        w.ticker,

        w.mentions,
        w.w_mentions,
        w.news_mentions,
        w.reddit_mentions,

        w.sent_sum,
        w.sent_cnt,

        w.bull,
        w.bear,
        w.neu,

        w.conv_sum,

        w.joy_sum,
        w.optimism_sum,
        w.fear_sum,
        w.sadness_sum,
        w.anger_sum,

        COALESCE(b.base_mentions, 0) AS base_mentions,
        COALESCE(b.base_buckets, 0) AS base_buckets
      FROM win w
      LEFT JOIN base b ON b.ticker = w.ticker
    )
    SELECT
      ticker,

      mentions,
      w_mentions,
      news_mentions,
      reddit_mentions,

      -- Baseline rate per 5m bucket (avg mentions per bucket)
      CASE
        WHEN base_buckets > 0 THEN base_mentions::double precision / base_buckets
        ELSE 0
      END AS base_rate_5m,

      -- Spike ratio vs baseline (add 1 to stabilize low baselines)
      (mentions::double precision / ( (CASE WHEN base_buckets > 0 THEN base_mentions::double precision / base_buckets ELSE 0 END) * (%s::double precision) + 1.0 ))
        AS spike_ratio,

      -- Means
      CASE WHEN sent_cnt > 0 THEN sent_sum / sent_cnt ELSE 0 END AS sentiment_mean,
      CASE WHEN mentions > 0 THEN conv_sum / mentions ELSE 0 END AS conviction_mean,

      -- Bullish agreement in [0, 1]
      CASE WHEN (bull + bear) > 0 THEN bull::double precision / (bull + bear) ELSE 0.5 END AS bullish_ratio,

      -- Emotion tilt (positive - negative)
      -- optimism+joy minus fear+sadness (anger not included in "hype")
      CASE
        WHEN mentions > 0 THEN
          ((optimism_sum + joy_sum) - (fear_sum + sadness_sum)) / mentions
        ELSE 0
      END AS emotion_tilt,

      -- Cross-source flag
      CASE WHEN (news_mentions > 0 AND reddit_mentions > 0) THEN 1 ELSE 0 END AS cross_source,

      -- Final score (weighted, explainable)
      (
        0.45 * LN(1 + (mentions::double precision)) +
        0.35 * LN(1 + (mentions::double precision)) * (mentions::double precision / ( ( (CASE WHEN base_buckets > 0 THEN base_mentions::double precision / base_buckets ELSE 0 END) * (%s::double precision) ) + 1.0 )) +
        0.10 * GREATEST(0, sentiment_mean) +
        0.10 * emotion_tilt +
        0.10 * conviction_mean +
        0.05 * (CASE WHEN (news_mentions > 0 AND reddit_mentions > 0) THEN 1 ELSE 0 END)
      ) AS score
    FROM joined
    WHERE mentions > 0
    ORDER BY score DESC
    LIMIT %s;
    """

    # Explanation of %s params:
    # - win_start
    # - base_start
    # - base_end (win_start)
    # - window buckets count approx = window_minutes / 5
    # - window buckets again for spike term
    # - limit
    win_buckets = max(1, window_minutes // 5)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (win_start, base_start, base_end, win_buckets, win_buckets, limit))
        rows = cur.fetchall()

    # Optional filter post-query (simple)
    if require_cross_source:
        rows = [r for r in rows if int(r["cross_source"]) == 1]

    # Normalize types for JSON response
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "ticker": r["ticker"],
                "score": float(r["score"] or 0.0),

                "mentions": int(r["mentions"] or 0),
                "weighted_mentions": float(r["w_mentions"] or 0.0),

                "spike_ratio": float(r["spike_ratio"] or 0.0),
                "base_rate_5m": float(r["base_rate_5m"] or 0.0),

                "sentiment_mean": float(r["sentiment_mean"] or 0.0),
                "bullish_ratio": float(r["bullish_ratio"] or 0.5),
                "conviction_mean": float(r["conviction_mean"] or 0.0),
                "emotion_tilt": float(r["emotion_tilt"] or 0.0),

                "news_mentions": int(r["news_mentions"] or 0),
                "reddit_mentions": int(r["reddit_mentions"] or 0),
                "cross_source": bool(int(r["cross_source"] or 0)),
            }
        )

    return out