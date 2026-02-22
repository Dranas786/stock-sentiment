# services/aggregator/main.py
from __future__ import annotations

import json
import os
import time
import hashlib
from dataclasses import asdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import psycopg
from kafka import KafkaConsumer

from shared.settings import settings
from shared.schemas import EnrichedPost


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or not v.strip():
        return default
    return int(v)


def floor_to_5min(dt: datetime) -> datetime:
    """
    Floor dt to 5-minute bucket start (UTC).
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)


def author_key(source: str, author: str) -> str:
    """
    Stable author identity key for unique counting (within a 5m bucket + ticker).
    We hash to keep the table small and avoid encoding issues.
    """
    h = hashlib.sha256(f"{source}:{author}".encode("utf-8")).hexdigest()
    return h[:24]


def connect() -> psycopg.Connection:
    return psycopg.connect(settings.DATABASE_URL_SYNC)


def ensure_support_tables(conn: psycopg.Connection) -> None:
    """
    Ensure helper table exists for unique author counting.
    (Your init-db.sql doesn't need to include this; aggregator will create it.)
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ticker_author_5m (
                bucket_start TIMESTAMPTZ NOT NULL,
                ticker TEXT NOT NULL,
                author_key TEXT NOT NULL,
                PRIMARY KEY (bucket_start, ticker, author_key)
            );
            """
        )
    conn.commit()


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def upsert_agg(
    conn: psycopg.Connection,
    bucket_start: datetime,
    ticker: str,
    source: str,
    finbert_score: float,
    stance_label: str,
    conviction: float,
    emotions: Dict[str, float],
    source_weight: float,
) -> None:
    """
    Upsert per (bucket_start, ticker).

    Emotion sums are stored as JSONB where each key maps to a numeric sum.
    We *numerically* add keys instead of doing naive JSON merge.
    """
    news_inc = 1 if source == "news" else 0
    reddit_inc = 1 if source == "reddit" else 0

    bullish_inc = 1 if stance_label == "bullish" else 0
    bearish_inc = 1 if stance_label == "bearish" else 0
    neutral_inc = 1 if stance_label not in ("bullish", "bearish") else 0

    # We store just the emotions we got (model label set can vary)
    emotions_json = json_dumps(emotions)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ticker_agg_5m (
                bucket_start, ticker,
                mention_count, weighted_mentions, unique_authors,
                news_count, reddit_count,
                sentiment_sum, sentiment_count,
                bullish_count, bearish_count, neutral_count,
                conviction_sum,
                emotion_sums
            )
            VALUES (
                %s, %s,
                1, %s, 0,
                %s, %s,
                %s, 1,
                %s, %s, %s,
                %s,
                %s::jsonb
            )
            ON CONFLICT (bucket_start, ticker)
            DO UPDATE SET
                mention_count = ticker_agg_5m.mention_count + 1,
                weighted_mentions = ticker_agg_5m.weighted_mentions + EXCLUDED.weighted_mentions,
                news_count = ticker_agg_5m.news_count + EXCLUDED.news_count,
                reddit_count = ticker_agg_5m.reddit_count + EXCLUDED.reddit_count,

                sentiment_sum = ticker_agg_5m.sentiment_sum + EXCLUDED.sentiment_sum,
                sentiment_count = ticker_agg_5m.sentiment_count + 1,

                bullish_count = ticker_agg_5m.bullish_count + EXCLUDED.bullish_count,
                bearish_count = ticker_agg_5m.bearish_count + EXCLUDED.bearish_count,
                neutral_count = ticker_agg_5m.neutral_count + EXCLUDED.neutral_count,

                conviction_sum = ticker_agg_5m.conviction_sum + EXCLUDED.conviction_sum,

                -- numeric JSONB sum across union of keys
                emotion_sums = (
                    SELECT jsonb_object_agg(k, to_jsonb(
                        COALESCE((ticker_agg_5m.emotion_sums->>k)::double precision, 0)
                        +
                        COALESCE((EXCLUDED.emotion_sums->>k)::double precision, 0)
                    ))
                    FROM (
                        SELECT jsonb_object_keys(ticker_agg_5m.emotion_sums) AS k
                        UNION
                        SELECT jsonb_object_keys(EXCLUDED.emotion_sums) AS k
                    ) keys
                )
            ;
            """,
            (
                bucket_start,
                ticker,
                float(source_weight),
                int(news_inc),
                int(reddit_inc),
                float(finbert_score),
                int(bullish_inc),
                int(bearish_inc),
                int(neutral_inc),
                float(conviction),
                emotions_json,
            ),
        )


def try_insert_unique_author(
    conn: psycopg.Connection,
    bucket_start: datetime,
    ticker: str,
    source: str,
    author: Optional[str],
) -> bool:
    """
    Returns True if this (bucket, ticker, author) is new and should increment unique_authors.
    """
    if not author:
        return False

    akey = author_key(source, author)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ticker_author_5m (bucket_start, ticker, author_key)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING;
            """,
            (bucket_start, ticker, akey),
        )
        # psycopg rowcount is 1 if inserted, 0 if conflict
        return cur.rowcount == 1


def bump_unique_authors(conn: psycopg.Connection, bucket_start: datetime, ticker: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ticker_agg_5m
            SET unique_authors = unique_authors + 1
            WHERE bucket_start = %s AND ticker = %s;
            """,
            (bucket_start, ticker),
        )


def persist_optional(conn: psycopg.Connection, post: EnrichedPost) -> None:
    """
    Optional persistence into raw_posts/enriched_posts if those tables exist.
    Safe to keep for visibility / debugging.
    """
    with conn.cursor() as cur:
        # raw_posts
        try:
            cur.execute(
                """
                INSERT INTO raw_posts (source, post_id, created_at, text, author, url, meta)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (source, post_id) DO NOTHING;
                """,
                (
                    post.source,
                    post.post_id,
                    post.created_at,
                    post.text,
                    post.author,
                    post.url,
                    json_dumps(post.meta or {}),
                ),
            )
        except psycopg.errors.UndefinedTable:
            conn.rollback()

        # enriched_posts
        try:
            cur.execute(
                """
                INSERT INTO enriched_posts (source, post_id, created_at, tickers, finbert_score, stance_label, conviction, emotions, source_weight)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (source, post_id) DO NOTHING;
                """,
                (
                    post.source,
                    post.post_id,
                    post.created_at,
                    post.tickers,
                    float(post.finbert_score),
                    post.stance_label,
                    float(post.conviction),
                    json_dumps(post.emotions or {}),
                    float(post.source_weight),
                ),
            )
        except psycopg.errors.UndefinedTable:
            conn.rollback()


def main() -> None:
    max_poll_records = _env_int("AGG_MAX_POLL_RECORDS", 200)
    print(f"[aggregator] starting (max_poll_records={max_poll_records})")

    consumer = KafkaConsumer(
        settings.TOPIC_ENRICHED_POSTS,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="aggregator-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        max_poll_records=max_poll_records,
    )

    conn = connect()
    ensure_support_tables(conn)

    print("[aggregator] consuming enriched posts...")

    for msg in consumer:
        try:
            post = EnrichedPost(**msg.value)

            # Skip posts with no tickers (still persisted optionally)
            persist_optional(conn, post)

            if not post.tickers:
                conn.commit()
                continue

            bucket = floor_to_5min(post.created_at)

            # For each ticker mentioned, update aggregates
            for t in post.tickers:
                upsert_agg(
                    conn=conn,
                    bucket_start=bucket,
                    ticker=t,
                    source=post.source,
                    finbert_score=float(post.finbert_score),
                    stance_label=post.stance_label,
                    conviction=float(post.conviction),
                    emotions=post.emotions or {},
                    source_weight=float(post.source_weight),
                )

                # unique authors (only increment if the author is new for this bucket+ticker)
                if try_insert_unique_author(conn, bucket, t, post.source, post.author):
                    bump_unique_authors(conn, bucket, t)

            conn.commit()

        except Exception as e:
            conn.rollback()
            print(f"[aggregator] ERROR: {type(e).__name__}: {e}")

    conn.close()


if __name__ == "__main__":
    main()