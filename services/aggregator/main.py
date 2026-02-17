# services/aggregator/main.py

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import psycopg
from kafka import KafkaConsumer

from shared.schemas import EnrichedPost
from shared.settings import settings


# --- Aggregation window size ---
WINDOW_MINUTES = 5


def floor_time(dt: datetime, minutes: int) -> datetime:
    """
    Floors a datetime to the nearest window boundary.
    Example:
        12:07 with 5-min window → 12:05
    """
    discard = timedelta(
        minutes=dt.minute % minutes,
        seconds=dt.second,
        microseconds=dt.microsecond
    )
    return dt - discard


def main() -> None:
    """
    Aggregator:
    - consumes enriched posts
    - aggregates per ticker per time window
    - writes to Postgres
    """

    consumer = KafkaConsumer(
        settings.TOPIC_ENRICHED_POSTS,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="aggregator-group",
        auto_offset_reset="earliest"
    )

    conn = psycopg.connect(settings.DATABASE_URL_SYNC)
    conn.autocommit = True

    print("Aggregator started...")

    for message in consumer:
        enriched_data = message.value
        post = EnrichedPost(**enriched_data)

        if not post.tickers:
            continue

        bucket = floor_time(post.created_at, WINDOW_MINUTES)

        for ticker in post.tickers:
            upsert_aggregate(conn, ticker, bucket, post)


def upsert_aggregate(
    conn: psycopg.Connection,
    ticker: str,
    bucket: datetime,
    post: EnrichedPost
) -> None:
    """
    Inserts or updates a 5-minute aggregate row.
    """

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ticker_agg_5m (
                bucket_start,
                ticker,
                mention_count,
                sentiment_sum,
                sentiment_count
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (bucket_start, ticker)
            DO UPDATE SET
                mention_count = ticker_agg_5m.mention_count + EXCLUDED.mention_count,
                sentiment_sum = ticker_agg_5m.sentiment_sum + EXCLUDED.sentiment_sum,
                sentiment_count = ticker_agg_5m.sentiment_count + EXCLUDED.sentiment_count;
            """,
            (
                bucket,
                ticker,
                1,
                post.sentiment,
                1
            )
        )

    print(f"Aggregated ticker {ticker} at {bucket}")
