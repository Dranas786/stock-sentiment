# services/collector/main.py
from __future__ import annotations

import json
import os
import time
from typing import List, Optional

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from shared.schemas import RawPost, to_jsonable_dict
from shared.settings import settings

from services.collector.cursor_store import CursorKey, CursorStore
from services.collector.reddit_api import RedditAPI, RedditConfig
from services.collector.news_api import NewsAPI, NewsAPIConfig


# -------- Config via env vars (simple MVP style) --------
#
# Reddit:
#   REDDIT_CLIENT_ID
#   REDDIT_CLIENT_SECRET
#   REDDIT_USER_AGENT
#   REDDIT_SUBREDDITS="wallstreetbets,stocks,investing"
#
# News:
#   NEWS_API_KEY
#   NEWS_QUERIES="NVDA,TSLA,AAPL,AMD,MSFT,stock market,earnings"
#
# Polling:
#   COLLECTOR_POLL_SECONDS=30
#   REDDIT_LIMIT_PER_SUBREDDIT=50
#   NEWS_PAGE_SIZE=50
#
# Cursor streams:
#   We store per-stream cursor keys:
#     reddit:<subreddit>
#     news:<query-set-hash>  (for now: a single stream for all queries)


def _split_csv(env_value: Optional[str]) -> List[str]:
    if not env_value:
        return []
    return [x.strip() for x in env_value.split(",") if x.strip()]


def _make_producer() -> KafkaProducer:
    """
    Create a Kafka producer with retries for broker readiness.

    Note: kafka-python is not a true idempotent producer client, but:
      - we publish stable keys
      - we store cursors in Postgres
      => practical "no-duplicate on resume" behavior.
    """
    last_err: Optional[Exception] = None
    for attempt in range(1, 61):  # ~2 minutes max
        try:
            return KafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                linger_ms=10,
                retries=5,
                request_timeout_ms=10000,
                api_version_auto_timeout_ms=5000,
            )
        except NoBrokersAvailable as e:
            last_err = e
            print(f"[collector] Kafka not ready yet (attempt {attempt}/60): {e}")
            time.sleep(2)
    raise RuntimeError(f"Kafka never became available: {last_err}")


def _publish_batch(producer: KafkaProducer, posts: List[RawPost]) -> None:
    """
    Publish a batch to Kafka.

    We set Kafka message key = post.post_id
    so the message identity is stable across retries/restarts.
    """
    futures = []
    for post in posts:
        payload = to_jsonable_dict(post)
        futures.append(
            producer.send(
                settings.TOPIC_RAW_POSTS,
                key=post.post_id,     # idempotent-ish key
                value=payload,
            )
        )

    # Force delivery + surface errors
    for f in futures:
        f.get(timeout=30)

    producer.flush()


def main() -> None:
    poll_s = int(os.getenv("COLLECTOR_POLL_SECONDS", "30"))
    reddit_limit = int(os.getenv("REDDIT_LIMIT_PER_SUBREDDIT", "50"))
    news_page_size = int(os.getenv("NEWS_PAGE_SIZE", "50"))

    # Cursor store in Postgres
    cursor_store = CursorStore(settings.DATABASE_URL_SYNC)

    # Kafka producer
    producer = _make_producer()

    # ---- Reddit client (optional) ----
    reddit_client_id = os.getenv("REDDIT_CLIENT_ID")
    reddit_client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    reddit_user_agent = os.getenv("REDDIT_USER_AGENT", "stock-emotional-analysis/0.1 (by u/your_username)")
    reddit_subreddits = _split_csv(os.getenv("REDDIT_SUBREDDITS", ""))

    reddit: Optional[RedditAPI] = None
    if reddit_client_id and reddit_client_secret and reddit_subreddits:
        reddit = RedditAPI(
            RedditConfig(
                client_id=reddit_client_id,
                client_secret=reddit_client_secret,
                user_agent=reddit_user_agent,
            )
        )
        print(f"[collector] Reddit enabled. Subreddits={reddit_subreddits}")
    else:
        print("[collector] Reddit disabled (missing env vars or subreddits).")

    # ---- News client (optional) ----
    news_api_key = os.getenv("NEWS_API_KEY")
    news_queries = _split_csv(os.getenv("NEWS_QUERIES", ""))

    news: Optional[NewsAPI] = None
    if news_api_key and news_queries:
        news = NewsAPI(NewsAPIConfig(api_key=news_api_key))
        print(f"[collector] News enabled. Queries={news_queries[:5]}{'...' if len(news_queries) > 5 else ''}")
    else:
        print("[collector] News disabled (missing NEWS_API_KEY or NEWS_QUERIES).")

    print(f"[collector] Started. Poll interval={poll_s}s. Producing to topic={settings.TOPIC_RAW_POSTS}")

    while True:
        # ---------------- Reddit ingestion ----------------
        if reddit:
            try:
                for sr in reddit_subreddits:
                    key = CursorKey(source="reddit", stream_id=f"reddit:{sr}")
                    last_fullname = cursor_store.get_cursor(key)

                    posts, newest_seen = reddit.fetch_new_posts(
                        subreddits=[sr],
                        since_fullname=last_fullname,
                        limit_per_subreddit=reddit_limit,
                    )

                    if posts:
                        _publish_batch(producer, posts)
                        print(f"[collector][reddit] published {len(posts)} posts from r/{sr}")

                    # Advance cursor only after publishing succeeds
                    if newest_seen and newest_seen != last_fullname:
                        cursor_store.set_cursor(key, newest_seen)

            except Exception as e:
                # Isolate reddit failures (auth, rate limits, etc.)
                print(f"[collector][reddit] ERROR: {type(e).__name__}: {e}")

        # ---------------- News ingestion ----------------
        if news:
            try:
                # Treat the whole query list as one stream cursor
                key = CursorKey(source="news", stream_id="news:default")
                last_iso = cursor_store.get_cursor(key)

                articles, newest_iso = news.fetch_new_articles(
                    queries=news_queries,
                    since_iso=last_iso,
                    page_size=news_page_size,
                    language="en",
                )

                if articles:
                    _publish_batch(producer, articles)
                    print(f"[collector][news] published {len(articles)} articles")

                if newest_iso and newest_iso != last_iso:
                    cursor_store.set_cursor(key, newest_iso)

            except Exception as e:
                # Isolate news failures (bad key, quota, etc.)
                print(f"[collector][news] ERROR: {type(e).__name__}: {e}")

        time.sleep(poll_s)



if __name__ == "__main__":
    main()
