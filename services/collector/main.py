# services/collector/main.py
from __future__ import annotations

import json
import os
import random
import time
from typing import List, Optional

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from shared.schemas import RawPost, to_jsonable_dict
from shared.settings import settings

from services.collector.cursor_store import CursorKey, CursorStore
from services.collector.reddit_api import RedditAPI, RedditConfig
from services.collector.news_api import NewsAPI, NewsAPIConfig


def _split_csv(env_value: Optional[str]) -> List[str]:
    if not env_value:
        return []
    return [x.strip() for x in env_value.split(",") if x.strip()]


def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _mode() -> str:
    return os.getenv("COLLECT_MODE", "slow").strip().lower()


def _poll_seconds(kind: str) -> int:
    m = _mode()
    if kind == "news":
        return _get_int("NEWS_POLL_SECONDS_FAST", 600) if m == "fast" else _get_int("NEWS_POLL_SECONDS_SLOW", 3600)
    if kind == "reddit":
        return _get_int("REDDIT_POLL_SECONDS_FAST", 60) if m == "fast" else _get_int("REDDIT_POLL_SECONDS_SLOW", 180)
    return 60


def _is_429(err: Exception) -> bool:
    # requests.HTTPError has .response with .status_code
    resp = getattr(err, "response", None)
    return bool(resp is not None and getattr(resp, "status_code", None) == 429)


def _retry_after_seconds(err: Exception) -> Optional[int]:
    resp = getattr(err, "response", None)
    if not resp:
        return None
    try:
        ra = resp.headers.get("Retry-After")
    except Exception:
        ra = None
    if not ra:
        return None
    try:
        return int(float(ra))
    except ValueError:
        return None


def _make_producer() -> KafkaProducer:
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
    futures = []
    for post in posts:
        payload = to_jsonable_dict(post)
        futures.append(
            producer.send(
                settings.TOPIC_RAW_POSTS,
                key=post.post_id,
                value=payload,
            )
        )

    for f in futures:
        f.get(timeout=30)

    producer.flush()


def main() -> None:
    cursor_store = CursorStore(settings.DATABASE_URL_SYNC)
    producer = _make_producer()

    # ---- Reddit (optional) ----
    reddit_client_id = os.getenv("REDDIT_CLIENT_ID")
    reddit_client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    reddit_user_agent = os.getenv("REDDIT_USER_AGENT", "stock-emotional-analysis/0.1 (by u/your_username)")
    reddit_subreddits = _split_csv(os.getenv("REDDIT_SUBREDDITS", ""))
    reddit_limit = _get_int("REDDIT_LIMIT_PER_SUBREDDIT", 50)

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

    # ---- News (optional) ----
    news_api_key = os.getenv("NEWS_API_KEY")
    news_queries = _split_csv(os.getenv("NEWS_QUERIES", ""))

    news: Optional[NewsAPI] = None
    if news_api_key and news_queries:
        news = NewsAPI(NewsAPIConfig(api_key=news_api_key))
        print(f"[collector] News enabled. Queries={news_queries[:5]}{'...' if len(news_queries) > 5 else ''}")
    else:
        print("[collector] News disabled (missing NEWS_API_KEY or NEWS_QUERIES).")

    # ---- Scheduling + throttles ----
    reddit_poll_s = _poll_seconds("reddit")
    news_poll_s = _poll_seconds("news")

    # News caps
    news_page_size = _get_int("NEWS_PAGE_SIZE", 20)
    news_max_queries_per_poll = _get_int("NEWS_MAX_QUERIES_PER_POLL", 2)
    news_max_articles_per_poll = _get_int("NEWS_MAX_ARTICLES_PER_POLL", 200)

    # Rate limit backoff
    backoff_base = _get_int("NEWS_BACKOFF_BASE_SECONDS", 30)
    backoff_max = _get_int("NEWS_BACKOFF_MAX_SECONDS", 1800)

    print(
        f"[collector] Started. mode={_mode()} "
        f"reddit_every={reddit_poll_s}s news_every={news_poll_s}s "
        f"news_page_size={news_page_size} max_queries_per_poll={news_max_queries_per_poll} "
        f"max_articles_per_poll={news_max_articles_per_poll} Producing to topic={settings.TOPIC_RAW_POSTS}"
    )

    next_reddit = time.time()
    next_news = time.time()

    news_backoff_until = 0.0
    news_backoff_s = backoff_base
    news_q_idx = 0  # rotate queries

    while True:
        now = time.time()

        # ---------------- Reddit ingestion ----------------
        if reddit and now >= next_reddit:
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

                    if newest_seen and newest_seen != last_fullname:
                        cursor_store.set_cursor(key, newest_seen)

            except Exception as e:
                print(f"[collector][reddit] ERROR: {type(e).__name__}: {e}")

            next_reddit = now + reddit_poll_s

        # ---------------- News ingestion ----------------
        if news and now >= next_news:
            try:
                if now < news_backoff_until:
                    wait_left = int(news_backoff_until - now)
                    print(f"[collector][news] backing off for {wait_left}s (rate limited)")
                else:
                    picked: List[str] = []
                    if news_queries:
                        n = min(news_max_queries_per_poll, len(news_queries))
                        for _ in range(n):
                            picked.append(news_queries[news_q_idx % len(news_queries)])
                            news_q_idx += 1

                    total_published = 0

                    key = CursorKey(source="news", stream_id="news:default")
                    last_iso = cursor_store.get_cursor(key)
                    newest_iso_overall = last_iso

                    for q in picked:
                        if total_published >= news_max_articles_per_poll:
                            break

                        articles, newest_iso = news.fetch_new_articles(
                            queries=[q],            # IMPORTANT: one query at a time
                            since_iso=last_iso,
                            page_size=news_page_size,
                            language="en",
                        )

                        if articles:
                            remaining = news_max_articles_per_poll - total_published
                            articles = articles[:remaining]

                            _publish_batch(producer, articles)
                            total_published += len(articles)
                            print(f"[collector][news] published {len(articles)} articles for query={q!r}")

                        if newest_iso and (newest_iso_overall is None or newest_iso > newest_iso_overall):
                            newest_iso_overall = newest_iso

                    print(f"[collector][news] cycle done. total_published={total_published}")

                    if newest_iso_overall and newest_iso_overall != last_iso:
                        cursor_store.set_cursor(key, newest_iso_overall)

                    news_backoff_s = backoff_base

            except Exception as e:
                if _is_429(e):
                    ra = _retry_after_seconds(e)
                    if ra is not None:
                        wait_s = ra
                    else:
                        wait_s = min(backoff_max, news_backoff_s) + random.randint(0, 10)
                        news_backoff_s = min(backoff_max, max(news_backoff_s * 2, backoff_base))

                    news_backoff_until = time.time() + wait_s
                    print(f"[collector][news] ERROR 429. Backing off {wait_s}s")
                else:
                    print(f"[collector][news] ERROR: {type(e).__name__}: {e}")

            next_news = now + news_poll_s

        time.sleep(1)


if __name__ == "__main__":
    main()