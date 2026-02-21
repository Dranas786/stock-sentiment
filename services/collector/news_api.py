# services/collector/news_api.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple
import hashlib

import requests

from shared.schemas import RawPost


@dataclass(frozen=True)
class NewsAPIConfig:
    """
    Minimal config for NewsAPI-like providers.

    Typical base_url for NewsAPI.org:
      https://newsapi.org/v2

    We'll call /everything with q=... and sortBy=publishedAt.
    """
    api_key: str
    base_url: str = "https://newsapi.org/v2"
    timeout_s: int = 10


def _iso_to_dt(s: str) -> datetime:
    """
    News APIs often return ISO strings like:
      2026-02-17T06:52:37Z
    """
    # Handle trailing 'Z' = UTC
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _stable_article_id(url: str, published_at: str) -> str:
    """
    Build a stable idempotent id for an article.

    Why not use title? Titles can change.
    URL is usually stable. We include published_at for safety.
    """
    h = hashlib.sha256(f"{url}|{published_at}".encode("utf-8")).hexdigest()[:24]
    return f"news_{h}"


class NewsAPI:
    """
    Pulls news articles from a NewsAPI-like endpoint and converts to RawPost.

    Cursor strategy:
      - cursor is an ISO timestamp (publishedAt)
      - we request items sorted by publishedAt DESC
      - we keep only articles strictly newer than the cursor
      - then return newest publishedAt seen as next cursor

    This is "practically idempotent" + resumes cleanly.
    """

    def __init__(self, cfg: NewsAPIConfig) -> None:
        self._cfg = cfg

    def fetch_new_articles(
        self,
        queries: Iterable[str],
        since_iso: Optional[str],
        page_size: int = 50,
        language: str = "en",
    ) -> Tuple[List[RawPost], Optional[str]]:
        """
        Fetch newest articles for each query.

        - queries: e.g. ["NVDA", "stock market", "earnings"]
        - since_iso: cursor ISO timestamp (publishedAt) previously stored
        - returns: (raw_posts, next_cursor_iso)

        Note:
          Many news APIs have rate limits. Keep page_size modest.
        """
        out: List[RawPost] = []
        newest_seen_dt: Optional[datetime] = None
        since_dt: Optional[datetime] = _iso_to_dt(since_iso) if since_iso else None

        for q in queries:
            q = q.strip()
            if not q:
                continue

            url = f"{self._cfg.base_url}/everything"
            params = {
                "q": q,
                "sortBy": "publishedAt",
                "pageSize": page_size,
                "language": language,
            }
            headers = {"X-Api-Key": self._cfg.api_key}

            r = requests.get(url, params=params, headers=headers, timeout=self._cfg.timeout_s)
            r.raise_for_status()
            data = r.json()

            articles = data.get("articles", []) or []
            if not articles:
                continue

            # Articles are usually newest first with sortBy=publishedAt
            # Track newest publishedAt we observed in this fetch
            first_pub = articles[0].get("publishedAt")
            if first_pub:
                first_dt = _iso_to_dt(first_pub)
                if newest_seen_dt is None or first_dt > newest_seen_dt:
                    newest_seen_dt = first_dt

            newer_articles = []
            for a in articles:
                pub = a.get("publishedAt")
                if not pub:
                    continue
                pub_dt = _iso_to_dt(pub)

                # If we have a cursor, only include strictly newer
                if since_dt and pub_dt <= since_dt:
                    continue

                newer_articles.append(a)

            # publish oldest -> newest (so downstream sees time order)
            newer_articles.sort(key=lambda a: a.get("publishedAt", ""))

            for a in newer_articles:
                pub = a.get("publishedAt")
                if not pub:
                    continue

                pub_dt = _iso_to_dt(pub)
                article_url = a.get("url") or ""
                title = (a.get("title") or "").strip()
                desc = (a.get("description") or "").strip()
                content = (a.get("content") or "").strip()
                source_name = ((a.get("source") or {}).get("name") or "").strip()

                # Stable ID: deterministic from url + publishedAt
                post_id = _stable_article_id(article_url, pub)

                text = "\n\n".join([p for p in [title, desc, content] if p])

                out.append(
                    RawPost(
                        source="news",
                        post_id=post_id,
                        created_at=pub_dt,
                        author=(a.get("author") or None),
                        text=text if text else title,
                        url=article_url or None,
                        meta={
                            "query": q,
                            "source_name": source_name,
                        },
                    )
                )

        next_cursor_iso = newest_seen_dt.isoformat().replace("+00:00", "Z") if newest_seen_dt else None
        return out, next_cursor_iso
