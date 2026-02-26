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
    api_key: str
    base_url: str = "https://newsapi.org/v2"
    timeout_s: int = 10


def _iso_to_dt(s: str) -> datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _stable_article_id(url: str, published_at: str) -> str:
    h = hashlib.sha256(f"{url}|{published_at}".encode("utf-8")).hexdigest()[:24]
    return f"news_{h}"


class NewsAPI:
    def __init__(self, cfg: NewsAPIConfig) -> None:
        self._cfg = cfg

    def fetch_new_articles(
        self,
        queries: Iterable[str],
        since_iso: Optional[str],
        page_size: int = 50,
        language: str = "en",
    ) -> Tuple[List[RawPost], Optional[str]]:
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
                if since_dt and pub_dt <= since_dt:
                    continue
                newer_articles.append(a)

            newer_articles.sort(key=lambda a: a.get("publishedAt", ""))

            for a in newer_articles:
                pub = a.get("publishedAt")
                if not pub:
                    continue

                pub_dt = _iso_to_dt(pub)
                article_url = (a.get("url") or "").strip()
                title = (a.get("title") or "").strip()
                desc = (a.get("description") or "").strip()
                content = (a.get("content") or "").strip()
                source_name = ((a.get("source") or {}).get("name") or "").strip()

                # If URL is missing, still generate a stable-ish id using title+publishedAt
                stable_url = article_url if article_url else f"title:{title}"
                post_id = _stable_article_id(stable_url, pub)

                text = "\n\n".join([p for p in [title, desc, content] if p])

                out.append(
                    RawPost(
                        source="news",
                        post_id=post_id,
                        created_at=pub_dt,
                        author=(a.get("author") or None),
                        text=text if text else title,
                        url=article_url or None,
                        meta={"query": q, "source_name": source_name},
                    )
                )

        next_cursor_iso = newest_seen_dt.isoformat().replace("+00:00", "Z") if newest_seen_dt else None
        return out, next_cursor_iso