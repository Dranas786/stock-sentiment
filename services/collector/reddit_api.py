# services/collector/reddit_api.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple

import praw

from shared.schemas import RawPost


@dataclass(frozen=True)
class RedditConfig:
    client_id: str
    client_secret: str
    user_agent: str


class RedditAPI:
    """
    Thin wrapper around PRAW.

    We use `post.fullname` (e.g., "t3_abc123") as:
      - a cursor (last seen)
      - the idempotent Kafka key

    This lets us resume ingestion safely even after restarts.
    """

    def __init__(self, cfg: RedditConfig) -> None:
        self._reddit = praw.Reddit(
            client_id=cfg.client_id,
            client_secret=cfg.client_secret,
            user_agent=cfg.user_agent,
        )

    def fetch_new_posts(
        self,
        subreddits: Iterable[str],
        since_fullname: Optional[str],
        limit_per_subreddit: int = 50,
    ) -> Tuple[List[RawPost], Optional[str]]:
        """
        Fetch newest posts from each subreddit.

        Strategy:
          - Pull the newest `limit_per_subreddit` items.
          - Stop when we hit `since_fullname` (the last cursor we published).
          - Return only posts newer than the cursor.
          - Also return the newest fullname we saw, so caller can persist it.

        Notes:
          - `subreddit.new()` returns newest first.
          - We reverse before returning so publishing happens oldest→newest
            (more natural ordering for downstream).
        """
        out: List[RawPost] = []
        newest_seen: Optional[str] = None

        for sr in subreddits:
            sr = sr.strip()
            if not sr:
                continue

            subreddit = self._reddit.subreddit(sr)
            batch = list(subreddit.new(limit=limit_per_subreddit))

            if not batch:
                continue

            # newest item is first in `.new()`
            if newest_seen is None:
                newest_seen = batch[0].fullname

            # If we have a cursor, take items until we reach it
            if since_fullname:
                newer = []
                for post in batch:
                    if post.fullname == since_fullname:
                        break
                    newer.append(post)
            else:
                newer = batch

            # publish oldest -> newest
            newer.reverse()

            for post in newer:
                created = datetime.fromtimestamp(post.created_utc, tz=timezone.utc)

                out.append(
                    RawPost(
                        source="reddit",
                        post_id=post.fullname,  # stable id across time
                        created_at=created,
                        author=str(post.author) if post.author else None,
                        text=f"{post.title}\n\n{post.selftext or ''}".strip(),
                        url=getattr(post, "url", None),
                        meta={
                            "subreddit": sr,
                            "permalink": f"https://www.reddit.com{post.permalink}",
                            "score": post.score,
                            "num_comments": post.num_comments,
                        },
                    )
                )

        # If we saw *any* posts at all, we want to advance the cursor to newest_seen.
        # Even if out is empty, keeping newest_seen can help avoid re-scanning.
        return out, newest_seen
