# services/collector/cursor_store.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import psycopg


@dataclass(frozen=True)
class CursorKey:
    """
    Identifies a stream we want to resume.
    Example:
      source="reddit"
      stream_id="reddit:wallstreetbets"
    """
    source: str
    stream_id: str


class CursorStore:
    """
    Postgres-backed cursor store.

    We store the "last published id" (cursor) so restarts don't duplicate.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with psycopg.connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS cursor_store (
                    source TEXT NOT NULL,
                    stream_id TEXT NOT NULL,
                    cursor TEXT NULL,
                    updated_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (source, stream_id)
                );
                """
            )
            conn.commit()

    def get_cursor(self, key: CursorKey) -> Optional[str]:
        with psycopg.connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT cursor
                FROM cursor_store
                WHERE source = %s AND stream_id = %s;
                """,
                (key.source, key.stream_id),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def set_cursor(self, key: CursorKey, cursor: str) -> None:
        now = datetime.now(timezone.utc)
        with psycopg.connect(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cursor_store (source, stream_id, cursor, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (source, stream_id)
                DO UPDATE SET
                    cursor = EXCLUDED.cursor,
                    updated_at = EXCLUDED.updated_at;
                """,
                (key.source, key.stream_id, cursor, now),
            )
            conn.commit()
