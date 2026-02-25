# services/enricher/main.py
from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections import deque
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Deque, Dict, Iterable, List, Optional, Set, Tuple

from kafka import KafkaConsumer, KafkaProducer

from shared.settings import settings
from shared.schemas import RawPost, EnrichedPost, to_jsonable_dict

from services.enricher.models import HFModels, normalize_text, make_conviction_score
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]   # /repo/services/enricher/main.py -> parents[2] == /repo
SYMBOLS_PATH = Path(os.getenv("SYMBOLS_PATH", str(REPO_ROOT / "shared" / "symbols.txt")))

def load_symbol_allowlist(path: Path = SYMBOLS_PATH) -> Optional[Set[str]]:
    """
    Returns a set of allowed ticker symbols.
    Returns None if file doesn't exist -> allowlist disabled (regex-only).
    """
    if not path.exists():
        return None

    symbols: Set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip().upper()
        if not s or s.startswith("#"):
            continue
        symbols.add(s)
    return symbols

ALLOWLIST = load_symbol_allowlist()
if ALLOWLIST is None:
    print(f"[enricher] allowlist disabled (missing {SYMBOLS_PATH})")
else:
    print(f"[enricher] loaded allowlist with {len(ALLOWLIST)} symbols")


# Example: supports $TSLA and TSLA, 1–5 letters (your current behavior)
TICKER_RE = re.compile(r"(?<![A-Z])\$?([A-Z]{1,5})(?![A-Z])")

STOP_TOKENS = {
    "A", "AN", "AND", "ARE", "AS", "AT",
    "BE", "BY",
    "CEO", "CFO", "COO",
    "FOR", "FROM",
    "HE", "HER", "HIS",
    "I", "IN", "IS", "IT",
    "OF", "ON", "OR", "OUR", "OUT",
    "THE", "TO",
    "US", "USA",
    "WAS", "WE", "WITH", "YOU",
    "DAY",
}

def extract_tickers(text: str, allowlist: set[str] | None = ALLOWLIST) -> list[str]:
    if not text:
        return []

    candidates = [m.group(1) for m in TICKER_RE.finditer(text.upper())]

    # basic cleanup + stopwords
    cleaned: list[str] = []
    for sym in candidates:
        if sym in STOP_TOKENS:
            continue
        # your old rule: 1–5 letters only (already enforced by regex)
        cleaned.append(sym)

    if not cleaned:
        return []

    # allowlist filter (if enabled)
    if allowlist is not None:
        cleaned = [s for s in cleaned if s in allowlist]

    # de-dupe while preserving order
    seen = set()
    out = []
    for s in cleaned:
        if s not in seen:
            seen.add(s)
            out.append(s)

    return out


# --- Dedup (prevents news repost spam) ---
def stable_hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def dedup_key(raw: RawPost) -> str:
    """
    Prefer URL-based dedup if url exists, else normalized text hash.
    """
    if raw.url:
        return "url:" + stable_hash(raw.url.strip().lower())
    txt = normalize_text(raw.text or "")
    return "txt:" + stable_hash(txt.lower())


class TTLSet:
    """
    Tiny in-memory TTL set for dedup across a time window.
    MVP: size-bounded + time-bounded deque.
    """
    def __init__(self, ttl_s: int = 3600, max_items: int = 20000) -> None:
        self.ttl_s = ttl_s
        self.max_items = max_items
        self.q: Deque[Tuple[float, str]] = deque()
        self.s: Set[str] = set()

    def add(self, key: str) -> None:
        now = time.time()
        self.q.append((now, key))
        self.s.add(key)
        self._evict(now)

    def contains(self, key: str) -> bool:
        now = time.time()
        self._evict(now)
        return key in self.s

    def _evict(self, now: float) -> None:
        cutoff = now - self.ttl_s
        while self.q and (self.q[0][0] < cutoff or len(self.s) > self.max_items):
            _, k = self.q.popleft()
            self.s.discard(k)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def main() -> None:
    # Batch size: tuneable for throughput on CPU
    batch_size = int(os.getenv("ENRICH_BATCH_SIZE", "16"))
    max_length = int(os.getenv("ENRICH_MAX_LENGTH", "256"))

    models = HFModels(
        batch_size=batch_size,
        max_length=max_length,
        device=None,  # auto CPU unless GPU available
    )

    consumer = KafkaConsumer(
        settings.TOPIC_RAW_POSTS,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="enricher-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=200,
    )

    producer = KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=10,
    )

    seen = TTLSet(ttl_s=3600, max_items=50000)

    print(
        f"[enricher] started (batch_size={batch_size}, max_length={max_length}, "
        f"allowlist={'on' if ALLOWLIST is not None else 'off'})"
    )

    # Micro-batching from Kafka
    buffer: List[RawPost] = []

    def flush(posts: List[RawPost]) -> None:
        if not posts:
            return

        texts = [normalize_text(p.text or "") for p in posts]

        fin = models.finbert_batch(texts)
        emo = models.emotion_batch(texts)
        stance = models.stance_batch(texts)

        for p, fin_s, emo_d, st in zip(posts, fin, emo, stance):
            tickers = extract_tickers(p.text or "", allowlist=ALLOWLIST)

            # Quality/flags (model-driven, explainable)
            flags: List[str] = []
            if not tickers:
                flags.append("no_ticker")
            if not (p.text or "").strip():
                flags.append("empty_text")

            # Source weighting: news tends to be more "catalyst" reliable than random social posts
            source_weight = 1.2 if p.source == "news" else 1.0

            conv = make_conviction_score(st, fin_s)

            enriched = EnrichedPost(
                source=p.source,
                post_id=p.post_id,
                created_at=p.created_at,
                author=p.author,
                text=p.text,
                url=p.url,
                meta=p.meta or {},

                tickers=tickers,

                # Finance sentiment
                finbert_pos=fin_s.pos,
                finbert_neg=fin_s.neg,
                finbert_neutral=fin_s.neutral,
                finbert_score=fin_s.score,

                # Emotion distribution (pass-through)
                emotions=emo_d,

                # Stance / intent
                stance_label=st.label,
                stance_confidence=st.confidence,
                stance_probs=st.probs,

                # Derived signals
                conviction=conv,
                source_weight=source_weight,
                dedup_key=dedup_key(p),
                flags=flags,
                enriched_at=utc_now(),
            )

            producer.send(
                settings.TOPIC_ENRICHED_POSTS,
                key=p.post_id,
                value=to_jsonable_dict(enriched),
            )

        producer.flush()

    for msg in consumer:
        try:
            raw = RawPost(**msg.value)

            # Dedup filter
            dk = dedup_key(raw)
            if seen.contains(dk):
                continue
            seen.add(dk)

            buffer.append(raw)

            if len(buffer) >= batch_size:
                flush(buffer)
                consumer.commit()
                buffer.clear()

        except Exception as e:
            print(f"[enricher] ERROR: {type(e).__name__}: {e}")

    # If loop ever exits, flush remainder
    flush(buffer)
    consumer.commit()


if __name__ == "__main__":
    main()
