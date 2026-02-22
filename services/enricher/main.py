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


# --- Ticker extraction (entity-linking friendly) ---
CASHTAG_PATTERN = re.compile(r"(?<![A-Z0-9])\$?([A-Z]{1,5})(?![A-Z0-9])")
STOP_TOKENS = {
    "A", "I", "THE", "THIS", "THAT", "IMO", "USA", "GDP", "CEO", "CPI", "FED", "ETF",
    "YOLO", "DD", "EOD", "ATH", "FOMO", "SEC", "AI", "WSB", "LOL", "LMAO",
}

# Optional allowlist file (recommended for best quality)
# shared/symbols.txt: one ticker per line (e.g., TSLA, NVDA, AAPL)
def load_symbol_allowlist() -> Set[str]:
    here = os.path.dirname(__file__)
    # repo_root/services/enricher -> repo_root
    repo_root = os.path.abspath(os.path.join(here, "..", ".."))
    p = os.path.join(repo_root, "shared", "symbols.txt")
    if not os.path.exists(p):
        return set()
    allow = set()
    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            t = line.strip().upper()
            if not t or t.startswith("#"):
                continue
            allow.add(t)
    return allow


ALLOWLIST = load_symbol_allowlist()


def extract_tickers(text: str) -> List[str]:
    """
    High-precision ticker extraction:
      - supports $TSLA and TSLA tokens
      - filters stop tokens
      - if allowlist is present, requires membership (best precision)
    """
    t = text.upper()
    found: Set[str] = set()

    for m in CASHTAG_PATTERN.finditer(t):
        sym = m.group(1)
        if sym in STOP_TOKENS:
            continue
        if len(sym) == 1:
            continue
        if ALLOWLIST and sym not in ALLOWLIST:
            continue
        found.add(sym)

    return sorted(found)


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
        enable_auto_commit=True,
        max_poll_records=200,
    )

    producer = KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=10,
    )

    seen = TTLSet(ttl_s=3600, max_items=50000)

    print(
        f"[enricher] started (batch_size={batch_size}, max_length={max_length}, "
        f"allowlist={'on' if ALLOWLIST else 'off'})"
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
            tickers = extract_tickers(p.text or "")

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

            producer.send(settings.TOPIC_ENRICHED_POSTS, value=to_jsonable_dict(enriched))

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
                buffer.clear()

        except Exception as e:
            print(f"[enricher] ERROR: {type(e).__name__}: {e}")

    # If loop ever exits, flush remainder
    flush(buffer)


if __name__ == "__main__":
    main()
