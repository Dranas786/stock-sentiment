# services/enricher/main.py

from __future__ import annotations

import json
import re
from typing import Dict, List

from kafka import KafkaConsumer, KafkaProducer

from shared.schemas import RawPost, EnrichedPost, to_jsonable_dict
from shared.settings import settings


# --- Simple ticker extractor (replace later with better NLP) ---
TICKER_PATTERN = re.compile(r"\b[A-Z]{2,5}\b")


def extract_tickers(text: str) -> List[str]:
    """
    Very naive ticker extractor.
    Matches uppercase words 2-5 letters long.
    """
    return list(set(TICKER_PATTERN.findall(text)))


def simple_sentiment(text: str) -> float:
    """
    Dummy sentiment scorer.
    Replace with real model later (transformers, etc).
    Returns value in range [-1, 1].
    """
    positive_words = ["strong", "grow", "dominate", "good"]
    negative_words = ["bad", "overhyped", "crash", "panic"]

    score = 0
    for word in positive_words:
        if word in text.lower():
            score += 1

    for word in negative_words:
        if word in text.lower():
            score -= 1

    # normalize to [-1,1]
    return max(min(score / 3, 1), -1)


def simple_emotions(sentiment: float) -> Dict[str, float]:
    """
    Dummy emotion distribution derived from sentiment.
    Replace with real classifier later.
    """
    if sentiment > 0:
        return {
            "joy": sentiment,
            "fear": 0.1,
            "anger": 0.05,
            "sadness": 0.05,
            "surprise": 0.2
        }
    else:
        return {
            "joy": 0.05,
            "fear": abs(sentiment),
            "anger": 0.3,
            "sadness": 0.2,
            "surprise": 0.1
        }


def main() -> None:
    """
    Enricher worker:
    - consumes raw posts
    - applies NLP
    - publishes enriched posts
    """

    consumer = KafkaConsumer(
        settings.TOPIC_RAW_POSTS,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="enricher-group",
        auto_offset_reset="earliest"
    )

    producer = KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all"
    )

    print("Enricher started. Waiting for messages...")

    for message in consumer:
        raw_data = message.value

        raw_post = RawPost(**raw_data)

        tickers = extract_tickers(raw_post.text)
        sentiment = simple_sentiment(raw_post.text)
        emotions = simple_emotions(sentiment)

        flags: List[str] = []

        if not tickers:
            flags.append("no_ticker")

        if abs(sentiment) < 0.1:
            flags.append("low_confidence")

        enriched = EnrichedPost(
            source=raw_post.source,
            post_id=raw_post.post_id,
            created_at=raw_post.created_at,
            text=raw_post.text,
            tickers=tickers,
            language="en",
            sentiment=sentiment,
            emotions=emotions,
            confidence=0.8,
            flags=flags
        )

        producer.send(
            settings.TOPIC_ENRICHED_POSTS,
            value=to_jsonable_dict(enriched)
        )

        print(f"Enriched post {raw_post.post_id} with tickers {tickers}")
