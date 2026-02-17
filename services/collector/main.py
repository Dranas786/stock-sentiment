# services/collector/main.py

from __future__ import annotations

import json
import random
import time
from datetime import datetime, timezone
from typing import List

from kafka import KafkaProducer

from shared.schemas import RawPost, to_jsonable_dict
from shared.settings import settings


# --- Demo source (replace with real API connector later) ---

DEMO_TEXTS: List[str] = [
    "I think NVDA is going to dominate AI chips.",
    "TSLA is overhyped right now.",
    "AAPL earnings look strong this quarter.",
    "Why is everyone talking about AMD suddenly?",
    "MSFT cloud revenue keeps growing."
]

DEMO_SOURCES = ["demo_forum", "demo_news"]


def build_demo_post() -> RawPost:
    """
    Create a fake RawPost message.
    This simulates a connector pulling from an external API.
    """
    return RawPost(
        source=random.choice(DEMO_SOURCES),
        post_id=str(random.randint(100000, 999999)),
        created_at=datetime.now(timezone.utc),
        author="demo_user",
        text=random.choice(DEMO_TEXTS),
        url=None,
        meta={}
    )


def main() -> None:
    """
    Collector entrypoint.
    Creates a Kafka producer and continuously publishes raw posts.
    """

    producer = KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
        acks="all"
    )

    print("Collector started. Publishing demo posts...")

    while True:
        post = build_demo_post()
        payload = to_jsonable_dict(post)

        producer.send(settings.TOPIC_RAW_POSTS, value=payload)

        print(f"Produced: {post.text}")

        time.sleep(2)


if __name__ == "__main__":
    main()
