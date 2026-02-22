# shared/schemas.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator


def utcnow() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


class RawPost(BaseModel):
    """
    Raw event coming from a permitted connector (API feed, licensed source, etc.)
    This is what producers publish to TOPIC_RAW_POSTS.
    """

    source: str = Field(..., description="Connector name, e.g. 'reddit', 'news', 'demo'")
    post_id: str = Field(..., description="Unique id within the source")
    created_at: datetime = Field(default_factory=utcnow)
    author: Optional[str] = None
    text: str = Field(..., min_length=1)
    url: Optional[str] = None
    meta: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("created_at")
    @classmethod
    def ensure_tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            # make it explicit UTC if producer forgot tz
            return v.replace(tzinfo=timezone.utc)
        return v



# Widen flags so we don't fight the model pipeline.
EnrichedFlag = Literal[
    "low_confidence",
    "no_ticker",
    "non_english",
    "neutral",
    "low_signal",
    "empty_text",
    "duplicate",
]


class EnrichedPost(BaseModel):
    """
    Output of the HF enricher worker, published to TOPIC_ENRICHED_POSTS.
    """

    # Identity
    source: str
    post_id: str
    created_at: datetime

    # Carry-through content
    text: str
    author: Optional[str] = None
    url: Optional[str] = None
    meta: Dict[str, Any] = Field(default_factory=dict)

    # Entities
    tickers: List[str] = Field(default_factory=list)
    language: Optional[str] = None

    # Finance sentiment (FinBERT)
    finbert_pos: float = 0.0
    finbert_neg: float = 0.0
    finbert_neutral: float = 0.0
    finbert_score: float = Field(0.0, ge=-1.0, le=1.0)  # pos - neg

    # Emotions (model-dependent labels, probs typically [0,1])
    emotions: Dict[str, float] = Field(default_factory=dict)

    # Stance / intent (zero-shot)
    stance_label: str = "neutral"
    stance_confidence: float = Field(0.0, ge=0.0, le=1.0)
    stance_probs: Dict[str, float] = Field(default_factory=dict)

    # Derived signals
    conviction: float = Field(0.0, ge=0.0, le=1.0)
    source_weight: float = Field(1.0, ge=0.0)
    dedup_key: str = ""

    # Overall confidence (you can keep using it; for HF version we can set it from stance_confidence etc.)
    confidence: float = Field(0.5, ge=0.0, le=1.0)

    flags: List[EnrichedFlag] = Field(default_factory=list)

    enriched_at: datetime = Field(default_factory=utcnow)

    @field_validator("created_at", "enriched_at")
    @classmethod
    def ensure_tz_aware(cls, v: datetime) -> datetime:
        return v if v.tzinfo is not None else v.replace(tzinfo=timezone.utc)

def to_jsonable_dict(model: BaseModel) -> Dict[str, Any]:
    """
    Convert a Pydantic model to a JSON-serializable dict.
    Pydantic will convert datetime to ISO format when using mode="json".
    """
    return model.model_dump(mode="json")
