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


class EnrichedPost(BaseModel):
    """
    Output of the NLP enricher worker, published to TOPIC_ENRICHED_POSTS.
    """

    source: str
    post_id: str
    created_at: datetime
    text: str

    tickers: List[str] = Field(default_factory=list, description="Extracted tickers like ['AAPL']")
    language: Optional[str] = Field(default=None, description="ISO language code if detected")

    # Sentiment in range [-1, 1] (convention). Keep it consistent across the project.
    sentiment: float = Field(..., ge=-1.0, le=1.0)

    # Emotion scores in range [0, 1], do not force they sum to 1 unless your model guarantees it
    emotions: Dict[str, float] = Field(default_factory=dict)

    # Confidence in [0, 1] for the enrichment output as a whole
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)

    # Optional flags for downstream aggregation / filtering
    flags: List[Literal["low_confidence", "no_ticker", "non_english"]] = Field(default_factory=list)

    @field_validator("created_at")
    @classmethod
    def ensure_tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v


def to_jsonable_dict(model: BaseModel) -> Dict[str, Any]:
    """
    Convert a Pydantic model to a JSON-serializable dict.
    Pydantic will convert datetime to ISO format when using mode="json".
    """
    return model.model_dump(mode="json")
