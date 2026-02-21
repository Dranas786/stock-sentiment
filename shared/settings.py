# shared/settings.py
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Single source of truth for config across services.
    Reads from environment variables (and optionally a .env file).
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Kafka / Redpanda (your docker-compose exposes 9092)
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="redpanda:9092")

    # Topic names
    TOPIC_RAW_POSTS: str = Field(default="raw.posts")
    TOPIC_ENRICHED_POSTS: str = Field(default="enriched.posts")
    TOPIC_TICKER_AGG: str = Field(default="agg.ticker_window")

    # Postgres (matches your docker-compose)
    POSTGRES_HOST: str = Field(default="postgres")
    POSTGRES_PORT: int = Field(default=5432)
    POSTGRES_DB: str = Field(default="sentiment")
    POSTGRES_USER: str = Field(default="app")
    POSTGRES_PASSWORD: str = Field(default="app")

    # (FastAPI can use it; workers can use sync if you prefer)

    @property
    def DATABASE_URL_SYNC(self) -> str:
        # psycopg.connect() wants a normal PostgreSQL URI (NO "postgresql+psycopg://")
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )



settings = Settings()
