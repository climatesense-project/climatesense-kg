"""Configuration helpers for the analytics API service."""

from __future__ import annotations

from functools import lru_cache
import os

from pydantic import BaseModel, Field


def _build_default_dsn() -> str:
    env_dsn = os.getenv("ANALYTICS_DATABASE_DSN")
    if env_dsn:
        return env_dsn

    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "climatesense_cache")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")

    return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"


class Settings(BaseModel):
    """Runtime configuration sourced from environment variables."""

    database_dsn: str = Field(default_factory=_build_default_dsn)
    virtuoso_endpoint: str = Field(
        default_factory=lambda: os.getenv(
            "ANALYTICS_SPARQL_ENDPOINT",
            "http://virtuoso:8890/sparql",
        )
    )
    virtuoso_user: str | None = Field(
        default_factory=lambda: os.getenv("ANALYTICS_SPARQL_USER")
    )
    virtuoso_password: str | None = Field(
        default_factory=lambda: os.getenv("ANALYTICS_SPARQL_PASSWORD")
    )
    allowed_origins: list[str] = Field(
        default_factory=lambda: os.getenv(
            "ANALYTICS_ALLOWED_ORIGINS",
            "http://localhost:3000",
        ).split(",")
    )
    cache_ttl_seconds: int = Field(
        default_factory=lambda: int(os.getenv("ANALYTICS_CACHE_TTL", "60"))
    )
    sparql_timeout_seconds: int = Field(
        default_factory=lambda: int(os.getenv("ANALYTICS_SPARQL_TIMEOUT", "20"))
    )


@lru_cache
def get_settings() -> Settings:
    """Return cached settings instance."""

    return Settings()


settings = get_settings()
