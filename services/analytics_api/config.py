"""Configuration helpers for the analytics API service."""

from __future__ import annotations

from functools import lru_cache
import os

from pydantic import BaseModel, Field


def _build_default_dsn() -> str:
    env_dsn = os.getenv("ANALYTICS_DATABASE_DSN")
    if env_dsn:
        return env_dsn

    required_vars = {
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
    }
    missing = [k for k, v in required_vars.items() if not v]
    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}"
        )

    return (
        f"postgresql+asyncpg://{required_vars['POSTGRES_USER']}:"
        f"{required_vars['POSTGRES_PASSWORD']}@"
        f"{required_vars['POSTGRES_HOST']}:"
        f"{required_vars['POSTGRES_PORT']}/"
        f"{required_vars['POSTGRES_DB']}"
    )


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
    sparql_timeout_seconds: int = Field(
        default_factory=lambda: int(os.getenv("ANALYTICS_SPARQL_TIMEOUT", "20"))
    )


@lru_cache
def get_settings() -> Settings:
    """Return cached settings instance."""

    return Settings()


settings = get_settings()
