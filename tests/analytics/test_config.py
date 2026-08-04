"""Tests for analytics API configuration."""

import importlib

import pytest
from sqlalchemy import make_url


def test_default_dsn_encodes_structured_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection_value = "p@ss:/?#[] value"
    for name, value in {
        "POSTGRES_HOST": "database.internal",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "climate db",
        "POSTGRES_USER": "analytics:user",
        "POSTGRES_PASSWORD": connection_value,
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("ANALYTICS_DATABASE_DSN", raising=False)
    config = importlib.import_module("services.analytics_api.config")

    parsed = make_url(config._build_default_dsn())

    assert parsed.username == "analytics:user"
    assert parsed.password == connection_value
    assert parsed.database == "climate db"
