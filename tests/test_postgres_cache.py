"""Tests for the PostgreSQL-backed enrichment cache."""

from unittest.mock import patch

from psycopg.conninfo import conninfo_to_dict

from climatesense_kg.cache.postgres_cache import PostgresCache


def test_connection_parameters_are_escaped_structurally() -> None:
    connection_value = "quote' slash\\ space "
    with (
        patch("climatesense_kg.cache.postgres_cache.ConnectionPool") as pool,
        patch.object(PostgresCache, "_ensure_table_exists"),
    ):
        PostgresCache(
            host="database host",
            database="climate db",
            user="cache user",
            password=connection_value,
        )

    conninfo = pool.call_args.kwargs["conninfo"]
    parsed = conninfo_to_dict(conninfo)
    assert parsed["host"] == "database host"
    assert parsed["dbname"] == "climate db"
    assert parsed["user"] == "cache user"
    assert parsed["password"] == connection_value
