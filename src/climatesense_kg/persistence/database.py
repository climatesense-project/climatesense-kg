"""PostgreSQL connection ownership and schema migration."""

from __future__ import annotations

from importlib.resources import files
import logging
import os

from psycopg.conninfo import make_conninfo
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)


class PostgresDatabase:
    """Own the required PostgreSQL pool used by persistence services."""

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 5432,
        database: str = "climatesense",
        user: str = "postgres",
        password: str | None = None,
        min_connections: int = 1,
        max_connections: int = 20,
    ) -> None:
        conninfo = make_conninfo(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
        )
        self.pool = ConnectionPool(
            conninfo=conninfo,
            min_size=min_connections,
            max_size=max_connections,
            open=True,
        )
        self.require_available()
        self.migrate()

    @classmethod
    def from_environment(cls) -> PostgresDatabase:
        """Create the durable state database from standard environment variables."""

        return cls(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "climatesense"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )

    def require_available(self) -> None:
        """Fail immediately when authoritative persistence is unavailable."""

        with self.pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                if cursor.fetchone() != (1,):
                    raise RuntimeError("PostgreSQL readiness check failed")

    def migrate(self) -> None:
        """Apply packaged SQL migrations once, transactionally and in order."""

        migration_root = files("climatesense_kg.persistence.migrations")
        migrations = sorted(
            (
                resource
                for resource in migration_root.iterdir()
                if resource.name.endswith(".sql")
            ),
            key=lambda resource: resource.name,
        )
        with self.pool.connection() as connection:
            with connection.transaction(), connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        name TEXT PRIMARY KEY,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                cursor.execute("SELECT name FROM schema_migrations")
                applied = {row[0] for row in cursor.fetchall()}
                for migration in migrations:
                    if migration.name in applied:
                        continue
                    sql = migration.read_text(encoding="utf-8")
                    cursor.execute(sql, prepare=False)
                    cursor.execute(
                        "INSERT INTO schema_migrations (name) VALUES (%s)",
                        (migration.name,),
                    )
                    logger.info("Applied database migration %s", migration.name)

    def close(self) -> None:
        """Close all pooled PostgreSQL connections."""

        self.pool.close()

    def __enter__(self) -> PostgresDatabase:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
