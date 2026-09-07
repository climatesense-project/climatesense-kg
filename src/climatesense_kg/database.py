"""PostgreSQL ownership and pipeline-run lifecycle."""

from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files
import logging
import os
from typing import Any, Literal, LiteralString, cast
from uuid import UUID, uuid4

from psycopg import Connection
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)

RunStatus = Literal["complete", "failed"]
_LOCK_NAME = "climatesense.pipeline"


@dataclass(frozen=True)
class PipelineRun:
    """Handle for one exclusively locked pipeline execution."""

    id: UUID


class Database:
    """Own the required PostgreSQL pool and authoritative schema."""

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
        self.pool = ConnectionPool(
            conninfo=make_conninfo(
                host=host,
                port=port,
                dbname=database,
                user=user,
                password=password,
            ),
            min_size=min_connections,
            max_size=max_connections,
            open=True,
        )
        self._run_connections: dict[UUID, Connection[Any]] = {}
        self.require_available()
        self.migrate()

    @classmethod
    def from_environment(cls) -> Database:
        """Create the database from the pipeline environment variables."""

        return cls(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "climatesense"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )

    def require_available(self) -> None:
        with self.pool.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            if cursor.fetchone() != (1,):
                raise RuntimeError("PostgreSQL readiness check failed")

    def migrate(self) -> None:
        """Apply ordered packaged migrations atomically, retaining existing data."""

        migrations = sorted(
            (
                item
                for item in files("climatesense_kg.persistence.migrations").iterdir()
                if item.name.endswith(".sql")
            ),
            key=lambda item: item.name,
        )
        with self.pool.connection() as connection:
            with connection.transaction(), connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    ("climatesense.schema",),
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        name TEXT PRIMARY KEY,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                cursor.execute("SELECT name FROM schema_migrations ORDER BY name")
                applied = [row[0] for row in cursor.fetchall()]
                names = [migration.name for migration in migrations]
                if applied != names[: len(applied)]:
                    raise RuntimeError(
                        "Database migrations do not match packaged history"
                    )
                pending = migrations[len(applied) :]
                if not pending:
                    return
                cursor.execute(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))",
                    (_LOCK_NAME,),
                )
                if cursor.fetchone() != (True,):
                    raise RuntimeError("Cannot migrate while a pipeline run is active")
                for migration in pending:
                    logger.info("Applying database schema %s", migration.name)
                    cursor.execute(
                        cast(LiteralString, migration.read_text(encoding="utf-8"))
                    )
                    cursor.execute(
                        "INSERT INTO schema_migrations (name) VALUES (%s)",
                        (migration.name,),
                    )

    def start_run(self, config_hash: str) -> PipelineRun:
        """Acquire the writer lock, recover an abandoned run, and start a run."""

        run = PipelineRun(uuid4())
        connection = self.pool.getconn()
        locked = False
        try:
            with connection.transaction(), connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
                    (_LOCK_NAME,),
                )
                row = cursor.fetchone()
                if row is None or not row[0]:
                    raise RuntimeError("Another pipeline run is already active")
                locked = True
                cursor.execute(
                    """
                    UPDATE pipeline_runs
                    SET status = 'failed',
                        error = 'Pipeline process ended before finalization',
                        updated_at = CURRENT_TIMESTAMP,
                        finished_at = CURRENT_TIMESTAMP
                    WHERE status = 'running'
                    """
                )
                cursor.execute(
                    """
                    INSERT INTO pipeline_runs (id, config_hash, status)
                    VALUES (%s, %s, 'running')
                    """,
                    (run.id, config_hash),
                )
        except Exception:
            if locked:
                self._unlock(connection)
            else:
                self.pool.putconn(connection)
            raise
        self._run_connections[run.id] = connection
        return run

    def finish_run(
        self,
        run: PipelineRun,
        *,
        status: RunStatus,
        error: str | None = None,
        summary: dict[str, Any] | None = None,
    ) -> None:
        """Persist the final run outcome and release its session lock."""

        connection = self._run_connections.pop(run.id, None)
        if connection is None:
            raise RuntimeError(f"Pipeline run {run.id} is not active")
        try:
            with connection.transaction(), connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE pipeline_runs
                    SET status = %s,
                        error = %s,
                        summary = %s,
                        updated_at = CURRENT_TIMESTAMP,
                        finished_at = CURRENT_TIMESTAMP
                    WHERE id = %s AND status = 'running'
                    """,
                    (status, error, Jsonb(summary or {}), run.id),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError(f"Pipeline run {run.id} could not be finalized")
        finally:
            self._unlock(connection)

    def _unlock(self, connection: Connection[Any]) -> None:
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                    (_LOCK_NAME,),
                )
            connection.commit()
        finally:
            self.pool.putconn(connection)

    def close(self) -> None:
        """Release owned sessions and close the pool."""

        for connection in self._run_connections.values():
            self._unlock(connection)
        self._run_connections.clear()
        self.pool.close()

    def __enter__(self) -> Database:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
