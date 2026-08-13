"""Run-scoped durable source observations for bounded pipeline execution."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
import logging
import time
from typing import Any, Protocol
from uuid import UUID, uuid4

from psycopg import Connection, Cursor
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from ..domain import (
    SourceReviewRecord,
    source_record_from_payload,
    source_record_to_payload,
)
from ..utils.memory import format_process_rss
from ..utils.text_processing import normalize_document_url

logger = logging.getLogger(__name__)

ObservationRow = tuple[UUID, str, int, str, str, str, Jsonb]


@dataclass(frozen=True)
class ObservationRun:
    """One materialized ingestion snapshot."""

    id: UUID


class ObservationStore(Protocol):
    """Persistence boundary for one pipeline run's source observations."""

    def start_run(self, signature: str) -> ObservationRun: ...

    def ingest_source(
        self,
        run_id: UUID,
        source_name: str,
        records: Iterable[SourceReviewRecord],
        *,
        batch_size: int,
    ) -> int: ...

    def iter_batches(
        self, run_id: UUID, *, batch_size: int, order_by_url: bool = False
    ) -> Iterator[list[SourceReviewRecord]]: ...

    def count(self, run_id: UUID) -> int: ...

    def finish_run(
        self,
        run_id: UUID,
        *,
        status: str,
        error: str | None = None,
        discard_observations: bool = True,
    ) -> None: ...


class InMemoryObservationStore:
    """Small transactional reference adapter used by orchestration tests."""

    def __init__(self) -> None:
        self._runs: dict[UUID, list[SourceReviewRecord]] = {}

    def start_run(self, signature: str) -> ObservationRun:
        del signature
        run = ObservationRun(uuid4())
        self._runs[run.id] = []
        return run

    def ingest_source(
        self,
        run_id: UUID,
        source_name: str,
        records: Iterable[SourceReviewRecord],
        *,
        batch_size: int,
    ) -> int:
        if batch_size <= 0:
            raise ValueError("Observation batch size must be positive")
        materialized = list(records)
        self._runs[run_id].extend(materialized)
        return len(materialized)

    def iter_batches(
        self, run_id: UUID, *, batch_size: int, order_by_url: bool = False
    ) -> Iterator[list[SourceReviewRecord]]:
        if batch_size <= 0:
            raise ValueError("Observation batch size must be positive")
        records = self._runs[run_id]
        if not order_by_url:
            for start in range(0, len(records), batch_size):
                yield records[start : start + batch_size]
            return
        ordered_records = sorted(
            records,
            key=lambda record: (
                normalize_document_url(record.document.observed_url)
                or record.document.observed_url,
                record.source.source_name,
                record.source.record_key,
            ),
        )
        keyed_records = (
            (
                normalize_document_url(record.document.observed_url)
                or record.document.observed_url,
                record,
            )
            for record in ordered_records
        )
        yield from _group_preserving_batches(keyed_records, batch_size=batch_size)

    def count(self, run_id: UUID) -> int:
        return len(self._runs[run_id])

    def finish_run(
        self,
        run_id: UUID,
        *,
        status: str,
        error: str | None = None,
        discard_observations: bool = True,
    ) -> None:
        del status, error
        if discard_observations:
            self._runs.pop(run_id, None)


class PostgresObservationStore:
    """PostgreSQL adapter that commits each source atomically and streams reads."""

    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool
        self._run_locks: dict[UUID, Connection[Any]] = {}

    def start_run(self, signature: str) -> ObservationRun:
        run = ObservationRun(uuid4())
        connection = self.pool.getconn()
        locked = False
        try:
            with connection.transaction(), connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT pg_try_advisory_lock(
                        hashtextextended('climatesense.pipeline', 0)
                    )
                    """
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
                    DELETE FROM ingestion_records
                    WHERE run_id IN (
                        SELECT id FROM pipeline_runs WHERE status = 'failed'
                          AND error = 'Pipeline process ended before finalization'
                    )
                    """
                )
                cursor.execute(
                    """
                    INSERT INTO pipeline_runs (id, signature, status)
                    VALUES (%s, %s, 'running')
                    """,
                    (run.id, signature),
                )
        except Exception:
            if locked:
                self._release_run_lock(connection)
            else:
                self.pool.putconn(connection)
            raise
        self._run_locks[run.id] = connection
        return run

    def ingest_source(
        self,
        run_id: UUID,
        source_name: str,
        records: Iterable[SourceReviewRecord],
        *,
        batch_size: int,
    ) -> int:
        if batch_size <= 0:
            raise ValueError("Observation batch size must be positive")

        count = 0
        pending: list[ObservationRow] = []
        started = time.monotonic()
        last_logged = started
        with self.pool.connection() as connection:
            with connection.transaction(), connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM ingestion_records WHERE run_id = %s AND source_name = %s",
                    (run_id, source_name),
                )
                for position, record in enumerate(records):
                    pending.append(
                        (
                            run_id,
                            source_name,
                            position,
                            record.source.record_key,
                            record.document.observed_url,
                            normalize_document_url(record.document.observed_url)
                            or record.document.observed_url,
                            Jsonb(source_record_to_payload(record)),
                        )
                    )
                    if len(pending) >= batch_size:
                        self._insert_batch(cursor, pending)
                        count += len(pending)
                        pending.clear()
                        now = time.monotonic()
                        if now - last_logged >= 10:
                            elapsed = now - started
                            logger.info(
                                "Ingestion: %s stored %d observations; "
                                "rate=%.2f/s; RSS=%s",
                                source_name,
                                count,
                                count / elapsed if elapsed > 0 else 0,
                                format_process_rss(),
                            )
                            last_logged = now
                if pending:
                    self._insert_batch(cursor, pending)
                    count += len(pending)
                cursor.execute(
                    "UPDATE pipeline_runs SET updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                    (run_id,),
                )
        elapsed = time.monotonic() - started
        logger.info(
            "Ingestion finished: %s stored %d observations; rate=%.2f/s; RSS=%s",
            source_name,
            count,
            count / elapsed if elapsed > 0 else 0,
            format_process_rss(),
        )
        return count

    @staticmethod
    def _insert_batch(cursor: Cursor[object], rows: list[ObservationRow]) -> None:
        cursor.executemany(
            """
            INSERT INTO ingestion_records (
                run_id, source_name, position, record_key,
                observed_url, document_key, payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )

    def iter_batches(
        self, run_id: UUID, *, batch_size: int, order_by_url: bool = False
    ) -> Iterator[list[SourceReviewRecord]]:
        if batch_size <= 0:
            raise ValueError("Observation batch size must be positive")
        cursor_name = f"observations_{run_id.hex}"
        with self.pool.connection() as connection:
            with connection.cursor(name=cursor_name, row_factory=dict_row) as cursor:
                query = (
                    """
                    SELECT document_key, payload
                    FROM ingestion_records
                    WHERE run_id = %s
                    ORDER BY document_key, source_name, position
                    """
                    if order_by_url
                    else """
                    SELECT payload
                    FROM ingestion_records
                    WHERE run_id = %s
                    ORDER BY source_name, position
                    """
                )
                cursor.execute(query, (run_id,))
                if not order_by_url:
                    while rows := cursor.fetchmany(batch_size):
                        yield [
                            source_record_from_payload(row["payload"]) for row in rows
                        ]
                    return

                keyed_records = (
                    (row["document_key"], source_record_from_payload(row["payload"]))
                    for rows in iter(lambda: cursor.fetchmany(batch_size), [])
                    for row in rows
                )
                yield from _group_preserving_batches(
                    keyed_records,
                    batch_size=batch_size,
                )

    def count(self, run_id: UUID) -> int:
        with self.pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM ingestion_records WHERE run_id = %s",
                    (run_id,),
                )
                row = cursor.fetchone()
        return int(row[0]) if row else 0

    def finish_run(
        self,
        run_id: UUID,
        *,
        status: str,
        error: str | None = None,
        discard_observations: bool = True,
    ) -> None:
        if status not in {"complete", "failed"}:
            raise ValueError(f"Unsupported pipeline run status: {status}")
        connection = self._run_locks.pop(run_id, None)
        owns_connection = connection is not None
        if connection is None:
            connection = self.pool.getconn()
        try:
            with connection.transaction(), connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE pipeline_runs
                    SET status = %s, error = %s,
                        updated_at = CURRENT_TIMESTAMP,
                        finished_at = %s
                    WHERE id = %s
                    """,
                    (status, error, datetime.now(UTC), run_id),
                )
                if discard_observations:
                    cursor.execute(
                        "DELETE FROM ingestion_records WHERE run_id = %s",
                        (run_id,),
                    )
        finally:
            if owns_connection:
                self._release_run_lock(connection)
            else:
                self.pool.putconn(connection)

    def _release_run_lock(self, connection: Connection[Any]) -> None:
        try:
            connection.rollback()
            with connection.transaction(), connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT pg_advisory_unlock(
                        hashtextextended('climatesense.pipeline', 0)
                    )
                    """
                )
        except Exception:
            connection.close()
            raise
        finally:
            self.pool.putconn(connection)


def _group_preserving_batches(
    records: Iterable[tuple[str, SourceReviewRecord]],
    *,
    batch_size: int,
) -> Iterator[list[SourceReviewRecord]]:
    """Yield target-sized batches without splitting adjacent document groups."""

    batch: list[SourceReviewRecord] = []
    group: list[SourceReviewRecord] = []
    group_key: str | None = None

    def append_group() -> Iterator[list[SourceReviewRecord]]:
        nonlocal batch, group
        if not group:
            return
        if batch and len(batch) + len(group) > batch_size:
            yield batch
            batch = []
        if len(group) >= batch_size:
            yield group
        else:
            batch.extend(group)
        group = []

    for document_key, record in records:
        if group and document_key != group_key:
            yield from append_group()
        group.append(record)
        group_key = document_key
    yield from append_group()
    if batch:
        yield batch
