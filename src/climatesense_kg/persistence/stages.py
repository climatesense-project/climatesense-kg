"""Versioned semantic-stage result persistence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
import hashlib
import json
from threading import RLock
from typing import Any, Protocol

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


def stable_hash(value: Any) -> str:
    """Hash JSON-compatible stage input or configuration deterministically."""

    serialized = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(serialized.encode()).hexdigest()


@dataclass(frozen=True)
class StageResultKey:
    """Complete persistence identity for one semantic transformation."""

    subject_key: str
    stage_name: str
    stage_version: str
    input_hash: str
    config_hash: str

    @classmethod
    def build(
        cls,
        *,
        subject_key: str,
        stage_name: str,
        stage_version: str,
        input_value: Any,
        config_value: Any,
    ) -> StageResultKey:
        return cls(
            subject_key=subject_key,
            stage_name=stage_name,
            stage_version=stage_version,
            input_hash=stable_hash(input_value),
            config_hash=stable_hash(config_value),
        )


class StageResultStatus(StrEnum):
    """Durable outcome and retry disposition for a stage result."""

    SUCCESS = "success"
    RETRYABLE_FAILURE = "retryable_failure"
    PERMANENT_FAILURE = "permanent_failure"


@dataclass(frozen=True)
class StageResult:
    """Stored stage outcome including its retry schedule."""

    status: StageResultStatus
    payload: dict[str, Any]
    retry_at: datetime | None = None

    def __post_init__(self) -> None:
        if (
            self.status is not StageResultStatus.RETRYABLE_FAILURE
            and self.retry_at is not None
        ):
            raise ValueError("Only retryable failures may define retry_at")
        if self.retry_at is not None and self.retry_at.tzinfo is None:
            raise ValueError("retry_at must include timezone information")

    @property
    def success(self) -> bool:
        """Return whether this result can be restored and applied."""

        return self.status is StageResultStatus.SUCCESS

    @property
    def permanent(self) -> bool:
        """Return whether normal execution must retain rather than retry this result."""

        return self.status is StageResultStatus.PERMANENT_FAILURE

    def deferred(self, at: datetime | None = None) -> bool:
        """Return whether a retry is scheduled after the supplied instant."""

        return bool(self.retry_at and self.retry_at > (at or datetime.now(UTC)))

    @classmethod
    def succeeded(cls, payload: dict[str, Any]) -> StageResult:
        """Construct a successful reusable result."""

        return cls(status=StageResultStatus.SUCCESS, payload=payload)

    @classmethod
    def retryable_failure(
        cls,
        payload: dict[str, Any],
        *,
        retry_at: datetime | None = None,
    ) -> StageResult:
        """Construct a failure that may be attempted again."""

        return cls(
            status=StageResultStatus.RETRYABLE_FAILURE,
            payload=payload,
            retry_at=retry_at,
        )

    @classmethod
    def permanent_failure(cls, payload: dict[str, Any]) -> StageResult:
        """Construct a failure that requires explicit invalidation to retry."""

        return cls(status=StageResultStatus.PERMANENT_FAILURE, payload=payload)


class StageResultStore(Protocol):
    """Persistence contract for versioned semantic stage results."""

    def get(self, key: StageResultKey) -> StageResult | None: ...

    def get_many(
        self, keys: list[StageResultKey]
    ) -> dict[StageResultKey, StageResult]: ...

    def put(self, key: StageResultKey, result: StageResult) -> None: ...

    def put_many(self, results: dict[StageResultKey, StageResult]) -> None: ...

    def clear(self) -> int:
        """Delete recomputable stage state and return the result count."""
        ...


class InMemoryStageResultStore:
    """Small reference store used in stage unit tests."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._results: dict[StageResultKey, StageResult] = {}

    def get(self, key: StageResultKey) -> StageResult | None:
        with self._lock:
            return self._results.get(key)

    def get_many(self, keys: list[StageResultKey]) -> dict[StageResultKey, StageResult]:
        with self._lock:
            return {key: self._results[key] for key in keys if key in self._results}

    def put(self, key: StageResultKey, result: StageResult) -> None:
        with self._lock:
            self._results[key] = result

    def put_many(self, results: dict[StageResultKey, StageResult]) -> None:
        with self._lock:
            self._results.update(results)

    def clear(self) -> int:
        with self._lock:
            count = len(self._results)
            self._results.clear()
            return count


class PostgresStageResultStore:
    """PostgreSQL implementation of versioned semantic stage state."""

    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    def get(self, key: StageResultKey) -> StageResult | None:
        return self.get_many([key]).get(key)

    def get_many(self, keys: list[StageResultKey]) -> dict[StageResultKey, StageResult]:
        if not keys:
            return {}
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """
                    WITH requested AS (
                        SELECT *
                        FROM UNNEST(
                            %s::text[], %s::text[], %s::text[],
                            %s::text[], %s::text[]
                        ) AS keys(
                            subject_key, stage_name, stage_version,
                            input_hash, config_hash
                        )
                    )
                    SELECT results.subject_key, results.stage_name,
                           results.stage_version, results.input_hash,
                           results.config_hash, results.status, results.retry_at,
                           results.payload
                    FROM stage_results AS results
                    JOIN requested USING (
                        subject_key, stage_name, stage_version,
                        input_hash, config_hash
                    )
                    """,
                    (
                        [key.subject_key for key in keys],
                        [key.stage_name for key in keys],
                        [key.stage_version for key in keys],
                        [key.input_hash for key in keys],
                        [key.config_hash for key in keys],
                    ),
                )
                rows = cursor.fetchall()
        return {
            StageResultKey(
                subject_key=row["subject_key"],
                stage_name=row["stage_name"],
                stage_version=row["stage_version"],
                input_hash=row["input_hash"],
                config_hash=row["config_hash"],
            ): StageResult(
                status=StageResultStatus(row["status"]),
                payload=row["payload"],
                retry_at=row["retry_at"],
            )
            for row in rows
        }

    def put(self, key: StageResultKey, result: StageResult) -> None:
        self.put_many({key: result})

    def put_many(self, results: dict[StageResultKey, StageResult]) -> None:
        if not results:
            return
        rows = [
            (
                key.subject_key,
                key.stage_name,
                key.stage_version,
                key.input_hash,
                key.config_hash,
                result.status.value,
                result.retry_at,
                json.dumps(result.payload),
            )
            for key, result in results.items()
        ]
        with self.pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO stage_results (
                        subject_key, stage_name, stage_version,
                        input_hash, config_hash, status, retry_at, payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (
                        subject_key, stage_name, stage_version,
                        input_hash, config_hash
                    ) DO UPDATE SET
                        status = EXCLUDED.status,
                        retry_at = EXCLUDED.retry_at,
                        payload = EXCLUDED.payload,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    rows,
                )
                cursor.executemany(
                    """
                    INSERT INTO stage_result_attempts (
                        subject_key, stage_name, stage_version,
                        input_hash, config_hash, status, retry_at, payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    rows,
                )

    def clear(self) -> int:
        """Delete only recomputable stage results and their attempt history."""

        with self.pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM stage_results")
                row = cursor.fetchone()
                count = int(row[0]) if row else 0
                cursor.execute("DELETE FROM stage_result_attempts")
                cursor.execute("DELETE FROM stage_results")
        return count
