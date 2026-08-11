"""Versioned semantic-stage result persistence."""

from __future__ import annotations

from dataclasses import dataclass
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


@dataclass(frozen=True)
class StageResult:
    """Stored stage outcome including explicit failure state."""

    success: bool
    payload: dict[str, Any]


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
                           results.config_hash, results.success, results.payload
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
            ): StageResult(success=row["success"], payload=row["payload"])
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
                result.success,
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
                        input_hash, config_hash, success, payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (
                        subject_key, stage_name, stage_version,
                        input_hash, config_hash
                    ) DO UPDATE SET
                        success = EXCLUDED.success,
                        payload = EXCLUDED.payload,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    rows,
                )
                cursor.executemany(
                    """
                    INSERT INTO stage_result_attempts (
                        subject_key, stage_name, stage_version,
                        input_hash, config_hash, success, payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
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
