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
    """Complete cache identity for one semantic transformation."""

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

    def put(self, key: StageResultKey, result: StageResult) -> None: ...


class InMemoryStageResultStore:
    """Small reference store used in stage unit tests."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._results: dict[StageResultKey, StageResult] = {}

    def get(self, key: StageResultKey) -> StageResult | None:
        with self._lock:
            return self._results.get(key)

    def put(self, key: StageResultKey, result: StageResult) -> None:
        with self._lock:
            self._results[key] = result


class PostgresStageResultStore:
    """PostgreSQL implementation of versioned semantic stage state."""

    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    def get(self, key: StageResultKey) -> StageResult | None:
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """
                    SELECT success, payload
                    FROM stage_results
                    WHERE subject_key = %s
                      AND stage_name = %s
                      AND stage_version = %s
                      AND input_hash = %s
                      AND config_hash = %s
                    """,
                    (
                        key.subject_key,
                        key.stage_name,
                        key.stage_version,
                        key.input_hash,
                        key.config_hash,
                    ),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        return StageResult(success=row["success"], payload=row["payload"])

    def put(self, key: StageResultKey, result: StageResult) -> None:
        with self.pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
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
                    (
                        key.subject_key,
                        key.stage_name,
                        key.stage_version,
                        key.input_hash,
                        key.config_hash,
                        result.success,
                        json.dumps(result.payload),
                    ),
                )
