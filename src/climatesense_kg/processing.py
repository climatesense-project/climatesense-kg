"""Small shared values for durable external processing results."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
import hashlib
import json
from typing import Any


def stable_hash(value: Any) -> str:
    """Hash a JSON-compatible semantic value deterministically."""

    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class ResultStatus(StrEnum):
    SUCCESS = "success"
    RETRYABLE_FAILURE = "retryable_failure"
    PERMANENT_FAILURE = "permanent_failure"


@dataclass(frozen=True)
class ProcessingResult:
    """One successful or failed external computation."""

    status: ResultStatus
    payload: dict[str, Any]
    retry_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.status is not ResultStatus.RETRYABLE_FAILURE and self.retry_at:
            raise ValueError("Only retryable failures may have a retry time")
        if self.retry_at is not None and self.retry_at.tzinfo is None:
            raise ValueError("Retry time must include timezone information")

    @property
    def succeeded(self) -> bool:
        return self.status is ResultStatus.SUCCESS

    @property
    def permanent(self) -> bool:
        return self.status is ResultStatus.PERMANENT_FAILURE

    def deferred(self, now: datetime | None = None) -> bool:
        return bool(self.retry_at and self.retry_at > (now or datetime.now(UTC)))

    @classmethod
    def success(cls, payload: dict[str, Any]) -> ProcessingResult:
        return cls(ResultStatus.SUCCESS, payload)

    @classmethod
    def retryable(
        cls,
        payload: dict[str, Any],
        *,
        retry_at: datetime | None = None,
    ) -> ProcessingResult:
        return cls(ResultStatus.RETRYABLE_FAILURE, payload, retry_at)

    @classmethod
    def permanent_failure(cls, payload: dict[str, Any]) -> ProcessingResult:
        return cls(ResultStatus.PERMANENT_FAILURE, payload)


@dataclass
class StageSummary:
    """Compact operational outcome for one stage."""

    name: str
    eligible: int = 0
    cached: int = 0
    succeeded: int = 0
    retryable_failures: int = 0
    permanent_failures: int = 0
    missing: int = 0
    available: bool | None = None

    @property
    def complete(self) -> bool:
        return self.missing == 0

    def merge(self, other: StageSummary) -> None:
        if self.name != other.name:
            raise ValueError("Stage summaries must have the same name")
        self.eligible += other.eligible
        self.cached += other.cached
        self.succeeded += other.succeeded
        self.retryable_failures += other.retryable_failures
        self.permanent_failures += other.permanent_failures
        self.missing += other.missing
        if other.available is False:
            self.available = False
        elif self.available is None:
            self.available = other.available

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "eligible": self.eligible,
            "cached": self.cached,
            "succeeded": self.succeeded,
            "retryable_failures": self.retryable_failures,
            "permanent_failures": self.permanent_failures,
            "missing": self.missing,
            "available": self.available,
            "complete": self.complete,
        }
