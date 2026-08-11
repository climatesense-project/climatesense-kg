"""Shared execution lifecycle for persisted semantic stages."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
import logging
import time
from typing import Any, TypedDict, TypeVar

from ..persistence import StageResult, StageResultKey, StageResultStore
from ..utils.progress import format_duration

SubjectT = TypeVar("SubjectT")


class StageExecutionSummary(TypedDict):
    """JSON-compatible representation of a stage execution report."""

    stage_name: str
    available: bool | None
    eligible_subjects: int
    stored_successes: int
    stored_failures: int
    computed_successes: int
    computed_failures: int
    missing_results: int
    complete: bool


class StageExecutionPolicy(Enum):
    """Whether a stage may compute results missing from durable state."""

    COMPUTE = "compute"
    STORED_ONLY = "stored_only"


@dataclass(frozen=True)
class StageExecutionReport:
    """Completeness evidence for one persisted semantic stage."""

    stage_name: str
    available: bool | None
    eligible_subjects: int
    stored_successes: int
    stored_failures: int
    computed_successes: int
    computed_failures: int
    missing_results: int

    @property
    def complete(self) -> bool:
        """Return whether every eligible subject has a successful result."""

        return self.missing_results == 0

    def to_dict(self) -> StageExecutionSummary:
        """Return a JSON-compatible operational summary."""

        return {
            "stage_name": self.stage_name,
            "available": self.available,
            "eligible_subjects": self.eligible_subjects,
            "stored_successes": self.stored_successes,
            "stored_failures": self.stored_failures,
            "computed_successes": self.computed_successes,
            "computed_failures": self.computed_failures,
            "missing_results": self.missing_results,
            "complete": self.complete,
        }


@dataclass(frozen=True)
class StageProgress:
    """Live progress snapshot emitted while a persisted stage computes."""

    stage_name: str
    eligible_subjects: int
    stored_successes: int
    stored_failures: int
    computed_successes: int
    computed_failures: int
    elapsed_seconds: float

    @property
    def computed_subjects(self) -> int:
        return self.computed_successes + self.computed_failures

    @property
    def processed_subjects(self) -> int:
        return self.stored_successes + self.computed_subjects

    @property
    def remaining_subjects(self) -> int:
        return max(0, self.eligible_subjects - self.processed_subjects)

    @property
    def percent_complete(self) -> float:
        if not self.eligible_subjects:
            return 100.0
        return 100 * self.processed_subjects / self.eligible_subjects

    @property
    def computation_rate(self) -> float | None:
        if self.elapsed_seconds <= 0 or not self.computed_subjects:
            return None
        return self.computed_subjects / self.elapsed_seconds

    @property
    def eta_seconds(self) -> float | None:
        rate = self.computation_rate
        if not rate or not self.remaining_subjects:
            return None
        return self.remaining_subjects / rate


class StageProgressLogger:
    """Rate-limit consistent live progress logs for a persisted stage."""

    def __init__(
        self,
        stage_logger: logging.Logger,
        *,
        label: str,
        interval_seconds: float = 10.0,
    ) -> None:
        if interval_seconds < 0:
            raise ValueError("Progress interval must be non-negative")
        self.stage_logger = stage_logger
        self.label = label
        self.interval_seconds = interval_seconds
        self.last_logged_elapsed: float | None = None

    def __call__(self, progress: StageProgress) -> None:
        should_log = (
            self.last_logged_elapsed is None
            or progress.remaining_subjects == 0
            or progress.elapsed_seconds - self.last_logged_elapsed
            >= self.interval_seconds
        )
        if not should_log:
            return
        self.last_logged_elapsed = progress.elapsed_seconds
        rate = progress.computation_rate
        self.stage_logger.info(
            "%s %s: %d/%d processed (%.1f%%); "
            "restored=%d, stored_failures=%d, computed=%d, failed=%d; "
            "rate=%s; ETA=%s",
            self.label,
            progress.stage_name,
            progress.processed_subjects,
            progress.eligible_subjects,
            progress.percent_complete,
            progress.stored_successes,
            progress.stored_failures,
            progress.computed_successes,
            progress.computed_failures,
            f"{rate:.2f}/s" if rate is not None else "n/a",
            format_duration(progress.eta_seconds),
        )


def execute_persisted_stage(
    *,
    stage_name: str,
    subjects: dict[StageResultKey, SubjectT],
    store: StageResultStore,
    compute_many: Callable[[list[SubjectT]], list[StageResult]],
    apply_result: Callable[[SubjectT, dict[str, Any]], None],
    policy: StageExecutionPolicy = StageExecutionPolicy.COMPUTE,
    force: bool = False,
    availability_check: Callable[[], bool] | None = None,
    stage_logger: logging.Logger,
    compute_batch_size: int | None = None,
    checkpoint_size: int | None = None,
    progress_callback: Callable[[StageProgress], None] | None = None,
) -> StageExecutionReport:
    """Restore, compute, checkpoint, and count semantic-subject results."""

    if compute_batch_size is not None and compute_batch_size <= 0:
        raise ValueError("compute_batch_size must be greater than zero")
    if checkpoint_size is not None and checkpoint_size <= 0:
        raise ValueError("checkpoint_size must be greater than zero")

    started = time.monotonic()
    keys = list(subjects)
    stored_results = {} if force else store.get_many(keys)
    pending: list[tuple[StageResultKey, SubjectT]] = []
    stored_successes = 0
    stored_failures = 0

    for key, subject in subjects.items():
        stored = stored_results.get(key)
        if stored is not None and stored.success:
            stored_successes += 1
            apply_result(subject, stored.payload)
            continue
        if stored is not None:
            stored_failures += 1
        pending.append((key, subject))

    computed_successes = 0
    computed_failures = 0

    def notify_progress() -> None:
        if progress_callback is None:
            return
        progress_callback(
            StageProgress(
                stage_name=stage_name,
                eligible_subjects=len(keys),
                stored_successes=stored_successes,
                stored_failures=stored_failures,
                computed_successes=computed_successes,
                computed_failures=computed_failures,
                elapsed_seconds=max(0.0, time.monotonic() - started),
            )
        )

    notify_progress()

    available: bool | None = None
    missing_results = 0
    if pending and policy is StageExecutionPolicy.STORED_ONLY:
        missing_results = len(pending)
    elif pending:
        if availability_check is None:
            available = True
        else:
            try:
                available = availability_check()
            except Exception as exc:
                stage_logger.warning(
                    "%s availability check failed: %s", stage_name, exc
                )
                available = False
        if available is False:
            stage_logger.warning(
                "%s unavailable; applying only stored successful results",
                stage_name,
            )
            missing_results = len(pending)
        else:
            batch_size = compute_batch_size or min(100, len(pending))
            persist_size = checkpoint_size or len(pending)
            checkpoint: dict[StageResultKey, StageResult] = {}

            def persist_checkpoint() -> None:
                if not checkpoint:
                    return
                store.put_many(checkpoint)
                checkpoint.clear()

            try:
                for start in range(0, len(pending), batch_size):
                    batch = pending[start : start + batch_size]
                    batch_subjects = [subject for _key, subject in batch]
                    try:
                        computed = compute_many(batch_subjects)
                        if len(computed) != len(batch):
                            raise ValueError(
                                f"{stage_name} returned {len(computed)} results "
                                f"for {len(batch)} subjects"
                            )
                    except Exception as exc:
                        stage_logger.error("%s batch failed: %s", stage_name, exc)
                        computed = [
                            StageResult(
                                success=False,
                                payload={
                                    "error_type": "stage_error",
                                    "error": str(exc),
                                },
                            )
                            for _entry in batch
                        ]

                    for (key, subject), result in zip(batch, computed, strict=True):
                        checkpoint[key] = result
                        if result.success:
                            computed_successes += 1
                            apply_result(subject, result.payload)
                        else:
                            computed_failures += 1
                            missing_results += 1
                        if len(checkpoint) >= persist_size:
                            persist_checkpoint()
                    notify_progress()
            except (KeyboardInterrupt, SystemExit):
                persist_checkpoint()
                raise
            persist_checkpoint()

    return StageExecutionReport(
        stage_name=stage_name,
        available=available,
        eligible_subjects=len(keys),
        stored_successes=stored_successes,
        stored_failures=stored_failures,
        computed_successes=computed_successes,
        computed_failures=computed_failures,
        missing_results=missing_results,
    )
