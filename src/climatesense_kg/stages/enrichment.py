"""Semantic enrichment orchestration and completeness reporting."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
import logging
from typing import Any, Protocol, TypeVar

from ..domain import CanonicalClaimReview
from ..persistence import StageResult, StageResultKey, StageResultStore

logger = logging.getLogger(__name__)
SubjectT = TypeVar("SubjectT")


class EnrichmentExecutionPolicy(Enum):
    """Whether a stage may compute results missing from durable state."""

    COMPUTE = "compute"
    STORED_ONLY = "stored_only"


@dataclass(frozen=True)
class EnrichmentStageReport:
    """Completeness evidence for one enabled semantic stage."""

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

    def to_dict(self) -> dict[str, str | bool | int | None]:
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
class EnrichmentRunReport:
    """Enriched items and per-stage completeness evidence for one run."""

    items: list[CanonicalClaimReview]
    stages: list[EnrichmentStageReport]

    @property
    def complete(self) -> bool:
        return all(stage.complete for stage in self.stages)

    @property
    def incomplete_stage_names(self) -> set[str]:
        return {stage.stage_name for stage in self.stages if not stage.complete}


class EnrichmentStage(Protocol):
    """Contract implemented by persisted semantic enrichment stages."""

    name: str
    stage_name: str
    availability_key: str

    def is_available(self) -> bool: ...

    def enrich(
        self,
        items: list[CanonicalClaimReview],
        *,
        policy: EnrichmentExecutionPolicy = EnrichmentExecutionPolicy.COMPUTE,
        force: bool = False,
        availability_check: Callable[[], bool] | None = None,
    ) -> EnrichmentStageReport: ...


def execute_persisted_stage(
    *,
    stage_name: str,
    subjects: dict[StageResultKey, SubjectT],
    store: StageResultStore,
    compute_many: Callable[[list[SubjectT]], list[StageResult]],
    apply_result: Callable[[SubjectT, dict[str, Any]], None],
    policy: EnrichmentExecutionPolicy,
    force: bool,
    availability_check: Callable[[], bool] | None,
    stage_logger: logging.Logger,
) -> EnrichmentStageReport:
    """Restore, compute, persist, and count one stage's semantic subjects."""

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

    available: bool | None = None
    computed_successes = 0
    computed_failures = 0
    missing_results = 0
    if pending and policy is EnrichmentExecutionPolicy.STORED_ONLY:
        missing_results = len(pending)
    elif pending:
        try:
            available = availability_check() if availability_check else True
        except Exception as exc:
            stage_logger.warning("%s availability check failed: %s", stage_name, exc)
            available = False
        if not available:
            stage_logger.warning(
                "%s unavailable; applying only stored successful results",
                stage_name,
            )
            missing_results = len(pending)
        else:
            pending_subjects = [subject for _key, subject in pending]
            try:
                computed = compute_many(pending_subjects)
                if len(computed) != len(pending):
                    raise ValueError(
                        f"{stage_name} returned {len(computed)} results "
                        f"for {len(pending)} subjects"
                    )
            except Exception as exc:
                stage_logger.error("%s batch failed: %s", stage_name, exc)
                computed = [
                    StageResult(
                        success=False,
                        payload={"error_type": "stage_error", "error": str(exc)},
                    )
                    for _entry in pending
                ]

            new_results = {
                key: result
                for (key, _subject), result in zip(pending, computed, strict=True)
            }
            store.put_many(new_results)
            for (_key, subject), result in zip(pending, computed, strict=True):
                if result.success:
                    computed_successes += 1
                    apply_result(subject, result.payload)
                else:
                    computed_failures += 1
                    missing_results += 1

    return EnrichmentStageReport(
        stage_name=stage_name,
        available=available,
        eligible_subjects=len(keys),
        stored_successes=stored_successes,
        stored_failures=stored_failures,
        computed_successes=computed_successes,
        computed_failures=computed_failures,
        missing_results=missing_results,
    )


class EnrichmentRunner:
    """Coordinate enabled enrichers and aggregate completeness evidence."""

    def __init__(self, stages: list[EnrichmentStage]) -> None:
        self.stages = stages

    def run(
        self,
        items: list[CanonicalClaimReview],
        *,
        stored_only: bool = False,
        force: bool = False,
    ) -> EnrichmentRunReport:
        availability: dict[str, bool] = {}
        reports: list[EnrichmentStageReport] = []
        for stage in self.stages:

            def check_availability(current: EnrichmentStage = stage) -> bool:
                if current.availability_key not in availability:
                    try:
                        availability[current.availability_key] = current.is_available()
                    except Exception as exc:
                        logger.warning(
                            "%s availability check failed: %s", current.name, exc
                        )
                        availability[current.availability_key] = False
                return availability[current.availability_key]

            policy = (
                EnrichmentExecutionPolicy.STORED_ONLY
                if stored_only
                else EnrichmentExecutionPolicy.COMPUTE
            )
            reports.append(
                stage.enrich(
                    items,
                    policy=policy,
                    force=force
                    if policy is EnrichmentExecutionPolicy.COMPUTE
                    else False,
                    availability_check=(None if stored_only else check_availability),
                )
            )
        return EnrichmentRunReport(items=items, stages=reports)
