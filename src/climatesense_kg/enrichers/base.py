"""Base class for persisted semantic enrichment stages."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable
import logging
from typing import Any

from ..domain import CanonicalClaimReview
from ..persistence import StageResult, StageResultKey, StageResultStore
from ..stages.persisted import (
    StageExecutionPolicy,
    StageExecutionReport,
    StageProgressLogger,
    execute_persisted_stage,
)


class Enricher(ABC):
    """Apply one semantic transformation with explicit persistent state."""

    def __init__(
        self,
        name: str,
        *,
        version: str,
        store: StageResultStore,
        semantic_config: dict[str, Any] | None = None,
        availability_key: str | None = None,
        compute_batch_size: int = 25,
        checkpoint_size: int = 100,
        progress_interval_seconds: float = 10.0,
    ) -> None:
        if compute_batch_size <= 0:
            raise ValueError("Compute batch size must be positive")
        if checkpoint_size <= 0:
            raise ValueError("Checkpoint size must be positive")
        if progress_interval_seconds < 0:
            raise ValueError("Progress interval must be non-negative")
        self.name = name
        self.stage_name = f"enrichment.{name}"
        self.version = version
        self.store = store
        self.semantic_config = semantic_config or {}
        self.availability_key = availability_key or self.stage_name
        self.compute_batch_size = min(compute_batch_size, checkpoint_size)
        self.checkpoint_size = checkpoint_size
        self.progress_interval_seconds = progress_interval_seconds
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def enrich(
        self,
        items: list[CanonicalClaimReview],
        *,
        policy: StageExecutionPolicy = StageExecutionPolicy.COMPUTE,
        force: bool = False,
        availability_check: Callable[[], bool] | None = None,
        report_progress: bool = True,
    ) -> StageExecutionReport:
        """Apply successful results and report semantic-subject completeness."""

        items_by_key: dict[StageResultKey, list[CanonicalClaimReview]] = defaultdict(
            list
        )
        for item in self._eligible_items(items):
            items_by_key[self.result_key(item)].append(item)

        def apply_group(
            group: list[CanonicalClaimReview], payload: dict[str, Any]
        ) -> None:
            for item in group:
                self._apply(item, payload)

        report = execute_persisted_stage(
            stage_name=self.stage_name,
            subjects=dict(items_by_key),
            store=self.store,
            compute_many=lambda groups: self._compute_many(
                [group[0] for group in groups], force=force
            ),
            apply_result=apply_group,
            policy=policy,
            force=force,
            availability_check=availability_check,
            stage_logger=self.logger,
            compute_batch_size=self.compute_batch_size,
            checkpoint_size=self.checkpoint_size,
            progress_callback=(
                StageProgressLogger(
                    self.logger,
                    label="Enrichment",
                    interval_seconds=self.progress_interval_seconds,
                )
                if report_progress
                else None
            ),
        )
        if items and report_progress:
            self.logger.info(
                "%s completeness: %d eligible, %d stored, %d computed, %d missing",
                self.name,
                report.eligible_subjects,
                report.stored_successes,
                report.computed_successes,
                report.missing_results,
            )
        return report

    def _eligible_items(
        self, items: list[CanonicalClaimReview]
    ) -> list[CanonicalClaimReview]:
        return items

    def _compute_many(
        self,
        items: list[CanonicalClaimReview],
        *,
        force: bool,
    ) -> list[StageResult]:
        """Compute results independently; batch stages can override this hook."""

        results: list[StageResult] = []
        for item in items:
            try:
                results.append(self._compute(item, force=force))
            except Exception as exc:
                self.logger.error("%s failed for %s: %s", self.name, item.uri, exc)
                results.append(
                    StageResult.retryable_failure(
                        {"error_type": "stage_error", "error": str(exc)}
                    )
                )
        return results

    def result_key(self, item: CanonicalClaimReview) -> StageResultKey:
        return StageResultKey.build(
            subject_key=self._subject_key(item),
            stage_name=self.stage_name,
            stage_version=self.version,
            input_value=self._input_value(item),
            config_value=self.semantic_config,
        )

    def _subject_key(self, item: CanonicalClaimReview) -> str:
        return item.key

    @abstractmethod
    def is_available(self) -> bool:
        """Return whether the external dependency is ready."""

    @abstractmethod
    def _input_value(self, item: CanonicalClaimReview) -> Any:
        """Return only semantic input consumed by this stage."""

    @abstractmethod
    def _compute(
        self, item: CanonicalClaimReview, *, force: bool = False
    ) -> StageResult:
        """Compute one stage result without mutating the review."""

    @abstractmethod
    def _apply(self, item: CanonicalClaimReview, payload: dict[str, Any]) -> None:
        """Apply a successful stage payload to the typed domain model."""
