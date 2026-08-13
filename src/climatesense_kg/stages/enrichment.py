"""Semantic enrichment orchestration and completeness reporting."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import logging
from typing import Protocol

from ..domain import CanonicalClaimReview
from .persisted import StageExecutionPolicy, StageExecutionReport

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EnrichmentRunReport:
    """Enriched items and per-stage completeness evidence for one run."""

    items: list[CanonicalClaimReview]
    stages: list[StageExecutionReport]

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
        policy: StageExecutionPolicy = StageExecutionPolicy.COMPUTE,
        force: bool = False,
        availability_check: Callable[[], bool] | None = None,
        report_progress: bool = True,
    ) -> StageExecutionReport: ...


class EnrichmentRunner:
    """Coordinate enabled enrichers and aggregate completeness evidence."""

    def __init__(self, stages: list[EnrichmentStage]) -> None:
        self.stages = stages
        self._availability: dict[str, bool] = {}

    def start_run(self) -> None:
        """Reset dependency availability once per complete pipeline run."""

        self._availability.clear()

    def run(
        self,
        items: list[CanonicalClaimReview],
        *,
        stored_only: bool = False,
        force: bool = False,
        report_progress: bool = True,
    ) -> EnrichmentRunReport:
        reports: list[StageExecutionReport] = []
        for index, stage in enumerate(self.stages, start=1):
            if report_progress:
                logger.info(
                    "Enrichment stage %d/%d starting: %s",
                    index,
                    len(self.stages),
                    stage.stage_name,
                )

            def check_availability(current: EnrichmentStage = stage) -> bool:
                if current.availability_key not in self._availability:
                    try:
                        self._availability[current.availability_key] = (
                            current.is_available()
                        )
                    except Exception as exc:
                        logger.warning(
                            "%s availability check failed: %s", current.name, exc
                        )
                        self._availability[current.availability_key] = False
                return self._availability[current.availability_key]

            policy = (
                StageExecutionPolicy.STORED_ONLY
                if stored_only
                else StageExecutionPolicy.COMPUTE
            )
            report = stage.enrich(
                items,
                policy=policy,
                force=force if policy is StageExecutionPolicy.COMPUTE else False,
                availability_check=(None if stored_only else check_availability),
                report_progress=report_progress,
            )
            reports.append(report)
            if report_progress:
                logger.info(
                    "Enrichment stage %d/%d finished: %s; "
                    "eligible=%d, restored=%d, computed=%d, failed=%d "
                    "(deferred=%d, permanent=%d), missing=%d",
                    index,
                    len(self.stages),
                    stage.stage_name,
                    report.eligible_subjects,
                    report.stored_successes,
                    report.computed_successes,
                    report.computed_failures,
                    report.computed_deferred_failures,
                    report.computed_permanent_failures,
                    report.missing_results,
                )
        return EnrichmentRunReport(items=items, stages=reports)
