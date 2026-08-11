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
    ) -> StageExecutionReport: ...


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
        reports: list[StageExecutionReport] = []
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
                StageExecutionPolicy.STORED_ONLY
                if stored_only
                else StageExecutionPolicy.COMPUTE
            )
            reports.append(
                stage.enrich(
                    items,
                    policy=policy,
                    force=force if policy is StageExecutionPolicy.COMPUTE else False,
                    availability_check=(None if stored_only else check_availability),
                )
            )
        return EnrichmentRunReport(items=items, stages=reports)
