"""Ordered semantic enrichment orchestration."""

from __future__ import annotations

import logging
from typing import Protocol

from ..domain import CanonicalClaimReview

logger = logging.getLogger(__name__)


class EnrichmentStage(Protocol):
    """Minimal contract implemented by versioned enrichers."""

    name: str

    def is_available(self) -> bool: ...

    def enrich(
        self,
        items: list[CanonicalClaimReview],
        *,
        cached_only: bool = False,
        force: bool = False,
    ) -> list[CanonicalClaimReview]: ...


class EnrichmentRunner:
    """Run enabled enrichers without giving them orchestration concerns."""

    def __init__(self, stages: list[EnrichmentStage]) -> None:
        self.stages = stages

    def run(
        self,
        items: list[CanonicalClaimReview],
        *,
        cached_only: bool = False,
        force: bool = False,
    ) -> list[CanonicalClaimReview]:
        for stage in self.stages:
            if cached_only:
                stage.enrich(items, cached_only=True, force=False)
                continue
            if not stage.is_available():
                logger.warning(
                    "%s unavailable; applying only stored results", stage.name
                )
                stage.enrich(items, cached_only=True, force=False)
                continue
            stage.enrich(items, force=force)
        return items
