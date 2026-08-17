"""Minimal enrichment extension contract."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import logging
from typing import Any

from ..domain import CanonicalClaimReview
from ..processing import ProcessingResult, stable_hash


@dataclass
class EnrichmentSubject:
    """One unique semantic subject and all batch-local projections using it."""

    key: str
    input_hash: str
    targets: list[Any]


class Enricher:
    """Describe one external enrichment without owning persistence."""

    def __init__(
        self,
        name: str,
        *,
        version: str,
        semantic_config: dict[str, Any] | None = None,
        availability_key: str | None = None,
        batch_size: int = 25,
        max_workers: int = 1,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("Enricher batch size must be positive")
        if max_workers <= 0:
            raise ValueError("Enricher worker count must be positive")
        self.name = name
        self.version = version
        self.semantic_config = semantic_config or {}
        self.config_hash = stable_hash(self.semantic_config)
        self.availability_key = availability_key or name
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def subjects(self, items: list[CanonicalClaimReview]) -> list[EnrichmentSubject]:
        grouped: dict[str, list[CanonicalClaimReview]] = defaultdict(list)
        inputs: dict[str, str] = {}
        for item in self.eligible_items(items):
            key = self.subject_key(item)
            input_hash = stable_hash(self.input_value(item))
            previous = inputs.setdefault(key, input_hash)
            if previous != input_hash:
                raise RuntimeError(
                    f"Enricher {self.name} received conflicting input for {key}"
                )
            grouped[key].append(item)
        return [
            EnrichmentSubject(key, inputs[key], targets)
            for key, targets in grouped.items()
        ]

    def compute_batch(
        self,
        subjects: list[EnrichmentSubject],
    ) -> list[ProcessingResult]:
        """Compute one bounded work unit in subject order."""

        return self.compute_items([subject.targets[0] for subject in subjects])

    def apply(self, subject: EnrichmentSubject, payload: dict[str, Any]) -> None:
        for target in subject.targets:
            self.apply_item(target, payload)

    def eligible_items(
        self,
        items: list[CanonicalClaimReview],
    ) -> list[CanonicalClaimReview]:
        return items

    def compute_items(
        self,
        items: list[CanonicalClaimReview],
    ) -> list[ProcessingResult]:
        results: list[ProcessingResult] = []
        for item in items:
            try:
                results.append(self.compute_item(item))
            except Exception as exc:
                self.logger.error(
                    "Enricher %s failed for %s: %s", self.name, item.uri, exc
                )
                results.append(
                    ProcessingResult.retryable(
                        {"error_type": "stage_error", "error": str(exc)}
                    )
                )
        return results

    def is_available(self) -> bool:
        """Return whether the external dependency is ready."""
        raise NotImplementedError

    def subject_key(self, item: CanonicalClaimReview) -> str:
        """Return the durable identity of the semantic input."""
        raise NotImplementedError

    def input_value(self, item: CanonicalClaimReview) -> Any:
        """Return only the semantic input consumed by the enricher."""
        raise NotImplementedError

    def compute_item(self, item: CanonicalClaimReview) -> ProcessingResult:
        """Compute one result without mutating the review."""
        raise NotImplementedError

    def apply_item(
        self,
        item: CanonicalClaimReview,
        payload: dict[str, Any],
    ) -> None:
        """Apply one successful payload to a bounded projection."""
        raise NotImplementedError
