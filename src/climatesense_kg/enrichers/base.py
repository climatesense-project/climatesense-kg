"""Enricher contract shared by all external enrichment stages.

An enricher groups canonical reviews into deduplicated subjects, computes one
durable result per subject through an external service, and applies the stored
payload back onto every review that shares the subject. The enrichment service
owns caching, availability probing, batching, and retries; subclasses fill in
the hooks below.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import logging
from typing import Any

from ..domain import CanonicalClaimReview
from ..processing import ProcessingResult, stable_hash


@dataclass
class EnrichmentSubject:
    """One deduplicated enrichment work unit.

    Computation runs once per subject, and ``apply`` passes the stored
    payload to every target of that subject.

    Attributes:
        key: Identity of the subject in the results cache.
        input_hash: Hash of the semantic input. The service recomputes a
            subject when this changes.
        targets: Every review sharing that key.
    """

    key: str
    input_hash: str
    targets: list[Any]


class Enricher:
    """Base class for enrichers backed by durable external computations.

    Cached results stay valid while the enricher version, the subject input
    hash, and the config hash all match what was stored. Subclasses implement
    the abstract hooks and may override the grouping and batching helpers; the
    enrichment service drives grouping, caching, healthchecks, concurrency, and
    applying stored payloads.
    """

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
        """Configure identity and cache invalidation for this enricher.

        Args:
            name: Identifier used as the cache namespace and in log lines.
            version: Version of the enrichment logic; cached results stored
                under a different version are recomputed.
            semantic_config: Settings that change computed results. Hashed
                into ``config_hash`` for cache lookups.
            availability_key: Key under which the availability probe is
                cached, so related enrichers share one check. Defaults to
                ``name``.
            batch_size: Subjects per work unit handed to ``compute_batch``.
            max_workers: Threads computing work units concurrently.

        Raises:
            ValueError: If ``batch_size`` or ``max_workers`` is not positive.
        """
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
        """Group reviews into subjects for external computation.

        Args:
            items: Canonical reviews from the current projection batch.

        Returns:
            One subject per distinct ``subject_key``, in first-seen order.

        Raises:
            RuntimeError: If two reviews sharing a key disagree on
                ``input_value``, because one stored result cannot serve both.
        """
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
        """Compute one result per subject, positionally aligned with them.

        The default implementation computes the first target of each subject
        through ``compute_items``. A batch API can override ``compute_items``
        to cover every item in one external call, or override this method
        directly.

        Args:
            subjects: Pending subjects to compute.

        Returns:
            One ``ProcessingResult`` per subject, in the same order.
        """
        return self.compute_items([subject.targets[0] for subject in subjects])

    def apply(self, subject: EnrichmentSubject, payload: dict[str, Any]) -> None:
        """Apply a stored payload to every review the subject targets.

        Args:
            subject: Subject whose targets receive the payload.
            payload: Result payload previously persisted for the subject key.
        """
        for target in subject.targets:
            self.apply_item(target, payload)

    def eligible_items(
        self,
        items: list[CanonicalClaimReview],
    ) -> list[CanonicalClaimReview]:
        """Return the reviews this enricher applies to.

        Args:
            items: Canonical reviews from the current projection batch.

        Returns:
            The reviews to enrich; all items by default.
        """
        return items

    def compute_items(
        self,
        items: list[CanonicalClaimReview],
    ) -> list[ProcessingResult]:
        """Compute each item in order and contain individual failures.

        Args:
            items: Representative reviews to compute, one per subject.

        Returns:
            One ``ProcessingResult`` per item, in the same order. An
            exception from ``compute_item`` is logged and returned as a
            retryable result, so one failing item never discards the others.
        """
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
        """Report whether the external service is reachable.

        Implementations should catch their own connection errors and return
        False.

        Returns:
            Whether computation may proceed. The enrichment service probes at
            most once per availability key per run and skips all computation
            when this returns False.
        """
        raise NotImplementedError

    def subject_key(self, item: CanonicalClaimReview) -> str:
        """Return the stable cache identity shared by equivalent reviews.

        Reviews with the same key share one computation and one stored result.

        Args:
            item: Canonical review to identify.

        Returns:
            Cache identity for ``item``; must be deterministic across runs.
        """
        raise NotImplementedError

    def input_value(self, item: CanonicalClaimReview) -> Any:
        """Return the JSON-compatible semantic input of one review.

        Args:
            item: Canonical review whose semantic input to describe.

        Returns:
            A JSON-serializable value. ``stable_hash`` turns it into
            ``input_hash``, so changing it recomputes cached subjects.
        """
        raise NotImplementedError

    def compute_item(self, item: CanonicalClaimReview) -> ProcessingResult:
        """Compute the external result for one review without mutating it.

        Mutations belong in ``apply_item`` so every target of the subject
        receives them. ``compute_item`` may run concurrently on worker
        threads. ``compute_items`` converts exceptions raised here into
        retryable results.

        Args:
            item: Representative review of the subject to compute.

        Returns:
            A success, retryable, or permanent result whose payload is
            JSON-serializable; it is persisted as JSONB and later passed to
            ``apply_item``.
        """
        raise NotImplementedError

    def apply_item(
        self,
        item: CanonicalClaimReview,
        payload: dict[str, Any],
    ) -> None:
        """Mutate one review with a previously stored payload.

        Runs for freshly computed and cached results alike. Implementations
        must handle any payload this enricher may have stored, and applying
        the same payload twice must leave the review in the same state as
        applying it once.

        Args:
            item: Review to mutate in place.
            payload: Result payload previously returned by ``compute_item``.
        """
        raise NotImplementedError
