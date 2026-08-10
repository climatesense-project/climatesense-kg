"""Versioned enrichment-stage base class."""

from __future__ import annotations

from abc import ABC, abstractmethod
import logging
from typing import Any

from ..domain import CanonicalClaimReview
from ..persistence import StageResult, StageResultKey, StageResultStore


class Enricher(ABC):
    """Apply one semantic transformation with explicit persistent state."""

    def __init__(
        self,
        name: str,
        *,
        version: str,
        store: StageResultStore,
        **config: Any,
    ) -> None:
        self.name = name
        self.stage_name = f"enrichment.{name}"
        self.version = version
        self.store = store
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def enrich(
        self,
        items: list[CanonicalClaimReview],
        *,
        cached_only: bool = False,
        force: bool = False,
    ) -> list[CanonicalClaimReview]:
        """Apply stored or newly computed enrichment to canonical reviews."""

        keyed_items = [(item, self.result_key(item)) for item in items]
        stored_results = (
            {} if force else self.store.get_many([key for _item, key in keyed_items])
        )
        pending: list[tuple[CanonicalClaimReview, StageResultKey]] = []
        for item, key in keyed_items:
            stored = stored_results.get(key)
            if stored is not None:
                self._apply(item, stored.payload)
            elif not cached_only:
                pending.append((item, key))

        if pending:
            pending_items = [item for item, _key in pending]
            try:
                computed = self._compute_many(pending_items, force=force)
                if len(computed) != len(pending):
                    raise ValueError(
                        f"{self.name} returned {len(computed)} results "
                        f"for {len(pending)} inputs"
                    )
            except Exception as exc:
                self.logger.error("%s batch failed: %s", self.name, exc)
                computed = [
                    StageResult(
                        success=False,
                        payload={"error_type": "stage_error", "error": str(exc)},
                    )
                    for _item, _key in pending
                ]
            new_results = {
                key: result
                for (_item, key), result in zip(pending, computed, strict=True)
            }
            self.store.put_many(new_results)
            for (item, _key), result in zip(pending, computed, strict=True):
                self._apply(item, result.payload)

        if items:
            self.logger.info(
                "Applied %s to %d reviews (%d computed)",
                self.name,
                len(items),
                len(pending),
            )
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
                    StageResult(
                        success=False,
                        payload={"error_type": "stage_error", "error": str(exc)},
                    )
                )
        return results

    def result_key(self, item: CanonicalClaimReview) -> StageResultKey:
        return StageResultKey.build(
            subject_key=item.key,
            stage_name=self.stage_name,
            stage_version=self.version,
            input_value=self._input_value(item),
            config_value=self.config,
        )

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
        """Apply a stored stage payload to the typed domain model."""
