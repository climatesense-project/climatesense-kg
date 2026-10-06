"""Behavioral tests for the enrichment service contract."""

import logging
from threading import Barrier, Lock
import time
from unittest.mock import Mock

from climatesense_kg.domain import CanonicalClaimReview
from climatesense_kg.enrichers import Enricher
from climatesense_kg.enrichment import EnrichmentService
from climatesense_kg.processing import ProcessingResult, StageSummary


def test_disabled_enrichment_does_not_scan_review_projections() -> None:
    reader = Mock()
    service = EnrichmentService(Mock(), reader, [])

    assert service.run() == []
    reader.count.assert_not_called()
    reader.iter_batches.assert_not_called()


class _ConcurrentFixtureEnricher(Enricher):
    def __init__(self) -> None:
        super().__init__(
            "fixture.concurrent",
            version="1",
            batch_size=1,
            max_workers=3,
        )
        self.barrier = Barrier(3)
        self.lock = Lock()
        self.active = 0
        self.max_active = 0

    def is_available(self) -> bool:
        return True

    def subject_key(self, item: CanonicalClaimReview) -> str:
        return item.claim.uri

    def input_value(self, item: CanonicalClaimReview) -> str:
        return item.claim.text

    def compute_item(self, item: CanonicalClaimReview) -> ProcessingResult:
        index = int(item.claim.text)
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            if index < 3:
                self.barrier.wait(timeout=2)
            time.sleep(0.01 * (6 - index))
            return ProcessingResult.success({"value": item.claim.text})
        finally:
            with self.lock:
                self.active -= 1

    def apply_item(
        self,
        item: CanonicalClaimReview,
        payload: dict[str, object],
    ) -> None:
        item.claim.analysis.sentiment = str(payload["value"])


def test_enrichment_work_units_are_bounded_checkpointed_and_mapped(
    make_review,
    caplog,
) -> None:
    enricher = _ConcurrentFixtureEnricher()
    reviews = [make_review(claim=str(index)) for index in range(6)]
    service = EnrichmentService(
        Mock(),
        Mock(),
        [enricher],
        progress_interval_seconds=0,
    )
    service._load = Mock(return_value={})
    service._store = Mock()

    with caplog.at_level(logging.INFO, logger="climatesense_kg.enrichment"):
        summary = service._process_stage(
            enricher,
            reviews,
            offline=False,
            ignore_cache=False,
            batch_start=1,
            batch_end=6,
            total_reviews=6,
        )

    assert enricher.max_active == 3
    assert summary == StageSummary(
        enricher.name,
        eligible=6,
        succeeded=6,
        available=True,
    )
    assert service._store.call_count == 6
    assert [review.claim.analysis.sentiment for review in reviews] == [
        str(index) for index in range(6)
    ]
    assert "Enrichment [fixture.concurrent]: reviews 1-6/6" in caplog.text
    assert "Enrichment [fixture.concurrent] batch 1-6" in caplog.text
