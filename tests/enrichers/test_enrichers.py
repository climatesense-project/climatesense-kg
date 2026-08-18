"""Behavioral tests for the minimal enrichment extension contract."""

import logging
from threading import Barrier, Lock
import time
from unittest.mock import Mock, patch
from uuid import uuid4

from climatesense_kg.domain import (
    CanonicalClaim,
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalReviewDocument,
    EntityMention,
)
from climatesense_kg.enrichers import (
    CimpleModelEnricher,
    DBpediaPropertyEnricher,
    DBpediaSpotlightEnricher,
)
from climatesense_kg.enrichers.base import Enricher
from climatesense_kg.enrichment import EnrichmentService
from climatesense_kg.processing import ProcessingResult, StageSummary


def _review(
    *, claim: str = "A climate claim", body: str = "A review body"
) -> CanonicalClaimReview:
    return CanonicalClaimReview(
        id=uuid4(),
        claim=CanonicalClaim(claim),
        organization=CanonicalOrganization(
            uri="https://example.test/organization",
            name="Example",
            website="https://example.test",
        ),
        document=CanonicalReviewDocument(
            id=uuid4(),
            urls={"https://example.test/review"},
            preferred_url="https://example.test/review",
            content=body,
        ),
        source_record_keys={"record"},
        source_names={"source"},
    )


def test_disabled_enrichment_does_not_scan_review_projections() -> None:
    reader = Mock()
    service = EnrichmentService(Mock(), reader, [])

    assert service.run() == []
    reader.count.assert_not_called()
    reader.iter_batches.assert_not_called()


def test_spotlight_claim_subject_is_shared_by_claim_uri() -> None:
    enricher = DBpediaSpotlightEnricher(target="claim")
    reviews = [_review(), _review()]

    subjects = enricher.subjects(reviews)

    assert len(subjects) == 1
    assert len(subjects[0].targets) == 2
    assert subjects[0].key == reviews[0].claim.uri


def test_spotlight_review_subject_is_shared_by_exact_body() -> None:
    enricher = DBpediaSpotlightEnricher(target="review")
    reviews = [_review(body="The same exact body"), _review(body="The same exact body")]

    subjects = enricher.subjects(reviews)

    assert len(subjects) == 1
    assert subjects[0].key.startswith("review-text/")


def test_spotlight_computes_and_applies_entity_payload() -> None:
    enricher = DBpediaSpotlightEnricher(target="claim")
    review = _review()
    entity = EntityMention(
        uri="http://dbpedia.org/resource/Climate_change",
        source="dbpedia_spotlight",
    )
    with patch.object(enricher, "_extract_entities", return_value=[entity]):
        subject = enricher.subjects([review])[0]
        result = enricher.compute_batch([subject])[0]
        enricher.apply(subject, result.payload)

    assert result.succeeded
    assert review.claim.analysis.entities[0].uri == entity.uri


def test_spotlight_worker_count_is_operational_configuration() -> None:
    serial = DBpediaSpotlightEnricher(target="claim", max_workers=1)
    concurrent = DBpediaSpotlightEnricher(target="claim", max_workers=8)

    assert serial.config_hash == concurrent.config_hash


def test_cimple_semantic_configuration_controls_cache_identity() -> None:
    first = CimpleModelEnricher(
        model="emotion", model_version="1", max_length=128, rate_limit_delay=0
    )
    operational_change = CimpleModelEnricher(
        model="emotion",
        model_version="1",
        max_length=128,
        timeout=999,
        rate_limit_delay=0,
    )
    semantic_change = CimpleModelEnricher(
        model="emotion", model_version="2", max_length=128, rate_limit_delay=0
    )

    assert first.config_hash == operational_change.config_hash
    assert first.config_hash != semantic_change.config_hash


def test_cimple_batch_result_is_applied_to_claim_analysis() -> None:
    enricher = CimpleModelEnricher(model="emotion", rate_limit_delay=0)
    review = _review()
    subject = enricher.subjects([review])[0]
    with patch.object(
        enricher,
        "_call_model",
        return_value=[{"value": "concern"}],
    ):
        result = enricher.compute_batch([subject])[0]
    enricher.apply(subject, result.payload)

    assert result == ProcessingResult.success({"value": "concern"})
    assert review.claim.analysis.emotion == "concern"


def test_property_enricher_groups_and_applies_entity_properties() -> None:
    property_uri = "http://example.test/property"
    enricher = DBpediaPropertyEnricher(properties=[property_uri])
    reviews = [_review(), _review()]
    for review in reviews:
        review.claim.analysis.entities.append(
            EntityMention(
                uri="http://dbpedia.org/resource/Climate_change",
                source="dbpedia_spotlight",
            )
        )

    subject = enricher.subjects(reviews)[0]
    payload = {
        "properties": {
            property_uri: [
                {
                    "value": "example",
                    "value_type": "literal",
                    "datatype": None,
                    "language": "en",
                }
            ]
        }
    }
    enricher.apply(subject, payload)

    assert len(subject.targets) == 2
    assert all(
        entity.properties[property_uri][0].value == "example"
        for entity in subject.targets
    )


def test_property_dependency_healthcheck_is_bounded() -> None:
    enricher = DBpediaPropertyEnricher(properties=[])
    response = Mock(status_code=200)
    with patch("requests.get", return_value=response) as request:
        assert enricher.is_available()
    request.assert_called_once()


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
    caplog,
) -> None:
    enricher = _ConcurrentFixtureEnricher()
    reviews = [_review(claim=str(index)) for index in range(6)]
    service = EnrichmentService(
        Mock(),
        Mock(),
        [enricher],
        progress_interval_seconds=0,
    )
    service._load = Mock(return_value={})  # type: ignore[method-assign]
    service._store = Mock()  # type: ignore[method-assign]

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
