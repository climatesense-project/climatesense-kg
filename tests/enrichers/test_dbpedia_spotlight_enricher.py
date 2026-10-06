"""Behavioral tests for the DBpedia Spotlight enricher."""

from unittest.mock import patch

from climatesense_kg.domain import EntityMention
from climatesense_kg.enrichers import DBpediaSpotlightEnricher


def test_spotlight_claim_subject_is_shared_by_claim_uri(make_review) -> None:
    enricher = DBpediaSpotlightEnricher(target="claim")
    reviews = [make_review(), make_review()]

    subjects = enricher.subjects(reviews)

    assert len(subjects) == 1
    assert len(subjects[0].targets) == 2
    assert subjects[0].key == reviews[0].claim.uri


def test_spotlight_review_subject_is_shared_by_exact_body(make_review) -> None:
    enricher = DBpediaSpotlightEnricher(target="review")
    reviews = [
        make_review(body="The same exact body"),
        make_review(body="The same exact body"),
    ]

    subjects = enricher.subjects(reviews)

    assert len(subjects) == 1
    assert subjects[0].key.startswith("review-text/")


def test_spotlight_computes_and_applies_entity_payload(make_review) -> None:
    enricher = DBpediaSpotlightEnricher(target="claim")
    review = make_review()
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
