"""Focused tests for typed, versioned enrichers."""

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
    BertFactorsEnricher,
    DBpediaEnricher,
    DBpediaPropertyEnricher,
)
from climatesense_kg.persistence import InMemoryStageResultStore


def _review(text: str = "Climate change is a reviewed claim") -> CanonicalClaimReview:
    url = "https://example.test/review"
    return CanonicalClaimReview(
        id=uuid4(),
        claim=CanonicalClaim(text=text),
        organization=CanonicalOrganization(
            uri="https://data.example.test/organization/example",
            name="Example",
            website="https://example.test",
        ),
        document=CanonicalReviewDocument(id=uuid4(), urls={url}, preferred_url=url),
        source_record_keys={"record"},
        source_names={"source"},
    )


@patch("climatesense_kg.enrichers.dbpedia_enricher.requests.post")
def test_dbpedia_stage_restores_typed_entities_from_versioned_state(post: Mock) -> None:
    response = post.return_value
    response.json.return_value = {
        "Resources": [
            {
                "@URI": "http://dbpedia.org/resource/Climate_change",
                "@surfaceForm": "Climate change",
                "@types": "DBpedia:Concept",
                "@similarityScore": "0.9",
                "@support": "100",
                "@offset": "0",
            }
        ]
    }
    store = InMemoryStageResultStore()
    enricher = DBpediaEnricher(store=store, rate_limit_delay=0)
    review = _review()

    enricher.enrich([review])
    first_call_count = post.call_count
    review.claim.analysis.entities.clear()
    enricher.enrich([review])

    assert first_call_count == 1
    assert post.call_count == 1
    assert review.claim.analysis.entities == [
        EntityMention(
            uri="http://dbpedia.org/resource/Climate_change",
            source="dbpedia_spotlight",
            surface_form="Climate change",
            types=["DBpedia:Concept"],
            confidence=0.9,
            support=100,
            offset=0,
        )
    ]


@patch("climatesense_kg.enrichers.dbpedia_property_enricher.requests.get")
def test_dbpedia_entity_result_is_reused_across_reviews(get: Mock) -> None:
    get.return_value.json.return_value = {
        "results": {
            "bindings": [
                {
                    "property": {"value": "http://example.test/property"},
                    "value": {"type": "literal", "value": "42"},
                }
            ]
        }
    }
    store = InMemoryStageResultStore()
    enricher = DBpediaPropertyEnricher(
        store=store,
        properties=["http://example.test/property"],
        rate_limit_delay=0,
    )
    reviews = [_review("First climate claim"), _review("Second climate claim")]
    for review in reviews:
        review.claim.analysis.entities.append(
            EntityMention(
                uri="http://dbpedia.org/resource/Climate_change",
                source="dbpedia_spotlight",
            )
        )

    enricher.enrich(reviews)

    assert get.call_count == 1
    for review in reviews:
        value = review.claim.analysis.entities[0].properties[
            "http://example.test/property"
        ][0]
        assert value.value == "42"
        assert value.value_type == "literal"

    enricher.enrich(reviews, force=True)

    assert get.call_count == 2


def test_cimple_stage_batches_each_model_once_for_multiple_reviews() -> None:
    store = InMemoryStageResultStore()
    enricher = BertFactorsEnricher(store=store, batch_size=32, rate_limit_delay=0)
    reviews = [_review("First climate claim"), _review("Second climate claim")]

    def call_model(model: str, texts: list[str]) -> list[dict[str, object]]:
        value: object = model == "climate_related" or f"{model}-value"
        return [{"value": value} for _text in texts]

    with (
        patch.object(enricher, "is_available", return_value=True),
        patch.object(enricher, "_call_model", side_effect=call_model) as model_call,
    ):
        enricher.enrich(reviews)

    assert model_call.call_count == len(enricher.MODEL_KEYS)
    assert all(review.claim.analysis.climate_related is True for review in reviews)
    assert all(review.claim.analysis.emotion == "emotion-value" for review in reviews)
