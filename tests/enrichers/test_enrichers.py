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
    CimpleModelEnricher,
    DBpediaPropertyEnricher,
    DBpediaSpotlightEnricher,
)
from climatesense_kg.persistence import (
    InMemoryStageResultStore,
    StageResult,
)
from climatesense_kg.stages import EnrichmentExecutionPolicy, EnrichmentRunner


def _review(
    text: str = "Climate change is a reviewed claim",
    *,
    review_text: str | None = None,
) -> CanonicalClaimReview:
    url = "https://example.test/review"
    return CanonicalClaimReview(
        id=uuid4(),
        claim=CanonicalClaim(text=text),
        organization=CanonicalOrganization(
            uri="https://data.example.test/organization/example",
            name="Example",
            website="https://example.test",
        ),
        document=CanonicalReviewDocument(
            id=uuid4(),
            urls={url},
            preferred_url=url,
            content=review_text,
        ),
        source_record_keys={"record"},
        source_names={"source"},
    )


@patch("climatesense_kg.enrichers.dbpedia_spotlight_enricher.requests.post")
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
    enricher = DBpediaSpotlightEnricher(target="claim", store=store, rate_limit_delay=0)
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
                    "entity": {
                        "type": "uri",
                        "value": "http://dbpedia.org/resource/Climate_change",
                    },
                    "property": {"value": "http://example.test/property"},
                    "value": {"type": "literal", "value": "42"},
                },
                {
                    "entity": {
                        "type": "uri",
                        "value": "http://dbpedia.org/resource/Global_warming",
                    },
                    "property": {"value": "http://example.test/property"},
                    "value": {"type": "literal", "value": "43"},
                },
            ]
        }
    }
    store = InMemoryStageResultStore()
    enricher = DBpediaPropertyEnricher(
        store=store,
        properties=["http://example.test/property"],
        rate_limit_delay=0,
    )
    reviews = [
        _review("First climate claim"),
        _review("Second climate claim"),
        _review("Third climate claim"),
    ]
    entity_uris = [
        "http://dbpedia.org/resource/Climate_change",
        "http://dbpedia.org/resource/Climate_change",
        "http://dbpedia.org/resource/Global_warming",
    ]
    for review, entity_uri in zip(reviews, entity_uris, strict=True):
        review.claim.analysis.entities.append(
            EntityMention(
                uri=entity_uri,
                source="dbpedia_spotlight",
            )
        )

    enricher.enrich(reviews)

    assert get.call_count == 1
    for review, expected in zip(reviews, ["42", "42", "43"], strict=True):
        value = review.claim.analysis.entities[0].properties[
            "http://example.test/property"
        ][0]
        assert value.value == expected
        assert value.value_type == "literal"

    enricher.enrich(reviews, force=True)

    assert get.call_count == 2


def test_cimple_models_are_batched_and_persisted_independently() -> None:
    store = InMemoryStageResultStore()
    reviews = [_review("First climate claim"), _review("Second climate claim")]

    values: dict[str, object] = {
        "emotion": "anger",
        "sentiment": "negative",
        "political_leaning": "other",
        "tropes": ["appeal"],
        "persuasion_techniques": ["repetition"],
        "conspiracies": {"mentioned": ["antivax"], "promoted": []},
        "climate_related": True,
    }
    for model in CimpleModelEnricher.MODEL_KEYS:
        enricher = CimpleModelEnricher(
            model=model,
            store=store,
            batch_size=32,
            rate_limit_delay=0,
        )
        with patch.object(
            enricher,
            "_call_model",
            return_value=[{"value": values[model]} for _review in reviews],
        ) as model_call:
            report = enricher.enrich(reviews)
        model_call.assert_called_once()
        assert report.computed_successes == 2

    assert all(review.claim.analysis.climate_related is True for review in reviews)
    assert all(review.claim.analysis.emotion == "anger" for review in reviews)


def test_cimple_result_identity_separates_semantic_and_operational_settings() -> None:
    review = _review()
    store = InMemoryStageResultStore()
    first = CimpleModelEnricher(
        model="emotion",
        store=store,
        model_version="1",
        max_length=128,
        batch_size=8,
        timeout=5,
        rate_limit_delay=0,
    )
    operational_change = CimpleModelEnricher(
        model="emotion",
        store=store,
        model_version="1",
        max_length=128,
        batch_size=64,
        timeout=90,
        rate_limit_delay=2,
    )
    semantic_change = CimpleModelEnricher(
        model="emotion",
        store=store,
        model_version="2",
        max_length=128,
    )

    assert first.result_key(review) == operational_change.result_key(review)
    assert first.result_key(review) != semantic_change.result_key(review)


def test_dbpedia_property_identity_separates_config_types() -> None:
    store = InMemoryStageResultStore()
    entity_uri = "http://dbpedia.org/resource/Climate_change"
    first = DBpediaPropertyEnricher(
        store=store,
        sparql_endpoint="https://first.example/sparql",
        properties=["http://example.test/property"],
        timeout=5,
        max_retries=0,
    )
    operational_change = DBpediaPropertyEnricher(
        store=store,
        sparql_endpoint="https://second.example/sparql",
        properties=["http://example.test/property"],
        timeout=90,
        max_retries=5,
    )
    semantic_change = DBpediaPropertyEnricher(
        store=store,
        properties=["http://example.test/other-property"],
    )

    assert first._result_key(entity_uri) == operational_change._result_key(entity_uri)
    assert first._result_key(entity_uri) != semantic_change._result_key(entity_uri)


def test_spotlight_claim_result_is_shared_by_claim_uri() -> None:
    store = InMemoryStageResultStore()
    enricher = DBpediaSpotlightEnricher(target="claim", store=store, rate_limit_delay=0)
    reviews = [_review(), _review()]

    with patch.object(enricher, "_extract_entities", return_value=[]) as extract:
        report = enricher.enrich(reviews)

    extract.assert_called_once()
    assert report.eligible_subjects == 1
    assert report.computed_successes == 1


def test_spotlight_review_result_is_shared_by_exact_text_digest() -> None:
    store = InMemoryStageResultStore()
    enricher = DBpediaSpotlightEnricher(
        target="review", store=store, rate_limit_delay=0
    )
    reviews = [
        _review("First claim", review_text="The same exact review body."),
        _review("Second claim", review_text="The same exact review body."),
    ]

    with patch.object(enricher, "_extract_entities", return_value=[]) as extract:
        report = enricher.enrich(reviews)

    extract.assert_called_once_with("The same exact review body.")
    assert report.eligible_subjects == 1


def test_operational_settings_do_not_invalidate_spotlight_result() -> None:
    store = InMemoryStageResultStore()
    review = _review()
    first = DBpediaSpotlightEnricher(
        target="claim",
        store=store,
        api_url="https://first.example/annotate",
        timeout=1,
        rate_limit_delay=0,
    )
    with patch.object(first, "_extract_entities", return_value=[]):
        first.enrich([review])

    second = DBpediaSpotlightEnricher(
        target="claim",
        store=store,
        api_url="https://second.example/annotate",
        timeout=99,
        rate_limit_delay=5,
    )
    with patch.object(second, "_extract_entities") as extract:
        report = second.enrich([review])

    extract.assert_not_called()
    assert report.stored_successes == 1


def test_semantic_settings_invalidate_spotlight_result() -> None:
    store = InMemoryStageResultStore()
    review = _review()
    first = DBpediaSpotlightEnricher(
        target="claim", store=store, confidence=0.5, rate_limit_delay=0
    )
    with patch.object(first, "_extract_entities", return_value=[]):
        first.enrich([review])

    second = DBpediaSpotlightEnricher(
        target="claim", store=store, confidence=0.8, rate_limit_delay=0
    )
    with patch.object(second, "_extract_entities", return_value=[]) as extract:
        report = second.enrich([review])

    extract.assert_called_once()
    assert report.computed_successes == 1


def test_stored_failure_is_retried_and_not_applied_as_success() -> None:
    store = InMemoryStageResultStore()
    review = _review()
    enricher = DBpediaSpotlightEnricher(target="claim", store=store, rate_limit_delay=0)
    store.put(
        enricher.result_key(review),
        StageResult(success=False, payload={"error": "temporarily unavailable"}),
    )

    with patch.object(enricher, "_extract_entities", return_value=[]) as extract:
        report = enricher.enrich([review])

    extract.assert_called_once()
    assert report.stored_failures == 1
    assert report.computed_successes == 1
    assert report.missing_results == 0


def test_successful_empty_result_is_complete() -> None:
    store = InMemoryStageResultStore()
    review = _review("Short")
    enricher = DBpediaSpotlightEnricher(target="claim", store=store, rate_limit_delay=0)

    first = enricher.enrich([review])
    second = enricher.enrich([review], policy=EnrichmentExecutionPolicy.STORED_ONLY)

    assert first.computed_successes == 1
    assert second.stored_successes == 1
    assert second.complete is True


def test_unavailable_dependency_reuses_complete_stored_results() -> None:
    store = InMemoryStageResultStore()
    review = _review()
    enricher = DBpediaSpotlightEnricher(target="claim", store=store, rate_limit_delay=0)
    with patch.object(enricher, "_extract_entities", return_value=[]):
        enricher.enrich([review])

    with patch.object(enricher, "is_available", return_value=False) as available:
        run = EnrichmentRunner([enricher]).run([review])

    available.assert_not_called()
    assert run.complete is True
    assert run.stages[0].available is None
    assert run.stages[0].stored_successes == 1


def test_unavailable_dependency_reports_stored_miss() -> None:
    review = _review()
    enricher = DBpediaSpotlightEnricher(
        target="claim",
        store=InMemoryStageResultStore(),
        rate_limit_delay=0,
    )

    with patch.object(enricher, "is_available", return_value=False):
        run = EnrichmentRunner([enricher]).run([review])

    assert run.complete is False
    assert run.stages[0].missing_results == 1
