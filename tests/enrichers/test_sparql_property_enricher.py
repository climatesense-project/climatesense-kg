"""Behavioral tests for the SPARQL entity property enricher."""

from collections.abc import Callable
from unittest.mock import Mock, patch

import requests

from climatesense_kg.domain import CanonicalClaimReview, EntityMention
from climatesense_kg.enrichers import SparqlEntityPropertyEnricher
from climatesense_kg.enrichers.base import EnrichmentSubject
from climatesense_kg.processing import ResultStatus


def _property_enricher(
    *,
    properties: list[str] | None = None,
    rate_limit_delay: float = 0.1,
    max_retries: int = 4,
) -> SparqlEntityPropertyEnricher:
    return SparqlEntityPropertyEnricher(
        name="dbpedia_entity_properties",
        entity_sources=frozenset({"dbpedia_spotlight"}),
        sparql_endpoint="https://dbpedia.org/sparql",
        availability_key="dbpedia_sparql",
        availability_probe_entity="http://dbpedia.org/resource/Paris",
        availability_probe_property="http://www.w3.org/2003/01/geo/wgs84_pos#lat",
        properties=properties,
        rate_limit_delay=rate_limit_delay,
        max_retries=max_retries,
    )


def _property_subjects(
    make_review: Callable[..., CanonicalClaimReview],
    enricher: SparqlEntityPropertyEnricher,
    entity_uri: str,
) -> list[EnrichmentSubject]:
    review = make_review()
    review.claim.analysis.entities.append(
        EntityMention(uri=entity_uri, source="dbpedia_spotlight")
    )
    return enricher.subjects([review])


def test_property_enricher_groups_and_applies_entity_properties(make_review) -> None:
    property_uri = "http://example.test/property"
    enricher = _property_enricher(properties=[property_uri])
    reviews = [make_review(), make_review()]
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


def test_property_dependency_healthcheck_probes_the_property_query_shape() -> None:
    enricher = _property_enricher(properties=[])
    response = Mock(status_code=200)
    with patch("requests.get", return_value=response) as request:
        assert enricher.is_available()
    query = request.call_args.kwargs["params"]["query"]
    assert query.startswith("SELECT")
    assert "http://dbpedia.org/resource/Paris" in query
    assert "wgs84_pos#lat" in query
    request.assert_called_once()


def test_property_enricher_ignores_entities_from_other_sources(make_review) -> None:
    enricher = _property_enricher(properties=["http://example.test/property"])
    review = make_review()
    review.claim.analysis.entities.append(
        EntityMention(
            uri="http://www.wikidata.org/entity/Q3052772",
            source="opentapioca",
        )
    )

    subjects = enricher.subjects([review])

    assert subjects == []


def test_property_query_retry_honors_retry_after_header(make_review) -> None:
    property_uri = "http://example.test/property"
    entity_uri = "http://dbpedia.org/resource/Climate_change"
    enricher = _property_enricher(properties=[property_uri], rate_limit_delay=0)
    unavailable = Mock(status_code=503, headers={"Retry-After": "7"})
    unavailable.raise_for_status.side_effect = requests.HTTPError(
        "503 Server Error", response=unavailable
    )
    available = Mock(status_code=200)
    available.raise_for_status.return_value = None
    available.json.return_value = {"results": {"bindings": []}}
    sleeps: list[float] = []
    with (
        patch("requests.get", side_effect=[unavailable, available]),
        patch(
            "climatesense_kg.enrichers.sparql_property_enricher.time.sleep",
            side_effect=sleeps.append,
        ),
    ):
        results = enricher.compute_batch(
            _property_subjects(make_review, enricher, entity_uri)
        )

    assert sleeps[0] == 7.0
    assert [result.status for result in results] == [ResultStatus.SUCCESS]


def test_property_query_failures_stay_retryable_with_bounded_delays(
    make_review,
) -> None:
    property_uri = "http://example.test/property"
    entity_uri = "http://dbpedia.org/resource/Climate_change"
    enricher = _property_enricher(
        properties=[property_uri], rate_limit_delay=0, max_retries=3
    )
    unavailable = Mock(status_code=503, headers={})
    unavailable.raise_for_status.side_effect = requests.HTTPError(
        "503 Server Error", response=unavailable
    )
    sleeps: list[float] = []
    with (
        patch("requests.get", return_value=unavailable),
        patch(
            "climatesense_kg.enrichers.sparql_property_enricher.time.sleep",
            side_effect=sleeps.append,
        ),
    ):
        results = enricher.compute_batch(
            _property_subjects(make_review, enricher, entity_uri)
        )

    assert len(sleeps) == 3
    assert all(1.0 <= delay <= 30.0 for delay in sleeps)
    assert [result.status for result in results] == [ResultStatus.RETRYABLE_FAILURE]
    assert results[0].payload["error_type"] == "property_query_error"
    assert "503" in results[0].payload["error"]
