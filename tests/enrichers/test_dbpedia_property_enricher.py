"""Tests for DBpediaPropertyEnricher."""

from typing import Any
from unittest.mock import Mock, patch

import pytest
import requests
from src.climatesense_kg.config.models import CanonicalClaimReview
from src.climatesense_kg.enrichers.dbpedia_property_enricher import (
    DBpediaPropertyEnricher,
)


@pytest.fixture
def dbpedia_property_enricher(mock_cache: Mock) -> DBpediaPropertyEnricher:
    """Create property enricher instance with mock cache."""
    return DBpediaPropertyEnricher(
        cache=mock_cache,
        properties=[
            "http://www.w3.org/2003/01/geo/wgs84_pos#lat",
            "http://www.w3.org/2003/01/geo/wgs84_pos#long",
        ],
        rate_limit_delay=0.0,
    )


class TestDBpediaPropertyEnricherInit:
    """Initialization tests."""

    def test_property_normalization(self) -> None:
        properties = [
            "http://www.w3.org/2003/01/geo/wgs84_pos#lat",
            "http://dbpedia.org/ontology/country",
        ]
        enricher = DBpediaPropertyEnricher(properties=properties)
        assert properties == enricher.properties


class TestDBpediaPropertyEnricherAvailability:
    """Availability check tests."""

    @patch("src.climatesense_kg.enrichers.dbpedia_property_enricher.requests.get")
    def test_is_available_success(
        self, mock_get: Mock, dbpedia_property_enricher: DBpediaPropertyEnricher
    ) -> None:
        mock_response = Mock(status_code=200)
        mock_get.return_value = mock_response

        assert dbpedia_property_enricher.is_available() is True
        mock_get.assert_called_once()

    @patch("src.climatesense_kg.enrichers.dbpedia_property_enricher.requests.get")
    def test_is_available_failure(
        self, mock_get: Mock, dbpedia_property_enricher: DBpediaPropertyEnricher
    ) -> None:
        mock_get.side_effect = requests.RequestException("boom")

        assert dbpedia_property_enricher.is_available() is False


class TestDBpediaPropertyEnricherProcessing:
    """Processing tests."""

    @patch("src.climatesense_kg.enrichers.dbpedia_property_enricher.time.sleep")
    @patch("src.climatesense_kg.enrichers.dbpedia_property_enricher.requests.get")
    def test_enrich_success(
        self,
        mock_get: Mock,
        mock_sleep: Mock,
        dbpedia_property_enricher: DBpediaPropertyEnricher,
        sample_claim_review: CanonicalClaimReview,
        mock_cache: Mock,
    ) -> None:
        entity_uri = "http://dbpedia.org/resource/Paris"
        sample_claim_review.claim.entities.append({"uri": entity_uri})

        mock_cache.get_many.return_value = {}

        mock_response = Mock(status_code=200)
        mock_response.json.return_value = {
            "head": {"vars": ["property", "value"]},
            "results": {
                "bindings": [
                    {
                        "property": {
                            "type": "uri",
                            "value": "http://www.w3.org/2003/01/geo/wgs84_pos#lat",
                        },
                        "value": {
                            "type": "typed-literal",
                            "value": "48.8566",
                            "datatype": "http://www.w3.org/2001/XMLSchema#float",
                        },
                    },
                    {
                        "property": {
                            "type": "uri",
                            "value": "http://www.w3.org/2003/01/geo/wgs84_pos#long",
                        },
                        "value": {
                            "type": "typed-literal",
                            "value": "2.3522",
                            "datatype": "http://www.w3.org/2001/XMLSchema#float",
                        },
                    },
                ]
            },
        }
        mock_get.return_value = mock_response

        result = dbpedia_property_enricher.enrich([sample_claim_review])[0]

        entity_properties = result.claim.entities[0]["dbpedia_properties"]
        assert (
            entity_properties["http://www.w3.org/2003/01/geo/wgs84_pos#lat"][0]["value"]
            == "48.8566"
        )
        assert (
            entity_properties["http://www.w3.org/2003/01/geo/wgs84_pos#long"][0][
                "value"
            ]
            == "2.3522"
        )

        mock_cache.set.assert_called_once()
        payload: dict[str, Any] = mock_cache.set.call_args[0][2]
        assert payload["success"] is True
        assert entity_uri in payload["data"]["entities"]

        mock_sleep.assert_called_once_with(0.0)

    def test_apply_cached_data(
        self,
        dbpedia_property_enricher: DBpediaPropertyEnricher,
        sample_claim_review: CanonicalClaimReview,
    ) -> None:
        entity_uri = "http://dbpedia.org/resource/Paris"
        sample_claim_review.claim.entities.append({"uri": entity_uri})

        cached_data = {
            "data": {
                "entities": {
                    entity_uri: {
                        "http://www.w3.org/2003/01/geo/wgs84_pos#lat": [
                            {
                                "value": "48.8566",
                                "type": "typed-literal",
                                "datatype": "http://www.w3.org/2001/XMLSchema#float",
                            }
                        ]
                    }
                }
            }
        }

        result = dbpedia_property_enricher.apply_cached_data(
            sample_claim_review, cached_data
        )

        entity_properties = result.claim.entities[0]["dbpedia_properties"]
        assert (
            entity_properties["http://www.w3.org/2003/01/geo/wgs84_pos#lat"][0]["value"]
            == "48.8566"
        )

    @patch("src.climatesense_kg.enrichers.dbpedia_property_enricher.time.sleep")
    @patch("src.climatesense_kg.enrichers.dbpedia_property_enricher.requests.get")
    def test_enrich_failure_caches_error(
        self,
        mock_get: Mock,
        mock_sleep: Mock,
        dbpedia_property_enricher: DBpediaPropertyEnricher,
        sample_claim_review: CanonicalClaimReview,
        mock_cache: Mock,
    ) -> None:
        entity_uri = "http://dbpedia.org/resource/Paris"
        sample_claim_review.claim.entities.append({"uri": entity_uri})

        mock_cache.get_many.return_value = {}
        mock_get.side_effect = requests.RequestException("boom")

        dbpedia_property_enricher.enrich([sample_claim_review])

        mock_cache.set.assert_called_once()
        payload: dict[str, Any] = mock_cache.set.call_args[0][2]
        assert payload["success"] is False
        assert payload["data"]["failed_entities"][0]["uri"] == entity_uri
