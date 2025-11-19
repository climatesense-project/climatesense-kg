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

        assert mock_cache.set.call_count == 2

        entity_cache_call = mock_cache.set.call_args_list[0]
        entity_payload: dict[str, Any] = entity_cache_call[0][2]
        assert entity_cache_call[0][0] == entity_uri
        assert entity_cache_call[0][1] == DBpediaPropertyEnricher.ENTITY_CACHE_STEP
        assert (
            entity_payload["data"]["properties"][
                "http://www.w3.org/2003/01/geo/wgs84_pos#lat"
            ][0]["value"]
            == "48.8566"
        )

        claim_cache_call = mock_cache.set.call_args_list[1]
        payload: dict[str, Any] = claim_cache_call[0][2]
        assert claim_cache_call[0][1] == dbpedia_property_enricher.step_name
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

    @patch("src.climatesense_kg.enrichers.dbpedia_property_enricher.time.sleep")
    @patch("src.climatesense_kg.enrichers.dbpedia_property_enricher.requests.get")
    def test_entity_cache_hit_skips_http(
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
        mock_cache.get.return_value = {
            "success": True,
            "data": {
                "properties": {
                    "http://www.w3.org/2003/01/geo/wgs84_pos#lat": [
                        {
                            "value": "48.8566",
                            "type": "typed-literal",
                            "datatype": "http://www.w3.org/2001/XMLSchema#float",
                        }
                    ]
                }
            },
        }

        result = dbpedia_property_enricher.enrich([sample_claim_review])[0]

        mock_get.assert_not_called()
        mock_cache.get.assert_called_once_with(
            entity_uri, DBpediaPropertyEnricher.ENTITY_CACHE_STEP
        )

        entity_properties = result.claim.entities[0]["dbpedia_properties"]
        assert (
            entity_properties["http://www.w3.org/2003/01/geo/wgs84_pos#lat"][0]["value"]
            == "48.8566"
        )

        mock_cache.set.assert_called_once()
        claim_payload: dict[str, Any] = mock_cache.set.call_args[0][2]
        assert claim_payload["success"] is True
