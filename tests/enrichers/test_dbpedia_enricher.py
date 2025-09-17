"""Tests for DBpediaEnricher."""

from typing import Any
from unittest.mock import Mock, patch

import pytest
from src.climatesense_kg.config.models import CanonicalClaimReview
from src.climatesense_kg.enrichers.dbpedia_enricher import DBpediaEnricher


@pytest.fixture
def dbpedia_enricher(mock_cache: Mock) -> DBpediaEnricher:
    """Create DBpediaEnricher instance with mock cache."""
    return DBpediaEnricher(
        cache=mock_cache,
        confidence=0.5,
        support=20,
        rate_limit_delay=0.1,
    )


class TestDBpediaEnricherInit:
    """Test DBpediaEnricher initialization."""

    def test_init_default_config(self) -> None:
        """Test initialization with default configuration."""
        enricher = DBpediaEnricher()
        assert enricher.name == "dbpedia_spotlight"
        assert enricher.confidence == 0.5
        assert enricher.support == 20
        assert enricher.timeout == 20
        assert enricher.rate_limit_delay == 0.1
        assert "api.dbpedia-spotlight.org" in enricher.api_url

    def test_init_custom_config(self, mock_cache: Mock) -> None:
        """Test initialization with custom configuration."""
        enricher = DBpediaEnricher(
            cache=mock_cache,
            api_url="https://custom.api.url",
            confidence=0.7,
            support=50,
            timeout=30,
            rate_limit_delay=0.2,
        )
        assert enricher.api_url == "https://custom.api.url"
        assert enricher.confidence == 0.7
        assert enricher.support == 50
        assert enricher.timeout == 30
        assert enricher.rate_limit_delay == 0.2


class TestDBpediaEnricherAvailability:
    """Test DBpedia API availability checking."""

    @patch("src.climatesense_kg.enrichers.dbpedia_enricher.requests.post")
    def test_is_available_success(
        self, mock_post: Mock, dbpedia_enricher: DBpediaEnricher
    ) -> None:
        """Test successful API availability check."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        result = dbpedia_enricher.is_available()

        assert result is True
        mock_post.assert_called_once()

    @patch("src.climatesense_kg.enrichers.dbpedia_enricher.requests.post")
    def test_is_available_failure(
        self, mock_post: Mock, dbpedia_enricher: DBpediaEnricher
    ) -> None:
        """Test API availability check failure."""
        mock_post.side_effect = Exception("Connection error")

        result = dbpedia_enricher.is_available()

        assert result is False


class TestDBpediaEnricherEnrichment:
    """Test enrichment functionality."""

    @patch("src.climatesense_kg.enrichers.dbpedia_enricher.time.sleep")
    @patch("src.climatesense_kg.enrichers.dbpedia_enricher.requests.post")
    def test_enrich_success(
        self,
        mock_post: Mock,
        mock_sleep: Mock,
        dbpedia_enricher: DBpediaEnricher,
        sample_claim_review: CanonicalClaimReview,
        mock_cache: Mock,
    ) -> None:
        """Test successful enrichment with entity extraction."""
        sample_dbpedia_response = {
            "Resources": [
                {
                    "@URI": "http://dbpedia.org/resource/Climate_change",
                    "@surfaceForm": "climate change",
                    "@types": "Agent,Organisation",
                    "@similarityScore": "0.85",
                    "@support": "100",
                    "@offset": "10",
                }
            ]
        }

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_dbpedia_response
        mock_post.return_value = mock_response
        mock_cache.get_many.return_value = {}

        result = dbpedia_enricher.enrich([sample_claim_review])[0]

        assert len(result.claim.entities) == 1
        assert (
            result.claim.entities[0]["uri"]
            == "http://dbpedia.org/resource/Climate_change"
        )
        assert result.claim.entities[0]["surface_form"] == "climate change"
        assert result.claim.entities[0]["confidence"] == 0.85
        assert result.claim.entities[0]["source"] == "dbpedia_spotlight"

        mock_cache.get_many.assert_called_once_with(
            [sample_claim_review.uri], "enricher.dbpedia_spotlight"
        )
        mock_cache.set.assert_called_once()
        call_args = mock_cache.set.call_args[0]
        assert call_args[0] == sample_claim_review.uri
        cached_payload = call_args[2]
        assert "claim_entities" in cached_payload
        assert len(cached_payload["claim_entities"]) == 1

        mock_sleep.assert_called_once_with(0.1)

    def test_enrich_with_cache_hit(
        self,
        dbpedia_enricher: DBpediaEnricher,
        sample_claim_review: CanonicalClaimReview,
        mock_cache: Mock,
    ) -> None:
        """Test enrichment with cache hit."""
        cached_data: dict[str, Any] = {
            "claim_entities": [{"uri": "cached_entity", "surface_form": "cached"}],
            "review_entities": [],
        }
        mock_cache.get_many.return_value = {sample_claim_review.uri: cached_data}

        result = dbpedia_enricher.enrich([sample_claim_review])[0]

        assert len(result.claim.entities) == 1
        assert result.claim.entities[0]["uri"] == "cached_entity"

        mock_cache.get_many.assert_called_once_with(
            [sample_claim_review.uri], "enricher.dbpedia_spotlight"
        )
        mock_cache.set.assert_not_called()


class TestDBpediaEnricherBatch:
    """Test batch enrichment."""

    @patch("src.climatesense_kg.enrichers.dbpedia_enricher.time.sleep")
    @patch("src.climatesense_kg.enrichers.dbpedia_enricher.requests.post")
    def test_enrich_batch(
        self,
        mock_post: Mock,
        mock_sleep: Mock,
        dbpedia_enricher: DBpediaEnricher,
        sample_claim_reviews: list[CanonicalClaimReview],
        mock_cache: Mock,
    ) -> None:
        """Test batch enrichment of multiple claim reviews."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Resources": []}
        mock_post.return_value = mock_response
        mock_cache.get_many.return_value = {}

        results = dbpedia_enricher.enrich(sample_claim_reviews)

        assert len(results) == 3
        assert all(isinstance(r, CanonicalClaimReview) for r in results)
        assert mock_post.call_count >= 3
