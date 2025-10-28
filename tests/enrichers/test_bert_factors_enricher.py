"""Tests for BertFactorsEnricher."""

import os
from typing import Any
from unittest.mock import Mock, patch

import pytest
import requests
from src.climatesense_kg.config.models import CanonicalClaimReview
from src.climatesense_kg.enrichers.bert_factors_enricher import BertFactorsEnricher


@pytest.fixture
def bert_enricher() -> BertFactorsEnricher:
    """Create BertFactorsEnricher instance."""
    return BertFactorsEnricher(
        batch_size=2,
        max_length=64,
        timeout=10,
    )


@pytest.fixture
def bert_enricher_with_api() -> BertFactorsEnricher:
    """Create BertFactorsEnricher instance with API configuration."""
    with patch.dict(os.environ, {"CIMPLE_FACTORS_API_URL": "http://test-api:8000"}):
        return BertFactorsEnricher()


class TestBertFactorsEnricherInit:
    """Test BertFactorsEnricher initialization."""

    def test_init_default_config(self) -> None:
        """Test initialization with default configuration."""
        enricher = BertFactorsEnricher()

        assert enricher.name == "bert_factors"
        assert enricher.api_url == "http://localhost:8000"
        assert enricher.batch_size == 32
        assert enricher.max_length == 128
        assert enricher.timeout == 60
        assert enricher.rate_limit_delay == 0.1

    def test_init_custom_config(self) -> None:
        """Test initialization with custom configuration."""
        enricher = BertFactorsEnricher(
            batch_size=16,
            max_length=256,
            timeout=30,
            rate_limit_delay=0.5,
        )

        assert enricher.batch_size == 16
        assert enricher.max_length == 256
        assert enricher.timeout == 30
        assert enricher.rate_limit_delay == 0.5

    def test_init_with_environment_variable(self) -> None:
        """Test initialization with environment variable for API URL."""
        with patch.dict(
            os.environ, {"CIMPLE_FACTORS_API_URL": "http://custom-api:9000"}
        ):
            enricher = BertFactorsEnricher()
            assert enricher.api_url == "http://custom-api:9000"


class TestBertFactorsEnricherAvailability:
    """Test API availability checking."""

    @patch("requests.get")
    def test_is_available_api_healthy(
        self, mock_get: Mock, bert_enricher: BertFactorsEnricher
    ) -> None:
        """Test is_available returns True when API is healthy."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        assert bert_enricher.is_available() is True
        mock_get.assert_called_once_with(
            "http://localhost:8000/health", headers=bert_enricher.headers, timeout=5
        )

    @patch("requests.get")
    def test_is_available_api_unhealthy(
        self, mock_get: Mock, bert_enricher: BertFactorsEnricher
    ) -> None:
        """Test is_available returns False when API is unhealthy."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        assert bert_enricher.is_available() is False

    @patch("requests.get")
    def test_is_available_api_connection_error(
        self, mock_get: Mock, bert_enricher: BertFactorsEnricher
    ) -> None:
        """Test is_available returns False when API connection fails."""
        mock_get.side_effect = requests.ConnectionError("Connection failed")

        assert bert_enricher.is_available() is False


class TestBertFactorsEnricherEnrichment:
    """Test enrichment functionality."""

    @patch.object(BertFactorsEnricher, "is_available", return_value=False)
    def test_enrich_not_available(
        self,
        mock_available: Mock,
        bert_enricher: BertFactorsEnricher,
        sample_claim_review: CanonicalClaimReview,
        mock_cache: Mock,
    ) -> None:
        """Test enrichment when enricher is not available."""
        bert_enricher.cache = mock_cache
        mock_cache.get_many.return_value = {}

        result = bert_enricher.enrich([sample_claim_review])[0]
        assert result == sample_claim_review
        assert sample_claim_review.claim.emotion is None
        assert sample_claim_review.claim.sentiment is None
        assert sample_claim_review.claim.tropes == []
        assert sample_claim_review.claim.persuasion_techniques == []

        mock_cache.set.assert_called_once()
        _, step_name, payload = mock_cache.set.call_args[0]
        assert step_name == "enricher.bert_factors"
        assert payload["success"] is False
        assert payload["error"]["type"] == "service_unavailable"

    @patch("requests.post")
    @patch.object(BertFactorsEnricher, "is_available", return_value=True)
    def test_enrich_success_with_mocked_api(
        self,
        mock_available: Mock,
        mock_post: Mock,
        sample_claim_review: CanonicalClaimReview,
        mock_cache: Mock,
    ) -> None:
        """Test successful enrichment with mocked API."""
        enricher = BertFactorsEnricher()
        enricher.cache = mock_cache
        mock_cache.get_many.return_value = {}

        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {
                    "emotion": "Anger",
                    "sentiment": "Negative",
                    "political_leaning": "Left",
                    "tropes": ["Time Will Tell"],
                    "persuasion_techniques": ["Appeal to authority"],
                    "conspiracies": {"mentioned": [], "promoted": []},
                    "climate_related": True,
                }
            ],
            "processed_count": 1,
            "total_count": 1,
        }
        mock_post.return_value = mock_response

        result = enricher.enrich([sample_claim_review])[0]

        assert result.claim.emotion == "Anger"
        assert result.claim.sentiment == "Negative"
        assert result.claim.political_leaning == "Left"
        assert result.claim.tropes == ["Time Will Tell"]
        assert result.claim.persuasion_techniques == ["Appeal to authority"]
        assert result.claim.conspiracies == {"mentioned": [], "promoted": []}
        assert result.claim.climate_related is True

    def test_enrich_with_cached_data(
        self,
        sample_claim_review: CanonicalClaimReview,
        mock_cache: Mock,
    ) -> None:
        """Test enrichment with cached data."""
        enricher = BertFactorsEnricher()
        enricher.cache = mock_cache
        cached_factors: dict[str, Any] = {
            "data": {
                "emotion": "Happiness",
                "sentiment": "Positive",
                "political_leaning": "Right",
                "tropes": ["Test trope"],
                "persuasion_techniques": ["Loaded language"],
                "conspiracies": {"mentioned": [], "promoted": ["New World Order"]},
                "climate_related": False,
            }
        }
        mock_cache.get_many.return_value = {sample_claim_review.uri: cached_factors}

        with patch.object(enricher, "is_available", return_value=True):
            result = enricher.enrich([sample_claim_review])[0]

        assert result.claim.emotion == "Happiness"
        assert result.claim.sentiment == "Positive"
        assert result.claim.political_leaning == "Right"
        assert result.claim.tropes == ["Test trope"]
        assert result.claim.persuasion_techniques == ["Loaded language"]
        assert result.claim.conspiracies == {
            "mentioned": [],
            "promoted": ["New World Order"],
        }
        assert result.claim.climate_related is False

        mock_cache.get_many.assert_called_once_with(
            [sample_claim_review.uri], "enricher.bert_factors"
        )
        mock_cache.set.assert_not_called()

    @patch.object(BertFactorsEnricher, "is_available", return_value=True)
    def test_enrich_missing_text_records_error(
        self,
        mock_available: Mock,
        sample_claim_review: CanonicalClaimReview,
        mock_cache: Mock,
    ) -> None:
        """Ensure items without claim text cache an error so they are not retried forever."""
        sample_claim_review.claim.text = ""
        enricher = BertFactorsEnricher()
        enricher.cache = mock_cache
        mock_cache.get_many.return_value = {}

        result = enricher.enrich([sample_claim_review])[0]

        assert result.claim.emotion is None
        assert result.claim.climate_related is None
        mock_cache.set.assert_called_once()
        _, step_name, payload = mock_cache.set.call_args[0]
        assert step_name == "enricher.bert_factors"
        assert payload["success"] is False
        assert payload["error"]["type"] == "missing_text"


class TestBertFactorsEnricherBatch:
    """Test batch enrichment."""

    @patch.object(BertFactorsEnricher, "is_available", return_value=False)
    def test_enrich_batch_not_available(
        self,
        mock_available: Mock,
        bert_enricher: BertFactorsEnricher,
        sample_claim_reviews: list[CanonicalClaimReview],
    ) -> None:
        """Test batch enrichment when enricher is not available."""
        results = bert_enricher.enrich(sample_claim_reviews)

        assert len(results) == 3
        assert all(
            result == original
            for result, original in zip(results, sample_claim_reviews, strict=False)
        )

    @patch("requests.post")
    def test_enrich_batch_with_mixed_cache(
        self,
        mock_post: Mock,
        sample_claim_reviews: list[CanonicalClaimReview],
        mock_cache: Mock,
    ) -> None:
        """Test batch enrichment with mixed cached and uncached items."""
        enricher = BertFactorsEnricher()
        enricher.cache = mock_cache

        def cache_side_effect(
            uris: list[str], namespace: str = ""
        ) -> dict[str, dict[str, Any]]:
            result: dict[str, dict[str, Any]] = {}
            for uri in uris:
                if sample_claim_reviews[0].uri == uri:
                    result[uri] = {"emotion": "Fear", "sentiment": "Negative"}
            return result

        mock_cache.get_many.side_effect = cache_side_effect

        with patch.object(enricher, "is_available", return_value=True):
            results = enricher.enrich(sample_claim_reviews)

        assert len(results) == 3
        assert all(isinstance(r, CanonicalClaimReview) for r in results)


class TestBertFactorsEnricherAPIIntegration:
    """Test API integration functionality through public interface."""

    @patch("requests.post")
    @patch.object(BertFactorsEnricher, "is_available", return_value=True)
    def test_enrich_api_error_handling(
        self,
        mock_available: Mock,
        mock_post: Mock,
        bert_enricher: BertFactorsEnricher,
        sample_claim_review: CanonicalClaimReview,
        mock_cache: Mock,
    ) -> None:
        """Test enrichment with API error handling."""
        enricher = BertFactorsEnricher()
        enricher.cache = mock_cache
        mock_cache.get_many.return_value = {}

        # Test API error response
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_post.return_value = mock_response

        result = enricher.enrich([sample_claim_review])[0]

        # Should still return the original claim review, but without enrichment
        assert result == sample_claim_review
        assert result.claim.emotion is None
        assert result.claim.sentiment is None

    @patch("requests.post")
    @patch.object(BertFactorsEnricher, "is_available", return_value=True)
    def test_enrich_api_connection_error(
        self,
        mock_available: Mock,
        mock_post: Mock,
        bert_enricher: BertFactorsEnricher,
        sample_claim_review: CanonicalClaimReview,
        mock_cache: Mock,
    ) -> None:
        """Test enrichment with API connection error."""
        enricher = BertFactorsEnricher()
        enricher.cache = mock_cache
        mock_cache.get_many.return_value = {}

        # Test connection error
        mock_post.side_effect = requests.ConnectionError("Connection failed")

        result = enricher.enrich([sample_claim_review])[0]

        # Should still return the original claim review, but without enrichment
        assert result == sample_claim_review
        assert result.claim.emotion is None
        assert result.claim.sentiment is None

    @patch("requests.post")
    @patch.object(BertFactorsEnricher, "is_available", return_value=True)
    def test_enrich_empty_text_handling(
        self,
        mock_available: Mock,
        mock_post: Mock,
        bert_enricher: BertFactorsEnricher,
        sample_claim_review: CanonicalClaimReview,
        mock_cache: Mock,
    ) -> None:
        """Test enrichment with empty claim text."""
        enricher = BertFactorsEnricher()
        enricher.cache = mock_cache
        mock_cache.get_many.return_value = {}

        # Set empty normalized text
        sample_claim_review.claim.text = ""

        result = enricher.enrich([sample_claim_review])[0]

        # Should not make API call for empty text
        mock_post.assert_not_called()
        assert result == sample_claim_review
