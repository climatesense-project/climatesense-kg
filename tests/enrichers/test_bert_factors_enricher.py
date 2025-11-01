"""Tests for BertFactorsEnricher."""

import json
import os
from typing import Any
from unittest.mock import Mock, patch

import pytest
import requests
from src.climatesense_kg.config.models import CanonicalClaimReview
from src.climatesense_kg.enrichers.bert_factors_enricher import BertFactorsEnricher


class MockHTTPResponse:
    """Helper response object for mocking requests."""

    def __init__(
        self, status_code: int = 200, json_data: Any | None = None, text: str = ""
    ) -> None:
        self.status_code = status_code
        self._json_data: Any = json_data if json_data is not None else {}
        self.text = text

    def json(self) -> Any:
        return self._json_data


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

        def empty_get_many(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {}

        mock_cache.get_many.side_effect = empty_get_many

        result = bert_enricher.enrich([sample_claim_review])[0]
        assert result == sample_claim_review
        assert sample_claim_review.claim.emotion is None
        assert sample_claim_review.claim.sentiment is None
        assert sample_claim_review.claim.tropes == []
        assert sample_claim_review.claim.persuasion_techniques == []

        assert mock_cache.set.call_count == len(BertFactorsEnricher.MODEL_KEYS)
        expected_steps = {
            f"enricher.bert_factors.{model}" for model in BertFactorsEnricher.MODEL_KEYS
        }
        actual_steps = {call.args[1] for call in mock_cache.set.call_args_list}
        assert actual_steps == expected_steps
        for call in mock_cache.set.call_args_list:
            payload = call.args[2]
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

        def empty_get_many(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {}

        mock_cache.get_many.side_effect = empty_get_many

        model_payloads: dict[str, dict[str, Any]] = {
            "emotion": {"value": "Anger"},
            "sentiment": {"value": "Negative"},
            "political-leaning": {"value": "Left"},
            "tropes": {"value": ["Time Will Tell"]},
            "persuasion-techniques": {"value": ["Appeal to authority"]},
            "conspiracy": {"value": {"mentioned": [], "promoted": []}},
            "climate-related": {"value": True},
        }

        def post_side_effect(
            url: str, headers: dict[str, str], data: str, timeout: int
        ) -> MockHTTPResponse:
            endpoint = url.split("/predict/")[-1]
            assert endpoint in model_payloads
            _ = json.loads(data)
            return MockHTTPResponse(200, {"results": [model_payloads[endpoint]]})

        mock_post.side_effect = post_side_effect

        result = enricher.enrich([sample_claim_review])[0]

        assert result.claim.emotion == "Anger"
        assert result.claim.sentiment == "Negative"
        assert result.claim.political_leaning == "Left"
        assert result.claim.tropes == ["Time Will Tell"]
        assert result.claim.persuasion_techniques == ["Appeal to authority"]
        assert result.claim.conspiracies == {"mentioned": [], "promoted": []}
        assert result.claim.climate_related is True
        assert mock_post.call_count == len(BertFactorsEnricher.MODEL_KEYS)
        assert mock_cache.set.call_count == len(BertFactorsEnricher.MODEL_KEYS)

    def test_enrich_with_cached_data(
        self,
        sample_claim_review: CanonicalClaimReview,
        mock_cache: Mock,
    ) -> None:
        """Test enrichment with cached data."""
        enricher = BertFactorsEnricher()
        enricher.cache = mock_cache
        uri = sample_claim_review.uri

        def get_many_side_effect(uris: list[str], step: str) -> dict[str, Any]:
            assert uris == [uri]
            if step == "enricher.bert_factors.emotion":
                return {uri: {"success": True, "data": "Happiness"}}
            if step == "enricher.bert_factors.sentiment":
                return {uri: {"success": True, "data": "Positive"}}
            if step == "enricher.bert_factors.political_leaning":
                return {uri: {"success": True, "data": "Right"}}
            if step == "enricher.bert_factors.tropes":
                return {uri: {"success": True, "data": ["Test trope"]}}
            if step == "enricher.bert_factors.persuasion_techniques":
                return {uri: {"success": True, "data": ["Loaded language"]}}
            if step == "enricher.bert_factors.conspiracies":
                return {
                    uri: {
                        "success": True,
                        "data": {
                            "mentioned": [],
                            "promoted": ["New World Order"],
                        },
                    }
                }
            if step == "enricher.bert_factors.climate_related":
                return {uri: {"success": True, "data": False}}
            if step == "enricher.bert_factors":
                return {}
            return {}

        mock_cache.get_many.side_effect = get_many_side_effect

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

        assert mock_cache.get_many.call_count == len(BertFactorsEnricher.MODEL_KEYS)
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

        def empty_get_many(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {}

        mock_cache.get_many.side_effect = empty_get_many

        result = enricher.enrich([sample_claim_review])[0]

        assert result.claim.emotion is None
        assert result.claim.climate_related is None
        assert mock_cache.set.call_count == len(BertFactorsEnricher.MODEL_KEYS)
        for call in mock_cache.set.call_args_list:
            assert call.args[1].startswith("enricher.bert_factors.")
            payload = call.args[2]
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

        first_uri = sample_claim_reviews[0].uri

        def cache_side_effect(uris: list[str], step: str) -> dict[str, dict[str, Any]]:
            result: dict[str, dict[str, Any]] = {}
            if first_uri not in uris:
                return result
            if step == "enricher.bert_factors.emotion":
                result[first_uri] = {"success": True, "data": "Fear"}
            elif step == "enricher.bert_factors.sentiment":
                result[first_uri] = {"success": True, "data": "Negative"}
            elif step == "enricher.bert_factors":
                return {}
            return result

        mock_cache.get_many.side_effect = cache_side_effect

        def post_side_effect(
            url: str, headers: dict[str, str], data: str, timeout: int
        ) -> MockHTTPResponse:
            endpoint = url.split("/predict/")[-1]
            payload = json.loads(data)
            assert "texts" in payload
            if endpoint == "emotion":
                return MockHTTPResponse(200, {"results": [{"value": "Joy"}]})
            if endpoint == "sentiment":
                return MockHTTPResponse(200, {"results": [{"value": "Neutral"}]})
            if endpoint == "political-leaning":
                return MockHTTPResponse(200, {"results": [{"value": "Center"}]})
            if endpoint == "tropes":
                return MockHTTPResponse(200, {"results": [{"value": []}]})  # type: ignore
            if endpoint == "persuasion-techniques":
                return MockHTTPResponse(200, {"results": [{"value": []}]})  # type: ignore
            if endpoint == "conspiracy":
                return MockHTTPResponse(
                    200,
                    {  # type: ignore
                        "results": [
                            {
                                "value": {
                                    "mentioned": [],
                                    "promoted": [],
                                }
                            }
                        ]
                    },
                )
            if endpoint == "climate-related":
                return MockHTTPResponse(200, {"results": [{"value": False}]})
            raise AssertionError(f"Unexpected endpoint {endpoint}")

        mock_post.side_effect = post_side_effect

        with patch.object(enricher, "is_available", return_value=True):
            results = enricher.enrich(sample_claim_reviews)

        assert len(results) == 3
        assert all(isinstance(r, CanonicalClaimReview) for r in results)
        assert mock_post.call_count >= len(BertFactorsEnricher.MODEL_KEYS)


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

        def empty_get_many(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {}

        mock_cache.get_many.side_effect = empty_get_many

        # Test API error response
        mock_post.return_value = MockHTTPResponse(
            status_code=500, text="Internal Server Error"
        )

        result = enricher.enrich([sample_claim_review])[0]

        # Should still return the original claim review, but without enrichment
        assert result == sample_claim_review
        assert result.claim.emotion is None
        assert result.claim.sentiment is None
        assert mock_post.call_count == len(BertFactorsEnricher.MODEL_KEYS)

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

        def empty_get_many(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {}

        mock_cache.get_many.side_effect = empty_get_many

        # Test connection error
        mock_post.side_effect = requests.ConnectionError("Connection failed")

        result = enricher.enrich([sample_claim_review])[0]

        # Should still return the original claim review, but without enrichment
        assert result == sample_claim_review
        assert result.claim.emotion is None
        assert result.claim.sentiment is None
        assert mock_post.call_count == len(BertFactorsEnricher.MODEL_KEYS)

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

        def empty_get_many(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            return {}

        mock_cache.get_many.side_effect = empty_get_many

        # Set empty normalized text
        sample_claim_review.claim.text = ""

        result = enricher.enrich([sample_claim_review])[0]

        # Should not make API call for empty text
        mock_post.assert_not_called()
        assert result == sample_claim_review
