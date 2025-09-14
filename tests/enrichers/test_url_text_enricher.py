"""Tests for URLTextEnricher."""

from unittest.mock import Mock, patch

import pytest
from src.climatesense_kg.config.models import CanonicalClaimReview
from src.climatesense_kg.enrichers.url_text_enricher import URLTextEnricher
from src.climatesense_kg.utils.text_processing import TextExtractionResult


class TestURLTextEnricherInit:
    """Test URLTextEnricher initialization."""

    def test_init_default_config(self) -> None:
        """Test initialization with default configuration."""
        enricher = URLTextEnricher()
        assert enricher.name == "url_text_extractor"
        assert enricher.rate_limit_delay == 0.5
        assert enricher.max_retries == 1

    def test_init_custom_config(self) -> None:
        """Test initialization with custom configuration."""
        enricher = URLTextEnricher(rate_limit_delay=1.0, max_retries=5)
        assert enricher.rate_limit_delay == 1.0
        assert enricher.max_retries == 5

    def test_init_negative_rate_limit_raises_error(self) -> None:
        """Test initialization with negative rate_limit_delay raises ValueError."""
        with pytest.raises(ValueError, match="rate_limit_delay must be non-negative"):
            URLTextEnricher(rate_limit_delay=-1.0)


class TestURLTextEnricherEnrichment:
    """Test enrichment functionality."""

    @patch("src.climatesense_kg.enrichers.url_text_enricher.fetch_and_extract_text")
    def test_successful_extraction(
        self,
        mock_fetch: Mock,
        sample_claim_review: CanonicalClaimReview,
        mock_cache: Mock,
    ) -> None:
        """Test successful text extraction."""
        extracted_text = "This is the extracted text from the URL."
        mock_result = TextExtractionResult(success=True, content=extracted_text)
        mock_fetch.return_value = mock_result

        enricher = URLTextEnricher(cache=mock_cache, rate_limit_delay=0)
        mock_cache.get.return_value = None

        result = enricher.enrich(sample_claim_review)

        assert result.review_url_text == extracted_text
        mock_fetch.assert_called_once_with(sample_claim_review.review_url)

        mock_cache.get.assert_called_once_with(
            sample_claim_review.uri, "enricher.url_text_extractor"
        )
        mock_cache.set.assert_called_once()
        call_args = mock_cache.set.call_args[0]
        assert call_args[0] == sample_claim_review.uri
        cached_payload = call_args[2]
        assert cached_payload["review_url_text"] == extracted_text

    def test_extraction_with_cache_hit(
        self,
        sample_claim_review: CanonicalClaimReview,
        mock_cache: Mock,
    ) -> None:
        """Test extraction with cache hit."""
        cached_data = {"review_url_text": "Cached text"}
        mock_cache.get.return_value = cached_data

        enricher = URLTextEnricher(cache=mock_cache, rate_limit_delay=0)
        result = enricher.enrich(sample_claim_review)

        assert result.review_url_text == "Cached text"

        mock_cache.get.assert_called_once_with(
            sample_claim_review.uri, "enricher.url_text_extractor"
        )
        mock_cache.set.assert_not_called()

    def test_empty_review_url(self, sample_claim_review: CanonicalClaimReview) -> None:
        """Test enrichment with empty review URL."""
        sample_claim_review.review_url = ""
        enricher = URLTextEnricher()
        result = enricher.enrich(sample_claim_review)
        assert result == sample_claim_review
        assert result.review_url_text is None

    @patch("src.climatesense_kg.enrichers.url_text_enricher.fetch_and_extract_text")
    def test_batch_enrichment(
        self,
        mock_fetch: Mock,
        sample_claim_reviews: list[CanonicalClaimReview],
    ) -> None:
        """Test batch enrichment."""
        mock_fetch.return_value = TextExtractionResult(success=True, content="Text")

        enricher = URLTextEnricher(rate_limit_delay=0)
        results = enricher.enrich_batch(sample_claim_reviews)

        assert len(results) == 3
        assert all(isinstance(r, CanonicalClaimReview) for r in results)
