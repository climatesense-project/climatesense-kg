"""Tests for BertFactorsEnricher."""

from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
from src.climatesense_kg.config.models import CanonicalClaimReview
from src.climatesense_kg.enrichers.bert_factors_enricher import BertFactorsEnricher


@pytest.fixture
def bert_enricher(temp_models_path: Path) -> BertFactorsEnricher:
    """Create BertFactorsEnricher instance with temporary models path."""
    return BertFactorsEnricher(
        models_path=str(temp_models_path),
        batch_size=2,
        max_length=64,
        device="cpu",
        auto_download=False,
    )


@pytest.fixture
def bert_enricher_with_download(temp_models_path: Path) -> BertFactorsEnricher:
    """Create BertFactorsEnricher instance with auto_download enabled."""
    return BertFactorsEnricher(
        models_path=str(temp_models_path),
        auto_download=True,
    )


class TestBertFactorsEnricherInit:
    """Test BertFactorsEnricher initialization."""

    def test_init_default_config(self, temp_models_path: Path) -> None:
        """Test initialization with default configuration."""
        enricher = BertFactorsEnricher(models_path=str(temp_models_path))

        assert enricher.name == "bert_factors"
        assert enricher.models_path == temp_models_path
        assert enricher.batch_size == 32
        assert enricher.max_length == 128
        assert enricher.device == "auto"
        assert enricher.auto_download is True

    def test_init_custom_config(self, temp_models_path: Path) -> None:
        """Test initialization with custom configuration."""
        enricher = BertFactorsEnricher(
            models_path=str(temp_models_path),
            batch_size=16,
            max_length=256,
            device="cuda",
            auto_download=False,
        )

        assert enricher.batch_size == 16
        assert enricher.max_length == 256
        assert enricher.device == "cuda"
        assert enricher.auto_download is False

    def test_init_missing_models_path_raises_error(self) -> None:
        """Test initialization without models_path raises ValueError."""
        with pytest.raises(ValueError, match="models_path must be provided"):
            BertFactorsEnricher()


class TestBertFactorsEnricherAvailability:
    """Test model availability checking."""

    def test_is_available_with_auto_download_true(
        self, bert_enricher_with_download: BertFactorsEnricher
    ) -> None:
        """Test is_available returns True when auto_download is enabled."""
        assert bert_enricher_with_download.is_available() is True

    def test_is_available_with_auto_download_false_missing_models(
        self, bert_enricher: BertFactorsEnricher
    ) -> None:
        """Test is_available returns False when auto_download is disabled and models missing."""
        assert bert_enricher.is_available() is False

    def test_is_available_with_auto_download_false_models_exist(
        self, temp_models_path: Path
    ) -> None:
        """Test is_available returns True when auto_download is disabled but models exist."""
        with patch("pathlib.Path.exists", return_value=True):
            enricher = BertFactorsEnricher(
                models_path=str(temp_models_path),
                auto_download=False,
            )
            assert enricher.is_available() is True


class TestBertFactorsEnricherEnrichment:
    """Test enrichment functionality."""

    def test_enrich_not_available(
        self,
        bert_enricher: BertFactorsEnricher,
        sample_claim_review: CanonicalClaimReview,
    ) -> None:
        """Test enrichment when enricher is not available."""
        result = bert_enricher.enrich([sample_claim_review])[0]
        assert result == sample_claim_review
        assert sample_claim_review.claim.emotion is None
        assert sample_claim_review.claim.sentiment is None

    def test_enrich_no_uri(
        self,
        bert_enricher: BertFactorsEnricher,
        sample_claim_review: CanonicalClaimReview,
    ) -> None:
        """Test enrichment when claim review has no URI."""
        sample_claim_review.review_url = ""
        result = bert_enricher.enrich([sample_claim_review])[0]
        assert result == sample_claim_review

    def test_enrich_success_with_mocked_models(
        self,
        sample_claim_review: CanonicalClaimReview,
        mock_cache: Mock,
        temp_models_path: Path,
    ) -> None:
        """Test successful enrichment with mocked models."""
        enricher = BertFactorsEnricher(
            models_path=str(temp_models_path),
            auto_download=False,
        )
        enricher.cache = mock_cache
        mock_cache.get_many.return_value = {}

        sample_claim_review.claim.emotion = "Anger"
        sample_claim_review.claim.sentiment = "Negative"
        sample_claim_review.claim.political_leaning = "Left"
        sample_claim_review.claim.conspiracies = {"mentioned": [], "promoted": []}

        assert sample_claim_review.claim.emotion == "Anger"
        assert sample_claim_review.claim.sentiment == "Negative"
        assert sample_claim_review.claim.political_leaning == "Left"
        assert sample_claim_review.claim.conspiracies == {
            "mentioned": [],
            "promoted": [],
        }

    def test_enrich_with_cached_data(
        self,
        sample_claim_review: CanonicalClaimReview,
        mock_cache: Mock,
        temp_models_path: Path,
    ) -> None:
        """Test enrichment with cached data."""
        enricher = BertFactorsEnricher(
            models_path=str(temp_models_path),
            auto_download=False,
        )
        enricher.cache = mock_cache
        cached_factors: dict[str, Any] = {
            "emotion": "Happiness",
            "sentiment": "Positive",
            "political_leaning": "Right",
            "conspiracies": {"mentioned": [], "promoted": ["New World Order"]},
        }
        mock_cache.get_many.return_value = {sample_claim_review.uri: cached_factors}

        with patch.object(enricher, "is_available", return_value=True):
            result = enricher.enrich([sample_claim_review])[0]

        assert result.claim.emotion == "Happiness"
        assert result.claim.sentiment == "Positive"
        assert result.claim.political_leaning == "Right"
        assert result.claim.conspiracies == {
            "mentioned": [],
            "promoted": ["New World Order"],
        }

        mock_cache.get_many.assert_called_once_with(
            [sample_claim_review.uri], "enricher.bert_factors"
        )
        mock_cache.set.assert_not_called()


class TestBertFactorsEnricherBatch:
    """Test batch enrichment."""

    def test_enrich_batch_not_available(
        self,
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

    def test_enrich_batch_with_mixed_cache(
        self,
        sample_claim_reviews: list[CanonicalClaimReview],
        mock_cache: Mock,
        temp_models_path: Path,
    ) -> None:
        """Test batch enrichment with mixed cached and uncached items."""
        enricher = BertFactorsEnricher(
            models_path=str(temp_models_path),
            auto_download=False,
        )
        enricher.cache = mock_cache

        def cache_side_effect(
            uris: list[str], namespace: str = ""
        ) -> dict[str, dict[str, Any]]:
            result = {}
            for uri in uris:
                if sample_claim_reviews[0].uri == uri:
                    result[uri] = {"emotion": "Fear", "sentiment": "Negative"}
            return result

        mock_cache.get_many.side_effect = cache_side_effect
        results = enricher.enrich(sample_claim_reviews)

        assert len(results) == 3
        assert all(isinstance(r, CanonicalClaimReview) for r in results)
