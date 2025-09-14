"""Shared test fixtures for the ClimateSense KG Pipeline tests."""

from pathlib import Path
from unittest.mock import Mock

import pytest
from src.climatesense_kg.config.models import (
    CanonicalClaim,
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalRating,
)


@pytest.fixture
def sample_claim_review():
    """Create a sample CanonicalClaimReview for testing."""
    organization = CanonicalOrganization(name="Test Org", website="https://test.org")
    rating = CanonicalRating(label="false", original_label="False")
    claim = CanonicalClaim(text="This is a test claim about climate change.")

    return CanonicalClaimReview(
        claim=claim,
        review_url="https://test.org/review/123",
        organization=organization,
        rating=rating,
        date_published="2023-01-01",
        language="en",
    )


@pytest.fixture
def sample_claim_reviews(
    sample_claim_review: CanonicalClaimReview,
) -> list[CanonicalClaimReview]:
    """Create multiple sample CanonicalClaimReview objects."""
    reviews: list[CanonicalClaimReview] = []
    for i in range(3):
        org = CanonicalOrganization(
            name=f"Test Org {i}", website=f"https://test{i}.org"
        )
        rating = CanonicalRating(label="false", original_label="False")
        claim = CanonicalClaim(text=f"This is test claim {i} about climate change.")

        review = CanonicalClaimReview(
            claim=claim,
            review_url=f"https://test{i}.org/review/123",
            organization=org,
            rating=rating,
            date_published="2023-01-01",
            language="en",
        )
        reviews.append(review)

    return reviews


@pytest.fixture
def mock_cache():
    """Mock cache interface."""
    cache = Mock()
    cache.get = Mock(return_value=None)
    cache.set = Mock(return_value=True)
    return cache


@pytest.fixture
def temp_models_path(tmp_path: Path) -> Path:
    """Create a temporary directory for model files."""
    models_dir: Path = tmp_path / "models"
    models_dir.mkdir()
    return models_dir
