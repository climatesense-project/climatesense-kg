"""Shared fixtures for the enricher behavior tests."""

from collections.abc import Callable
from uuid import uuid4

import pytest

from climatesense_kg.domain import (
    CanonicalClaim,
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalReviewDocument,
)


@pytest.fixture
def make_review() -> Callable[..., CanonicalClaimReview]:
    """Build a canonical claim review with defaults overridable per test."""

    def _review(
        *, claim: str = "A climate claim", body: str = "A review body"
    ) -> CanonicalClaimReview:
        return CanonicalClaimReview(
            id=uuid4(),
            claim=CanonicalClaim(claim),
            organization=CanonicalOrganization(
                uri="https://example.test/organization",
                name="Example",
                website="https://example.test",
            ),
            document=CanonicalReviewDocument(
                id=uuid4(),
                urls={"https://example.test/review"},
                preferred_url="https://example.test/review",
                content=body,
            ),
            source_record_keys={"record"},
            source_names={"source"},
        )

    return _review
