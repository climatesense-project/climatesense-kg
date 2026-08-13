"""Tests for bounded canonical-review publication."""

from unittest.mock import MagicMock, Mock
from uuid import UUID

from climatesense_kg.domain import (
    CanonicalClaim,
    CanonicalOrganization,
    CanonicalRating,
    OrganizationReference,
    ReviewDocument,
    SourceReference,
    SourceReviewRecord,
    source_record_to_payload,
)
from climatesense_kg.persistence.publication import PostgresPublicationReader


def test_publication_rebuilds_current_observations_with_document_url_history() -> None:
    review_id = UUID("00000000-0000-0000-0000-000000000001")
    document_id = UUID("00000000-0000-0000-0000-000000000002")
    claim = CanonicalClaim(text="A current claim")
    record = SourceReviewRecord(
        source=SourceReference.from_observation(
            source_name="source",
            source_type="dataset",
            observed_url="https://example.test/current",
            claim_text=claim.text,
        ),
        claim=claim,
        organization=OrganizationReference(
            name="Example",
            website="https://example.test",
        ),
        document=ReviewDocument(
            observed_url="https://example.test/current",
            source_text="Current source text",
        ),
        rating=CanonicalRating(label="credible", original_label="True"),
    )
    pool = MagicMock()
    connection = Mock()
    pool.connection.return_value.__enter__.return_value = connection
    reader = PostgresPublicationReader(pool)
    reader._identity_rows = Mock(  # type: ignore[method-assign]
        return_value=[
            {
                "id": review_id,
                "document_id": document_id,
                "organization_uri": "https://data.test/organization/example",
                "claim_uri": claim.uri,
                "preferred_url": "https://example.test/current",
                "extracted_text": "Extracted review text",
                "normalized_text_hash": "digest",
                "word_count": 3,
            }
        ]
    )
    reader._document_urls = Mock(  # type: ignore[method-assign]
        return_value={
            document_id: {
                "https://example.test/historical",
                "https://example.test/current",
            }
        }
    )
    reader._observation_rows = Mock(  # type: ignore[method-assign]
        return_value=[
            {
                "claim_review_id": review_id,
                "payload": source_record_to_payload(record),
            }
        ]
    )
    organization = CanonicalOrganization(
        uri="https://data.test/organization/example",
        name="Example",
        website="https://example.test",
    )

    reviews = reader._load_batch(
        UUID("00000000-0000-0000-0000-000000000003"),
        [review_id],
        resolve_organization=lambda _reference: organization,
    )

    assert len(reviews) == 1
    review = reviews[0]
    assert review.id == review_id
    assert review.claim == claim
    assert review.document.content == "Extracted review text"
    assert review.document.urls == {
        "https://example.test/historical",
        "https://example.test/current",
    }
    assert review.source_names == {"source"}
    assert review.rating == record.rating
