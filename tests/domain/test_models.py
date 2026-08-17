"""Tests for identity-bearing and source-observation models."""

from uuid import UUID

from climatesense_kg.domain import (
    CanonicalClaim,
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalRating,
    CanonicalReviewDocument,
    OrganizationReference,
    SourceReference,
)


def test_source_record_key_is_stable_and_distinguishes_claims() -> None:
    first = SourceReference.from_observation(
        source_name="claimreviewdata",
        source_type="claimreviewdata",
        observed_url="https://example.test/fact-check",
        claim_text="First claim",
    )
    repeated = SourceReference.from_observation(
        source_name="claimreviewdata",
        source_type="claimreviewdata",
        observed_url="https://example.test/fact-check",
        claim_text="First claim",
    )
    second_claim = SourceReference.from_observation(
        source_name="claimreviewdata",
        source_type="claimreviewdata",
        observed_url="https://example.test/fact-check",
        claim_text="Second claim",
    )

    assert first.record_key == repeated.record_key
    assert first.record_key != second_claim.record_key


def test_native_source_identifier_is_the_stable_record_anchor() -> None:
    first = SourceReference.from_observation(
        source_name="native-source",
        source_type="graphql",
        native_id="record-42",
        observed_url="https://example.test/old-path",
        claim_text="Original claim text",
    )
    edited = SourceReference.from_observation(
        source_name="native-source",
        source_type="graphql",
        native_id="record-42",
        observed_url="https://example.test/new-path",
        claim_text="Edited claim text",
    )

    assert edited.record_key == first.record_key


def test_source_ratings_are_organization_scoped() -> None:
    rating = CanonicalRating(label="not_credible", original_label="False")

    assert rating.uri_for("https://example.test/organization/one") != rating.uri_for(
        "https://example.test/organization/two"
    )
    assert rating.normalized_uri == "rating/not_credible"


def test_claim_review_uri_depends_only_on_assigned_uuid() -> None:
    review_id = UUID("550e8400-e29b-41d4-a716-446655440000")
    organization = CanonicalOrganization(
        uri="https://example.test/organization/factual",
        name="Factual",
        website="https://factual.ro",
    )
    review = CanonicalClaimReview(
        id=review_id,
        claim=CanonicalClaim(text="A reviewed claim"),
        organization=organization,
        document=CanonicalReviewDocument(
            id=UUID("550e8400-e29b-41d4-a716-446655440001"),
            urls={"https://factual.ro/first-path"},
            preferred_url="https://factual.ro/first-path",
        ),
        source_record_keys={"source-record"},
        source_names={"claimreviewdata"},
        rating=CanonicalRating(label="not_credible", original_label="False"),
    )

    original_uri = review.uri
    review.document.urls.add("https://factual.ro/second-path")
    review.document.preferred_url = "https://factual.ro/second-path"
    review.rating = CanonicalRating(label="credible", original_label="True")

    assert review.uri == original_uri
    assert review.uri == f"claim-review/{review_id}"


def test_organization_reference_normalizes_website() -> None:
    reference = OrganizationReference(
        name="Factual",
        website="https://www.factual.ro/dezinformari/",
    )

    assert reference.website == "https://factual.ro"
