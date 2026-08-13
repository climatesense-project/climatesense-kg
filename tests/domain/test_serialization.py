"""Tests for durable source-observation serialization."""

from climatesense_kg.domain import (
    CanonicalClaim,
    CanonicalPerson,
    CanonicalRating,
    EntityMention,
    OrganizationReference,
    ReviewDocument,
    SourceReference,
    SourceReviewRecord,
    source_record_from_payload,
    source_record_to_payload,
)


def test_source_observation_json_round_trip_preserves_nested_domain_values() -> None:
    claim = CanonicalClaim(text="A reviewed claim")
    claim.analysis.entities.append(
        EntityMention(
            uri="http://dbpedia.org/resource/Climate_change",
            source="dbpedia-spotlight-en",
            confidence=0.91,
        )
    )
    record = SourceReviewRecord(
        source=SourceReference.from_observation(
            source_name="source",
            source_type="dataset",
            observed_url="https://example.com/review",
            claim_text=claim.text,
            native_id="native-1",
        ),
        claim=claim,
        organization=OrganizationReference(
            name="Example",
            website="https://example.com",
        ),
        document=ReviewDocument(
            observed_url="https://example.com/review",
            source_text="Complete review text",
            description="Description",
        ),
        rating=CanonicalRating(label="credible", original_label="True"),
        authors=[CanonicalPerson(name="Author")],
    )

    restored = source_record_from_payload(source_record_to_payload(record))

    assert restored == record
    assert isinstance(restored.rating, CanonicalRating)
    assert isinstance(restored.authors[0], CanonicalPerson)
    assert isinstance(restored.claim.analysis.entities[0], EntityMention)
