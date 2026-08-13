"""Tests for run-scoped source observations."""

from climatesense_kg.domain import (
    CanonicalClaim,
    OrganizationReference,
    ReviewDocument,
    SourceReference,
    SourceReviewRecord,
)
from climatesense_kg.persistence import InMemoryObservationStore


def _record(name: str, url: str) -> SourceReviewRecord:
    claim = CanonicalClaim(text=f"Claim {name}")
    return SourceReviewRecord(
        source=SourceReference.from_observation(
            source_name="source",
            source_type="dataset",
            observed_url=url,
            claim_text=claim.text,
            discriminator=name,
        ),
        claim=claim,
        organization=OrganizationReference(
            name="Example",
            website="https://example.test",
        ),
        document=ReviewDocument(observed_url=url),
    )


def test_url_ordered_batches_do_not_split_one_document_subject() -> None:
    store = InMemoryObservationStore()
    run = store.start_run("signature")
    records = [
        _record("first", "https://example.test/review#first"),
        _record("other", "https://example.test/another"),
        _record("second", "https://example.test/review#second"),
        _record("third", "https://example.test/review"),
    ]
    store.ingest_source(run.id, "source", records, batch_size=2)

    batches = list(store.iter_batches(run.id, batch_size=2, order_by_url=True))

    assert [len(batch) for batch in batches] == [1, 3]
    assert {record.document.observed_url for record in batches[1]} == {
        "https://example.test/review#first",
        "https://example.test/review#second",
        "https://example.test/review",
    }
