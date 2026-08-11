"""Acceptance tests for claim-review identity resolution."""

from collections.abc import Iterator
from contextlib import contextmanager
import logging

import pytest

from climatesense_kg.domain import (
    CanonicalClaim,
    CanonicalOrganization,
    CanonicalRating,
    OrganizationReference,
    ReviewDocument,
    SourceReference,
    SourceReviewRecord,
)
from climatesense_kg.identity import (
    IdentityResolver,
    IdentityTransaction,
    InMemoryIdentityRegistry,
)

ORGANIZATION = CanonicalOrganization(
    uri="https://data.example.test/organization/factual",
    name="Factual",
    website="https://factual.ro",
)


class CountingIdentityRegistry(InMemoryIdentityRegistry):
    def __init__(self) -> None:
        super().__init__()
        self.transaction_count = 0

    @contextmanager
    def transaction(self) -> Iterator[IdentityTransaction]:
        self.transaction_count += 1
        with super().transaction() as transaction:
            yield transaction


def _record(
    record_name: str,
    *,
    url: str,
    claim: str = "A reviewed claim",
    text: str | None = None,
    rating: str = "not_credible",
    final_url: str | None = None,
    canonical_url: str | None = None,
    native_id: str | None = None,
) -> SourceReviewRecord:
    return SourceReviewRecord(
        source=SourceReference.from_observation(
            source_name="claimreviewdata",
            source_type="claimreviewdata",
            observed_url=url,
            claim_text=claim,
            native_id=native_id,
            discriminator=record_name,
        ),
        claim=CanonicalClaim(text=claim),
        organization=OrganizationReference(
            name=ORGANIZATION.name,
            website=ORGANIZATION.website,
        ),
        document=ReviewDocument(
            observed_url=url,
            final_url=final_url,
            canonical_url=canonical_url,
            extracted_text=text,
        ),
        rating=CanonicalRating(label=rating),
    )


def test_exact_document_evidence_merges_url_aliases() -> None:
    resolver = IdentityResolver(InMemoryIdentityRegistry())
    text = " ".join(f"word-{index}" for index in range(80))

    first = resolver.resolve(
        _record("first", url="https://factual.ro/old", text=text), ORGANIZATION
    )
    second = resolver.resolve(
        _record("second", url="https://factual.ro/new", text=text), ORGANIZATION
    )

    assert second.id == first.id
    assert second.document.id == first.document.id
    assert second.document.urls == {
        "https://factual.ro/old",
        "https://factual.ro/new",
    }
    assert len(second.source_record_keys) == 2


def test_canonical_url_merges_content_variants() -> None:
    resolver = IdentityResolver(InMemoryIdentityRegistry())
    canonical_url = "https://factual.ro/canonical"

    first = resolver.resolve(
        _record(
            "first",
            url="https://factual.ro/tracking-a",
            canonical_url=canonical_url,
            text="Original short article text",
        ),
        ORGANIZATION,
    )
    second = resolver.resolve(
        _record(
            "second",
            url="https://factual.ro/tracking-b",
            canonical_url=canonical_url,
            text="Edited and expanded article text",
        ),
        ORGANIZATION,
    )

    assert second.id == first.id
    assert second.document.id == first.document.id
    assert canonical_url in second.document.urls


def test_resolved_document_selects_one_coherent_longest_variant() -> None:
    resolver = IdentityResolver(InMemoryIdentityRegistry())
    canonical_url = "https://factual.ro/canonical"
    long_text = " ".join(f"word{index}" for index in range(80))

    resolver.resolve(
        _record(
            "long",
            url="https://factual.ro/long",
            canonical_url=canonical_url,
            text=long_text,
        ),
        ORGANIZATION,
    )
    resolved = resolver.resolve(
        _record(
            "short",
            url="https://factual.ro/short",
            canonical_url=canonical_url,
            text="Short text variant",
        ),
        ORGANIZATION,
    )

    assert resolved.review_text == long_text
    assert resolved.document.word_count == 80


def test_existing_source_record_keeps_identity_after_content_edit() -> None:
    resolver = IdentityResolver(InMemoryIdentityRegistry())
    original = _record(
        "stable", url="https://factual.ro/article", text="Original article"
    )
    edited = _record(
        "stable", url="https://factual.ro/article", text="Completely edited article"
    )

    first = resolver.resolve(original, ORGANIZATION)
    second = resolver.resolve(edited, ORGANIZATION)

    assert second.id == first.id
    assert second.document.id == first.document.id
    assert second.review_text == "Completely edited article"


def test_one_document_can_have_two_claim_reviews() -> None:
    resolver = IdentityResolver(InMemoryIdentityRegistry())
    canonical_url = "https://factual.ro/multiple-claims"

    first = resolver.resolve(
        _record("first", url=canonical_url, claim="First reviewed claim"),
        ORGANIZATION,
    )
    second = resolver.resolve(
        _record("second", url=canonical_url, claim="Second reviewed claim"),
        ORGANIZATION,
    )

    assert second.document.id == first.document.id
    assert second.id != first.id


def test_similarity_only_records_candidate_without_merging() -> None:
    registry = InMemoryIdentityRegistry()
    resolver = IdentityResolver(registry)
    shared = [f"shared{index}" for index in range(70)]
    first_text = " ".join([*shared, "first", "ending"])
    second_text = " ".join([*shared, "second", "ending"])

    first = resolver.resolve(
        _record("first", url="https://factual.ro/a", text=first_text), ORGANIZATION
    )
    second = resolver.resolve(
        _record("second", url="https://factual.ro/b", text=second_text), ORGANIZATION
    )

    assert second.id != first.id
    assert registry.candidates
    _, candidate = registry.candidates[0]
    assert candidate.candidate_review_id == first.id
    assert candidate.evidence["kind"] == "body_similarity"
    assert candidate.similarity >= 0.9


def test_cross_organization_similarity_does_not_create_candidate() -> None:
    registry = InMemoryIdentityRegistry()
    resolver = IdentityResolver(registry)
    other = CanonicalOrganization(
        uri="https://data.example.test/organization/other",
        name="Other",
        website="https://other.example",
    )
    text = " ".join(f"shared{index}" for index in range(80))

    first = resolver.resolve(
        _record("first", url="https://factual.ro/a", text=text), ORGANIZATION
    )
    second_record = _record("second", url="https://other.example/a", text=text)
    second_record.organization = OrganizationReference(
        name=other.name, website=other.website
    )
    second = resolver.resolve(second_record, other)

    assert second.id != first.id
    assert not registry.candidates


def test_rating_change_does_not_change_deterministically_matched_identity() -> None:
    registry = InMemoryIdentityRegistry()
    resolver = IdentityResolver(registry)
    canonical_url = "https://factual.ro/conflicting-rating"

    first = resolver.resolve(
        _record(
            "first",
            url="https://factual.ro/a",
            canonical_url=canonical_url,
            rating="not_credible",
        ),
        ORGANIZATION,
    )
    second = resolver.resolve(
        _record(
            "second",
            url="https://factual.ro/b",
            canonical_url=canonical_url,
            rating="credible",
        ),
        ORGANIZATION,
    )

    assert second.id == first.id
    assert second.document.id == first.document.id
    assert not registry.candidates


def test_resolve_many_commits_bounded_batches_and_logs_progress(
    caplog: pytest.LogCaptureFixture,
) -> None:
    registry = CountingIdentityRegistry()
    resolver = IdentityResolver(
        registry,
        batch_size=2,
        progress_interval_seconds=0,
    )
    records = [
        (
            _record(
                str(index),
                url=f"https://factual.ro/review-{index}",
            ),
            ORGANIZATION,
        )
        for index in range(5)
    ]

    with caplog.at_level(logging.INFO, logger="climatesense_kg.identity.resolver"):
        reviews = resolver.resolve_many(records)

    assert registry.transaction_count == 3
    assert len(reviews) == 5
    assert "Identity resolution: 0/5 committed" in caplog.text
    assert "Identity resolution: 2/5 committed" in caplog.text
    assert "Identity resolution: 4/5 committed" in caplog.text
    assert "Identity resolution: 5/5 committed" in caplog.text
