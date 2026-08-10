"""Tests for pre-identity document extraction."""

from unittest.mock import Mock, patch

from climatesense_kg.domain import (
    CanonicalClaim,
    OrganizationReference,
    ReviewDocument,
    SourceReference,
    SourceReviewRecord,
)
from climatesense_kg.persistence import InMemoryStageResultStore
from climatesense_kg.stages import DocumentExtractor
from climatesense_kg.utils.text_processing import TextExtractionResult


def _record() -> SourceReviewRecord:
    url = "https://example.test/observed"
    return SourceReviewRecord(
        source=SourceReference.from_observation(
            source_name="test",
            source_type="test",
            observed_url=url,
            claim_text="A reviewed claim",
        ),
        claim=CanonicalClaim(text="A reviewed claim"),
        organization=OrganizationReference(
            name="Example", website="https://example.test"
        ),
        document=ReviewDocument(observed_url=url),
    )


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_extraction_populates_identity_evidence_and_reuses_stage_result(
    fetch: Mock,
) -> None:
    fetch.return_value = TextExtractionResult(
        success=True,
        content="Extracted review body",
        final_url="https://example.test/final",
        canonical_url="https://example.test/canonical",
    )
    store = InMemoryStageResultStore()
    extractor = DocumentExtractor(store, rate_limit_delay=0)

    first = extractor.extract(_record())
    second = extractor.extract(_record())

    assert first.document.extracted_text == "Extracted review body"
    assert first.document.final_url == "https://example.test/final"
    assert first.document.canonical_url == "https://example.test/canonical"
    assert second.document.extracted_text == first.document.extracted_text
    fetch.assert_called_once_with("https://example.test/observed", timeout=15)


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_non_http_source_document_is_not_fetched(fetch: Mock) -> None:
    record = _record()
    record.document.observed_url = "urn:dataset:review:42"

    result = DocumentExtractor(InMemoryStageResultStore(), rate_limit_delay=0).extract(
        record
    )

    assert result is record
    fetch.assert_not_called()


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_failure_state_retains_url_for_operational_diagnostics(fetch: Mock) -> None:
    fetch.return_value = TextExtractionResult(
        success=False,
        error_message="Timed out",
    )
    record = _record()
    store = InMemoryStageResultStore()
    extractor = DocumentExtractor(store, rate_limit_delay=0)

    extractor.extract(record)

    result = store.get(extractor._key(record))
    assert result is not None
    assert result.success is False
    assert result.payload["url"] == record.document.observed_url
