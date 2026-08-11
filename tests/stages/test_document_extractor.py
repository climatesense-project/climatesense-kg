"""Tests for pre-identity document extraction."""

import logging
from unittest.mock import Mock, patch

import pytest

from climatesense_kg.domain import (
    CanonicalClaim,
    OrganizationReference,
    ReviewDocument,
    SourceReference,
    SourceReviewRecord,
)
from climatesense_kg.persistence import (
    InMemoryStageResultStore,
    StageResult,
    StageResultKey,
)
from climatesense_kg.stages import DocumentExtractor
from climatesense_kg.utils.text_processing import TextExtractionResult


class TrackingStageResultStore(InMemoryStageResultStore):
    def __init__(self) -> None:
        super().__init__()
        self.put_batch_sizes: list[int] = []

    def put_many(self, results: dict[StageResultKey, StageResult]) -> None:
        self.put_batch_sizes.append(len(results))
        super().put_many(results)


def _record(identifier: str = "observed") -> SourceReviewRecord:
    url = f"https://example.test/{identifier}"
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


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_stored_document_failure_is_retried(fetch: Mock) -> None:
    fetch.side_effect = [
        TextExtractionResult(success=False, error_message="Timed out"),
        TextExtractionResult(success=True, content="Recovered content"),
    ]
    store = InMemoryStageResultStore()
    extractor = DocumentExtractor(store, rate_limit_delay=0)

    extractor.extract(_record())
    recovered = extractor.extract(_record())

    assert fetch.call_count == 2
    assert recovered.document.extracted_text == "Recovered content"


def test_operational_extraction_settings_do_not_change_result_identity() -> None:
    store = InMemoryStageResultStore()
    record = _record()
    baseline = DocumentExtractor(
        store, rate_limit_delay=0, timeout=5, max_retries=0
    )._key(record)
    tuned = DocumentExtractor(
        store,
        rate_limit_delay=3,
        timeout=60,
        max_retries=5,
        checkpoint_size=100,
        progress_interval_seconds=60,
    )._key(record)

    assert tuned == baseline


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_results_are_persisted_in_checkpoints(fetch: Mock) -> None:
    fetch.return_value = TextExtractionResult(success=True, content="Extracted")
    store = TrackingStageResultStore()
    extractor = DocumentExtractor(
        store,
        rate_limit_delay=0,
        checkpoint_size=2,
        progress_interval_seconds=60,
    )

    report = extractor.extract_many([_record(str(index)) for index in range(5)])

    assert store.put_batch_sizes == [2, 2, 1]
    assert report.eligible_subjects == 5
    assert report.computed_successes == 5
    assert report.missing_results == 0


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_interruption_flushes_completed_checkpoint_work(fetch: Mock) -> None:
    fetch.side_effect = [
        TextExtractionResult(success=True, content="First"),
        TextExtractionResult(success=True, content="Second"),
        KeyboardInterrupt(),
    ]
    store = TrackingStageResultStore()
    extractor = DocumentExtractor(
        store,
        rate_limit_delay=0,
        checkpoint_size=10,
        progress_interval_seconds=60,
    )
    records = [_record(str(index)) for index in range(3)]

    with pytest.raises(KeyboardInterrupt):
        extractor.extract_many(records)

    assert store.put_batch_sizes == [2]
    assert store.get(extractor._key(records[0])) is not None
    assert store.get(extractor._key(records[1])) is not None
    assert store.get(extractor._key(records[2])) is None


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_live_progress_reports_counts(
    fetch: Mock, caplog: pytest.LogCaptureFixture
) -> None:
    fetch.return_value = TextExtractionResult(success=True, content="Extracted")
    extractor = DocumentExtractor(
        InMemoryStageResultStore(),
        rate_limit_delay=0,
        checkpoint_size=2,
        progress_interval_seconds=0,
    )

    with caplog.at_level(
        logging.INFO, logger="climatesense_kg.stages.document_extractor"
    ):
        extractor.extract_many([_record("first"), _record("second")])

    assert "Document extraction: 0/2 processed" in caplog.text
    assert "Document extraction: 1/2 processed" in caplog.text
    assert "Document extraction: 2/2 processed" in caplog.text
    assert "fetched=2, failed=0" in caplog.text
