"""Tests for pre-identity document extraction."""

from datetime import UTC, datetime, timedelta
import logging
from threading import Lock
import time
from unittest.mock import Mock, patch
from urllib.parse import urlparse

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
    StageResultStatus,
)
from climatesense_kg.stages import DocumentExtractor, DocumentRetryPolicy
from climatesense_kg.utils.text_processing import (
    ExtractionErrorType,
    TextExtractionResult,
)


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
    extractor = DocumentExtractor(
        store,
        rate_limit_delay=0,
        retry_policy=DocumentRetryPolicy(transient_delay=timedelta(0)),
    )

    extractor.extract(_record())
    recovered = extractor.extract(_record())

    assert fetch.call_count == 2
    assert recovered.document.extracted_text == "Recovered content"


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_permanent_http_failure_is_not_retried_without_force(fetch: Mock) -> None:
    fetch.return_value = TextExtractionResult(
        success=False,
        error_type=ExtractionErrorType.HTTP_ERROR,
        error_message="HTTP 404",
        http_status=404,
    )
    store = InMemoryStageResultStore()
    extractor = DocumentExtractor(store, rate_limit_delay=0)
    record = _record()

    first = extractor.extract_many([record])
    second = extractor.extract_many([_record()])

    assert fetch.call_count == 1
    assert first.computed_permanent_failures == 1
    assert second.stored_permanent_failures == 1
    result = store.get(extractor._key(record))
    assert result is not None
    assert result.status is StageResultStatus.PERMANENT_FAILURE


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_force_regenerate_retries_permanent_http_failure(fetch: Mock) -> None:
    fetch.side_effect = [
        TextExtractionResult(
            success=False,
            error_type=ExtractionErrorType.HTTP_ERROR,
            error_message="HTTP 410",
            http_status=410,
        ),
        TextExtractionResult(success=True, content="Recovered"),
    ]
    store = InMemoryStageResultStore()
    extractor = DocumentExtractor(store, rate_limit_delay=0)

    extractor.extract(_record())
    recovered = extractor.extract(_record(), force=True)

    assert fetch.call_count == 2
    assert recovered.document.extracted_text == "Recovered"


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_access_block_is_deferred_until_cooldown_expires(fetch: Mock) -> None:
    fetch.return_value = TextExtractionResult(
        success=False,
        error_type=ExtractionErrorType.HTTP_ERROR,
        error_message="HTTP 403",
        http_status=403,
    )
    store = InMemoryStageResultStore()
    extractor = DocumentExtractor(store, rate_limit_delay=0)
    record = _record()

    extractor.extract(record)
    report = extractor.extract_many([_record()])

    assert fetch.call_count == 1
    assert report.stored_deferred_failures == 1
    result = store.get(extractor._key(record))
    assert result is not None
    assert result.retry_at is not None
    assert result.retry_at > datetime.now(UTC) + timedelta(days=29)


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_due_deferred_failure_is_retried(fetch: Mock) -> None:
    fetch.return_value = TextExtractionResult(success=True, content="Recovered")
    store = InMemoryStageResultStore()
    extractor = DocumentExtractor(store, rate_limit_delay=0)
    record = _record()
    store.put(
        extractor._key(record),
        StageResult.retryable_failure(
            {"error_type": "http", "http_status": 403},
            retry_at=datetime.now(UTC) - timedelta(seconds=1),
        ),
    )

    recovered = extractor.extract(record)

    fetch.assert_called_once()
    assert recovered.document.extracted_text == "Recovered"
    result = store.get(extractor._key(record))
    assert result is not None
    assert result.status is StageResultStatus.SUCCESS


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_changed_url_is_attempted_after_permanent_failure(fetch: Mock) -> None:
    fetch.side_effect = [
        TextExtractionResult(
            success=False,
            error_type=ExtractionErrorType.HTTP_ERROR,
            error_message="HTTP 404",
            http_status=404,
        ),
        TextExtractionResult(success=True, content="Replacement document"),
    ]
    extractor = DocumentExtractor(InMemoryStageResultStore(), rate_limit_delay=0)
    record = _record()

    extractor.extract(record)
    record.document.observed_url = "https://example.test/replacement"
    recovered = extractor.extract(record)

    assert fetch.call_count == 2
    assert recovered.document.extracted_text == "Replacement document"


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_transient_failure_is_retried_during_current_run(fetch: Mock) -> None:
    fetch.side_effect = [
        TextExtractionResult(
            success=False,
            error_type=ExtractionErrorType.TIMEOUT,
            error_message="Timed out",
        ),
        TextExtractionResult(success=True, content="Recovered"),
    ]
    extractor = DocumentExtractor(
        InMemoryStageResultStore(), rate_limit_delay=0, max_retries=2
    )

    recovered = extractor.extract(_record())

    assert fetch.call_count == 2
    assert recovered.document.extracted_text == "Recovered"


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_server_retry_after_is_persisted(fetch: Mock) -> None:
    retry_at = datetime.now(UTC) + timedelta(hours=6)
    fetch.return_value = TextExtractionResult(
        success=False,
        error_type=ExtractionErrorType.HTTP_ERROR,
        error_message="HTTP 429",
        http_status=429,
        retry_at=retry_at,
    )
    store = InMemoryStageResultStore()
    extractor = DocumentExtractor(store, rate_limit_delay=0)
    record = _record()

    extractor.extract(record)

    result = store.get(extractor._key(record))
    assert result is not None
    assert result.retry_at == retry_at


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_rate_limited_host_is_deferred_while_other_hosts_continue(
    fetch: Mock,
) -> None:
    retry_at = datetime.now(UTC) + timedelta(hours=6)
    first = _record("first")
    deferred = _record("deferred")
    other = _record("other")
    first.document.observed_url = "https://limited.test/first"
    deferred.document.observed_url = "https://limited.test/deferred"
    other.document.observed_url = "https://available.test/other"

    def extract(url: str, *, timeout: int) -> TextExtractionResult:
        assert timeout == 15
        if url == first.document.observed_url:
            return TextExtractionResult(
                success=False,
                error_type=ExtractionErrorType.HTTP_ERROR,
                error_message="HTTP 429",
                http_status=429,
                retry_at=retry_at,
            )
        return TextExtractionResult(success=True, content="Available document")

    fetch.side_effect = extract
    store = InMemoryStageResultStore()
    extractor = DocumentExtractor(
        store,
        max_workers=1,
        rate_limit_delay=0,
    )

    report = extractor.extract_many([first, deferred, other])

    assert [call.args[0] for call in fetch.call_args_list] == [
        first.document.observed_url,
        other.document.observed_url,
    ]
    assert other.document.extracted_text == "Available document"
    first_result = store.get(extractor._key(first))
    deferred_result = store.get(extractor._key(deferred))
    assert first_result is not None
    assert deferred_result is not None
    assert first_result.retry_at == retry_at
    assert deferred_result.retry_at == retry_at
    assert first_result.payload["request_attempted"] is True
    assert deferred_result.payload == {
        "url": deferred.document.observed_url,
        "error_type": "host_cooldown",
        "error_message": (
            "Request deferred because limited.test returned HTTP 429 earlier "
            "in this extraction run"
        ),
        "failure_category": "transient",
        "http_status": 429,
        "request_attempted": False,
    }
    assert report.computed_successes == 1
    assert report.computed_deferred_failures == 2


def test_operational_extraction_settings_do_not_change_result_identity() -> None:
    store = InMemoryStageResultStore()
    record = _record()
    baseline = DocumentExtractor(
        store, rate_limit_delay=0, timeout=5, max_retries=0
    )._key(record)
    tuned = DocumentExtractor(
        store,
        max_workers=32,
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
        max_workers=1,
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
        max_workers=1,
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
    assert "Document extraction: 2/2 processed" in caplog.text
    assert "fetched=2, failed=0" in caplog.text


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_duplicate_urls_are_fetched_once_and_applied_to_every_record(
    fetch: Mock,
) -> None:
    fetch.return_value = TextExtractionResult(
        success=True,
        content="Shared document",
        final_url="https://example.test/shared",
    )
    first = _record("first")
    second = _record("second")
    second.document.observed_url = first.document.observed_url

    report = DocumentExtractor(
        InMemoryStageResultStore(), max_workers=2, rate_limit_delay=0
    ).extract_many([first, second])

    fetch.assert_called_once_with(first.document.observed_url, timeout=15)
    assert first.document.extracted_text == "Shared document"
    assert second.document.extracted_text == "Shared document"
    assert report.eligible_subjects == 1
    assert report.computed_successes == 1


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_duplicate_url_reuses_any_existing_record_result(fetch: Mock) -> None:
    store = InMemoryStageResultStore()
    extractor = DocumentExtractor(store, max_workers=2, rate_limit_delay=0)
    first = _record("first")
    second = _record("second")
    second.document.observed_url = first.document.observed_url
    store.put(
        extractor._key(second),
        StageResult.succeeded({"content": "Already extracted"}),
    )

    report = extractor.extract_many([first, second])

    fetch.assert_not_called()
    assert first.document.extracted_text == "Already extracted"
    assert second.document.extracted_text == "Already extracted"
    assert report.stored_successes == 1


@patch("climatesense_kg.stages.document_extractor.fetch_and_extract_text")
def test_extraction_runs_across_hosts_but_serializes_each_host(fetch: Mock) -> None:
    lock = Lock()
    active = 0
    maximum_active = 0
    active_by_host: dict[str, int] = {}
    maximum_by_host: dict[str, int] = {}

    def extract(url: str, *, timeout: int) -> TextExtractionResult:
        del timeout
        nonlocal active, maximum_active
        host = urlparse(url).hostname or ""
        with lock:
            active += 1
            active_by_host[host] = active_by_host.get(host, 0) + 1
            maximum_active = max(maximum_active, active)
            maximum_by_host[host] = max(
                maximum_by_host.get(host, 0), active_by_host[host]
            )
        time.sleep(0.05)
        with lock:
            active -= 1
            active_by_host[host] -= 1
        return TextExtractionResult(success=True, content=url)

    fetch.side_effect = extract
    records = [_record(str(index)) for index in range(4)]
    records[0].document.observed_url = "https://first.test/one"
    records[1].document.observed_url = "https://first.test/two"
    records[2].document.observed_url = "https://second.test/one"
    records[3].document.observed_url = "https://second.test/two"

    DocumentExtractor(
        InMemoryStageResultStore(), max_workers=4, rate_limit_delay=0
    ).extract_many(records)

    assert maximum_active == 2
    assert maximum_by_host == {"first.test": 1, "second.test": 1}
