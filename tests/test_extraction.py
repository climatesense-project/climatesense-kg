"""Document extraction retry-policy tests."""

from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

from climatesense_kg.extraction import (
    DocumentExtractionService,
    FailureCategory,
    RetryPolicy,
)
from climatesense_kg.processing import ResultStatus
from climatesense_kg.utils.text_processing import (
    ExtractionErrorType,
    TextExtractionResult,
)


def _service() -> DocumentExtractionService:
    return DocumentExtractionService(
        Mock(),
        rate_limit_delay=0,
        retry_policy=RetryPolicy(
            transient_delay=timedelta(hours=1),
            blocked_delay=timedelta(days=30),
            dns_delay=timedelta(days=7),
            content_delay=timedelta(days=30),
        ),
    )


def test_forbidden_response_is_deferred_as_access_blocked() -> None:
    result = _service()._classify_failure(
        TextExtractionResult(
            False,
            error_type=ExtractionErrorType.HTTP_ERROR,
            http_status=403,
        ),
        {"http_status": 403},
    )

    assert result.status is ResultStatus.RETRYABLE_FAILURE
    assert result.payload["failure_category"] == FailureCategory.ACCESS_BLOCKED
    assert result.retry_at is not None


def test_dead_link_is_a_permanent_failure() -> None:
    result = _service()._classify_failure(
        TextExtractionResult(
            False,
            error_type=ExtractionErrorType.HTTP_ERROR,
            http_status=404,
        ),
        {"http_status": 404},
    )

    assert result.status is ResultStatus.PERMANENT_FAILURE
    assert result.payload["failure_category"] == FailureCategory.PERMANENT


def test_server_retry_time_takes_precedence_over_local_cooldown() -> None:
    retry_at = datetime.now(UTC) + timedelta(hours=4)
    result = _service()._classify_failure(
        TextExtractionResult(
            False,
            error_type=ExtractionErrorType.HTTP_ERROR,
            http_status=429,
            retry_at=retry_at,
        ),
        {"http_status": 429},
    )

    assert result.retry_at == retry_at
