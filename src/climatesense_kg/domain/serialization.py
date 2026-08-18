"""Lossless JSON representation of source observations."""

from dataclasses import asdict
from typing import Any

from dacite import Config, from_dict

from .models import SourceReviewRecord


def source_record_to_payload(record: SourceReviewRecord) -> dict[str, Any]:
    """Serialize one source observation for durable batch processing."""

    return asdict(record)


def source_record_from_payload(payload: dict[str, Any]) -> SourceReviewRecord:
    """Restore one source observation from its durable representation."""

    return from_dict(
        data_class=SourceReviewRecord,
        data=payload,
        config=Config(strict=True),
    )
