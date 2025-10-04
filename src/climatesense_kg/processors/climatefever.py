"""CLIMATE-FEVER dataset processor."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
import json
from typing import Any

from pydantic import BaseModel, Field, field_validator

from ..config.models import (
    CanonicalClaim,
    CanonicalClaimReview,
    CanonicalOrganization,
    CanonicalRating,
)
from ..utils.text_processing import sanitize_url
from .base import BaseProcessor

_REVIEW_URN_PREFIX = "urn:climate-fever:review:"
_DATASET_ORGANIZATION = CanonicalOrganization(
    name="CLIMATE-FEVER Dataset",
    website="https://www.sustainablefinance.uzh.ch/en/research/climate-fever.html",
    language="en",
)

_CLAIM_LABEL_MAP = {
    "SUPPORTS": "Supports",
    "REFUTES": "Refutes",
    "NOT_ENOUGH_INFO": "Not Enough Info",
}


class EvidenceItem(BaseModel):
    """Evidence item."""

    article: str = Field(..., min_length=1)
    evidence: str = Field(..., min_length=1)
    evidence_label: str | None = None

    @field_validator("article", "evidence", mode="before")
    @classmethod
    def strip_strings(cls, value: Any) -> Any:
        """Strip whitespace from string fields."""
        if isinstance(value, str):
            return value.strip()
        return value


class ClimateFeverItem(BaseModel):
    """Dataset item."""

    claim_id: int | str
    claim: str = Field(..., min_length=1)
    claim_label: str | None = None
    evidences: list[EvidenceItem] = Field(..., min_length=1)

    @field_validator("claim", mode="before")
    @classmethod
    def strip_claim(cls, value: Any) -> Any:
        """Strip whitespace from claim text."""
        if isinstance(value, str):
            return value.strip()
        return value


class ClimateFeverProcessor(BaseProcessor):
    """Processor for the CLIMATE-FEVER JSONL dataset."""

    def process(self, raw_data: bytes) -> Iterator[CanonicalClaimReview]:
        try:
            text_stream = raw_data.decode("utf-8")
        except UnicodeDecodeError as exc:
            self.logger.error("Failed to decode CLIMATE-FEVER payload: %s", exc)
            return

        for line_number, line in enumerate(text_stream.splitlines(), start=1):
            if not line.strip():
                continue

            try:
                raw_item = json.loads(line)
            except json.JSONDecodeError as exc:
                self.logger.warning(
                    "Skipping line %d due to JSON error: %s", line_number, exc.msg
                )
                continue

            try:
                item = ClimateFeverItem.model_validate(raw_item)
            except Exception as exc:
                self.logger.warning(
                    "Skipping line %d due to validation error: %s", line_number, exc
                )
                continue

            try:
                yield self._normalize_item(item)
            except Exception as exc:
                self.logger.warning(
                    "Failed to normalize CLIMATE-FEVER item %s: %s",
                    item.claim_id,
                    exc,
                )

    def _normalize_item(self, item: ClimateFeverItem) -> CanonicalClaimReview:
        claim_id = str(item.claim_id)
        review_url = self._build_review_url(claim_id)

        claim = CanonicalClaim(
            text=item.claim,
            appearances=self._build_appearances(item.evidences),
        )

        rating = self._build_rating(item.claim_label)
        review_text = self._build_review_text(item.evidences)

        return CanonicalClaimReview(
            claim=claim,
            review_url=review_url,
            organization=_DATASET_ORGANIZATION,
            language="en",
            rating=rating,
            review_text=review_text if review_text else None,
            source_type="climate-fever",
            source_name=self.name,
        )

    def _build_review_url(self, claim_id: str) -> str:
        return f"{_REVIEW_URN_PREFIX}{claim_id}"

    def _build_appearances(self, evidences: Iterable[EvidenceItem]) -> list[str]:
        seen: set[str] = set()
        appearances: list[str] = []

        def add_url(candidate: str | None) -> None:
            if not candidate:
                return
            sanitized = sanitize_url(candidate)
            if not sanitized or sanitized in seen:
                return
            seen.add(sanitized)
            appearances.append(sanitized)

        for evidence in evidences:
            article = evidence.article
            if not article:
                continue
            wikipedia_url = f"https://en.wikipedia.org/wiki/{article.replace(' ', '_')}"
            add_url(wikipedia_url)

        return appearances

    def _build_rating(self, label: str | None) -> CanonicalRating | None:
        if not label:
            return None
        mapped = _CLAIM_LABEL_MAP.get(label)
        if not mapped:
            self.logger.debug("Unknown CLIMATE-FEVER label: %s", label)
            return CanonicalRating(label=label, original_label=label)
        return CanonicalRating(label=mapped, original_label=label)

    def _build_review_text(self, evidences: Iterable[EvidenceItem]) -> str:
        lines: list[str] = []
        for index, evidence in enumerate(evidences, start=1):
            sentence = evidence.evidence
            if not sentence:
                continue
            label = evidence.evidence_label
            article = evidence.article
            prefix_parts = [f"Evidence {index}"]
            if label:
                prefix_parts.append(f"[{label}]")
            if article:
                prefix_parts.append(f"({article})")
            prefix = " ".join(prefix_parts)
            lines.append(f"{prefix}: {sentence}")
        return "\n".join(lines)
