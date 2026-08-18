"""ClaimReviewData data processor."""

import codecs
from collections.abc import Iterator
from io import BytesIO
import json
from typing import Any, BinaryIO

from ..domain import (
    CanonicalClaim,
    CanonicalRating,
    OrganizationReference,
    SourceReviewRecord,
)
from .base import BaseProcessor


class ClaimReviewDataProcessor(BaseProcessor):
    """Processor for ClaimReviewData JSON data."""

    def process(self, raw_data: bytes) -> Iterator[SourceReviewRecord]:
        """Process ClaimReviewData raw data into source observations."""
        yield from self.process_stream(BytesIO(raw_data))

    def process_stream(self, raw_data: BinaryIO) -> Iterator[SourceReviewRecord]:
        """Incrementally decode the top-level JSON array."""

        try:
            for item in self._iter_json_array(raw_data):
                try:
                    is_valid, errors = self._validate_item(item)
                    if not is_valid:
                        review_url = (
                            item.get("review_url", "unknown")
                            if isinstance(item, dict)
                            else "unknown"
                        )
                        self.logger.warning(
                            "Skipping invalid item %s: %s",
                            review_url,
                            "; ".join(errors),
                        )
                        continue

                    yield from self._normalize_item(item)
                except Exception as e:
                    review_url = (
                        item.get("review_url", "unknown")
                        if isinstance(item, dict)
                        else "unknown"
                    )
                    self.logger.warning(
                        "Failed to normalize item %s: %s", review_url, e
                    )
                    continue

        except json.JSONDecodeError as exc:
            self.logger.error("Invalid JSON data: %s", exc)
            raise
        except Exception as exc:
            self.logger.error("Error processing ClaimReviewData data: %s", exc)
            raise

    @staticmethod
    def _iter_json_array(raw_data: BinaryIO) -> Iterator[Any]:
        """Yield values from a UTF-8 JSON array with bounded buffering."""

        reader = codecs.getreader("utf-8")(raw_data)
        decoder = json.JSONDecoder()
        buffer = ""
        eof = False
        state = "start"

        def fill() -> bool:
            nonlocal buffer, eof
            chunk = reader.read(64 * 1024)
            if not chunk:
                eof = True
                return False
            buffer += chunk
            return True

        def finish() -> None:
            nonlocal buffer
            while not eof and fill():
                pass
            if buffer[1:].strip():
                raise ValueError("Unexpected data after ClaimReviewData JSON array")

        while True:
            buffer = buffer.lstrip()
            if not buffer and not eof:
                fill()
                continue
            if state == "start":
                if not buffer:
                    raise ValueError("ClaimReviewData payload is empty")
                if buffer[0] != "[":
                    raise ValueError("ClaimReviewData payload must be a list")
                buffer = buffer[1:]
                state = "value"
                continue
            if state == "value":
                if buffer.startswith("]"):
                    finish()
                    return
                try:
                    item, end = decoder.raw_decode(buffer)
                except json.JSONDecodeError:
                    if not eof and fill():
                        continue
                    raise
                yield item
                buffer = buffer[end:]
                state = "separator"
                continue
            if buffer.startswith(","):
                buffer = buffer[1:]
                state = "value"
                continue
            if buffer.startswith("]"):
                finish()
                return
            if not eof and fill():
                continue
            raise ValueError("ClaimReviewData JSON array is missing a separator")

    def _normalize_item(self, item: dict[str, Any]) -> list[SourceReviewRecord]:
        """Convert every unambiguous claim/rating pair in one source item."""
        claim_texts: list[str] = item["claim_text"]
        reviews: list[dict[str, Any]] = item["reviews"]
        if len(claim_texts) == len(reviews):
            claim_review_pairs = list(zip(claim_texts, reviews, strict=True))
        elif len(claim_texts) == 1:
            claim_review_pairs = [(claim_texts[0], review) for review in reviews]
        elif len(reviews) == 1:
            claim_review_pairs = [
                (claim_text, reviews[0]) for claim_text in claim_texts
            ]
        else:
            raise ValueError(
                "Ambiguous multi-claim record: claim_text and reviews must have "
                "equal lengths unless one side contains a single value"
            )

        fact_checker = item.get("fact_checker", {})
        organization = OrganizationReference(
            name=fact_checker.get("name", ""),
            website=fact_checker.get("website", ""),
            language=fact_checker.get("language", ""),
        )

        source_records: list[SourceReviewRecord] = []
        for index, (claim_text, source_review) in enumerate(claim_review_pairs):
            try:
                claim = CanonicalClaim(
                    text=claim_text,
                    appearances=[
                        appearance
                        for appearance in item.get("appearances", [])
                        if isinstance(appearance, str) and appearance
                    ],
                )
            except ValueError as exc:
                self.logger.warning(
                    "Skipping invalid claim text for %s: %s",
                    item.get("review_url", "unknown"),
                    exc,
                )
                continue
            rating = CanonicalRating(
                label=source_review.get("label", ""),
                original_label=source_review.get("original_label", ""),
            )

            date_published = item.get("date_published") or source_review.get(
                "date_published"
            )
            source_records.append(
                self._source_record(
                    source_type="claimreviewdata",
                    claim=claim,
                    organization=organization,
                    review_url=item.get("review_url", ""),
                    discriminator=str(index),
                    date_published=str(date_published) if date_published else None,
                    language=item.get("language") or fact_checker.get("language"),
                    rating=rating,
                )
            )

        return source_records

    def _validate_item(self, item: Any) -> tuple[bool, list[str]]:
        """Validate a ClaimReviewData item and return validation errors.

        Returns:
            (is_valid, errors)
        """
        if not isinstance(item, dict) or not item:
            return False, ["item must be a non-empty mapping"]

        errors: list[str] = []

        claim_text = item.get("claim_text")
        if (
            not isinstance(claim_text, list)
            or not claim_text
            or not all(isinstance(text, str) and text for text in claim_text)
        ):
            errors.append("claim_text must be a non-empty list of strings")

        if not isinstance(item.get("review_url"), str) or not item["review_url"]:
            errors.append("missing review_url")

        fact_checker = item.get("fact_checker", {})
        if not isinstance(fact_checker, dict):
            errors.append("fact_checker must be a mapping")
        else:
            if (
                not isinstance(fact_checker.get("name"), str)
                or not fact_checker["name"]
            ):
                errors.append("fact_checker is missing name")
            if (
                not isinstance(fact_checker.get("website"), str)
                or not fact_checker["website"]
            ):
                errors.append("fact_checker is missing website")

        appearances = item.get("appearances", [])
        if not isinstance(appearances, list):
            errors.append("appearances must be a list")

        reviews = item.get("reviews")
        if not isinstance(reviews, list) or not reviews:
            errors.append("reviews must be a non-empty list")
        else:
            for review in reviews:
                if not isinstance(review, dict):
                    errors.append("review must be a mapping")
                elif not isinstance(
                    review.get("original_label"), str
                ) or not review.get("original_label"):
                    errors.append("review is missing original_label")

        return not errors, errors
