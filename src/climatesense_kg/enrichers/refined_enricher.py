"""ReFinED entity-extraction stages."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Literal

import requests

from .. import USER_AGENT
from ..domain import CanonicalClaimReview, EntityMention
from ..processing import ProcessingResult, stable_hash
from .base import Enricher
from .wikidata import wikidata_entity_uri


class RefinedEnricher(Enricher):
    """Extract Wikidata entities for either canonical claim text or exact review text."""

    def __init__(
        self,
        *,
        target: Literal["claim", "review"],
        api_url: str = "https://refined.tools.eurecom.fr/annotate_text",
        confidence: float = 0.5,
        timeout: int = 20,
        max_workers: int = 8,
    ) -> None:
        super().__init__(
            f"refined.{target}",
            version="1",
            semantic_config={"confidence": confidence},
            availability_key="refined",
            batch_size=1,
            max_workers=max_workers,
        )
        self.target = target
        self.api_url = api_url
        self.confidence = confidence
        self.timeout = timeout
        self.headers = {
            "accept": "application/json",
            "User-Agent": USER_AGENT,
        }

    def is_available(self) -> bool:
        try:
            response = requests.post(
                self.api_url,
                headers=self.headers,
                json={"text": "test"},
                timeout=5,
            )
            return response.status_code == 200
        except Exception as exc:
            self.logger.warning("ReFinED unavailable: %s", exc)
            return False

    def eligible_items(
        self, items: list[CanonicalClaimReview]
    ) -> list[CanonicalClaimReview]:
        if self.target == "claim":
            return items
        return [item for item in items if (item.review_text or "").strip()]

    def subject_key(self, item: CanonicalClaimReview) -> str:
        if self.target == "claim":
            return item.claim.uri
        review_text = item.review_text or ""
        digest = stable_hash(review_text)
        return f"review-text/{digest}"

    def input_value(self, item: CanonicalClaimReview) -> Any:
        return {"text": self._text(item)}

    def compute_item(self, item: CanonicalClaimReview) -> ProcessingResult:
        entities = self._extract_entities(self._text(item))
        return ProcessingResult.success(
            {"entities": [asdict(entity) for entity in entities]},
        )

    def apply_item(self, item: CanonicalClaimReview, payload: dict[str, Any]) -> None:
        target_entities = (
            item.claim.analysis.entities
            if self.target == "claim"
            else item.analysis.entities
        )
        target_entities[:] = [
            entity for entity in target_entities if entity.source != "refined"
        ]
        target_entities.extend(self._deserialize_entities(payload.get("entities")))

    def _text(self, item: CanonicalClaimReview) -> str:
        return (
            item.claim.analysis_text
            if self.target == "claim"
            else item.review_text or ""
        )

    def _extract_entities(self, text: str) -> list[EntityMention]:
        if len(text.strip()) < 10:
            return []
        response = requests.post(
            self.api_url,
            headers=self.headers,
            json={"text": text},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return self._parse_refined_response(response.json())

    def _parse_refined_response(self, data: dict[str, Any]) -> list[EntityMention]:
        entities: list[EntityMention] = []
        spans = data.get("spans")
        if not isinstance(spans, list):
            return entities
        for span in spans:
            if not isinstance(span, dict):
                continue
            entity = self._parse_span(span)
            if entity is not None:
                entities.append(entity)
        return entities

    def _parse_span(self, span: dict[str, Any]) -> EntityMention | None:
        if span.get("coarse_type") != "MENTION":
            return None
        linked = span.get("entity")
        if not isinstance(linked, dict):
            return None
        qid = str(linked.get("qid") or "")
        if not qid:
            return None
        confidence = self._optional_float(span.get("confidence"))
        if confidence is not None and confidence < self.confidence:
            return None
        return EntityMention(
            uri=wikidata_entity_uri(qid),
            source="refined",
            surface_form=str(span.get("text") or ""),
            types=self._type_uris(span),
            confidence=confidence,
            offset=self._optional_int(span.get("start")),
        )

    @staticmethod
    def _type_uris(span: dict[str, Any]) -> list[str]:
        types = span.get("types")
        if not isinstance(types, list):
            return []
        return [
            wikidata_entity_uri(str(item["id"]))
            for item in types
            if isinstance(item, dict) and item.get("id")
        ]

    @staticmethod
    def _optional_float(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _optional_int(value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _deserialize_entities(value: Any) -> list[EntityMention]:
        if not isinstance(value, list):
            return []
        entities: list[EntityMention] = []
        for item in value:
            if not isinstance(item, dict) or not item.get("uri"):
                continue
            entities.append(
                EntityMention(
                    uri=str(item["uri"]),
                    source=str(item.get("source", "refined")),
                    surface_form=str(item.get("surface_form", "")),
                    types=[str(entity_type) for entity_type in item.get("types", [])],
                    confidence=item.get("confidence"),
                    support=item.get("support"),
                    offset=item.get("offset"),
                )
            )
        return entities
