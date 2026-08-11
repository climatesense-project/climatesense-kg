"""DBpedia Spotlight entity-extraction stages."""

from __future__ import annotations

from dataclasses import asdict
import time
from typing import Any, Literal

import requests

from .. import USER_AGENT
from ..domain import CanonicalClaimReview, EntityMention
from ..persistence import StageResult, StageResultStore, stable_hash
from .base import Enricher


class DBpediaSpotlightEnricher(Enricher):
    """Extract entities for either canonical claim text or exact review text."""

    def __init__(
        self,
        *,
        target: Literal["claim", "review"],
        store: StageResultStore,
        api_url: str = "https://api.dbpedia-spotlight.org/en/annotate",
        model_id: str = "dbpedia-spotlight-en",
        confidence: float = 0.5,
        support: int = 20,
        timeout: int = 20,
        rate_limit_delay: float = 0.1,
        checkpoint_size: int = 100,
        progress_interval_seconds: float = 10.0,
    ) -> None:
        super().__init__(
            f"dbpedia_spotlight.{target}",
            version="1",
            store=store,
            semantic_config={
                "model_id": model_id,
                "confidence": confidence,
                "support": support,
            },
            availability_key="dbpedia_spotlight",
            compute_batch_size=25,
            checkpoint_size=checkpoint_size,
            progress_interval_seconds=progress_interval_seconds,
        )
        self.target = target
        self.api_url = api_url
        self.confidence = confidence
        self.support = support
        self.timeout = timeout
        self.rate_limit_delay = rate_limit_delay
        self.headers = {
            "accept": "application/json",
            "User-Agent": USER_AGENT,
        }

    def is_available(self) -> bool:
        try:
            response = requests.post(
                self.api_url,
                headers=self.headers,
                data={"text": "test"},
                timeout=5,
            )
            return response.status_code == 200
        except Exception as exc:
            self.logger.warning("DBpedia Spotlight unavailable: %s", exc)
            return False

    def _eligible_items(
        self, items: list[CanonicalClaimReview]
    ) -> list[CanonicalClaimReview]:
        if self.target == "claim":
            return items
        return [item for item in items if (item.review_text or "").strip()]

    def _subject_key(self, item: CanonicalClaimReview) -> str:
        if self.target == "claim":
            return item.claim.uri
        review_text = item.review_text or ""
        digest = stable_hash(review_text)
        return f"review-text/{digest}"

    def _input_value(self, item: CanonicalClaimReview) -> Any:
        return {"text": self._text(item)}

    def _compute(
        self, item: CanonicalClaimReview, *, force: bool = False
    ) -> StageResult:
        del force
        entities = self._extract_entities(self._text(item))
        return StageResult(
            success=True,
            payload={"entities": [asdict(entity) for entity in entities]},
        )

    def _apply(self, item: CanonicalClaimReview, payload: dict[str, Any]) -> None:
        target_entities = (
            item.claim.analysis.entities
            if self.target == "claim"
            else item.analysis.entities
        )
        target_entities[:] = [
            entity for entity in target_entities if entity.source != "dbpedia_spotlight"
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
        try:
            response = requests.post(
                self.api_url,
                headers=self.headers,
                data={
                    "text": text,
                    "confidence": str(self.confidence),
                    "support": str(self.support),
                },
                timeout=self.timeout,
            )
            response.raise_for_status()
            return self._parse_dbpedia_response(response.json())
        finally:
            time.sleep(self.rate_limit_delay)

    def _parse_dbpedia_response(self, data: dict[str, Any]) -> list[EntityMention]:
        entities: list[EntityMention] = []
        resources = data.get("Resources", [])
        if not isinstance(resources, list):
            return entities
        for resource in resources:
            try:
                confidence = float(resource.get("@similarityScore", 0))
                if confidence < self.confidence:
                    continue
                entities.append(
                    EntityMention(
                        uri=resource.get("@URI", ""),
                        source="dbpedia_spotlight",
                        surface_form=resource.get("@surfaceForm", ""),
                        types=(
                            resource.get("@types", "").split(",")
                            if resource.get("@types")
                            else []
                        ),
                        confidence=confidence,
                        support=int(resource.get("@support", 0)),
                        offset=int(resource.get("@offset", -1)),
                    )
                )
            except (TypeError, ValueError):
                continue
        return entities

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
                    source=str(item.get("source", "dbpedia_spotlight")),
                    surface_form=str(item.get("surface_form", "")),
                    types=[str(entity_type) for entity_type in item.get("types", [])],
                    confidence=item.get("confidence"),
                    support=item.get("support"),
                    offset=item.get("offset"),
                )
            )
        return entities
