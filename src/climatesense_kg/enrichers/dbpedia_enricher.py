"""DBpedia Spotlight entity-extraction stage."""

from __future__ import annotations

from dataclasses import asdict
import json
import time
from typing import Any

import requests

from ..domain import CanonicalClaimReview, EntityMention
from ..persistence import StageResult, StageResultStore
from .base import Enricher


class DBpediaEnricher(Enricher):
    """Extract typed entity mentions from claim and review text."""

    def __init__(
        self,
        *,
        store: StageResultStore,
        api_url: str = "https://api.dbpedia-spotlight.org/en/annotate",
        confidence: float = 0.5,
        support: int = 20,
        timeout: int = 20,
        rate_limit_delay: float = 0.1,
    ) -> None:
        super().__init__(
            "dbpedia_spotlight",
            version="1",
            store=store,
            api_url=api_url,
            confidence=confidence,
            support=support,
            timeout=timeout,
        )
        self.api_url = api_url
        self.confidence = confidence
        self.support = support
        self.timeout = timeout
        self.rate_limit_delay = rate_limit_delay
        self.headers = {
            "accept": "application/json",
            "User-Agent": "ClimateSense-Pipeline/2.0 (+https://github.com/climatesense-project)",
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

    def _input_value(self, item: CanonicalClaimReview) -> Any:
        return {
            "claim_text": item.claim.analysis_text,
            "review_text": item.review_text,
        }

    def _compute(
        self, item: CanonicalClaimReview, *, force: bool = False
    ) -> StageResult:
        del force
        claim_entities = self._extract_entities(item.claim.analysis_text)
        review_entities = self._extract_entities(item.review_text or "")
        return StageResult(
            success=True,
            payload={
                "claim_entities": [asdict(entity) for entity in claim_entities],
                "review_entities": [asdict(entity) for entity in review_entities],
            },
        )

    def _apply(self, item: CanonicalClaimReview, payload: dict[str, Any]) -> None:
        item.claim.analysis.entities = [
            entity
            for entity in item.claim.analysis.entities
            if entity.source != "dbpedia_spotlight"
        ]
        item.analysis.entities = [
            entity
            for entity in item.analysis.entities
            if entity.source != "dbpedia_spotlight"
        ]
        item.claim.analysis.entities.extend(
            self._deserialize_entities(payload.get("claim_entities"))
        )
        item.analysis.entities.extend(
            self._deserialize_entities(payload.get("review_entities"))
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
        except json.JSONDecodeError:
            raise
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
