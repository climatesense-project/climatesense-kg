"""DBpedia entity-property enrichment stage."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
import json
import logging
import re
import time
from typing import Any
from urllib.parse import urlsplit

import requests

from .. import USER_AGENT
from ..config.graphs import DBPEDIA_ENTITY_SOURCES
from ..domain import CanonicalClaimReview, EntityMention, EntityPropertyValue
from ..persistence import StageResult, StageResultKey, StageResultStore
from ..stages.persisted import (
    StageExecutionPolicy,
    StageExecutionReport,
    execute_persisted_stage,
)


@dataclass(frozen=True)
class PropertyQueryResult:
    """One typed SPARQL result value."""

    value: str
    value_type: str
    datatype: str | None = None
    language: str | None = None


class DBpediaPropertyEnricher:
    """Persist selected properties once for each DBpedia entity URI."""

    name = "dbpedia_entity_properties"
    stage_name = "enrichment.dbpedia_entity_properties"
    version = "1"
    availability_key = "dbpedia_sparql"
    entity_batch_size = 50
    _FORBIDDEN_IRI_CHARACTERS = re.compile(r'[\x00-\x20<>"{}|^`\\]')
    _INVALID_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")

    def __init__(
        self,
        *,
        store: StageResultStore,
        sparql_endpoint: str = "https://dbpedia.org/sparql",
        properties: list[str] | None = None,
        timeout: int = 20,
        rate_limit_delay: float = 0.1,
        max_retries: int = 2,
    ) -> None:
        self.store = store
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.endpoint = sparql_endpoint
        self.properties = self._normalize_property_uris(properties or [])
        self.semantic_config = {"properties": self.properties}
        self.timeout = timeout
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self.headers = {
            "Accept": "application/sparql-results+json",
            "User-Agent": USER_AGENT,
        }

    def is_available(self) -> bool:
        try:
            response = requests.get(
                self.endpoint,
                params={
                    "query": "ASK { }",
                    "format": "application/sparql-results+json",
                },
                headers=self.headers,
                timeout=self.timeout,
            )
            return response.status_code == 200
        except Exception:
            return False

    def enrich(
        self,
        items: list[CanonicalClaimReview],
        *,
        policy: StageExecutionPolicy = StageExecutionPolicy.COMPUTE,
        force: bool = False,
        availability_check: Callable[[], bool] | None = None,
    ) -> StageExecutionReport:
        """Restore or fetch each distinct entity result exactly once."""

        entity_map = self._collect_all_entity_references(items)
        if not self.properties:
            entity_map = {}
        subjects = {
            self._result_key(entity_uri): (entity_uri, references)
            for entity_uri, references in entity_map.items()
        }
        return execute_persisted_stage(
            stage_name=self.stage_name,
            subjects=subjects,
            store=self.store,
            compute_many=lambda pending: self._fetch_entity_properties(
                [entity_uri for entity_uri, _references in pending]
            ),
            apply_result=lambda subject, payload: self._apply_result(
                subject[1], payload
            ),
            policy=policy,
            force=force,
            availability_check=availability_check,
            stage_logger=self.logger,
        )

    def _result_key(self, entity_uri: str) -> StageResultKey:
        return StageResultKey.build(
            subject_key=entity_uri,
            stage_name=self.stage_name,
            stage_version=self.version,
            input_value={"entity_uri": entity_uri},
            config_value=self.semantic_config,
        )

    def _fetch_entity_properties(self, entity_uris: list[str]) -> list[StageResult]:
        results: list[StageResult] = []
        for start in range(0, len(entity_uris), self.entity_batch_size):
            results.extend(
                self._fetch_entity_property_batch(
                    entity_uris[start : start + self.entity_batch_size]
                )
            )
        return results

    def _fetch_entity_property_batch(self, entity_uris: list[str]) -> list[StageResult]:
        last_exception: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                response = requests.get(
                    self.endpoint,
                    params={
                        "query": self._build_query(entity_uris),
                        "format": "application/sparql-results+json",
                    },
                    headers=self.headers,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                bindings = response.json().get("results", {}).get("bindings", [])
                parsed = self._parse_bindings_by_entity(bindings)
                time.sleep(self.rate_limit_delay)
                return [
                    StageResult(
                        success=True,
                        payload={"properties": parsed.get(entity_uri, {})},
                    )
                    for entity_uri in entity_uris
                ]
            except (requests.RequestException, json.JSONDecodeError, ValueError) as exc:
                last_exception = exc
                if attempt < self.max_retries:
                    time.sleep(min(2**attempt, 2))
        return [
            StageResult(
                success=False,
                payload={
                    "error_type": "property_query_error",
                    "entity_uri": entity_uri,
                    "error": str(last_exception or "Unknown DBpedia property error"),
                },
            )
            for entity_uri in entity_uris
        ]

    @staticmethod
    def _apply_result(
        entity_references: list[EntityMention], payload: dict[str, Any]
    ) -> None:
        raw_properties = payload.get("properties")
        if not isinstance(raw_properties, dict):
            return
        properties = DBpediaPropertyEnricher._deserialize_properties(raw_properties)
        for entity in entity_references:
            DBpediaPropertyEnricher._merge_properties(entity, properties)

    def _collect_all_entity_references(
        self, items: list[CanonicalClaimReview]
    ) -> dict[str, list[EntityMention]]:
        entity_map: dict[str, list[EntityMention]] = {}
        for item in items:
            for entity_uri, references in self._collect_entity_references(item).items():
                entity_map.setdefault(entity_uri, []).extend(references)
        return entity_map

    def _collect_entity_references(
        self, item: CanonicalClaimReview
    ) -> dict[str, list[EntityMention]]:
        entity_map: dict[str, list[EntityMention]] = {}
        entities = [*item.claim.analysis.entities, *item.analysis.entities]
        for entity in entities:
            if entity.uri and entity.source in DBPEDIA_ENTITY_SOURCES:
                entity_map.setdefault(entity.uri, []).append(entity)
        return entity_map

    @staticmethod
    def _merge_properties(
        entity: EntityMention,
        properties: dict[str, list[EntityPropertyValue]],
    ) -> None:
        for property_uri, values in properties.items():
            existing = entity.properties.setdefault(property_uri, [])
            for value in values:
                if value not in existing:
                    existing.append(value)

    def _build_query(self, entity_uris: list[str]) -> str:
        validated_entity_uris = [
            validated
            for entity_uri in entity_uris
            if (validated := self._validate_absolute_uri(entity_uri)) is not None
        ]
        if len(validated_entity_uris) != len(entity_uris):
            raise ValueError("Invalid DBpedia entity URI")
        validated_properties = [
            validated
            for prop in self.properties
            if (validated := self._validate_absolute_uri(prop)) is not None
        ]
        if len(validated_properties) != len(self.properties):
            raise ValueError("Invalid DBpedia property URI")
        entity_values = " ".join(f"<{uri}>" for uri in validated_entity_uris)
        property_values = " ".join(f"<{prop}>" for prop in validated_properties)
        return (
            "SELECT ?entity ?property ?value WHERE { "
            f"VALUES ?entity {{ {entity_values} }} "
            f"VALUES ?property {{ {property_values} }} "
            "?entity ?property ?value ."
            " }"
        )

    def _parse_bindings_by_entity(
        self, bindings: list[dict[str, Any]]
    ) -> dict[str, dict[str, list[dict[str, Any]]]]:
        results: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for binding in bindings:
            entity_binding = binding.get("entity")
            property_binding = binding.get("property")
            value_binding = binding.get("value")
            if not entity_binding or not property_binding or not value_binding:
                continue
            entity_uri = entity_binding.get("value")
            property_uri = property_binding.get("value")
            value_type = value_binding.get("type")
            if not entity_uri or not property_uri or value_type == "bnode":
                continue
            value = PropertyQueryResult(
                value=value_binding.get("value", ""),
                value_type=value_type or "literal",
                datatype=value_binding.get("datatype"),
                language=value_binding.get("xml:lang"),
            )
            serialized = asdict(value)
            entity_properties = results.setdefault(entity_uri, {})
            values = entity_properties.setdefault(property_uri, [])
            if serialized not in values:
                values.append(serialized)
        return results

    @staticmethod
    def _deserialize_properties(
        properties: dict[str, Any],
    ) -> dict[str, list[EntityPropertyValue]]:
        result: dict[str, list[EntityPropertyValue]] = {}
        for property_uri, raw_values in properties.items():
            if not isinstance(raw_values, list):
                continue
            values: list[EntityPropertyValue] = []
            for raw_value in raw_values:
                if not isinstance(raw_value, dict) or "value" not in raw_value:
                    continue
                values.append(
                    EntityPropertyValue(
                        value=str(raw_value["value"]),
                        value_type=str(raw_value.get("value_type", "literal")),
                        datatype=raw_value.get("datatype"),
                        language=raw_value.get("language"),
                    )
                )
            if values:
                result[property_uri] = values
        return result

    def _normalize_property_uris(self, properties: list[str]) -> list[str]:
        return sorted(
            {
                validated
                for prop in properties
                if (validated := self._validate_absolute_uri(prop)) is not None
            }
        )

    def _validate_absolute_uri(self, value: str) -> str | None:
        if (
            not isinstance(value, str)
            or self._FORBIDDEN_IRI_CHARACTERS.search(value)
            or self._INVALID_PERCENT_ESCAPE.search(value)
        ):
            return None
        try:
            parsed = urlsplit(value)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.hostname
                or parsed.username is not None
                or parsed.password is not None
            ):
                return None
            _ = parsed.port
        except ValueError:
            return None
        return value
