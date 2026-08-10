"""DBpedia entity-property enrichment stage."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import re
import time
from typing import Any
from urllib.parse import urlsplit

import requests

from ..config.graphs import DBPEDIA_ENTITY_SOURCES
from ..domain import CanonicalClaimReview, EntityMention, EntityPropertyValue
from ..persistence import StageResult, StageResultKey, StageResultStore
from .base import Enricher


@dataclass(frozen=True)
class PropertyQueryResult:
    """One typed SPARQL result value."""

    value: str
    value_type: str
    datatype: str | None = None
    language: str | None = None


class DBpediaPropertyEnricher(Enricher):
    """Attach selected DBpedia properties to typed entity mentions."""

    entity_stage_name = "enrichment.dbpedia_entity_properties.entity"
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
        normalized_properties = self._normalize_property_uris(properties or [])
        super().__init__(
            "dbpedia_entity_properties",
            version="1",
            store=store,
            sparql_endpoint=sparql_endpoint,
            properties=normalized_properties,
            timeout=timeout,
            max_retries=max_retries,
        )
        self.endpoint = sparql_endpoint
        self.properties = normalized_properties
        self.timeout = timeout
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self.headers = {
            "Accept": "application/sparql-results+json",
            "User-Agent": "ClimateSense-Pipeline/2.0 (+https://github.com/climatesense-project)",
        }
        self._run_entity_results: dict[str, StageResult] | None = None

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
        except Exception as exc:
            self.logger.warning("DBpedia SPARQL endpoint unavailable: %s", exc)
            return False

    def _input_value(self, item: CanonicalClaimReview) -> Any:
        return {
            "entities": sorted(self._collect_entity_references(item)),
        }

    def _compute_many(
        self,
        items: list[CanonicalClaimReview],
        *,
        force: bool,
    ) -> list[StageResult]:
        self._run_entity_results = {}
        try:
            return super()._compute_many(items, force=force)
        finally:
            self._run_entity_results = None

    def _compute(
        self, item: CanonicalClaimReview, *, force: bool = False
    ) -> StageResult:
        entity_map = self._collect_entity_references(item)
        aggregated: dict[str, dict[str, list[dict[str, Any]]]] = {}
        failures: list[dict[str, str]] = []
        for entity_uri in entity_map:
            try:
                properties = self._get_entity_properties(entity_uri, force=force)
            except Exception as exc:
                failures.append({"uri": entity_uri, "error": str(exc)})
                continue
            if properties:
                aggregated[entity_uri] = properties
        return StageResult(
            success=not failures,
            payload={"entities": aggregated, "failed_entities": failures},
        )

    def _apply(self, item: CanonicalClaimReview, payload: dict[str, Any]) -> None:
        entity_map = self._collect_entity_references(item)
        cached_entities = payload.get("entities")
        if not isinstance(cached_entities, dict):
            return
        for entity_uri, raw_properties in cached_entities.items():
            if not isinstance(raw_properties, dict):
                continue
            properties = self._deserialize_properties(raw_properties)
            for entity in entity_map.get(entity_uri, []):
                self._merge_properties(entity, properties)

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

    def _get_entity_properties(
        self, entity_uri: str, *, force: bool = False
    ) -> dict[str, list[dict[str, Any]]]:
        if not self.properties:
            return {}
        if (
            self._run_entity_results is not None
            and entity_uri in self._run_entity_results
        ):
            return self._properties_from_result(self._run_entity_results[entity_uri])
        key = StageResultKey.build(
            subject_key=entity_uri,
            stage_name=self.entity_stage_name,
            stage_version=self.version,
            input_value={"entity_uri": entity_uri},
            config_value=self.config,
        )
        stored = None if force else self.store.get(key)
        if stored is not None:
            if self._run_entity_results is not None:
                self._run_entity_results[entity_uri] = stored
            return self._properties_from_result(stored)

        last_exception: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                response = requests.get(
                    self.endpoint,
                    params={
                        "query": self._build_query(entity_uri),
                        "format": "application/sparql-results+json",
                    },
                    headers=self.headers,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                bindings = response.json().get("results", {}).get("bindings", [])
                parsed = self._parse_bindings(bindings)
                result = StageResult(success=True, payload={"properties": parsed})
                self.store.put(key, result)
                if self._run_entity_results is not None:
                    self._run_entity_results[entity_uri] = result
                time.sleep(self.rate_limit_delay)
                return parsed
            except (requests.RequestException, json.JSONDecodeError, ValueError) as exc:
                last_exception = exc
                if attempt < self.max_retries:
                    time.sleep(min(2**attempt, 2))
        error = str(last_exception or "Unknown DBpedia property error")
        result = StageResult(success=False, payload={"error": error})
        self.store.put(key, result)
        if self._run_entity_results is not None:
            self._run_entity_results[entity_uri] = result
        raise RuntimeError(error)

    @staticmethod
    def _properties_from_result(
        result: StageResult,
    ) -> dict[str, list[dict[str, Any]]]:
        if not result.success:
            raise RuntimeError(str(result.payload.get("error", "cached failure")))
        properties = result.payload.get("properties")
        return properties if isinstance(properties, dict) else {}

    def _build_query(self, entity_uri: str) -> str:
        validated_entity_uri = self._validate_absolute_uri(entity_uri)
        if validated_entity_uri is None:
            raise ValueError(f"Invalid DBpedia entity URI: {entity_uri!r}")
        validated_properties = [
            validated
            for prop in self.properties
            if (validated := self._validate_absolute_uri(prop)) is not None
        ]
        if len(validated_properties) != len(self.properties):
            raise ValueError("Invalid DBpedia property URI")
        values = " ".join(f"<{prop}>" for prop in validated_properties)
        return (
            "SELECT ?property ?value WHERE { "
            f"VALUES ?property {{ {values} }} "
            f"<{validated_entity_uri}> ?property ?value ."
            " }"
        )

    def _parse_bindings(
        self, bindings: list[dict[str, Any]]
    ) -> dict[str, list[dict[str, Any]]]:
        results: dict[str, list[dict[str, Any]]] = {}
        for binding in bindings:
            property_binding = binding.get("property")
            value_binding = binding.get("value")
            if not property_binding or not value_binding:
                continue
            property_uri = property_binding.get("value")
            value_type = value_binding.get("type")
            if not property_uri or value_type == "bnode":
                continue
            value = PropertyQueryResult(
                value=value_binding.get("value", ""),
                value_type=value_type or "literal",
                datatype=value_binding.get("datatype"),
                language=value_binding.get("xml:lang"),
            )
            serialized = asdict(value)
            property_values = results.setdefault(property_uri, [])
            if serialized not in property_values:
                property_values.append(serialized)
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
        return [
            validated
            for prop in properties
            if (validated := self._validate_absolute_uri(prop)) is not None
        ]

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
