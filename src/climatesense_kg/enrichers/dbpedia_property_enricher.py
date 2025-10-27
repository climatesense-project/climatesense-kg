"""Enricher that retrieves additional DBpedia properties for referenced entities."""

from __future__ import annotations

from dataclasses import dataclass
import json
import time
from typing import Any

import requests

from ..config.models import CanonicalClaimReview
from .base import Enricher


@dataclass(frozen=True)
class PropertyQueryResult:
    """Represents a property value returned from DBpedia."""

    value: str
    value_type: str
    datatype: str | None = None
    lang: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a serializable representation."""
        payload: dict[str, Any] = {
            "value": self.value,
            "type": self.value_type,
        }
        if self.datatype:
            payload["datatype"] = self.datatype
        if self.lang:
            payload["lang"] = self.lang
        return payload


class DBpediaPropertyEnricher(Enricher):
    """Enrich entities by fetching additional data from DBpedia."""

    DEFAULT_PROPERTIES: list[str] = []

    def __init__(self, **kwargs: Any) -> None:
        super().__init__("dbpedia_entity_properties", **kwargs)

        self.endpoint = kwargs.get("sparql_endpoint", "https://dbpedia.org/sparql")
        self.timeout = kwargs.get("timeout", 20)
        self.rate_limit_delay = kwargs.get("rate_limit_delay", 0.1)
        self.max_retries = kwargs.get("max_retries", 2)

        raw_properties = kwargs.get("properties", self.DEFAULT_PROPERTIES)
        self.properties = self._normalize_property_uris(raw_properties)

        self.headers = {
            "Accept": "application/sparql-results+json",
            "User-Agent": "ClimateSense-Pipeline/1.0 (+https://github.com/climatesense-project)",
        }

        # In-memory cache to avoid duplicate HTTP calls within the same run
        self._entity_cache: dict[str, dict[str, list[dict[str, Any]]]] = {}

    def is_available(self) -> bool:
        """Check whether the DBpedia SPARQL endpoint is reachable."""
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
        except Exception as exc:  # pragma: no cover - network errors are logged
            self.logger.warning("DBpedia SPARQL endpoint not available: %s", exc)
            return False

    def _process_item(self, claim_review: CanonicalClaimReview) -> CanonicalClaimReview:
        """Fetch additional DBpedia properties for all entities in a claim review."""
        if not claim_review.uri:
            return claim_review

        entity_map = self._collect_entity_references(claim_review)
        if not entity_map or not self.properties:
            # Nothing to do but cache the empty response for future runs
            self.cache_success(claim_review.uri, {"entities": {}})
            return claim_review

        aggregated_results: dict[str, dict[str, list[dict[str, Any]]]] = {}
        failed_entities: list[dict[str, str]] = []

        for entity_uri, references in entity_map.items():
            try:
                properties = self._get_entity_properties(entity_uri)
            except Exception as exc:  # pragma: no cover - guarded by tests via mocks
                self.logger.error(
                    "Failed to fetch DBpedia properties for %s: %s", entity_uri, exc
                )
                failed_entities.append({"uri": entity_uri, "error": str(exc)})
                continue

            if properties:
                aggregated_results[entity_uri] = properties

                for ref in references:
                    self._merge_properties(ref, properties)

        if failed_entities:
            self.cache_error(
                claim_review.uri,
                error_type="api_error",
                message="Failed to fetch properties for some DBpedia entities",
                data={
                    "entities": aggregated_results,
                    "failed_entities": failed_entities,
                },
            )
        else:
            self.cache_success(
                claim_review.uri,
                data={"entities": aggregated_results},
            )

        # Respect rate limits after processing a review with outgoing requests
        time.sleep(self.rate_limit_delay)

        return claim_review

    def apply_cached_data(
        self, claim_review: CanonicalClaimReview, cached_data: dict[str, Any]
    ) -> CanonicalClaimReview:
        """Apply cached DBpedia properties to the claim review entities."""
        data = cached_data.get("data", {})
        cached_entities: dict[str, dict[str, list[dict[str, Any]]]] = data.get(
            "entities", {}
        )

        if not cached_entities:
            return claim_review

        entity_map = self._collect_entity_references(claim_review)
        for entity_uri, properties in cached_entities.items():
            references = entity_map.get(entity_uri, [])
            for ref in references:
                self._merge_properties(ref, properties)

        return claim_review

    def _collect_entity_references(
        self, claim_review: CanonicalClaimReview
    ) -> dict[str, list[dict[str, Any]]]:
        """Collect all entities referenced in the claim review by their URI."""
        entity_map: dict[str, list[dict[str, Any]]] = {}

        # Claim-level entities
        for entity in claim_review.claim.entities:
            uri = entity.get("uri")
            if uri:
                entity_map.setdefault(uri, []).append(entity)

        # Review-level entities
        for entity in claim_review.entities_in_review:
            uri = entity.get("uri")
            if uri:
                entity_map.setdefault(uri, []).append(entity)

        return entity_map

    def _merge_properties(
        self,
        entity: dict[str, Any],
        properties: dict[str, list[dict[str, Any]]],
    ) -> None:
        """Attach fetched DBpedia properties to an entity dictionary."""
        if not properties:
            return

        entity_properties = entity.setdefault("dbpedia_properties", {})
        for property_uri, values in properties.items():
            existing_values = entity_properties.setdefault(property_uri, [])
            for value in values:
                if value not in existing_values:
                    existing_values.append(value)

    def _get_entity_properties(
        self, entity_uri: str
    ) -> dict[str, list[dict[str, Any]]]:
        """Fetch DBpedia properties for a given entity URI."""
        if entity_uri in self._entity_cache:
            return self._entity_cache[entity_uri]

        if not self.properties:
            return {}

        payload = {
            "query": self._build_query(entity_uri),
            "format": "application/sparql-results+json",
        }

        last_exception: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                response = requests.get(
                    self.endpoint,
                    params=payload,
                    headers=self.headers,
                    timeout=self.timeout,
                )
                if response.status_code != 200:
                    raise requests.RequestException(
                        f"HTTP {response.status_code}: {response.text[:200]}"
                    )

                data = response.json()
                bindings = data.get("results", {}).get("bindings", [])
                parsed = self._parse_bindings(bindings)
                self._entity_cache[entity_uri] = parsed
                return parsed
            except requests.RequestException as exc:
                last_exception = exc
                if attempt < self.max_retries:
                    time.sleep(min(2**attempt, 2))
                else:
                    break
            except json.JSONDecodeError as exc:
                last_exception = exc
                break

        if last_exception:
            raise last_exception

        return {}

    def _build_query(self, entity_uri: str) -> str:
        """Construct the SPARQL query for the given entity URI."""
        values = " ".join(f"<{prop}>" for prop in self.properties)
        query = (
            "SELECT ?property ?value WHERE { "
            f"VALUES ?property {{ {values} }} "
            f"<{entity_uri}> ?property ?value ."
            " }"
        )
        return query

    def _parse_bindings(
        self, bindings: list[dict[str, Any]]
    ) -> dict[str, list[dict[str, Any]]]:
        """Convert SPARQL JSON bindings into a structured dictionary."""
        results: dict[str, list[dict[str, Any]]] = {}

        for binding in bindings:
            property_binding = binding.get("property")
            value_binding = binding.get("value")

            if not property_binding or not value_binding:
                continue

            property_uri = property_binding.get("value")
            if not property_uri:
                continue

            value_type = value_binding.get("type")
            if value_type == "bnode":
                # Skip blank nodes to keep payload simple
                continue

            property_value = PropertyQueryResult(
                value=value_binding.get("value", ""),
                value_type=value_type or "literal",
                datatype=value_binding.get("datatype"),
                lang=value_binding.get("xml:lang"),
            ).to_dict()

            property_values = results.setdefault(property_uri, [])
            if property_value not in property_values:
                property_values.append(property_value)

        return results

    def _normalize_property_uris(self, properties: list[str]) -> list[str]:
        """Normalize property identifiers into full URIs."""
        normalized: list[str] = []

        for prop in properties:
            full_uri = self._expand_property(prop)
            if full_uri:
                normalized.append(full_uri)

        return normalized

    def _expand_property(self, prop: str) -> str | None:
        """Expand a single property identifier into a full URI."""
        if not prop:
            return None

        if prop.startswith("http://") or prop.startswith("https://"):
            return prop

        self.logger.warning("Ignoring non-URI DBpedia property identifier: %s", prop)
        return None
