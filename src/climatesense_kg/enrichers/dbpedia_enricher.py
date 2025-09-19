"""DBpedia Spotlight enrichment for entity extraction."""

from dataclasses import asdict, dataclass
import json
import time
from typing import Any

import requests

from ..config.models import CanonicalClaimReview
from .base import Enricher


@dataclass
class DBpediaSpotlightEntity:
    """Represents an entity extracted from DBpedia Spotlight."""

    uri: str
    surface_form: str
    types: list[str]
    confidence: float
    support: int
    offset: int
    source: str


class DBpediaEnricher(Enricher):
    """Enricher that uses DBpedia Spotlight for entity extraction."""

    def __init__(self, **kwargs: Any):
        super().__init__("dbpedia_spotlight", **kwargs)

        self.api_url = kwargs.get(
            "api_url", "https://api.dbpedia-spotlight.org/en/annotate"
        )
        self.confidence = kwargs.get("confidence", 0.5)
        self.support = kwargs.get("support", 20)
        self.timeout = kwargs.get("timeout", 20)
        self.rate_limit_delay = kwargs.get(
            "rate_limit_delay", 0.1
        )  # seconds between requests

        # Headers for API requests
        self.headers = {
            "accept": "application/json",
            "User-Agent": "ClimateSense-Pipeline/1.0 (+https://github.com/climatesense-project)",
        }

    def is_available(self) -> bool:
        """Check if DBpedia Spotlight API is available."""
        try:
            payload = {"text": "test"}
            response = requests.post(
                self.api_url, headers=self.headers, data=payload, timeout=5
            )
            return response.status_code == 200
        except Exception as e:
            self.logger.warning(f"DBpedia Spotlight not available: {e}")
            return False

    def _process_item(self, claim_review: CanonicalClaimReview) -> CanonicalClaimReview:
        """Process a single claim review with DBpedia entity extraction."""
        if not claim_review.uri:
            return claim_review

        try:
            # Perform enrichment
            claim_entities: list[DBpediaSpotlightEntity] = []
            review_entities: list[dict[str, Any]] = []

            # Extract entities from claim text
            if claim_review.claim.normalized_text:
                claim_entities = self._extract_entities(
                    claim_review.claim.normalized_text
                )
                claim_review.claim.entities.extend([asdict(e) for e in claim_entities])

            # Extract entities from review text if available
            if claim_review.review_text:
                review_text_entities = self._extract_entities(claim_review.review_text)
                review_entities.extend([asdict(e) for e in review_text_entities])

            # Extract entities from review URL text if available
            if claim_review.review_url_text:
                url_text_entities = self._extract_entities(claim_review.review_url_text)
                review_entities.extend([asdict(e) for e in url_text_entities])

            claim_review.entities_in_review.extend(review_entities)

            # Cache the successful results
            success_data = {
                "claim_entities": [asdict(e) for e in claim_entities],
                "review_entities": review_entities,
            }
            self.cache_success(claim_review.uri, success_data)

            # Rate limiting
            time.sleep(self.rate_limit_delay)

        except Exception as e:
            self.logger.error(f"DBpedia enrichment failed for {claim_review.uri}: {e}")

            self.cache_error(
                claim_review.uri,
                error_type="api_error",
                message=f"DBpedia Spotlight API error: {e!s}",
                data={
                    "claim_entities": [],
                    "review_entities": [],
                },
            )

        return claim_review

    def apply_cached_data(
        self, claim_review: CanonicalClaimReview, cached_data: dict[str, Any]
    ) -> CanonicalClaimReview:
        """Apply cached DBpedia enrichment data to a claim review."""
        data = cached_data["data"]
        if "claim_entities" in data:
            claim_review.claim.entities.extend(data["claim_entities"])
        if "review_entities" in data:
            claim_review.entities_in_review.extend(data["review_entities"])
        return claim_review

    def _extract_entities(self, text: str) -> list[DBpediaSpotlightEntity]:
        """
        Extract entities from text using DBpedia Spotlight.

        Args:
            text: Text to analyze

        Returns:
            List[DBpediaSpotlightEntity]: List of extracted entities
        """
        if not text or len(text.strip()) < 10:  # Skip very short texts
            return []

        try:
            payload = {
                "text": text,
                "confidence": str(self.confidence),
                "support": str(self.support),
            }

            response = requests.post(
                self.api_url, headers=self.headers, data=payload, timeout=self.timeout
            )

            if response.status_code == 200:
                data = response.json()
                return self._parse_dbpedia_response(data)
            else:
                self.logger.warning(
                    f"DBpedia API returned status {response.status_code}"
                )
                raise requests.RequestException(
                    f"API returned status {response.status_code}"
                )

        except requests.RequestException as e:
            self.logger.error(f"DBpedia Spotlight API request failed: {e}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode JSON from DBpedia Spotlight: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error extracting entities: {e}")
            raise

    def _parse_dbpedia_response(
        self, data: dict[str, Any]
    ) -> list[DBpediaSpotlightEntity]:
        """
        Parse DBpedia Spotlight response into entity format.

        Args:
            data: Raw response from DBpedia Spotlight

        Returns:
            List[DBpediaSpotlightEntity]: Entity data
        """
        entities: list[DBpediaSpotlightEntity] = []

        if "Resources" not in data:
            return entities

        resources = data["Resources"]

        for resource in resources:
            try:
                entity = DBpediaSpotlightEntity(
                    uri=resource.get("@URI", ""),
                    surface_form=resource.get("@surfaceForm", ""),
                    types=(
                        resource.get("@types", "").split(",")
                        if resource.get("@types")
                        else []
                    ),
                    confidence=float(resource.get("@similarityScore", 0)),
                    support=int(resource.get("@support", 0)),
                    offset=int(resource.get("@offset", -1)),
                    source="dbpedia_spotlight",
                )

                # Only include entities with minimum confidence
                if entity.confidence >= self.confidence:
                    entities.append(entity)

            except (ValueError, KeyError) as e:
                self.logger.warning(f"Error parsing DBpedia entity: {e}")
                continue

        return entities
