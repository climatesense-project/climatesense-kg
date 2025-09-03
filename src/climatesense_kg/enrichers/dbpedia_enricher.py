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

    def enrich(self, claim_review: CanonicalClaimReview) -> CanonicalClaimReview:
        """
        Enrich claim review with DBpedia entities.

        Args:
            claim_review: Claim review to enrich

        Returns:
            CanonicalClaimReview: Enriched claim review
        """
        # Extract entities from claim text
        if claim_review.claim.normalized_text:
            claim_entities = self._extract_entities(claim_review.claim.normalized_text)
            claim_review.claim.entities.extend([asdict(e) for e in claim_entities])

        # Extract entities from review text if available
        if claim_review.review_text:
            review_entities = self._extract_entities(claim_review.review_text)
            claim_review.entities_in_review.extend([asdict(e) for e in review_entities])

        # Extract entities from review URL text if available
        if claim_review.review_url_text:
            url_text_entities = self._extract_entities(claim_review.review_url_text)
            claim_review.entities_in_review.extend(
                [asdict(e) for e in url_text_entities]
            )

        # Rate limiting
        time.sleep(self.rate_limit_delay)

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
                return []

        except requests.exceptions.RequestException as e:
            self.logger.error(f"DBpedia Spotlight API request failed: {e}")
            return []
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode JSON from DBpedia Spotlight: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error extracting entities: {e}")
            return []

    def _parse_dbpedia_response(
        self, data: dict[str, Any]
    ) -> list[DBpediaSpotlightEntity]:
        """
        Parse DBpedia Spotlight response into standardized entity format.

        Args:
            data: Raw response from DBpedia Spotlight

        Returns:
            List[DBpediaSpotlightEntity]: Standardized entity data
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

    def enrich_batch(
        self, claim_reviews: list[CanonicalClaimReview]
    ) -> list[CanonicalClaimReview]:
        """
        Enrich a batch of claim reviews with rate limiting.

        Args:
            claim_reviews: List of claim reviews to enrich

        Returns:
            List[CanonicalClaimReview]: List of enriched claim reviews
        """
        enriched_reviews: list[CanonicalClaimReview] = []
        total = len(claim_reviews)

        for i, claim_review in enumerate(claim_reviews):
            try:
                enriched = self.enrich(claim_review)
                enriched_reviews.append(enriched)

                if i % 10 == 0:  # Log progress every 10 items
                    self.logger.info(f"DBpedia enrichment progress: {i + 1}/{total}")

            except Exception as e:
                self.logger.error(
                    f"Error enriching claim review {claim_review.uri}: {e}"
                )
                enriched_reviews.append(claim_review)

        self.logger.info(f"Completed DBpedia enrichment for {total} claim reviews")
        return enriched_reviews
