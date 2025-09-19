"""CIMPLE Factors API enrichment for emotion, sentiment, political leaning, and conspiracy detection."""

import json
import logging
import os
import time
from typing import Any

import requests

from ..config.models import CanonicalClaimReview
from .base import Enricher

logger = logging.getLogger(__name__)


class BertFactorsEnricher(Enricher):
    """Enricher that uses CIMPLE Factors API for emotion, sentiment, political leaning, and conspiracy detection."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__("bert_factors", **kwargs)

        # API configuration
        self.api_url = os.environ.get("CIMPLE_FACTORS_API_URL", "http://localhost:8000")
        self.batch_size = kwargs.get("batch_size", 32)
        self.max_length = kwargs.get("max_length", 128)
        self.timeout = kwargs.get("timeout", 60)
        self.rate_limit_delay = kwargs.get("rate_limit_delay", 0.1)

        # Headers for API requests
        self.headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "User-Agent": "ClimateSense-Pipeline/1.0 (+https://github.com/climatesense-project)",
        }

    def is_available(self) -> bool:
        """Check if CIMPLE Factors API is available."""
        try:
            response = requests.get(
                f"{self.api_url}/health", headers=self.headers, timeout=5
            )
            return response.status_code == 200
        except Exception as e:
            self.logger.warning(f"CIMPLE Factors API not available: {e}")
            return False

    def _process_item(self, claim_review: CanonicalClaimReview) -> CanonicalClaimReview:
        """Process a single claim review with CIMPLE Factors API."""
        if not self.is_available() or not claim_review.uri:
            return claim_review

        # Compute factors if text available
        if claim_review.claim.normalized_text:
            try:
                factors_list = self._compute_factors(
                    [claim_review.claim.normalized_text]
                )
                factors = factors_list[0] if factors_list else None
                if factors:
                    self._apply_factors(claim_review, factors, cache=True)
                else:
                    raise ValueError("CIMPLE Factors API returned None result")
            except Exception as e:
                self.logger.error(
                    f"CIMPLE Factors enrichment failed for {claim_review.uri}: {e}"
                )
                self.cache_error(
                    claim_review.uri,
                    error_type="api_error",
                    message=f"CIMPLE Factors API error: {e!s}",
                    data={
                        "emotion": None,
                        "sentiment": None,
                        "political_leaning": None,
                        "conspiracies": {"mentioned": [], "promoted": []},
                    },
                )

        # Rate limiting
        time.sleep(self.rate_limit_delay)

        return claim_review

    def apply_cached_data(
        self, claim_review: CanonicalClaimReview, cached_data: dict[str, Any]
    ) -> CanonicalClaimReview:
        """Apply cached CIMPLE factors data to a claim review."""
        self._apply_factors(claim_review, cached_data["data"])
        return claim_review

    def _apply_factors(
        self,
        claim_review: CanonicalClaimReview,
        factors: dict[str, Any],
        cache: bool = False,
    ) -> None:
        """Apply factors to a claim review and optionally cache them."""
        claim_review.claim.emotion = factors.get("emotion")
        claim_review.claim.sentiment = factors.get("sentiment")
        claim_review.claim.political_leaning = factors.get("political_leaning")
        claim_review.claim.conspiracies = factors.get(
            "conspiracies", {"mentioned": [], "promoted": []}
        )

        if cache and claim_review.uri:
            self.cache_success(claim_review.uri, factors)

    def _compute_factors(self, texts: list[str]) -> list[dict[str, Any] | None]:
        """
        Compute factors for a list of texts using CIMPLE Factors API.

        Args:
            texts: List of texts to analyze

        Returns:
            List of computed factors for each text
        """
        if not texts:
            return []

        try:
            # Filter out empty texts
            valid_items = [
                (i, text) for i, text in enumerate(texts) if text and text.strip()
            ]

            if not valid_items:
                return [None] * len(texts)

            valid_indices, valid_texts = zip(*valid_items, strict=False)
            valid_indices, valid_texts = list(valid_indices), list(valid_texts)

            # Prepare API request
            payload: dict[str, Any] = {
                "texts": valid_texts,
                "batch_size": self.batch_size,
                "max_length": self.max_length,
            }

            # Make API request
            response = requests.post(
                f"{self.api_url}/predict",
                headers=self.headers,
                data=json.dumps(payload),
                timeout=self.timeout,
            )

            if response.status_code == 200:
                api_data = response.json()
                api_results = api_data.get("results", [])

                # Convert API results to our format
                batch_results: list[dict[str, Any] | None] = []
                for api_result in api_results:
                    if api_result is None:
                        batch_results.append(None)
                        continue

                    result: dict[str, Any] = {}

                    # Map emotion (filter out None values)
                    if api_result.get("emotion"):
                        result["emotion"] = api_result["emotion"]

                    # Map sentiment
                    if api_result.get("sentiment"):
                        result["sentiment"] = api_result["sentiment"]

                    # Map political leaning
                    if api_result.get("political_leaning"):
                        result["political_leaning"] = api_result["political_leaning"]

                    # Map conspiracies
                    if "conspiracies" in api_result:
                        result["conspiracies"] = {
                            "mentioned": api_result["conspiracies"].get(
                                "mentioned", []
                            ),
                            "promoted": api_result["conspiracies"].get("promoted", []),
                        }

                    batch_results.append(result)

                # Map results back to original indices
                final_results: list[dict[str, Any] | None] = [None] * len(texts)
                for i, original_idx in enumerate(valid_indices):
                    if i < len(batch_results):
                        final_results[original_idx] = batch_results[i]

                return final_results
            else:
                self.logger.warning(
                    f"CIMPLE Factors API returned status {response.status_code}: {response.text}"
                )
                raise requests.RequestException(
                    f"API returned status {response.status_code}: {response.text}"
                )

        except requests.RequestException as e:
            self.logger.error(f"CIMPLE Factors API request failed: {e}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode JSON from CIMPLE Factors API: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error in CIMPLE factors computation: {e}")
            raise
