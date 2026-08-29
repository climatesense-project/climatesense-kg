"""Individually persisted CIMPLE claim-factor model stages."""

from __future__ import annotations

import json
import os
import time
from typing import Any, cast
from urllib.parse import urlparse

import requests

from .. import USER_AGENT
from ..config.enrichment import CIMPLE_MODELS, CimpleModelName
from ..domain import CanonicalClaimReview
from ..processing import ProcessingResult
from .base import Enricher


class CimpleModelEnricher(Enricher):
    """Apply one CIMPLE model to canonical claim text."""

    MODEL_KEYS = tuple(CIMPLE_MODELS)

    def __init__(
        self,
        *,
        model: CimpleModelName,
        model_version: str = "1",
        batch_size: int = 32,
        max_length: int = 128,
        timeout: int = 60,
        rate_limit_delay: float = 0.1,
        max_workers: int = 1,
    ) -> None:
        if model not in CIMPLE_MODELS:
            raise ValueError(f"Unknown CIMPLE model: {model}")
        endpoint = CIMPLE_MODELS[model].endpoint
        effective_batch_size = max(1, batch_size)
        super().__init__(
            f"cimple.{model}",
            version="1",
            semantic_config={
                "model_id": f"cimple-factors/{endpoint}",
                "model_version": model_version,
                "max_length": max_length,
            },
            availability_key="cimple_factors",
            batch_size=effective_batch_size,
            max_workers=max_workers,
        )
        self.model = model
        api_url = os.environ.get("CIMPLE_FACTORS_API_URL", "").strip().rstrip("/")
        parsed_url = urlparse(api_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            raise ValueError("CIMPLE_FACTORS_API_URL must be an absolute HTTP(S) URL")
        self.api_url = api_url
        self.batch_size = effective_batch_size
        self.max_length = max_length
        self.timeout = timeout
        self.rate_limit_delay = rate_limit_delay
        self.headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "User-Agent": USER_AGENT,
        }

    def is_available(self) -> bool:
        try:
            response = requests.get(
                f"{self.api_url}/health", headers=self.headers, timeout=5
            )
            return response.status_code == 200
        except Exception as exc:
            self.logger.warning("CIMPLE Factors API unavailable: %s", exc)
            return False

    def subject_key(self, item: CanonicalClaimReview) -> str:
        return item.claim.uri

    def input_value(self, item: CanonicalClaimReview) -> Any:
        return {"claim_text": item.claim.analysis_text}

    def compute_items(
        self,
        items: list[CanonicalClaimReview],
    ) -> list[ProcessingResult]:
        try:
            responses = self._call_model([item.claim.analysis_text for item in items])
            if len(responses) != len(items):
                raise ValueError(
                    f"Unexpected result count for CIMPLE model {self.model}"
                )
            return [
                ProcessingResult.success(
                    {
                        "value": self._extract_model_value(response),
                    },
                )
                for response in responses
            ]
        except Exception as exc:
            self.logger.error("CIMPLE model %s failed: %s", self.model, exc)
            return [
                ProcessingResult.retryable(
                    {
                        "error_type": "model_error",
                        "model": self.model,
                        "error": str(exc),
                    },
                )
                for _item in items
            ]
        finally:
            time.sleep(self.rate_limit_delay)

    def compute_item(self, item: CanonicalClaimReview) -> ProcessingResult:
        return self.compute_items([item])[0]

    def apply_item(self, item: CanonicalClaimReview, payload: dict[str, Any]) -> None:
        value = payload.get("value")
        analysis = item.claim.analysis
        if self.model == "emotion":
            analysis.emotion = self._optional_string(value)
        elif self.model == "sentiment":
            analysis.sentiment = self._optional_string(value)
        elif self.model == "political_leaning":
            analysis.political_leaning = self._optional_string(value)
        elif self.model == "tropes":
            analysis.tropes = self._string_list(value)
        elif self.model == "persuasion_techniques":
            analysis.persuasion_techniques = self._string_list(value)
        elif self.model == "conspiracies":
            conspiracies = value if isinstance(value, dict) else {}
            analysis.conspiracies = {
                "mentioned": self._string_list(conspiracies.get("mentioned")),
                "promoted": self._string_list(conspiracies.get("promoted")),
            }
        elif self.model == "climate_related":
            analysis.climate_related = value if isinstance(value, bool) else None

    def _call_model(self, texts: list[str]) -> list[dict[str, Any]]:
        endpoint = CIMPLE_MODELS[self.model].endpoint
        response = requests.post(
            f"{self.api_url}/predict/{endpoint}",
            headers=self.headers,
            data=json.dumps(
                {
                    "texts": texts,
                    "batch_size": self.batch_size,
                    "max_length": self.max_length,
                }
            ),
            timeout=self.timeout,
        )
        if response.status_code != 200:
            raise requests.RequestException(
                f"API returned status {response.status_code}: {response.text}"
            )
        data = response.json()
        results = data.get("results", [])
        if not isinstance(results, list):
            raise ValueError("CIMPLE Factors API returned an invalid payload")
        return cast(list[dict[str, Any]], results)

    def _extract_model_value(self, result: dict[str, Any]) -> Any:
        value = result.get("value")
        if self.model != "climate_related":
            return value
        if value is None or isinstance(value, bool):
            return value
        if isinstance(value, str) and value.strip().casefold() in {"true", "false"}:
            return value.strip().casefold() == "true"
        raise ValueError("Climate-related result must be boolean")

    @staticmethod
    def _optional_string(value: Any) -> str | None:
        return value if isinstance(value, str) else None

    @staticmethod
    def _string_list(value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return [entry for entry in value if isinstance(entry, str)]
