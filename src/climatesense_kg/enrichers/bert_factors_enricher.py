"""CIMPLE Factors API enrichment for emotion, sentiment, political leaning, narrative tropes, persuasion techniques, and conspiracy detection."""

import copy
import json
import logging
import os
import time
from typing import Any, cast

import requests

from ..config.models import CanonicalClaimReview
from .base import Enricher

logger = logging.getLogger(__name__)


class BertFactorsEnricher(Enricher):
    """Enrich claims with sentiment, emotion, political leaning, tropes, persuasion techniques, and conspiracy signals from the CIMPLE Factors API."""

    MODEL_CONFIG: dict[str, dict[str, Any]] = {
        "emotion": {"endpoint": "emotion", "default": None},
        "sentiment": {"endpoint": "sentiment", "default": None},
        "political_leaning": {"endpoint": "political-leaning", "default": None},
        "tropes": {"endpoint": "tropes", "default": []},
        "persuasion_techniques": {
            "endpoint": "persuasion-techniques",
            "default": [],
        },
        "conspiracies": {
            "endpoint": "conspiracy",
            "default": {"mentioned": [], "promoted": []},
        },
        "climate_related": {"endpoint": "climate-related", "default": None},
    }
    MODEL_KEYS: tuple[str, ...] = tuple(MODEL_CONFIG.keys())

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

    def required_cache_steps(self) -> list[str]:
        """Require each per-model cache entry before marking an item processed."""
        return [self._cache_step(model) for model in self.MODEL_KEYS]

    def enrich(self, items: list[CanonicalClaimReview]) -> list[CanonicalClaimReview]:
        """Enrich claim reviews while persisting results per model."""

        uris = [item.uri for item in items if item.uri]
        model_caches: dict[str, dict[str, dict[str, Any]]] = {
            model: {} for model in self.MODEL_KEYS
        }

        if self.cache and uris:
            for model in self.MODEL_KEYS:
                model_caches[model] = self.cache.get_many(uris, self._cache_step(model))

        results: list[CanonicalClaimReview] = []
        for idx, item in enumerate(items):
            try:
                enriched = self._process_item(item, model_caches)
                results.append(enriched)
                if (idx + 1) % 100 == 0 or (idx + 1) == len(items):
                    self.logger.info(
                        "Enriched %d/%d items (%.1f%%)",
                        idx + 1,
                        len(items),
                        ((idx + 1) / len(items)) * 100,
                    )
            except Exception as exc:  # pragma: no cover
                self.logger.error("Error enriching claim review %s: %s", item.uri, exc)
                results.append(item)

        return results

    def is_available(self) -> bool:
        """Check if CIMPLE Factors API is available."""
        try:
            response = requests.get(
                f"{self.api_url}/health", headers=self.headers, timeout=5
            )
            return response.status_code == 200
        except Exception as exc:
            self.logger.warning("CIMPLE Factors API not available: %s", exc)
            return False

    def _process_item(
        self,
        claim_review: CanonicalClaimReview,
        cached_models: dict[str, dict[str, dict[str, Any]]] | None = None,
    ) -> CanonicalClaimReview:
        """Process a single claim review and persist per-model enrichment data."""

        if not claim_review.uri:
            return claim_review

        cached_models = cached_models or {}
        model_payloads: dict[str, dict[str, Any] | None] = {}
        for model in self.MODEL_KEYS:
            cache_for_model = cached_models.get(model)
            if cache_for_model and claim_review.uri in cache_for_model:
                model_payloads[model] = cache_for_model[claim_review.uri]
                continue

            if self.cache:
                payload = self.cache.get(claim_review.uri, self._cache_step(model))
                model_payloads[model] = payload
                if payload is not None:
                    cached_models.setdefault(model, {})[claim_review.uri] = payload
            else:
                model_payloads[model] = None

        success_values: dict[str, Any] = {}
        error_values: dict[str, Any] = {}
        models_to_compute: list[str] = []

        for model in self.MODEL_KEYS:
            payload = model_payloads.get(model)
            if isinstance(payload, dict):
                if self._payload_success(payload):
                    success_values[model] = payload.get("data")
                else:
                    error_values[model] = payload.get(
                        "data", self._empty_model_value(model)
                    )
            else:
                models_to_compute.append(model)

        if models_to_compute and not claim_review.claim.normalized_text:
            self._cache_error_for_models(
                claim_review.uri,
                models_to_compute,
                cached_models,
                error_type="missing_text",
                message="No normalized claim text available for enrichment",
            )
            for model in models_to_compute:
                error_values[model] = self._empty_model_value(model)
            models_to_compute = []

        if models_to_compute and not self.is_available():
            self.logger.warning(
                "CIMPLE Factors API unavailable, caching failure for %s",
                claim_review.uri,
            )
            self._cache_error_for_models(
                claim_review.uri,
                models_to_compute,
                cached_models,
                error_type="service_unavailable",
                message="CIMPLE Factors API health check failed",
            )
            for model in models_to_compute:
                error_values[model] = self._empty_model_value(model)
            models_to_compute = []

        if models_to_compute:
            success, errors = self._compute_models_for_text(
                claim_review.claim.normalized_text or "",
                claim_review.uri,
                models_to_compute,
                cached_models,
            )
            success_values.update(success)
            error_values.update(errors)

        combined_values: dict[str, Any] = {}
        for model in self.MODEL_KEYS:
            if model in success_values:
                combined_values[model] = success_values[model]
            elif model in error_values:
                combined_values[model] = error_values[model]
            else:
                combined_values[model] = self._empty_model_value(model)

        factors = self._merge_model_data(combined_values)
        self._apply_factors(claim_review, factors)

        time.sleep(self.rate_limit_delay)
        return claim_review

    def _empty_factors_payload(self) -> dict[str, Any]:
        """Return an empty factors payload structure for caching errors."""
        return {
            "emotion": None,
            "sentiment": None,
            "political_leaning": None,
            "tropes": [],
            "persuasion_techniques": [],
            "conspiracies": {"mentioned": [], "promoted": []},
            "climate_related": None,
        }

    def apply_cached_data(
        self, claim_review: CanonicalClaimReview, cached_data: dict[str, Any] | None
    ) -> CanonicalClaimReview:
        """Apply cached CIMPLE factors data to a claim review."""
        if not isinstance(cached_data, dict):
            return claim_review

        data = cached_data.get("data")
        if isinstance(data, dict):
            self._apply_factors(claim_review, cast(dict[str, Any], data))
        return claim_review

    def _apply_factors(
        self,
        claim_review: CanonicalClaimReview,
        factors: dict[str, Any],
    ) -> None:
        """Apply factors to a claim review."""

        claim_review.claim.emotion = factors.get("emotion")
        claim_review.claim.sentiment = factors.get("sentiment")
        claim_review.claim.political_leaning = factors.get("political_leaning")
        claim_review.claim.tropes = list(factors.get("tropes", []) or [])
        claim_review.claim.persuasion_techniques = list(
            factors.get("persuasion_techniques", []) or []
        )
        conspiracies = cast(
            dict[str, Any],
            factors.get("conspiracies", {"mentioned": [], "promoted": []}),
        )
        claim_review.claim.conspiracies = {
            "mentioned": list(conspiracies.get("mentioned", []) or []),
            "promoted": list(conspiracies.get("promoted", []) or []),
        }
        if "climate_related" in factors:
            claim_review.claim.climate_related = factors.get("climate_related")

    def _cache_step(self, model_key: str) -> str:
        return f"{self.step_name}.{model_key}"

    def _payload_success(self, payload: dict[str, Any] | None) -> bool:
        return bool(payload) and payload.get("success") is True and "data" in payload

    def _empty_model_value(self, model_key: str) -> Any:
        default = self.MODEL_CONFIG[model_key]["default"]
        if isinstance(default, list):
            return list(cast(list[Any], default))
        if isinstance(default, dict):
            return copy.deepcopy(cast(dict[str, Any], default))
        return default

    def _merge_model_data(self, model_values: dict[str, Any]) -> dict[str, Any]:
        factors = self._empty_factors_payload()
        factors["emotion"] = model_values.get("emotion")
        factors["sentiment"] = model_values.get("sentiment")
        factors["political_leaning"] = model_values.get("political_leaning")
        factors["tropes"] = list(model_values.get("tropes", []) or [])
        factors["persuasion_techniques"] = list(
            model_values.get("persuasion_techniques", []) or []
        )
        conspiracies = cast(
            dict[str, Any],
            model_values.get("conspiracies", self._empty_model_value("conspiracies")),
        )
        factors["conspiracies"] = copy.deepcopy(conspiracies)
        factors["climate_related"] = model_values.get("climate_related")
        return factors

    def _extract_model_value(
        self, model_key: str, result: dict[str, Any] | None
    ) -> Any:
        result = result or {}
        value = result.get("value")
        if model_key == "climate_related":
            if value is None:
                return None
            return bool(value)
        return value

    def _call_model(self, model_key: str, texts: list[str]) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "texts": texts,
            "batch_size": self.batch_size,
            "max_length": self.max_length,
        }
        endpoint = self.MODEL_CONFIG[model_key]["endpoint"]
        response = requests.post(
            f"{self.api_url}/predict/{endpoint}",
            headers=self.headers,
            data=json.dumps(payload),
            timeout=self.timeout,
        )
        if response.status_code != 200:
            raise requests.RequestException(
                f"API returned status {response.status_code}: {response.text}"
            )
        try:
            data = response.json()
        except json.JSONDecodeError as exc:
            raise ValueError("Failed to decode JSON from CIMPLE Factors API") from exc
        raw_results = data.get("results", [])
        if not isinstance(raw_results, list):
            raise ValueError("CIMPLE Factors API returned unexpected payload format")
        return cast(list[dict[str, Any]], raw_results)

    def _compute_models_for_text(
        self,
        text: str,
        claim_uri: str,
        models: list[str],
        cached_models: dict[str, dict[str, dict[str, Any]]] | None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Call the API for each requested model and return successes and failures."""

        successes: dict[str, Any] = {}
        errors: dict[str, Any] = {}

        if not text.strip():
            return successes, errors

        for model in models:
            try:
                api_results = self._call_model(model, [text])
                if not api_results:
                    raise ValueError(
                        f"CIMPLE Factors API returned empty results for model {model}"
                    )
                value = self._extract_model_value(model, api_results[0])
                successes[model] = value
                self._cache_model_success(claim_uri, model, value, cached_models)
            except Exception as exc:  # pragma: no cover
                self.logger.error(
                    "CIMPLE Factors enrichment failed for %s model %s: %s",
                    claim_uri,
                    model,
                    exc,
                )
                default_value = self._empty_model_value(model)
                errors[model] = default_value
                self._cache_model_error(
                    claim_uri,
                    model,
                    "api_error",
                    f"CIMPLE Factors API error: {exc!s}",
                    default_value,
                    cached_models,
                )

        return successes, errors

    def _cache_model_success(
        self,
        uri: str,
        model: str,
        value: Any,
        cached_models: dict[str, dict[str, dict[str, Any]]] | None = None,
    ) -> None:
        payload: dict[str, Any] = {"success": True, "data": copy.deepcopy(value)}
        if self.cache:
            self.cache.set(uri, self._cache_step(model), payload)
        if cached_models is not None:
            cached_models.setdefault(model, {})[uri] = payload

    def _cache_model_error(
        self,
        uri: str,
        model: str,
        error_type: str,
        message: str,
        data: Any,
        cached_models: dict[str, dict[str, dict[str, Any]]] | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "success": False,
            "error": {"type": error_type, "message": message},
            "data": copy.deepcopy(data),
        }
        if self.cache:
            self.cache.set(uri, self._cache_step(model), payload)
        if cached_models is not None:
            cached_models.setdefault(model, {})[uri] = payload

    def _cache_error_for_models(
        self,
        uri: str,
        models: list[str],
        cached_models: dict[str, dict[str, dict[str, Any]]] | None,
        *,
        error_type: str,
        message: str,
    ) -> None:
        for model in models:
            default_value = self._empty_model_value(model)
            self._cache_model_error(
                uri,
                model,
                error_type,
                message,
                default_value,
                cached_models,
            )
