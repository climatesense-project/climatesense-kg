"""CIMPLE Factors claim-analysis stage."""

from __future__ import annotations

import copy
import json
import os
import time
from typing import Any, cast

import requests

from ..domain import CanonicalClaimReview
from ..persistence import StageResult, StageResultStore
from .base import Enricher


class BertFactorsEnricher(Enricher):
    """Batch the CIMPLE classification models into one typed claim analysis."""

    MODEL_CONFIG: dict[str, dict[str, Any]] = {
        "emotion": {"endpoint": "emotion", "default": None},
        "sentiment": {"endpoint": "sentiment", "default": None},
        "political_leaning": {
            "endpoint": "political-leaning",
            "default": None,
        },
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
    MODEL_KEYS = tuple(MODEL_CONFIG)

    def __init__(
        self,
        *,
        store: StageResultStore,
        batch_size: int = 32,
        max_length: int = 128,
        timeout: int = 60,
        rate_limit_delay: float = 0.1,
    ) -> None:
        api_url = os.environ.get("CIMPLE_FACTORS_API_URL", "http://localhost:8000")
        super().__init__(
            "bert_factors",
            version="1",
            store=store,
            api_url=api_url,
            batch_size=batch_size,
            max_length=max_length,
            timeout=timeout,
        )
        self.api_url = api_url
        self.batch_size = max(1, batch_size)
        self.max_length = max_length
        self.timeout = timeout
        self.rate_limit_delay = rate_limit_delay
        self.headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "User-Agent": "ClimateSense-Pipeline/2.0 (+https://github.com/climatesense-project)",
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

    def _compute_many(
        self,
        items: list[CanonicalClaimReview],
        *,
        force: bool,
    ) -> list[StageResult]:
        del force
        values: dict[str, dict[str, Any]] = {item.key: {} for item in items}
        failures: dict[str, list[dict[str, str]]] = {item.key: [] for item in items}
        for model in self.MODEL_KEYS:
            for start in range(0, len(items), self.batch_size):
                batch = items[start : start + self.batch_size]
                try:
                    responses = self._call_model(
                        model, [item.claim.analysis_text for item in batch]
                    )
                    if len(responses) != len(batch):
                        raise ValueError(
                            f"Unexpected result count for CIMPLE model {model}"
                        )
                    for item, response in zip(batch, responses, strict=True):
                        values[item.key][model] = self._extract_model_value(
                            model, response
                        )
                except Exception as exc:
                    self.logger.error("CIMPLE model %s failed: %s", model, exc)
                    for item in batch:
                        failures[item.key].append({"model": model, "error": str(exc)})
                        values[item.key][model] = self._empty_model_value(model)
                time.sleep(self.rate_limit_delay)

        results: list[StageResult] = []
        for item in items:
            payload = self._merge_model_data(values[item.key])
            item_failures = failures[item.key]
            if item_failures:
                payload.update(
                    {
                        "error_type": "model_error",
                        "errors": item_failures,
                    }
                )
            results.append(StageResult(success=not item_failures, payload=payload))
        return results

    def _input_value(self, item: CanonicalClaimReview) -> Any:
        return {"claim_text": item.claim.analysis_text}

    def _compute(
        self, item: CanonicalClaimReview, *, force: bool = False
    ) -> StageResult:
        return self._compute_many([item], force=force)[0]

    def _apply(self, item: CanonicalClaimReview, payload: dict[str, Any]) -> None:
        analysis = item.claim.analysis
        analysis.emotion = self._optional_string(payload.get("emotion"))
        analysis.sentiment = self._optional_string(payload.get("sentiment"))
        analysis.political_leaning = self._optional_string(
            payload.get("political_leaning")
        )
        analysis.tropes = self._string_list(payload.get("tropes"))
        analysis.persuasion_techniques = self._string_list(
            payload.get("persuasion_techniques")
        )
        conspiracies = payload.get("conspiracies")
        if not isinstance(conspiracies, dict):
            conspiracies = {}
        analysis.conspiracies = {
            "mentioned": self._string_list(conspiracies.get("mentioned")),
            "promoted": self._string_list(conspiracies.get("promoted")),
        }
        climate_related = payload.get("climate_related")
        analysis.climate_related = (
            climate_related if isinstance(climate_related, bool) else None
        )

    def _call_model(self, model: str, texts: list[str]) -> list[dict[str, Any]]:
        endpoint = self.MODEL_CONFIG[model]["endpoint"]
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

    def _extract_model_value(self, model: str, result: dict[str, Any]) -> Any:
        value = result.get("value")
        if model != "climate_related":
            return value
        if value is None or isinstance(value, bool):
            return value
        if isinstance(value, str) and value.strip().casefold() in {"true", "false"}:
            return value.strip().casefold() == "true"
        raise ValueError("Climate-related result must be boolean")

    def _empty_model_value(self, model: str) -> Any:
        return copy.deepcopy(self.MODEL_CONFIG[model]["default"])

    def _empty_factors_payload(self) -> dict[str, Any]:
        return {model: self._empty_model_value(model) for model in self.MODEL_KEYS}

    def _merge_model_data(self, values: dict[str, Any]) -> dict[str, Any]:
        payload = self._empty_factors_payload()
        payload.update(values)
        return payload

    @staticmethod
    def _optional_string(value: Any) -> str | None:
        return value if isinstance(value, str) else None

    @staticmethod
    def _string_list(value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return [entry for entry in value if isinstance(entry, str)]
