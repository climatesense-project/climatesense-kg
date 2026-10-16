"""CARDS taxonomy labelling of canonical claims with climafactskg classifiers."""

from __future__ import annotations

from importlib import metadata
import re
import threading
from typing import Any, get_args

from ..config.enrichment import CARDSClassifierName
from ..domain import CanonicalClaimReview
from ..processing import ProcessingResult
from .base import Enricher

# Misinformation categories only: the not-relevant sentinels "0" and "0_0" are
# not stored, so such claims get no CARDS triple.
CARDS_CODE = re.compile(r"[1-5](?:_[1-9]){0,2}")
# "N_0" is the catch-all of top-level category N, an exact match of concept N.
GENERAL_CODE = re.compile(r"([0-5])_0\Z")
CLASSIFIERS = get_args(CARDSClassifierName)
# The matcher and transformer models only understand English.
ENGLISH_ONLY = {"matcher", "transformer"}
ENGLISH_LANGUAGES = ["en", "eng", "english"]


def _library_version() -> str:
    try:
        return metadata.version("climafactskg")
    except metadata.PackageNotFoundError:
        return "unknown"


def _load_classifier(
    *,
    classifier: CARDSClassifierName,
    preset: str | None,
    provider: str | None,
    model: str | None,
    cache_path: str | None,
    use_preclassifier: bool = True,
) -> Any:
    try:
        from climafactskg.classifiers import cards  # ty: ignore[unresolved-import]
    except ImportError as exc:
        raise ImportError(
            "The CARDS enricher requires climafactskg. "
            "Install it with: uv sync --extra cards "
            "(--extra cards-transformer for the transformer and llm classifiers)"
        ) from exc
    if classifier == "matcher":
        return cards.CARDSMatcher()
    if classifier == "transformer":
        return cards.CARDSClassifier(cache_path=cache_path)
    overrides: dict[str, Any] = {
        key: value
        for key, value in (
            ("provider", provider),
            ("model", model),
            ("cache_path", cache_path),
        )
        if value is not None
    }
    if not use_preclassifier:
        overrides["use_preclassifier"] = False
    if preset:
        return cards.CARDSLLMClassifier.from_preset(preset, **overrides)
    return cards.CARDSLLMClassifier(**overrides)


class CARDSEnricher(Enricher):
    """Label canonical claim text with a CARDS taxonomy code."""

    def __init__(
        self,
        *,
        classifier: CARDSClassifierName = "matcher",
        min_threshold: float = 0.25,
        preset: str | None = None,
        provider: str | None = None,
        model: str | None = None,
        cache_path: str | None = None,
        use_preclassifier: bool = True,
        languages: list[str] | None = None,
        batch_size: int = 32,
        max_workers: int = 1,
    ) -> None:
        if classifier not in CLASSIFIERS:
            raise ValueError(f"Unknown CARDS classifier: {classifier}")
        semantic_config: dict[str, Any] = {
            "classifier": classifier,
            "library_version": _library_version(),
        }
        if classifier == "matcher":
            semantic_config["min_threshold"] = min_threshold
        elif classifier == "llm":
            semantic_config.update(
                preset=preset,
                provider=provider,
                model=model,
                use_preclassifier=use_preclassifier,
            )
        super().__init__(
            "cards",
            version="1",
            semantic_config=semantic_config,
            batch_size=batch_size,
            max_workers=max_workers,
        )
        self.classifier = classifier
        self.min_threshold = min_threshold
        self.preset = preset
        self.provider = provider
        self.model = model
        self.cache_path = cache_path
        self.use_preclassifier = use_preclassifier
        if languages is None and classifier in ENGLISH_ONLY:
            languages = ENGLISH_LANGUAGES
        self.languages = {language.casefold() for language in languages or ()}
        self._loaded: Any = None
        self._load_lock = threading.Lock()

    def is_available(self) -> bool:
        try:
            self._get_classifier()
        except Exception as exc:
            self.logger.warning(
                "CARDS %s classifier unavailable: %s", self.classifier, exc
            )
            return False
        return True

    def eligible_items(
        self, items: list[CanonicalClaimReview]
    ) -> list[CanonicalClaimReview]:
        if not self.languages:
            return items
        return [item for item in items if self._accepts_language(item.language)]

    def _accepts_language(self, language: str | None) -> bool:
        """Accept unknown languages, a listed value, or its primary subtag."""
        if not language or not language.strip():
            return True
        value = language.strip().casefold()
        return value in self.languages or re.split(r"[-_]", value)[0] in self.languages

    def subject_key(self, item: CanonicalClaimReview) -> str:
        return item.claim.uri

    def input_value(self, item: CanonicalClaimReview) -> Any:
        return {"claim_text": item.claim.analysis_text}

    def compute_items(
        self, items: list[CanonicalClaimReview]
    ) -> list[ProcessingResult]:
        texts = [item.claim.analysis_text for item in items]
        try:
            labels = self._get_classifier().classify_batch(
                texts, **self._batch_options()
            )
            if len(labels) != len(items):
                raise ValueError("CARDS classifier returned an unexpected result count")
        except Exception as exc:
            self.logger.error("CARDS %s classifier failed: %s", self.classifier, exc)
            return [
                ProcessingResult.retryable(
                    {
                        "error_type": "classifier_error",
                        "classifier": self.classifier,
                        "error": str(exc),
                    }
                )
                for _item in items
            ]
        return [self._label_result(label) for label in labels]

    def compute_item(self, item: CanonicalClaimReview) -> ProcessingResult:
        return self.compute_items([item])[0]

    def apply_item(self, item: CanonicalClaimReview, payload: dict[str, Any]) -> None:
        value = payload.get("value")
        if isinstance(value, str):
            value = GENERAL_CODE.sub(r"\1", value)
        item.claim.analysis.cards_category = (
            value if isinstance(value, str) and CARDS_CODE.fullmatch(value) else None
        )

    def _label_result(self, label: Any) -> ProcessingResult:
        if label is None:
            return ProcessingResult.retryable(
                {
                    "error_type": "classification_failed",
                    "classifier": self.classifier,
                }
            )
        return ProcessingResult.success({"value": label})

    def _batch_options(self) -> dict[str, Any]:
        if self.classifier == "matcher":
            return {"min_threshold": self.min_threshold}
        return {}

    def _get_classifier(self) -> Any:
        with self._load_lock:
            if self._loaded is None:
                self._loaded = _load_classifier(
                    classifier=self.classifier,
                    preset=self.preset,
                    provider=self.provider,
                    model=self.model,
                    cache_path=self.cache_path,
                    use_preclassifier=self.use_preclassifier,
                )
            return self._loaded
