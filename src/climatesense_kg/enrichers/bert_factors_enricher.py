# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportAttributeAccessIssue=false, reportUnknownArgumentType=false, reportCallIssue=false, reportPossiblyUnboundVariable=false
"""BERT-based factors enrichment for emotion, sentiment, political leaning, and conspiracy detection."""

import logging
from pathlib import Path
from typing import Any
import warnings

import requests
from tqdm import tqdm

from ..config.models import CanonicalClaimReview
from .base import Enricher

# Suppress specific huggingface-hub deprecation warning
warnings.filterwarnings(
    "ignore",
    message=".*resume_download.*deprecated.*",
    category=FutureWarning,
    module="huggingface_hub",
)

import torch  # noqa: E402
import torch.nn as nn  # noqa: E402
from transformers import (  # noqa: E402
    AutoTokenizer,
    BertForPreTraining,
)

logger = logging.getLogger(__name__)


class CovidTwitterBertClassifier(nn.Module):
    """BERT classifier for COVID Twitter data."""

    def __init__(self, n_classes: int):
        super().__init__()
        self.bert = BertForPreTraining.from_pretrained(
            "digitalepidemiologylab/covid-twitter-bert-v2",
            revision="b113bc3c2590d7b32ed62603fe1ebe32e1e5beee",
        )
        original_in_features = self.bert.cls.seq_relationship.in_features
        self.bert.cls.seq_relationship = nn.Linear(original_in_features, n_classes)

    def forward(
        self,
        input_ids: torch.Tensor,
        token_type_ids: torch.Tensor,
        input_mask: torch.Tensor,
    ) -> torch.Tensor:
        outputs = self.bert(
            input_ids=input_ids,
            token_type_ids=token_type_ids,
            attention_mask=input_mask,
        )
        logits: torch.Tensor = outputs[1]
        return logits


class BertFactorsEnricher(Enricher):
    """Enricher that uses BERT models for emotion, sentiment, political leaning, and conspiracy detection."""

    # Constants for factor categories
    EMOTIONS_LIST = ["None", "Happiness", "Anger", "Sadness", "Fear"]
    POLITICAL_BIAS_LIST = ["Left", "Other", "Right"]
    SENTIMENTS_LIST = ["Negative", "Neutral", "Positive"]
    CONSPIRACIES_LIST = [
        "Suppressed Cures",
        "Behaviour and mind Control",
        "Antivax",
        "Fake virus",
        "Intentional Pandemic",
        "Harmful Radiation",
        "Population Reduction",
        "New World Order",
        "Satanism",
    ]
    CONSPIRACY_LEVELS_LIST = ["No ", "Mentioning ", "Supporting "]

    # Model download configuration
    MODELS_BASE_URL = "https://data.cimple.eu/models"
    REQUIRED_MODELS = [
        "emotion.pth",
        "sentiment.pth",
        "political-leaning.pth",
        "conspiracy.pth",
    ]

    tokenizer: AutoTokenizer | None = None
    models: (
        tuple[
            CovidTwitterBertClassifier | None,
            CovidTwitterBertClassifier | None,
            CovidTwitterBertClassifier | None,
            CovidTwitterBertClassifier | None,
        ]
        | None
    ) = None
    torch_device: torch.device | None = None

    def __init__(self, **kwargs: Any) -> None:
        super().__init__("bert_factors", **kwargs)
        models_path = kwargs.get("models_path")
        if models_path is None:
            raise ValueError("models_path must be provided")
        self.models_path = Path(models_path)
        self.batch_size = kwargs.get("batch_size", 32)
        self.max_length = kwargs.get("max_length", 128)
        self.device = kwargs.get("device", "auto")
        self.auto_download = kwargs.get("auto_download", True)
        self._models_loaded = False

    def _setup_device(self) -> None:
        """Setup PyTorch device."""
        if self.device == "auto":
            self.torch_device = torch.device(
                "cuda" if torch.cuda.is_available() else "cpu"
            )
        else:
            self.torch_device = torch.device(self.device)

        self.logger.info(f"Using PyTorch device: {self.torch_device}")

    def _ensure_models_available(self) -> None:
        """Ensure all required models are available, download if missing and auto_download is enabled."""
        if not self.auto_download:
            return

        missing_models = self._get_missing_models()
        if missing_models:
            self.logger.info(f"Missing BERT models: {missing_models}")
            self.logger.info("Downloading missing models...")

            self.models_path.mkdir(parents=True, exist_ok=True)

            for model_file in missing_models:
                self._download_model(model_file)
        else:
            self.logger.info("All BERT models are available")

    def _get_missing_models(self) -> list[str]:
        """Get list of missing model files."""
        missing = []
        for model_file in self.REQUIRED_MODELS:
            model_path = self.models_path / model_file
            if not model_path.exists():
                missing.append(model_file)
        return missing

    def _download_model(self, model_file: str) -> None:
        """Download a single model file."""
        url = f"{self.MODELS_BASE_URL}/{model_file}"
        model_path = self.models_path / model_file

        self.logger.info(f"Downloading {model_file} from {url}")

        try:
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))

            with open(model_path, "wb") as f:
                with tqdm(
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    desc=model_file,
                    miniters=1,
                ) as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))

            self.logger.info(f"Successfully downloaded {model_file}")

        except Exception as e:
            self.logger.error(f"Failed to download {model_file}: {e}")
            # Clean up partial download
            if model_path.exists():
                model_path.unlink()
            raise RuntimeError(f"Could not download required model {model_file}") from e

    def _load_models(self) -> None:
        """Load BERT models and tokenizer."""
        if not self.models_path.exists():
            self.logger.error(f"Models path does not exist: {self.models_path}")
            return

        try:
            # Load tokenizer
            self.logger.info("Loading tokenizer...")
            self.tokenizer = AutoTokenizer.from_pretrained(
                "digitalepidemiologylab/covid-twitter-bert-v2",
                revision="b113bc3c2590d7b32ed62603fe1ebe32e1e5beee",
            )

            # Load models
            self.logger.info("Loading BERT factor models...")

            # Emotion model
            model_em: CovidTwitterBertClassifier | None = None
            try:
                model_em = CovidTwitterBertClassifier(len(self.EMOTIONS_LIST)).to(
                    self.torch_device
                )
                emotion_path = self.models_path / "emotion.pth"
                if emotion_path.exists():
                    model_em.load_state_dict(
                        torch.load(
                            emotion_path,
                            map_location=self.torch_device,
                            weights_only=True,
                        )
                    )
                    model_em.eval()
                else:
                    self.logger.warning(f"Emotion model not found at {emotion_path}")
                    model_em = None
            except Exception as e:
                self.logger.warning(f"Failed to load emotion model: {e}")
                model_em = None

            # Political leaning model
            model_pol: CovidTwitterBertClassifier | None = None
            try:
                model_pol = CovidTwitterBertClassifier(
                    len(self.POLITICAL_BIAS_LIST)
                ).to(self.torch_device)
                political_path = self.models_path / "political-leaning.pth"
                if political_path.exists():
                    model_pol.load_state_dict(
                        torch.load(
                            political_path,
                            map_location=self.torch_device,
                            weights_only=True,
                        )
                    )
                    model_pol.eval()
                else:
                    self.logger.warning(
                        f"Political leaning model not found at {political_path}"
                    )
                    model_pol = None
            except Exception as e:
                self.logger.warning(f"Failed to load political leaning model: {e}")
                model_pol = None

            # Sentiment model
            model_sent: CovidTwitterBertClassifier | None = None
            try:
                model_sent = CovidTwitterBertClassifier(len(self.SENTIMENTS_LIST)).to(
                    self.torch_device
                )
                sentiment_path = self.models_path / "sentiment.pth"
                if sentiment_path.exists():
                    model_sent.load_state_dict(
                        torch.load(
                            sentiment_path,
                            map_location=self.torch_device,
                            weights_only=True,
                        )
                    )
                    model_sent.eval()
                else:
                    self.logger.warning(
                        f"Sentiment model not found at {sentiment_path}"
                    )
                    model_sent = None
            except Exception as e:
                self.logger.warning(f"Failed to load sentiment model: {e}")
                model_sent = None

            # Conspiracy model
            model_con: CovidTwitterBertClassifier | None = None
            try:
                model_con = CovidTwitterBertClassifier(
                    len(self.CONSPIRACIES_LIST) * len(self.CONSPIRACY_LEVELS_LIST)
                ).to(self.torch_device)
                conspiracy_path = self.models_path / "conspiracy.pth"
                if conspiracy_path.exists():
                    model_con.load_state_dict(
                        torch.load(
                            conspiracy_path,
                            map_location=self.torch_device,
                            weights_only=True,
                        )
                    )
                    model_con.eval()
                else:
                    self.logger.warning(
                        f"Conspiracy model not found at {conspiracy_path}"
                    )
                    model_con = None
            except Exception as e:
                self.logger.warning(f"Failed to load conspiracy model: {e}")
                model_con = None

            self.models = (model_em, model_pol, model_sent, model_con)

        except Exception as e:
            self.logger.error(f"Error loading BERT models: {e}")
            self.models = None
            self.tokenizer = None

    def _ensure_models_loaded(self) -> None:
        """Ensure models are loaded when needed."""
        if not self._models_loaded:
            self._setup_device()
            self._ensure_models_available()
            self._load_models()
            self._models_loaded = True

    def is_available(self) -> bool:
        """Check if BERT enricher is available."""
        if not self.auto_download:
            missing_models = self._get_missing_models()
            if missing_models:
                self.logger.warning(f"Missing BERT models: {missing_models}")
                return False

        return True

    def enrich(self, claim_review: CanonicalClaimReview) -> CanonicalClaimReview:
        """
        Enrich claim review with BERT-based factors.

        Args:
            claim_review: Claim review to enrich

        Returns:
            CanonicalClaimReview: Enriched claim review
        """
        if not self.is_available() or not claim_review.uri:
            return claim_review

        # Check cache first
        cached_data = self.get_cached(claim_review.uri)
        if cached_data:
            self._apply_factors(claim_review, cached_data)
            self.logger.debug(f"Applied cached BERT factors for {claim_review.uri}")
            return claim_review

        # Compute factors if text available
        if claim_review.claim.normalized_text:
            self._ensure_models_loaded()
            factors = self._compute_factors(claim_review.claim.normalized_text)
            if factors:
                self._apply_factors(claim_review, factors, cache=True)

        return claim_review

    def enrich_batch(
        self, claim_reviews: list[CanonicalClaimReview]
    ) -> list[CanonicalClaimReview]:
        """
        Enrich a batch of claim reviews with optimized batch processing and caching.

        Args:
            claim_reviews: List of claim reviews to enrich

        Returns:
            List[CanonicalClaimReview]: List of enriched claim reviews
        """
        if not self.is_available() or not claim_reviews:
            return claim_reviews

        # First pass: handle cached items and collect uncached
        uncached_items = []
        cached_count = 0

        for claim_review in claim_reviews:
            if not claim_review.uri:
                continue

            cached_data = self.get_cached(claim_review.uri)
            if cached_data:
                self._apply_factors(claim_review, cached_data)
                cached_count += 1
                self.logger.debug(f"Applied cached BERT factors for {claim_review.uri}")
            elif claim_review.claim.normalized_text:
                uncached_items.append(claim_review)

        if not uncached_items:
            self.logger.info(f"All {len(claim_reviews)} claim reviews were cached")
            return claim_reviews

        self.logger.info(
            f"Processing {len(uncached_items)} uncached items (cached: {cached_count})"
        )

        # Batch compute factors for uncached items
        texts = [item.claim.normalized_text for item in uncached_items]
        self._ensure_models_loaded()
        all_factors = self._compute_factors_batch(texts)

        # Apply computed factors
        processed_count = 0
        for item, factors in zip(uncached_items, all_factors, strict=False):
            if factors:
                self._apply_factors(item, factors, cache=True)
                processed_count += 1

        self.logger.info(
            f"BERT factors progress: {processed_count}/{len(uncached_items)}"
        )
        self.logger.info(
            f"Completed BERT factors enrichment for {len(claim_reviews)} claim reviews"
        )

        return claim_reviews

    def _compute_factors(self, text: str) -> dict[str, Any] | None:
        """
        Compute factors for a single text.

        Args:
            text: Text to analyze

        Returns:
            Dict[str, Any]: Computed factors
        """
        if not text or not text.strip() or not self.models or not self.tokenizer:
            return None

        try:
            # Tokenize text
            tokenized = self.tokenizer(
                text,
                max_length=self.max_length,
                padding="max_length",
                truncation=True,
                return_tensors="pt",
            )

            input_ids = tokenized["input_ids"].to(self.torch_device)
            token_type_ids = tokenized["token_type_ids"].to(self.torch_device)
            attention_mask = tokenized["attention_mask"].to(self.torch_device)

            model_em, model_pol, model_sent, model_con = self.models

            with torch.no_grad():
                # Compute predictions
                results = {}

                # Emotion
                if model_em:
                    logits_em = model_em(input_ids, token_type_ids, attention_mask)
                    pred_em = logits_em.detach().cpu().numpy().argmax(axis=1)[0]
                    results["emotion"] = (
                        self.EMOTIONS_LIST[pred_em]
                        if self.EMOTIONS_LIST[pred_em] != "None"
                        else None
                    )

                # Political leaning
                if model_pol:
                    logits_pol = model_pol(input_ids, token_type_ids, attention_mask)
                    pred_pol = logits_pol.detach().cpu().numpy().argmax(axis=1)[0]
                    results["political_leaning"] = self.POLITICAL_BIAS_LIST[pred_pol]

                # Sentiment
                if model_sent:
                    logits_sent = model_sent(input_ids, token_type_ids, attention_mask)
                    pred_sent = logits_sent.detach().cpu().numpy().argmax(axis=1)[0]
                    results["sentiment"] = self.SENTIMENTS_LIST[pred_sent]

                # Conspiracies
                if model_con:
                    logits_con = model_con(input_ids, token_type_ids, attention_mask)

                    # Reshape conspiracy predictions
                    num_conspiracies = len(self.CONSPIRACIES_LIST)
                    num_levels = len(self.CONSPIRACY_LEVELS_LIST)

                    predictions_reshaped = (
                        logits_con.detach()
                        .cpu()
                        .numpy()
                        .reshape(-1, num_conspiracies, num_levels)
                    )
                    predictions_con = predictions_reshaped.argmax(axis=2)[0]

                    # Extract conspiracy mentions and promotions
                    mentioned_conspiracies = []
                    promoted_conspiracies = []

                    for i, level_idx in enumerate(predictions_con):
                        conspiracy_name = self.CONSPIRACIES_LIST[i]
                        if level_idx == 1:  # Mentioning
                            mentioned_conspiracies.append(conspiracy_name)
                        elif level_idx == 2:  # Supporting
                            promoted_conspiracies.append(conspiracy_name)

                    results["conspiracies"] = {
                        "mentioned": mentioned_conspiracies,
                        "promoted": promoted_conspiracies,
                    }

                return results

        except Exception as e:
            self.logger.error(f"Error computing BERT factors: {e}")
            return None

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
        claim_review.claim.conspiracies = factors.get("conspiracies", [])

        if cache and claim_review.uri:
            self.set_cached(claim_review.uri, factors)

    def _compute_factors_batch(self, texts: list[str]) -> list[dict[str, Any] | None]:
        """
        Compute factors for a batch of texts efficiently.

        Args:
            texts: List of texts to analyze

        Returns:
            List[Optional[Dict[str, Any]]]: List of computed factors
        """
        if not texts:
            return []

        if self.tokenizer is None or self.models is None:
            self.logger.error("Tokenizer or models not loaded")
            return [None] * len(texts)

        try:
            # Filter out empty texts
            valid_items = [
                (i, text) for i, text in enumerate(texts) if text and text.strip()
            ]
            valid_indices, valid_texts = (
                zip(*valid_items, strict=False) if valid_items else ([], [])
            )
            valid_indices, valid_texts = list(valid_indices), list(valid_texts)

            if not valid_texts:
                return [None] * len(texts)

            # Batch tokenization
            tokenized_batch = self.tokenizer(
                valid_texts,
                max_length=self.max_length,
                padding="max_length",
                truncation=True,
                return_tensors="pt",
            )

            input_ids = tokenized_batch["input_ids"].to(self.torch_device)
            token_type_ids = tokenized_batch["token_type_ids"].to(self.torch_device)
            attention_mask = tokenized_batch["attention_mask"].to(self.torch_device)

            model_em, model_pol, model_sent, model_con = self.models

            with torch.no_grad():
                # Batch predictions
                batch_results: list[dict[str, Any] | None] = []

                predictions_em = None
                predictions_pol = None
                predictions_sent = None
                predictions_con = None

                if model_em:
                    logits_em_batch = model_em(
                        input_ids, token_type_ids, attention_mask
                    )
                    predictions_em = (
                        logits_em_batch.detach().cpu().numpy().argmax(axis=1)
                    )

                if model_pol:
                    logits_pol_batch = model_pol(
                        input_ids, token_type_ids, attention_mask
                    )
                    predictions_pol = (
                        logits_pol_batch.detach().cpu().numpy().argmax(axis=1)
                    )

                if model_sent:
                    logits_sent_batch = model_sent(
                        input_ids, token_type_ids, attention_mask
                    )
                    predictions_sent = (
                        logits_sent_batch.detach().cpu().numpy().argmax(axis=1)
                    )

                if model_con:
                    logits_con_batch = model_con(
                        input_ids, token_type_ids, attention_mask
                    )

                    num_conspiracies = len(self.CONSPIRACIES_LIST)
                    num_levels = len(self.CONSPIRACY_LEVELS_LIST)

                    predictions_con_reshaped = (
                        logits_con_batch.detach()
                        .cpu()
                        .numpy()
                        .reshape(-1, num_conspiracies, num_levels)
                    )
                    predictions_con = predictions_con_reshaped.argmax(axis=2)

                # Process results for each valid text
                for i in range(len(valid_texts)):
                    results: dict[str, Any] = {}

                    if predictions_em is not None:
                        emotion = self.EMOTIONS_LIST[predictions_em[i]]
                        results["emotion"] = emotion if emotion != "None" else None

                    if predictions_pol is not None:
                        results["political_leaning"] = self.POLITICAL_BIAS_LIST[
                            predictions_pol[i]
                        ]

                    if predictions_sent is not None:
                        results["sentiment"] = self.SENTIMENTS_LIST[predictions_sent[i]]

                    if predictions_con is not None:
                        conspiracy_indices = predictions_con[i]
                        mentioned = []
                        promoted = []

                        for j, level_idx in enumerate(conspiracy_indices):
                            conspiracy_name = self.CONSPIRACIES_LIST[j]
                            if level_idx == 1:
                                mentioned.append(conspiracy_name)
                            elif level_idx == 2:
                                promoted.append(conspiracy_name)

                        results["conspiracies"] = {
                            "mentioned": mentioned,
                            "promoted": promoted,
                        }

                    batch_results.append(results)

                # Map results back to original indices
                final_results: list[dict[str, Any] | None] = [None] * len(texts)
                for i, original_idx in enumerate(valid_indices):
                    final_results[original_idx] = batch_results[i]

                return final_results

        except Exception as e:
            self.logger.error(f"Error in batch BERT factors computation: {e}")
            return [None] * len(texts)
