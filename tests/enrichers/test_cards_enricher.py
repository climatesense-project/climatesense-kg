"""Behavioral tests for the CARDS enricher."""

from unittest.mock import Mock, patch

import pytest

from climatesense_kg.enrichers import CARDSEnricher
from climatesense_kg.enrichers.cards_enricher import _load_classifier
from climatesense_kg.processing import ProcessingResult

_LOADER = "climatesense_kg.enrichers.cards_enricher._load_classifier"


def test_cards_semantic_configuration_controls_cache_identity() -> None:
    first = CARDSEnricher(classifier="matcher", min_threshold=0.25)
    operational_change = CARDSEnricher(
        classifier="matcher", min_threshold=0.25, batch_size=7, max_workers=3
    )
    semantic_changes = [
        CARDSEnricher(classifier="matcher", min_threshold=0.5),
        CARDSEnricher(classifier="transformer"),
        CARDSEnricher(
            classifier="llm", preset="climatesense-nslp", model="openai/gpt-4o"
        ),
        CARDSEnricher(
            classifier="llm", preset="climatesense-nslp", use_preclassifier=False
        ),
    ]

    assert first.config_hash == operational_change.config_hash
    hashes = {enricher.config_hash for enricher in semantic_changes}
    assert len(hashes) == len(semantic_changes)
    assert first.config_hash not in hashes


def test_cards_matcher_labels_a_batch_with_one_call(make_review) -> None:
    enricher = CARDSEnricher(classifier="matcher", min_threshold=0.4)
    classifier = Mock()
    classifier.classify_batch.return_value = ["3_5", "0"]
    subjects = enricher.subjects(
        [make_review(claim="CO2 is plant food"), make_review(claim="Nice weather")]
    )
    with patch(_LOADER, return_value=classifier):
        results = enricher.compute_batch(subjects)

    assert results == [
        ProcessingResult.success({"value": "3_5"}),
        ProcessingResult.success({"value": "0"}),
    ]
    classifier.classify_batch.assert_called_once_with(
        ["CO2 is plant food", "Nice weather"], min_threshold=0.4
    )


def test_cards_missing_label_is_retryable(make_review) -> None:
    enricher = CARDSEnricher(classifier="llm", preset="climatesense-nslp")
    classifier = Mock()
    classifier.classify_batch.return_value = [None]
    subject = enricher.subjects([make_review(claim="CO2 is plant food")])[0]
    with patch(_LOADER, return_value=classifier):
        result = enricher.compute_batch([subject])[0]

    assert result == ProcessingResult.retryable(
        {"error_type": "classification_failed", "classifier": "llm"}
    )
    classifier.classify_batch.assert_called_once_with(["CO2 is plant food"])


def test_cards_classifier_failure_makes_every_item_retryable(make_review) -> None:
    enricher = CARDSEnricher(classifier="transformer")
    classifier = Mock()
    classifier.classify_batch.side_effect = RuntimeError("model offline")
    subjects = enricher.subjects([make_review(claim="a"), make_review(claim="b")])
    with patch(_LOADER, return_value=classifier):
        results = enricher.compute_batch(subjects)

    failure = ProcessingResult.retryable(
        {
            "error_type": "classifier_error",
            "classifier": "transformer",
            "error": "model offline",
        }
    )
    assert results == [failure, failure]


@pytest.mark.parametrize(
    ("stored", "expected"),
    [
        ("2_1", "2_1"),
        ("1_0", "1"),
        ("0", None),
        ("0_0", None),
        ("7_1", None),
        (None, None),
    ],
)
def test_cards_apply_stores_only_misinformation_codes(
    make_review, stored, expected
) -> None:
    enricher = CARDSEnricher(classifier="matcher")
    review = make_review()

    enricher.apply_item(review, {"value": stored})

    assert review.claim.analysis.cards_category == expected


@pytest.mark.parametrize(
    ("classifier", "language", "eligible"),
    [
        ("matcher", "en", True),
        ("matcher", "English", True),
        ("matcher", "EN-us", True),
        ("matcher", None, True),
        ("matcher", "DE", False),
        ("matcher", "Albanian", False),
        ("transformer", "DE", False),
        ("llm", "DE", True),
    ],
)
def test_cards_english_only_classifiers_skip_other_languages(
    make_review, classifier, language, eligible
) -> None:
    enricher = CARDSEnricher(classifier=classifier)
    review = make_review()
    review.language = language

    assert bool(enricher.subjects([review])) is eligible


def test_cards_llm_can_disable_the_climatebert_prefilter() -> None:
    classifiers = Mock()
    with patch.dict(
        "sys.modules",
        {"climafactskg": Mock(), "climafactskg.classifiers": classifiers},
    ):
        _load_classifier(
            classifier="llm",
            preset="climatesense-nslp",
            provider=None,
            model=None,
            cache_path=None,
            use_preclassifier=False,
        )

    classifiers.cards.CARDSLLMClassifier.from_preset.assert_called_once_with(
        "climatesense-nslp", use_preclassifier=False
    )


@pytest.mark.parametrize(
    ("classifier", "languages", "language", "eligible"),
    [
        ("matcher", ["de"], "DE", True),
        ("matcher", ["de"], "de-AT", True),
        ("matcher", ["de"], "en", False),
        ("matcher", ["de"], None, True),
        ("llm", ["de", "albanian"], "Albanian", True),
        ("llm", ["de"], "en", False),
    ],
)
def test_cards_languages_option_overrides_the_default_filter(
    make_review, classifier, languages, language, eligible
) -> None:
    enricher = CARDSEnricher(classifier=classifier, languages=languages)
    review = make_review()
    review.language = language

    assert bool(enricher.subjects([review])) is eligible


def test_cards_is_unavailable_when_the_classifier_cannot_load() -> None:
    enricher = CARDSEnricher(classifier="matcher")

    with patch(_LOADER, side_effect=ImportError("climafactskg not installed")):
        assert enricher.is_available() is False
    with patch(_LOADER, return_value=Mock()):
        assert enricher.is_available() is True


def test_cards_preset_model_override_reaches_the_library() -> None:
    classifiers = Mock()
    with patch.dict(
        "sys.modules",
        {"climafactskg": Mock(), "climafactskg.classifiers": classifiers},
    ):
        _load_classifier(
            classifier="llm",
            preset="climatesense-nslp",
            provider=None,
            model="openai/gpt-4o",
            cache_path="cards.db",
        )

    classifiers.cards.CARDSLLMClassifier.from_preset.assert_called_once_with(
        "climatesense-nslp", model="openai/gpt-4o", cache_path="cards.db"
    )
