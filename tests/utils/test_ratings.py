"""Tests for shared constants and helpers."""

from src.climatesense_kg.utils.ratings import (
    VALID_NORMALIZED_RATINGS,
    normalize_rating_label,
)


def test_normalize_rating_label_maps_bool_values() -> None:
    assert normalize_rating_label("False") == "not_credible"
    assert normalize_rating_label("true") == "credible"


def test_normalize_rating_label_accepts_known_labels() -> None:
    label = "not_verifiable"
    assert label in VALID_NORMALIZED_RATINGS
    assert normalize_rating_label(label) == label


def test_normalize_rating_label_rejects_unknown() -> None:
    assert normalize_rating_label("unsupported") is None


def test_normalize_rating_label_handles_whitespace() -> None:
    assert normalize_rating_label("  credible  ") == "credible"
