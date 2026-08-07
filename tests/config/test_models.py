"""Tests for canonical data-model invariants."""

import pytest

from climatesense_kg.config.models import CanonicalClaim


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        ("", "empty after canonicalization"),
        ("https://example.org/post/1", "contains only a URL"),
        ("[…]", "does not contain meaningful content"),
    ],
)
def test_claim_rejects_non_meaningful_text(text: str, reason: str) -> None:
    with pytest.raises(ValueError, match=reason):
        CanonicalClaim(text=text)


def test_claim_identity_preserves_embedded_urls() -> None:
    first = CanonicalClaim(text="Evidence appears at https://example.org/post/1")
    second = CanonicalClaim(text="Evidence appears at https://example.org/post/2")

    assert first.text.endswith("https://example.org/post/1")
    assert first.uri != second.uri


def test_claim_analysis_text_removes_embedded_urls() -> None:
    claim = CanonicalClaim(text="Evidence appears at https://example.org/post/1")

    assert claim.analysis_text == "Evidence appears at"


def test_claim_validation_is_not_whitespace_language_dependent() -> None:
    claim = CanonicalClaim(text="气候变化是真的")

    assert claim.text == "气候变化是真的"


def test_source_neutral_validation_accepts_short_identifiers() -> None:
    claim = CanonicalClaim(text="B12")

    assert claim.text == "B12"
