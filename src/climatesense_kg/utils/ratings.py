"""Rating normalization helpers."""

from __future__ import annotations

from typing import Final

VALID_NORMALIZED_RATINGS: Final[set[str]] = {
    "misinformed_or_potentially_misleading",
    "not_misleading",
    "uncertain",
    "not_credible",
    "not_verifiable",
    "credible",
    "mostly_credible",
}

_BOOLEAN_MAPPING: Final[dict[str, str]] = {
    "false": "not_credible",
    "true": "credible",
}


def normalize_rating_label(label: str | None) -> str | None:
    """Normalize a raw rating label to the ontology values we support."""
    if not label:
        return None

    cleaned = label.strip()
    if not cleaned:
        return None

    lowered = cleaned.lower()
    if lowered in _BOOLEAN_MAPPING:
        return _BOOLEAN_MAPPING[lowered]

    normalized = lowered.replace("-", "_").replace("/", "_")
    normalized = "_".join(part for part in normalized.split() if part)

    if normalized in VALID_NORMALIZED_RATINGS:
        return normalized

    return None
