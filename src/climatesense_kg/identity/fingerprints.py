"""Interpretable document fingerprints used as identity evidence."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
import unicodedata

from ..domain import ReviewDocument

WORD_PATTERN = re.compile(r"\w+", flags=re.UNICODE)
DEFAULT_SHINGLE_SIZE = 5


@dataclass(frozen=True)
class DocumentFingerprint:
    """Normalized exact and near-duplicate representation of document content."""

    normalized_text_hash: str | None
    shingles: frozenset[str]
    word_count: int


def normalize_identity_text(text: str) -> str:
    """Normalize formatting while retaining all word-bearing content."""

    normalized_unicode = unicodedata.normalize("NFKC", text).casefold()
    return " ".join(WORD_PATTERN.findall(normalized_unicode))


def fingerprint_text(
    text: str | None, *, shingle_size: int = DEFAULT_SHINGLE_SIZE
) -> DocumentFingerprint:
    """Fingerprint text without using the result as an entity identifier."""

    if shingle_size < 1:
        raise ValueError("Shingle size must be at least one")
    normalized = normalize_identity_text(text or "")
    if not normalized:
        return DocumentFingerprint(None, frozenset(), 0)
    words = normalized.split()
    shingles = frozenset(
        hashlib.sha256(
            " ".join(words[index : index + shingle_size]).encode()
        ).hexdigest()
        for index in range(len(words) - shingle_size + 1)
    )
    return DocumentFingerprint(
        normalized_text_hash=hashlib.sha256(normalized.encode()).hexdigest(),
        shingles=shingles,
        word_count=len(words),
    )


def fingerprint_document(
    document: ReviewDocument, *, shingle_size: int = DEFAULT_SHINGLE_SIZE
) -> DocumentFingerprint:
    """Fingerprint and annotate one source document observation."""

    fingerprint = fingerprint_text(document.content, shingle_size=shingle_size)
    document.normalized_text_hash = fingerprint.normalized_text_hash
    document.shingle_signature = sorted(fingerprint.shingles)
    document.word_count = fingerprint.word_count
    return fingerprint


def shingle_containment(
    left: frozenset[str] | set[str], right: frozenset[str] | set[str]
) -> float:
    """Return the fraction of the smaller shingle set contained in the larger."""

    smaller_size = min(len(left), len(right))
    if smaller_size == 0:
        return 0.0
    return len(left & right) / smaller_size
