"""Interpretable document fingerprints used as identity evidence."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
import unicodedata

WORD_PATTERN = re.compile(r"\w+", flags=re.UNICODE)


@dataclass(frozen=True)
class DocumentFingerprint:
    """Bounded exact identity evidence derived from document content."""

    normalized_text_hash: str | None
    word_count: int


def normalize_identity_text(text: str) -> str:
    """Normalize formatting while retaining all word-bearing content."""

    normalized_unicode = unicodedata.normalize("NFKC", text).casefold()
    return " ".join(WORD_PATTERN.findall(normalized_unicode))


def fingerprint_text(text: str | None) -> DocumentFingerprint:
    """Build exact identity evidence from document text."""

    normalized = normalize_identity_text(text or "")
    if not normalized:
        return DocumentFingerprint(None, 0)
    words = normalized.split()
    return DocumentFingerprint(
        normalized_text_hash=hashlib.sha256(normalized.encode()).hexdigest(),
        word_count=len(words),
    )
