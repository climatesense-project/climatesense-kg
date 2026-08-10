"""Claim-review identity resolution."""

from .fingerprints import (
    DocumentFingerprint,
    fingerprint_document,
    normalize_identity_text,
    shingle_containment,
)
from .models import IdentityAssignment, IdentityCandidate, RegisteredDocument
from .registry import IdentityRegistry, IdentityTransaction, InMemoryIdentityRegistry
from .resolver import IdentityResolver

__all__ = [
    "DocumentFingerprint",
    "IdentityAssignment",
    "IdentityCandidate",
    "IdentityRegistry",
    "IdentityResolver",
    "IdentityTransaction",
    "InMemoryIdentityRegistry",
    "RegisteredDocument",
    "fingerprint_document",
    "normalize_identity_text",
    "shingle_containment",
]
