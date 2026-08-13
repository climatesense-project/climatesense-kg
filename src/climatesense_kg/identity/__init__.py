"""Claim-review identity resolution."""

from .audit import DuplicateAuditor, DuplicateAuditReport
from .fingerprints import (
    DocumentFingerprint,
    fingerprint_document,
    normalize_identity_text,
    shingle_containment,
)
from .models import IdentityAssignment, RegisteredDocument
from .planner import IdentityPlanner
from .registry import (
    IdentityRegistry,
    IdentityRepositoryBatch,
    InMemoryIdentityRegistry,
)
from .resolver import IdentityResolver

__all__ = [
    "DocumentFingerprint",
    "DuplicateAuditReport",
    "DuplicateAuditor",
    "IdentityAssignment",
    "IdentityPlanner",
    "IdentityRegistry",
    "IdentityRepositoryBatch",
    "IdentityResolver",
    "InMemoryIdentityRegistry",
    "RegisteredDocument",
    "fingerprint_document",
    "normalize_identity_text",
    "shingle_containment",
]
