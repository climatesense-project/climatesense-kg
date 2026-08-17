"""Claim-review identity resolution."""

from .audit import DuplicateAuditor, DuplicateAuditReport
from .fingerprints import (
    DocumentFingerprint,
    normalize_identity_text,
    shingle_containment,
)
from .service import IdentityService, IdentitySummary

__all__ = [
    "DocumentFingerprint",
    "DuplicateAuditReport",
    "DuplicateAuditor",
    "IdentityService",
    "IdentitySummary",
    "normalize_identity_text",
    "shingle_containment",
]
