"""Authentication helpers for the internal Virtuoso ISQL service."""

import secrets


def is_valid_bearer_token(authorization: str, expected_token: str) -> bool:
    """Validate the service bearer token without timing-sensitive comparison."""
    if not expected_token:
        return False
    return secrets.compare_digest(authorization, f"Bearer {expected_token}")
