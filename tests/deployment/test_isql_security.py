"""Tests for the internal ISQL service authentication policy."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

_SECURITY_MODULE_PATH = (
    Path(__file__).parents[2] / "docker" / "isql-service" / "src" / "security.py"
)
_SPEC = spec_from_file_location("isql_service_security", _SECURITY_MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_SECURITY = module_from_spec(_SPEC)
_SPEC.loader.exec_module(_SECURITY)
is_valid_bearer_token = getattr(_SECURITY, "is_valid_bearer_token")  # noqa: B009


@pytest.mark.parametrize("authorization", ["", "Bearer", "Bearer wrong-token"])
def test_rejects_missing_or_incorrect_bearer_token(authorization: str) -> None:
    assert not is_valid_bearer_token(authorization, "expected-token")


def test_accepts_configured_bearer_token() -> None:
    assert is_valid_bearer_token("Bearer expected-token", "expected-token")
