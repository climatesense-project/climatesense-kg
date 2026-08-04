"""Assertions for service-to-service Docker Compose defaults."""

from pathlib import Path
from typing import Any

import yaml

_COMPOSE_PATH = Path(__file__).parents[2] / "docker" / "docker-compose.yml"


def _compose_services() -> dict[str, dict[str, Any]]:
    compose = yaml.safe_load(_COMPOSE_PATH.read_text())
    return compose["services"]


def test_pipeline_requires_reachable_factors_api_url() -> None:
    pipeline_environment = _compose_services()["pipeline"]["environment"]

    assert pipeline_environment["CIMPLE_FACTORS_API_URL"] == (
        "${CIMPLE_FACTORS_API_URL:?CIMPLE_FACTORS_API_URL must point to a Factors API "
        "reachable from the pipeline container}"
    )
