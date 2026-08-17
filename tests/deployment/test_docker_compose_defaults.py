"""Assertions for service-to-service Docker Compose defaults."""

from pathlib import Path
from typing import Any

import yaml

_COMPOSE_PATH = Path(__file__).parents[2] / "docker" / "docker-compose.yml"
_DAILY_CONFIG_PATH = Path(__file__).parents[2] / "config" / "daily.yaml"


def _compose_services() -> dict[str, dict[str, Any]]:
    compose = yaml.safe_load(_COMPOSE_PATH.read_text())
    return compose["services"]


def test_pipeline_requires_reachable_factors_api_url() -> None:
    pipeline_environment = _compose_services()["pipeline"]["environment"]

    assert pipeline_environment["CIMPLE_FACTORS_API_URL"] == (
        "${CIMPLE_FACTORS_API_URL:?CIMPLE_FACTORS_API_URL must point to a Factors API "
        "reachable from the pipeline container}"
    )


def test_qlever_index_deployment_is_separate_from_pipeline() -> None:
    services = _compose_services()
    daily_config = yaml.safe_load(_DAILY_CONFIG_PATH.read_text())

    assert daily_config["deployment"]["backend"] == "none"
    assert "QLEVER_ENDPOINT" not in services["pipeline"]["environment"]
    assert services["analytics-api"]["environment"]["ANALYTICS_SPARQL_ENDPOINT"] == (
        "${ANALYTICS_SPARQL_ENDPOINT:-${QLEVER_ENDPOINT:-http://qlever:7019}}"
    )


def test_service_clients_use_postgres_container_port() -> None:
    services = _compose_services()

    assert services["pipeline"]["environment"]["POSTGRES_PORT"] == "5432"
    assert services["analytics-api"]["environment"]["POSTGRES_PORT"] == "5432"


def test_services_share_durable_postgres_database_default() -> None:
    services = _compose_services()

    assert services["postgres"]["environment"]["POSTGRES_DB"] == (
        "${POSTGRES_DB:-climatesense}"
    )
    assert services["pipeline"]["environment"]["POSTGRES_DB"] == (
        "${POSTGRES_DB:-climatesense}"
    )
    assert services["analytics-api"]["environment"]["POSTGRES_DB"] == (
        "${POSTGRES_DB:-climatesense}"
    )
