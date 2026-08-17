"""Tests for provider configuration validation."""

from pathlib import Path

import pytest

from climatesense_kg.config import load_config
from climatesense_kg.config.schemas import GraphQLProviderConfig


@pytest.mark.parametrize("batch_size", [0, -10])
def test_rejects_non_positive_graphql_batch_size(
    tmp_path: Path, batch_size: int
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "data_sources:\n"
        "  - name: graphql-source\n"
        "    type: dbkf\n"
        "    provider:\n"
        "      provider_type: graphql\n"
        "      endpoint: https://example.test/graphql\n"
        "      query: query Test { items { id } }\n"
        f"      batch_size: {batch_size}\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="batch_size must be greater than zero"):
        load_config(config_path)


def test_loads_discriminated_provider_config(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "data_sources:\n"
        "  - name: graphql-source\n"
        "    type: dbkf\n"
        "    provider:\n"
        "      provider_type: graphql\n"
        "      endpoint: https://example.test/graphql\n"
        "      query: query Test { items { id } }\n",
        encoding="utf-8",
    )

    provider = load_config(config_path).data_sources[0].provider

    assert isinstance(provider, GraphQLProviderConfig)
