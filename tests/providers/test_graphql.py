"""Tests for the GraphQL data provider."""

from unittest.mock import Mock, patch

import pytest

from climatesense_kg.config.schemas import ProviderConfig
from climatesense_kg.providers.graphql import GraphQLProvider, GraphQLResponseError


def test_partial_data_with_errors_is_not_accepted_as_success() -> None:
    response = Mock()
    response.json.return_value = {
        "data": {"items": [{"id": "partial"}]},
        "errors": [{"message": "resolver failed"}],
    }

    with (
        patch("climatesense_kg.providers.graphql.requests.post", return_value=response),
        pytest.raises(GraphQLResponseError, match="resolver failed"),
    ):
        GraphQLProvider("graphql")._make_request(
            "https://example.test/graphql",
            {"query": "query Test { items { id } }"},
            timeout=10,
            max_retries=1,
        )


@pytest.mark.parametrize(
    "response_data",
    [
        {"data": {"items": {"id": "mapping-not-list"}}},
        {"data": {"items": "string-not-list"}},
        {"data": {"items": [{"id": "valid"}, "not-a-mapping"]}},
        {"data": ["not-a-mapping"]},
    ],
)
def test_fetch_rejects_unexpected_item_collection_shapes(
    response_data: object,
) -> None:
    provider = GraphQLProvider("graphql")
    config = ProviderConfig(
        provider_type="graphql",
        endpoint="https://example.test/graphql",
        query="query Test { items { id } }",
    )

    with (
        patch.object(provider, "_make_request", return_value=response_data),
        pytest.raises(
            GraphQLResponseError, match=r"must be a mapping|list of mappings"
        ),
    ):
        provider.fetch(config)
