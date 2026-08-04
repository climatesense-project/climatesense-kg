"""Tests for the GraphQL data provider."""

from unittest.mock import Mock, patch

import pytest

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
