"""GraphQL provider."""

import json
import time
from typing import Any, cast

import requests

from ..config.schemas import ProviderConfig
from .base import BaseProvider


class GraphQLProvider(BaseProvider):
    """Provider for fetching data from GraphQL APIs."""

    def fetch(self, config: ProviderConfig) -> bytes:
        """Fetch data from GraphQL endpoint.

        Args:
            config: Must contain 'endpoint', 'query', optionally 'variables', 'batch_size', etc.

        Returns:
            All fetched data as JSON bytes
        """
        endpoint = config.endpoint
        if not endpoint:
            raise ValueError("GraphQLProvider requires 'endpoint' in config")

        query = config.query
        if not query:
            raise ValueError("GraphQLProvider requires 'query' in config")

        variables = config.variables
        batch_size = config.batch_size
        rate_limit_delay = config.rate_limit_delay
        max_retries = config.max_retries
        timeout = config.timeout

        self.logger.info(f"Fetching data from GraphQL endpoint: {endpoint}")

        all_items: list[dict[str, Any]] = []
        offset = 0

        while True:
            # Update variables with current offset and limit
            current_variables: dict[str, Any] = {
                **variables,
                "offset": offset,
                "limit": batch_size,
            }

            payload: dict[str, Any] = {"query": query, "variables": current_variables}

            self.logger.info(f"GraphQL request: offset={offset}, limit={batch_size}")
            response_data = self._make_request(endpoint, payload, timeout, max_retries)

            if not response_data or "data" not in response_data:
                self.logger.error("Invalid GraphQL response structure")
                break

            data = response_data["data"]
            if not data:
                break

            items = next(
                (
                    field_value
                    for field_value in data.values()
                    if field_value is not None
                ),
                None,
            )

            if not items:
                self.logger.info("No more data to fetch")
                break

            all_items.extend(items)
            self.logger.info(f"Fetched {len(items)} items (total: {len(all_items)})")

            # If we got fewer items than requested, we've reached the end
            if len(items) < batch_size:
                break

            offset += batch_size

            if rate_limit_delay > 0:
                time.sleep(rate_limit_delay)

        self.logger.info(f"Total fetched: {len(all_items)} items")

        return json.dumps(all_items, ensure_ascii=False).encode("utf-8")

    def _make_request(
        self, endpoint: str, payload: dict[str, Any], timeout: int, max_retries: int
    ) -> dict[str, Any] | None:
        """Make GraphQL request with retry logic."""
        headers = {
            "Content-Type": "application/json",
        }

        for attempt in range(max_retries):
            try:
                response = requests.post(
                    endpoint, json=payload, headers=headers, timeout=timeout
                )
                response.raise_for_status()
                result = response.json()
                if isinstance(result, dict):
                    return cast(dict[str, Any], result)
                return None

            except requests.RequestException as e:
                self.logger.warning(
                    f"GraphQL request attempt {attempt + 1} failed: {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
                else:
                    self.logger.error(
                        f"All {max_retries} GraphQL request attempts failed"
                    )
                    raise

        return None

    def get_cache_key_fields(self, config: ProviderConfig) -> dict[str, Any]:
        """Endpoint, query and variables affect cache."""
        return {
            "endpoint": config.endpoint,
            "query": config.query,
            "variables": config.variables,
        }
