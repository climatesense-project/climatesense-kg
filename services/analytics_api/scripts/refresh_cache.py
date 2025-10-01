#!/usr/bin/env python3
"""Script to refresh analytics API caches by calling the cache router helper.

This script should be executed inside the `analytics-api` container, for
example from the deploy script with:

    docker compose -f docker/docker-compose.yml exec -T analytics-api python /app/refresh-cache.py

The script imports `refresh_cache` from `analytics_api.routers.cache` and
invokes it while providing an AsyncSession from `analytics_api.db.session_scope`.
The output is printed as JSON.
"""

from __future__ import annotations

import asyncio
import json
import sys

from ..db import session_scope
from ..routers.cache import refresh_cache


async def main() -> int:
    try:
        async with session_scope() as session:
            result = await refresh_cache(session=session)
        output = result.model_dump()
        print(json.dumps(output))
        return 0 if output.get("success") else 1

    except Exception as exc:  # pragma: no cover - runtime wrapper
        print(json.dumps({"success": False, "message": f"Unhandled error: {exc!s}"}))
        return 2


if __name__ == "__main__":
    code = asyncio.run(main())
    sys.exit(code)
