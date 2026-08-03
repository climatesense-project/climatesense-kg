"""Configure ClimateSense as the default QLever UI backend."""

import os

from backend.models import Backend

backend, _ = Backend.objects.update_or_create(
    slug="climatesense",
    defaults={
        "name": "ClimateSense",
        "baseUrl": os.environ["QLEVER_UI_BACKEND_URL"],
        "isDefault": True,
        "isNoSlugMode": True,
        "dynamicSuggestions": 0,
    },
)
Backend.objects.exclude(pk=backend.pk).update(isDefault=False)
