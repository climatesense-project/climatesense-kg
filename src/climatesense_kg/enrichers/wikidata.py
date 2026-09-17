"""Wikidata URI helpers shared by Wikidata entity-linking enrichers."""

from __future__ import annotations

WIKIDATA_ENTITY_URI_PREFIX = "http://www.wikidata.org/entity/"


def wikidata_entity_uri(qid: str) -> str:
    """Return the canonical entity URI for one Wikidata QID."""

    return f"{WIKIDATA_ENTITY_URI_PREFIX}{qid}"
