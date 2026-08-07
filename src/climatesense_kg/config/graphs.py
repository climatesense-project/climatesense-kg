"""Curated graph catalog deployment settings."""

from pathlib import Path

GRAPH_CATALOG_PATH = Path("data/graphs.ttl")
GRAPH_CATALOG_SOURCE_NAME = "catalog"

DBPEDIA_ENRICHER_SOURCE_NAME = "dbpedia-enricher"
DBPEDIA_ENTITY_SOURCES = frozenset({"dbpedia_spotlight"})

ENRICHMENT_GRAPH_ENTITY_SOURCES = {
    DBPEDIA_ENRICHER_SOURCE_NAME: DBPEDIA_ENTITY_SOURCES,
}
ENRICHMENT_GRAPH_SOURCE_NAMES = frozenset(ENRICHMENT_GRAPH_ENTITY_SOURCES)
