"""Curated graph catalog deployment settings."""

from pathlib import Path

GRAPH_CATALOG_PATH = Path("data/graphs.ttl")

DBPEDIA_ENRICHER_SOURCE_NAME = "dbpedia-enricher"
DBPEDIA_ENTITY_SOURCES = frozenset({"dbpedia_spotlight"})

ENRICHMENT_GRAPH_ENTITY_SOURCES = {
    DBPEDIA_ENRICHER_SOURCE_NAME: DBPEDIA_ENTITY_SOURCES,
}
