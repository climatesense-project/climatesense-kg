"""Tests for the curated named-graph catalog."""

from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import DCTERMS, RDF

from climatesense_kg.config import load_config
from climatesense_kg.config.graphs import (
    DBPEDIA_ENRICHER_SOURCE_NAME,
    GRAPH_CATALOG_PATH,
)

DCAT = Namespace("http://www.w3.org/ns/dcat#")
VOID = Namespace("http://rdfs.org/ns/void#")
GRAPH = Namespace("http://data.climatesense-project.eu/graph/")
SPARQL_ENDPOINT = URIRef("https://data.climatesense-project.eu/sparql")


def test_graph_catalog_describes_every_published_graph() -> None:
    catalog = Graph().parse(GRAPH_CATALOG_PATH, format="turtle")
    daily_config = load_config("config/daily.yaml")
    expected_graphs = {
        *(GRAPH[source.name] for source in daily_config.data_sources if source.enabled),
        GRAPH[DBPEDIA_ENRICHER_SOURCE_NAME],
        GRAPH.organizations,
        GRAPH.vocabularies,
    }

    assert (GRAPH.catalog, RDF.type, DCAT.Catalog) in catalog
    assert set(catalog.objects(GRAPH.catalog, DCAT.dataset)) == expected_graphs
    for graph_uri in expected_graphs:
        assert (graph_uri, RDF.type, DCAT.Dataset) in catalog
        assert catalog.value(graph_uri, DCTERMS.title) is not None
        assert catalog.value(graph_uri, DCTERMS.description) is not None
        assert catalog.value(graph_uri, DCTERMS.publisher) is not None
        assert catalog.value(graph_uri, DCTERMS.source) is not None
        assert (graph_uri, VOID.sparqlEndpoint, SPARQL_ENDPOINT) in catalog
