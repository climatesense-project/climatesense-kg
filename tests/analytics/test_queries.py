"""Regression checks for analytics query semantics."""

from pathlib import Path

from rdflib import Dataset, URIRef
from rdflib.namespace import Namespace

_QUERY_DIR = Path(__file__).parents[2] / "services" / "analytics_api" / "queries" / "kg"
_PIPELINE_QUERY_DIR = _QUERY_DIR.parent / "pipeline"

_GRAPH_BASE = "http://data.climatesense-project.eu/graph/"
_SCHEMA = Namespace("http://schema.org/")


def test_enrichment_coverage_counts_distinct_claims() -> None:
    query = (_QUERY_DIR / "enrichment_coverage.rq").read_text(encoding="utf-8")

    assert query.count("COUNT(DISTINCT ?claim") == 8
    assert "COUNT(?" not in query


def test_enrichment_coverage_includes_promoted_conspiracies() -> None:
    query = (_QUERY_DIR / "enrichment_coverage.rq").read_text(encoding="utf-8")

    assert "cimple:mentionsConspiracy|cimple:promotesConspiracy" in query


def test_triple_volume_filters_graphs_before_limit() -> None:
    query = (_QUERY_DIR / "triple_volume.rq").read_text(encoding="utf-8")

    assert query.index("STRSTARTS") < query.index("LIMIT 25")
    assert "http://data.climatesense-project.eu/graph/" in query


def test_pipeline_metrics_use_current_processing_results() -> None:
    query_paths = list(_PIPELINE_QUERY_DIR.glob("*.sql"))
    queries = [path.read_text(encoding="utf-8") for path in query_paths]

    assert len(queries) == 5
    assert all("processing_results" in query for query in queries)
    assert all("stage_version" in query for query in queries)


def test_document_failure_query_uses_recorded_url() -> None:
    query = (_PIPELINE_QUERY_DIR / "stages_domain_failures.sql").read_text(
        encoding="utf-8"
    )

    assert "stage_name = 'document.extract'" in query
    assert "payload->>'url'" in query


def test_entities_query_ranks_per_graph() -> None:
    query = (_QUERY_DIR / "entities.rq").read_text(encoding="utf-8")
    dataset = Dataset()
    dbpedia = dataset.graph(URIRef(f"{_GRAPH_BASE}dbpedia-enricher"))
    wikidata = dataset.graph(URIRef(f"{_GRAPH_BASE}wikidata-enricher"))

    for rank in range(1, 27):
        for claim in range(1, rank + 1):
            dbpedia.add(
                (
                    URIRef(f"urn:c{claim}"),
                    _SCHEMA.mentions,
                    URIRef(f"urn:e{rank}"),
                )
            )
    wikidata.add((URIRef("urn:c1"), _SCHEMA.mentions, URIRef("urn:w1")))
    for claim in range(1, 10):
        wikidata.add((URIRef(f"urn:c{claim}"), _SCHEMA.mentions, URIRef("urn:e2")))

    rows = {
        (str(row.graph).removeprefix(_GRAPH_BASE), str(row.entity)): int(row.mentions)
        for row in dataset.query(query)
    }

    dbpedia_rows = {e: m for (g, e), m in rows.items() if g == "dbpedia-enricher"}
    wikidata_rows = {e: m for (g, e), m in rows.items() if g == "wikidata-enricher"}

    assert len(dbpedia_rows) == 25
    assert "urn:e1" not in dbpedia_rows
    assert dbpedia_rows["urn:e2"] == 2
    assert dbpedia_rows["urn:e26"] == 26
    assert wikidata_rows["urn:e2"] == 9
    assert wikidata_rows["urn:w1"] == 1
