"""Static regression checks for analytics query semantics."""

from pathlib import Path

_QUERY_DIR = Path(__file__).parents[2] / "services" / "analytics_api" / "queries" / "kg"
_PIPELINE_QUERY_DIR = _QUERY_DIR.parent / "pipeline"


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


def test_pipeline_metrics_use_versioned_stage_results() -> None:
    queries = [
        path.read_text(encoding="utf-8") for path in _PIPELINE_QUERY_DIR.glob("*.sql")
    ]

    assert len(queries) == 4
    assert all("stage_results" in query for query in queries)
    assert all("cache_entries" not in query for query in queries)
    assert all("stage_version" in query for query in queries)


def test_document_failure_query_uses_recorded_url() -> None:
    query = (_PIPELINE_QUERY_DIR / "stages_domain_failures.sql").read_text(
        encoding="utf-8"
    )

    assert "stage_name = 'document.extract'" in query
    assert "payload->>'url'" in query
