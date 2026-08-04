"""Static regression checks for analytics query semantics."""

from pathlib import Path

_QUERY_DIR = Path(__file__).parents[2] / "services" / "analytics_api" / "queries" / "kg"


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
