"""Static regression checks for analytics query semantics."""

from pathlib import Path

_QUERY_DIR = Path(__file__).parents[2] / "services" / "analytics_api" / "queries" / "kg"


def test_enrichment_coverage_counts_distinct_claims() -> None:
    query = (_QUERY_DIR / "enrichment_coverage.rq").read_text(encoding="utf-8")

    assert query.count("COUNT(DISTINCT ?claim") == 8
    assert "COUNT(?" not in query
