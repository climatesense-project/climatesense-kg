"""Authoritative schema contract tests."""

from importlib.resources import files


def test_pipeline_uses_one_fresh_schema() -> None:
    root = files("climatesense_kg.persistence.migrations")
    migrations = sorted(
        item.name for item in root.iterdir() if item.name.endswith(".sql")
    )

    assert migrations == ["0001_schema.sql"]


def test_schema_contains_one_authoritative_processing_model() -> None:
    schema = (
        files("climatesense_kg.persistence.migrations")
        .joinpath("0001_schema.sql")
        .read_text(encoding="utf-8")
    )

    for table in (
        "pipeline_runs",
        "source_observations",
        "document_extractions",
        "documents",
        "document_urls",
        "document_text_hashes",
        "claim_reviews",
        "enrichment_results",
        "duplicate_candidates",
    ):
        assert f"CREATE TABLE {table}" in schema
