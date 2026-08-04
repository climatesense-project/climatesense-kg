"""Tests for pipeline orchestration."""

from logging import getLogger
from types import SimpleNamespace

from src.climatesense_kg.pipeline import Pipeline
from src.climatesense_kg.rdf_generation.generator import RDFGenerator


def test_rdf_generation_only_marks_successful_reviews_processed(
    tmp_path, sample_claim_reviews, mock_cache, monkeypatch
) -> None:
    """A review that fails RDF generation must remain eligible for retry."""
    for review in sample_claim_reviews:
        review.source_name = "test-source"

    failed_review = sample_claim_reviews[1]
    generator = RDFGenerator(base_uri="https://example.org")
    generate_review = generator._generate_claim_review_rdf

    def fail_one_review(claim_review, generated_uris) -> None:
        if claim_review.uri == failed_review.uri:
            raise ValueError("malformed review")
        generate_review(claim_review, generated_uris)

    monkeypatch.setattr(generator, "_generate_claim_review_rdf", fail_one_review)

    pipeline = object.__new__(Pipeline)
    pipeline.cache = mock_cache
    pipeline.config = SimpleNamespace(
        output=SimpleNamespace(format="turtle", output_path=tmp_path / "{SOURCE}.ttl")
    )
    pipeline.logger = getLogger("test.pipeline")
    pipeline.rdf_generator = generator
    pipeline._run_datetime = None

    stats = pipeline._run_rdf_generation(sample_claim_reviews)

    cached_uris = {entry[0] for entry in mock_cache.set_many.call_args.args[0]}
    assert cached_uris == {
        review.uri for review in sample_claim_reviews if review is not failed_review
    }
    assert stats["successful_items"] == 2
    assert stats["failed_items"] == 1
    assert stats["generated_files"][0]["items"] == 2
