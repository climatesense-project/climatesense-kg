"""Tests for pipeline orchestration."""

from logging import getLogger
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from src.climatesense_kg.pipeline import Pipeline
from src.climatesense_kg.rdf_generation.generator import RDFGenerator


def test_pipeline_fails_when_every_enabled_source_fails() -> None:
    """An empty aggregate caused by total source failure must not report success."""
    sources = [
        SimpleNamespace(name="source-a", enabled=True),
        SimpleNamespace(name="source-b", enabled=True),
    ]

    pipeline = object.__new__(Pipeline)
    pipeline.cache = None
    pipeline.config = SimpleNamespace(data_sources=sources)
    pipeline.data_manager = Mock()
    pipeline.data_manager.get_data.side_effect = RuntimeError("source unavailable")
    pipeline.logger = getLogger("test.pipeline")
    pipeline._run_datetime = None

    results = pipeline.run()

    assert results["success"] is False
    assert results["error"] == (
        "All enabled data sources failed ingestion: source-a, source-b"
    )
    assert results["data_sources"] == {
        "total_items": 0,
        "sources_processed": 0,
        "sources_failed": 2,
        "successful_sources": [],
        "failed_sources": ["source-a", "source-b"],
    }


def test_pipeline_accepts_empty_ingestion_when_any_source_succeeds() -> None:
    """A successful empty source must remain distinct from total source failure."""
    sources = [
        SimpleNamespace(name="healthy-source", enabled=True),
        SimpleNamespace(name="failed-source", enabled=True),
    ]

    def get_data(source, *, skip_download=False):
        if source.name == "failed-source":
            raise RuntimeError("source unavailable")
        return []

    pipeline = object.__new__(Pipeline)
    pipeline.cache = None
    pipeline.config = SimpleNamespace(data_sources=sources)
    pipeline.data_manager = Mock()
    pipeline.data_manager.get_data.side_effect = get_data
    pipeline.logger = getLogger("test.pipeline")
    pipeline._run_datetime = None

    results = pipeline.run()

    assert results["success"] is True
    assert results["error"] is None
    assert results["data_sources"] == {
        "total_items": 0,
        "sources_processed": 1,
        "sources_failed": 1,
        "successful_sources": ["healthy-source"],
        "failed_sources": ["failed-source"],
    }


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


def test_rdf_generation_preserves_files_when_a_later_source_fails(
    tmp_path, sample_claim_reviews, mock_cache, monkeypatch
) -> None:
    """A source failure must not discard files completed for earlier sources."""
    sample_claim_reviews[0].source_name = "source-a"
    sample_claim_reviews[1].source_name = "source-b"
    sample_claim_reviews[2].source_name = "source-c"

    generator = RDFGenerator(base_uri="https://example.org")
    save = generator.save

    def fail_second_source(claim_reviews, output_path, output_format):
        if claim_reviews[0].source_name == "source-b":
            raise ValueError("serialization failed")
        return save(claim_reviews, output_path, output_format)

    monkeypatch.setattr(generator, "save", fail_second_source)

    pipeline = object.__new__(Pipeline)
    pipeline.cache = mock_cache
    pipeline.config = SimpleNamespace(
        output=SimpleNamespace(format="turtle", output_path=tmp_path / "{SOURCE}.ttl")
    )
    pipeline.logger = getLogger("test.pipeline")
    pipeline.rdf_generator = generator
    pipeline._run_datetime = None

    stats = pipeline._run_rdf_generation(sample_claim_reviews)

    expected_files = [
        {
            "source": source_name,
            "path": str(tmp_path / f"{source_name}.ttl"),
            "items": 1,
            "failed_items": 0,
            "file_size": (tmp_path / f"{source_name}.ttl").stat().st_size,
        }
        for source_name in ("source-a", "source-c")
    ]
    assert stats["generated_files"] == expected_files
    assert stats["total_files"] == 2
    assert stats["successful_items"] == 2
    assert stats["failed_items"] == 1
    assert stats["total_file_size"] == sum(
        file_info["file_size"] for file_info in expected_files
    )
    assert stats["error"] == "source-b: serialization failed"

    cached_uris = {
        entry[0]
        for call in mock_cache.set_many.call_args_list
        for entry in call.args[0]
    }
    assert cached_uris == {
        sample_claim_reviews[0].uri,
        sample_claim_reviews[2].uri,
    }


def test_rdf_generation_rejects_shared_output_path_for_multiple_sources(
    tmp_path, sample_claim_reviews, mock_cache
) -> None:
    """Multiple sources must not be allowed to overwrite one RDF output file."""
    sample_claim_reviews[0].source_name = "source-a"
    sample_claim_reviews[1].source_name = "source-b"

    pipeline = object.__new__(Pipeline)
    pipeline.cache = mock_cache
    pipeline.config = SimpleNamespace(
        output=SimpleNamespace(format="turtle", output_path=tmp_path / "combined.ttl")
    )
    pipeline.logger = getLogger("test.pipeline")
    pipeline.rdf_generator = SimpleNamespace(save=Mock(return_value=[]))
    pipeline._run_datetime = None

    with pytest.raises(ValueError, match=r"requires the \{SOURCE\} placeholder"):
        pipeline._run_rdf_generation(sample_claim_reviews[:2])

    pipeline.rdf_generator.save.assert_not_called()
    mock_cache.set_many.assert_not_called()
    assert not (tmp_path / "combined.ttl").exists()
