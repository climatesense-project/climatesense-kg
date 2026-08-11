"""Tests for the typed pipeline orchestration."""

from pathlib import Path
from unittest.mock import Mock, call, patch

from rdflib import Graph, URIRef
from rdflib.namespace import RDF

from climatesense_kg.config import PipelineConfig
from climatesense_kg.config.graphs import (
    DBPEDIA_ENRICHER_SOURCE_NAME,
    DBPEDIA_ENTITY_SOURCES,
)
from climatesense_kg.config.schemas import (
    DataSourceConfig,
    FileProviderConfig,
    OutputConfig,
)
from climatesense_kg.deployment import ArtifactDeployer
from climatesense_kg.domain import (
    CanonicalClaim,
    CanonicalOrganization,
    CanonicalRating,
    OrganizationReference,
    ReviewDocument,
    SourceReference,
    SourceReviewRecord,
)
from climatesense_kg.enrichers import CimpleModelEnricher, DBpediaSpotlightEnricher
from climatesense_kg.identity import IdentityResolver, InMemoryIdentityRegistry
from climatesense_kg.persistence import InMemoryStageResultStore, StageResult
from climatesense_kg.pipeline import Pipeline, PipelineDependencies
from climatesense_kg.rdf_generation import RdfArtifact, RdfArtifactBuilder, RDFGenerator
from climatesense_kg.stages import DocumentExtractor, EnrichmentRunner
from climatesense_kg.utils.text_processing import TextExtractionResult

BASE = "http://data.climatesense-project.eu"


def _config(tmp_path: Path, *source_names: str) -> PipelineConfig:
    return PipelineConfig(
        data_sources=[
            DataSourceConfig(
                name=name,
                type="claimreviewdata",
                provider=FileProviderConfig(provider_type="file", file_path="unused"),
            )
            for name in source_names
        ],
        output=OutputConfig(
            format="turtle",
            output_path=tmp_path / "{SOURCE}.ttl",
            base_uri=BASE,
        ),
    )


def _record(
    name: str,
    url: str,
    text: str,
    *,
    source_name: str = "source-a",
) -> SourceReviewRecord:
    claim_text = "A reviewed claim"
    return SourceReviewRecord(
        source=SourceReference.from_observation(
            source_name=source_name,
            source_type="claimreviewdata",
            observed_url=url,
            claim_text=claim_text,
            discriminator=name,
        ),
        claim=CanonicalClaim(text=claim_text),
        organization=OrganizationReference(
            name="Factual", website="https://factual.ro"
        ),
        document=ReviewDocument(observed_url=url, source_text=text),
    )


def _dependencies(
    data_manager: Mock,
    organization_catalog: Mock,
    tmp_path: Path,
    *,
    deployment_handler: Mock | None = None,
) -> PipelineDependencies:
    return PipelineDependencies(
        data_manager=data_manager,
        organization_catalog=organization_catalog,
        document_extractor=None,
        identity_resolver=IdentityResolver(InMemoryIdentityRegistry()),
        enrichment_runner=EnrichmentRunner([]),
        rdf_artifact_builder=RdfArtifactBuilder(
            RDFGenerator(BASE),
            output_path_template=str(tmp_path / "{SOURCE}.ttl"),
            output_format="turtle",
            enrichment_graphs={},
        ),
        artifact_deployer=ArtifactDeployer(deployment_handler),
    )


def test_pipeline_resolves_duplicate_observations_before_rdf(tmp_path: Path) -> None:
    article = " ".join(f"article-word-{index}" for index in range(80))
    records = [
        _record("first", "https://factual.ro/old", article),
        _record("second", "https://factual.ro/new", article),
    ]
    data_manager = Mock()
    data_manager.get_data.return_value = records
    catalog = Mock()
    catalog.resolve.return_value = CanonicalOrganization(
        uri=f"{BASE}/organization/factual",
        name="Factual",
        website="https://factual.ro",
    )
    pipeline = Pipeline(
        _config(tmp_path, "source-a"),
        _dependencies(data_manager, catalog, tmp_path),
    )

    results = pipeline.run(skip_deployment=True)

    assert results["success"] is True
    assert results["total_processed"] == 1
    graph = Graph().parse(tmp_path / "source-a.ttl", format="turtle")
    reviews = set(graph.subjects(RDF.type, URIRef("http://schema.org/ClaimReview")))
    assert len(reviews) == 1
    review = next(iter(reviews))
    assert set(graph.objects(review, URIRef("http://schema.org/url"))) == {
        URIRef("https://factual.ro/old"),
        URIRef("https://factual.ro/new"),
    }


def test_pipeline_reports_document_extraction_counts(tmp_path: Path) -> None:
    data_manager = Mock()
    data_manager.get_data.return_value = [
        _record(
            "record",
            "https://factual.ro/review",
            "A sufficiently detailed review body",
        )
    ]
    catalog = Mock()
    catalog.resolve.return_value = CanonicalOrganization(
        uri=f"{BASE}/organization/factual",
        name="Factual",
        website="https://factual.ro",
    )
    dependencies = _dependencies(data_manager, catalog, tmp_path)
    dependencies.document_extractor = DocumentExtractor(
        InMemoryStageResultStore(),
        rate_limit_delay=0,
        checkpoint_size=1,
        progress_interval_seconds=0,
    )
    pipeline = Pipeline(_config(tmp_path, "source-a"), dependencies)

    with patch(
        "climatesense_kg.stages.document_extractor.fetch_and_extract_text",
        return_value=TextExtractionResult(
            success=True,
            content="Fetched review content",
        ),
    ):
        results = pipeline.run(skip_deployment=True)

    assert results["success"] is True
    assert results["document_extraction"] is not None
    assert results["document_extraction"]["eligible_subjects"] == 1
    assert results["document_extraction"]["computed_successes"] == 1
    assert results["document_extraction"]["complete"] is True
    assert results["degraded"] is False


def test_skip_extraction_restores_stored_results_without_fetching(
    tmp_path: Path,
) -> None:
    records = [
        _record(
            "cached",
            "https://factual.ro/cached",
            "Source-provided cached review body",
        ),
        _record(
            "missing",
            "https://factual.ro/missing",
            "Source-provided missing review body",
        ),
    ]
    data_manager = Mock()
    data_manager.get_data.return_value = records
    catalog = Mock()
    catalog.resolve.return_value = CanonicalOrganization(
        uri=f"{BASE}/organization/factual",
        name="Factual",
        website="https://factual.ro",
    )
    dependencies = _dependencies(data_manager, catalog, tmp_path)
    store = InMemoryStageResultStore()
    extractor = DocumentExtractor(store, rate_limit_delay=0)
    store.put(
        extractor._key(records[0]),
        StageResult(success=True, payload={"content": "Stored extracted content"}),
    )
    dependencies.document_extractor = extractor
    pipeline = Pipeline(_config(tmp_path, "source-a"), dependencies)

    with patch(
        "climatesense_kg.stages.document_extractor.fetch_and_extract_text"
    ) as fetch:
        results = pipeline.run(skip_extraction=True, skip_deployment=True)

    fetch.assert_not_called()
    assert results["success"] is True
    assert results["document_extraction"] is not None
    assert results["document_extraction"]["stored_successes"] == 1
    assert results["document_extraction"]["computed_successes"] == 0
    assert results["document_extraction"]["missing_results"] == 1
    assert records[0].document.extracted_text == "Stored extracted content"
    assert records[1].document.extracted_text is None
    assert results["degraded"] is True


def test_incomplete_document_extraction_marks_pipeline_degraded(
    tmp_path: Path,
) -> None:
    data_manager = Mock()
    data_manager.get_data.return_value = [
        _record(
            "record",
            "https://factual.ro/review",
            "A sufficiently detailed review body",
        )
    ]
    catalog = Mock()
    catalog.resolve.return_value = CanonicalOrganization(
        uri=f"{BASE}/organization/factual",
        name="Factual",
        website="https://factual.ro",
    )
    dependencies = _dependencies(data_manager, catalog, tmp_path)
    dependencies.document_extractor = DocumentExtractor(
        InMemoryStageResultStore(),
        rate_limit_delay=0,
        checkpoint_size=1,
        progress_interval_seconds=0,
    )
    pipeline = Pipeline(_config(tmp_path, "source-a"), dependencies)

    with patch(
        "climatesense_kg.stages.document_extractor.fetch_and_extract_text",
        return_value=TextExtractionResult(
            success=False,
            error_message="unavailable",
        ),
    ):
        results = pipeline.run(skip_deployment=True)

    assert results["success"] is True
    assert results["document_extraction"] is not None
    assert results["document_extraction"]["complete"] is False
    assert results["degraded"] is True


def test_source_graphs_retain_source_owned_metadata(tmp_path: Path) -> None:
    article = " ".join(f"article-word-{index}" for index in range(80))
    first = _record(
        "first",
        "https://factual.ro/old",
        article,
        source_name="source-a",
    )
    first.date_published = "2026-01-01"
    first.rating = CanonicalRating(label="not_credible", original_label="Fals")
    second = _record(
        "second",
        "https://factual.ro/new",
        article,
        source_name="source-b",
    )
    second.date_published = "2026-02-02"
    second.rating = CanonicalRating(label="credible", original_label="Adevărat")
    records = {"source-a": [first], "source-b": [second]}
    data_manager = Mock()
    data_manager.get_data.side_effect = lambda source, **_kwargs: records[source.name]
    catalog = Mock()
    catalog.resolve.return_value = CanonicalOrganization(
        uri=f"{BASE}/organization/factual",
        name="Factual",
        website="https://factual.ro",
    )
    pipeline = Pipeline(
        _config(tmp_path, "source-a", "source-b"),
        _dependencies(data_manager, catalog, tmp_path),
    )

    results = pipeline.run(skip_deployment=True)

    assert results["success"] is True
    dates_by_source = {}
    ratings_by_source = {}
    for source_name in records:
        graph = Graph().parse(tmp_path / f"{source_name}.ttl", format="turtle")
        review = next(graph.subjects(RDF.type, URIRef("http://schema.org/ClaimReview")))
        dates_by_source[source_name] = str(
            next(graph.objects(review, URIRef("http://schema.org/datePublished")))
        )
        rating = next(graph.objects(review, URIRef("http://schema.org/reviewRating")))
        ratings_by_source[source_name] = str(
            next(graph.objects(rating, URIRef("http://schema.org/name")))
        )
    assert dates_by_source == {
        "source-a": "2026-01-01",
        "source-b": "2026-02-02",
    }
    assert ratings_by_source == {
        "source-a": "Fals",
        "source-b": "Adevărat",
    }


def test_full_snapshot_does_not_republish_historical_source_membership(
    tmp_path: Path,
) -> None:
    article = " ".join(f"article-word-{index}" for index in range(80))
    active_records = {
        "source-a": [
            _record(
                "first",
                "https://factual.ro/old",
                article,
                source_name="source-a",
            )
        ],
        "source-b": [
            _record(
                "second",
                "https://factual.ro/new",
                article,
                source_name="source-b",
            )
        ],
    }
    data_manager = Mock()
    data_manager.get_data.side_effect = lambda source, **_kwargs: active_records[
        source.name
    ]
    catalog = Mock()
    catalog.resolve.return_value = CanonicalOrganization(
        uri=f"{BASE}/organization/factual",
        name="Factual",
        website="https://factual.ro",
    )
    pipeline = Pipeline(
        _config(tmp_path, "source-a", "source-b"),
        _dependencies(data_manager, catalog, tmp_path),
    )

    assert pipeline.run(skip_deployment=True)["success"] is True
    active_records["source-b"] = []
    assert pipeline.run(skip_deployment=True)["success"] is True

    source_b_graph = Graph().parse(tmp_path / "source-b.ttl", format="turtle")
    assert not set(
        source_b_graph.subjects(RDF.type, URIRef("http://schema.org/ClaimReview"))
    )


def test_pipeline_reports_total_source_failure(tmp_path: Path) -> None:
    data_manager = Mock()
    data_manager.get_data.side_effect = RuntimeError("unavailable")
    pipeline = Pipeline(
        _config(tmp_path, "source-a", "source-b"),
        _dependencies(data_manager, Mock(), tmp_path),
    )

    results = pipeline.run(skip_deployment=True)

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


def test_pipeline_discards_partial_records_from_failed_source(tmp_path: Path) -> None:
    partial_record = _record(
        "partial",
        "https://factual.ro/partial",
        "Partial source content",
    )

    def fail_after_first_record():
        yield partial_record
        raise RuntimeError("truncated source payload")

    data_manager = Mock()
    data_manager.get_data.return_value = fail_after_first_record()
    pipeline = Pipeline(
        _config(tmp_path, "source-a"),
        _dependencies(data_manager, Mock(), tmp_path),
    )

    results = pipeline.run(skip_deployment=True)

    assert results["success"] is False
    data_sources = results["data_sources"]
    assert data_sources is not None
    assert data_sources["total_items"] == 0
    assert not (tmp_path / "source-a.ttl").exists()


def test_deployment_replaces_every_full_snapshot_graph(tmp_path: Path) -> None:
    handler = Mock()
    handler.deploy.return_value = True
    dependencies = _dependencies(Mock(), Mock(), tmp_path, deployment_handler=handler)
    source_path = tmp_path / "source-a.ttl"
    source_path.write_text("", encoding="utf-8")
    artifact = RdfArtifact(
        graph_name="source-a",
        kind="source",
        path=source_path,
        items=0,
        failed_items=0,
        file_size=0,
        review_uris=[],
    )

    result = dependencies.artifact_deployer.deploy([artifact])

    assert result.success is True
    assert handler.deploy.call_args_list[-1] == call(
        source_path, "source-a", replace=True
    )


def test_incomplete_enrichment_preserves_deployed_graph_and_marks_run_degraded(
    tmp_path: Path,
) -> None:
    data_manager = Mock()
    data_manager.get_data.return_value = [
        _record(
            "record",
            "https://factual.ro/review",
            "A sufficiently detailed review body",
        )
    ]
    catalog = Mock()
    catalog.resolve.return_value = CanonicalOrganization(
        uri=f"{BASE}/organization/factual",
        name="Factual",
        website="https://factual.ro",
    )
    handler = Mock()
    handler.deploy.return_value = True
    dependencies = _dependencies(
        data_manager,
        catalog,
        tmp_path,
        deployment_handler=handler,
    )
    spotlight = DBpediaSpotlightEnricher(
        target="claim",
        store=InMemoryStageResultStore(),
        rate_limit_delay=0,
    )
    dependencies.enrichment_runner = EnrichmentRunner([spotlight])
    dependencies.enrichment_graph_requirements = {
        DBPEDIA_ENRICHER_SOURCE_NAME: frozenset({spotlight.stage_name})
    }
    dependencies.rdf_artifact_builder = RdfArtifactBuilder(
        RDFGenerator(BASE),
        output_path_template=str(tmp_path / "{SOURCE}.ttl"),
        output_format="turtle",
        enrichment_graphs={
            DBPEDIA_ENRICHER_SOURCE_NAME: DBPEDIA_ENTITY_SOURCES,
        },
    )
    pipeline = Pipeline(_config(tmp_path, "source-a"), dependencies)

    with patch.object(spotlight, "is_available", return_value=False):
        results = pipeline.run()

    assert results["success"] is True
    assert results["degraded"] is True
    assert results["deployment"] is not None
    assert results["deployment"]["skipped_graphs"] == [DBPEDIA_ENRICHER_SOURCE_NAME]
    deployed_graphs = [args.args[1] for args in handler.deploy.call_args_list]
    assert "source-a" in deployed_graphs
    assert DBPEDIA_ENRICHER_SOURCE_NAME not in deployed_graphs


def test_missing_cimple_result_preserves_affected_source_graph(tmp_path: Path) -> None:
    data_manager = Mock()
    data_manager.get_data.return_value = [
        _record(
            "record",
            "https://factual.ro/review",
            "A sufficiently detailed review body",
        )
    ]
    catalog = Mock()
    catalog.resolve.return_value = CanonicalOrganization(
        uri=f"{BASE}/organization/factual",
        name="Factual",
        website="https://factual.ro",
    )
    handler = Mock()
    handler.deploy.return_value = True
    dependencies = _dependencies(
        data_manager,
        catalog,
        tmp_path,
        deployment_handler=handler,
    )
    cimple = CimpleModelEnricher(
        model="emotion",
        store=InMemoryStageResultStore(),
        rate_limit_delay=0,
    )
    dependencies.enrichment_runner = EnrichmentRunner([cimple])
    dependencies.source_graph_requirements = frozenset({cimple.stage_name})
    pipeline = Pipeline(_config(tmp_path, "source-a"), dependencies)

    with patch.object(cimple, "is_available", return_value=False):
        results = pipeline.run()

    assert results["success"] is True
    assert results["degraded"] is True
    assert results["deployment"] is not None
    assert results["deployment"]["skipped_graphs"] == ["source-a"]
    deployed_graphs = [args.args[1] for args in handler.deploy.call_args_list]
    assert "source-a" not in deployed_graphs
