"""Behavioral tests for the small pipeline coordinator."""

from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

from climatesense_kg.bootstrap import PipelineServices
from climatesense_kg.config.schemas import (
    OutputConfig,
    PipelineConfig,
    SnapshotRetentionConfig,
)
from climatesense_kg.database import PipelineRun
from climatesense_kg.export import ExportSummary, RdfArtifact
from climatesense_kg.identity import IdentitySummary
from climatesense_kg.ingestion import IngestionSummary
from climatesense_kg.pipeline import Pipeline
from climatesense_kg.processing import StageSummary


def _services(tmp_path: Path) -> tuple[PipelineServices, dict[str, Mock]]:
    database = Mock()
    database.start_run.return_value = PipelineRun(uuid4())
    ingestion = Mock()
    ingestion.run.return_value = IngestionSummary(2, ("source",), ())
    extraction = Mock()
    extraction.run.return_value = StageSummary("document.extract", eligible=2, cached=2)
    identity = Mock()
    identity.run.return_value = IdentitySummary(2, 2, 2)
    enrichment = Mock()
    enrichment.run.return_value = [StageSummary("cimple.emotion", eligible=2, cached=2)]
    exporter = Mock()
    artifact = RdfArtifact("source", "source", tmp_path / "source.nt.gz", 2, 0, 10)
    exporter.run.return_value = ExportSummary((artifact,), 2, 2, 0, 10, ())
    services = PipelineServices(
        database=database,
        ingestion=ingestion,
        extraction=extraction,
        identity=identity,
        enrichment=enrichment,
        exporter=exporter,
        source_enrichments=frozenset({"cimple.emotion"}),
        graph_enrichments={},
    )
    return services, {
        "database": database,
        "ingestion": ingestion,
        "extraction": extraction,
        "identity": identity,
        "enrichment": enrichment,
        "exporter": exporter,
    }


def test_pipeline_runs_single_ordered_service_path(tmp_path: Path) -> None:
    services, mocks = _services(tmp_path)
    pipeline = Pipeline(PipelineConfig(), services)

    result = pipeline.run()

    assert result.success
    assert result.reviews == 2
    mocks["ingestion"].run.assert_called_once()
    mocks["extraction"].run.assert_called_once()
    mocks["identity"].run.assert_called_once_with()
    mocks["enrichment"].run.assert_called_once()
    mocks["exporter"].run.assert_called_once()
    mocks["database"].finish_run.assert_called_once()
    assert mocks["database"].finish_run.call_args.kwargs["status"] == "complete"
    summary = mocks["database"].finish_run.call_args.kwargs["summary"]
    assert summary["reviews"] == 2
    assert summary["extraction"]["name"] == "document.extract"
    assert summary["enrichments"][0]["name"] == "cimple.emotion"


def test_failed_ingestion_stops_before_processing(tmp_path: Path) -> None:
    services, mocks = _services(tmp_path)
    mocks["ingestion"].run.return_value = IngestionSummary(0, (), ("source",))

    result = Pipeline(PipelineConfig(), services).run()

    assert not result.success
    assert result.error == "Every enabled source failed ingestion"
    mocks["extraction"].run.assert_not_called()
    mocks["identity"].run.assert_not_called()
    assert mocks["database"].finish_run.call_args.kwargs["status"] == "failed"


def test_incomplete_source_enrichment_prevents_graph_deployment(
    tmp_path: Path,
) -> None:
    services, mocks = _services(tmp_path)
    mocks["enrichment"].run.return_value = [
        StageSummary("cimple.emotion", eligible=2, cached=1, missing=1)
    ]

    result = Pipeline(PipelineConfig(), services).run()

    assert result.success
    assert result.degraded
    incomplete = mocks["exporter"].run.call_args.kwargs["incomplete_stages_by_graph"]
    assert incomplete == {"source": {"cimple.emotion"}}


def _retention_config(tmp_path: Path, keep_latest: int) -> PipelineConfig:
    return PipelineConfig(
        output=OutputConfig(
            output_path=str(tmp_path / "{DATETIME}" / "{SOURCE}.nt.gz"),
            retention=SnapshotRetentionConfig(keep_latest=keep_latest),
        )
    )


def _seed_snapshot_dirs(tmp_path: Path) -> list[Path]:
    dirs = []
    for name in ("2026-08-14_000000", "2026-08-15_000000"):
        snapshot = tmp_path / name
        snapshot.mkdir()
        (snapshot / "source.nt.gz").write_bytes(b"<s> <p> <o> .\n")
        dirs.append(snapshot)
    return dirs


def test_pipeline_prunes_old_snapshots_after_export(tmp_path: Path) -> None:
    services, _mocks = _services(tmp_path)
    dirs = _seed_snapshot_dirs(tmp_path)

    result = Pipeline(_retention_config(tmp_path, keep_latest=1), services).run()

    assert result.success
    assert not dirs[0].exists()
    assert dirs[1].is_dir()


def test_pipeline_keeps_snapshots_when_retention_disabled(tmp_path: Path) -> None:
    services, _mocks = _services(tmp_path)
    dirs = _seed_snapshot_dirs(tmp_path)

    result = Pipeline(_retention_config(tmp_path, keep_latest=0), services).run()

    assert result.success
    assert all(path.is_dir() for path in dirs)
