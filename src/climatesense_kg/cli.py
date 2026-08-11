"""Command-line interface."""

import argparse
import logging
from pathlib import Path
import sys
from typing import TYPE_CHECKING

from . import __version__

if TYPE_CHECKING:
    from .pipeline import (
        DeploymentResults,
        PipelineResults,
        RDFGenerationResults,
    )
    from .stages import StageExecutionSummary


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        description="ClimateSense Knowledge Graph Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  climatesense-kg run --config config/minimal.yaml --debug
        """,
    )

    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    run_parser = subparsers.add_parser("run", help="Run the pipeline")
    run_parser.add_argument("--config", "-c", type=str, help="Configuration file path")
    run_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable DEBUG level logging",
    )
    run_parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip data downloads and use only cached/already downloaded data",
    )
    run_parser.add_argument(
        "--skip-extraction",
        action="store_true",
        help="Skip external document fetches; apply stored successful results",
    )
    run_parser.add_argument(
        "--skip-enrichment",
        action="store_true",
        help=("Skip external enrichment calls; apply stored successful results"),
    )
    run_parser.add_argument(
        "--skip-deployment",
        action="store_true",
        help="Skip deployment step (e.g., Virtuoso upload)",
    )
    run_parser.add_argument(
        "--force-regenerate",
        action="store_true",
        help="Recompute stage results instead of restoring stored successes",
    )

    redeploy_parser = subparsers.add_parser(
        "redeploy",
        help="Redeploy existing RDF files without re-running the pipeline",
    )
    redeploy_parser.add_argument(
        "--config", "-c", type=str, required=True, help="Configuration file path"
    )
    redeploy_parser.add_argument(
        "--rdf-dir",
        type=str,
        default="data/rdf",
        help="Directory to scan for RDF files (default: data/rdf)",
    )
    redeploy_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable DEBUG level logging",
    )

    flush_parser = subparsers.add_parser(
        "flush-stage-results",
        help="Delete recomputable stage results while preserving identities",
    )
    flush_parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirm deletion of all persisted stage results",
    )
    return parser


def _print_rdf_generation_summary(rdf_data: "RDFGenerationResults") -> None:
    """Print RDF generation summary in a safe way."""
    if rdf_data.get("error"):
        print(f"RDF Generation: Failed - {rdf_data['error']}")
        return

    generated_files = rdf_data.get("generated_files", [])
    total_files = rdf_data.get("total_files", 0)
    total_size = rdf_data.get("total_file_size", 0)
    failed_items = rdf_data.get("failed_items", 0)

    if not generated_files:
        print("RDF Generation: No files generated")
        return

    print(f"RDF Generation: {total_files} files generated ({total_size} bytes total)")

    if failed_items:
        print(f"RDF Generation: {failed_items} items failed")

    for warning in rdf_data.get("warnings", []):
        print(f"RDF Generation warning: {warning}")

    for file_info in generated_files:
        failure_summary = ""
        if file_info["failed_items"]:
            failure_summary = f", {file_info['failed_items']} failed"
        print(
            f"  - {file_info['graph_name']}: {file_info['path']} "
            f"({file_info['items']} items{failure_summary}, "
            f"{file_info['file_size']} bytes)"
        )


def _print_deployment_summary(deployment_data: "DeploymentResults") -> None:
    """Print deployment summary in a safe way."""
    success = deployment_data["success"]
    files_deployed = deployment_data["files_deployed"]
    total_files = deployment_data["total_files"]
    skipped_files = deployment_data["skipped_files"]

    status = "Success" if success else "Failed"
    print(f"Deployment: {status} ({files_deployed}/{total_files} files)")
    if skipped_files:
        skipped_graphs = ", ".join(deployment_data["skipped_graphs"])
        print(
            f"Deployment: preserved {skipped_files} incomplete graph(s): "
            f"{skipped_graphs}"
        )


def _format_stage_counts(
    stage: "StageExecutionSummary",
    *,
    success_label: str = "computed_successes",
    failure_label: str = "computed_failures",
) -> str:
    """Format the counters shared by persisted-stage summaries."""

    return (
        f"eligible={stage['eligible_subjects']}, "
        f"stored_successes={stage['stored_successes']}, "
        f"stored_failures={stage['stored_failures']}, "
        f"{success_label}={stage['computed_successes']}, "
        f"{failure_label}={stage['computed_failures']}, "
        f"missing={stage['missing_results']}"
    )


def _print_enrichment_summary(results: "PipelineResults") -> None:
    enrichment = results.get("enrichment")
    if not enrichment:
        return
    status = "complete" if enrichment["complete"] else "incomplete"
    print(f"Enrichment: {status}")
    for stage in enrichment["stages"]:
        availability = stage["available"]
        availability_text = (
            "not checked" if availability is None else str(availability).lower()
        )
        print(
            f"  - {stage['stage_name']}: available={availability_text}, "
            f"{_format_stage_counts(stage)}"
        )


def _print_document_extraction_summary(results: "PipelineResults") -> None:
    extraction = results.get("document_extraction")
    if extraction is None:
        return
    status = "complete" if extraction["complete"] else "incomplete"
    print(
        f"Document extraction: {status}; "
        f"{_format_stage_counts(extraction, success_label='fetched', failure_label='failed')}"
    )


def _print_success_summary(results: "PipelineResults") -> None:
    """Print pipeline success summary."""
    if results["degraded"]:
        print("Pipeline completed with degraded coverage.")
    else:
        print("Pipeline completed successfully!")
    print(f"Processed {results['total_processed']} claim reviews")

    duration = results.get("duration")
    if duration is not None:
        print(f"Duration: {duration:.2f} seconds")

    _print_document_extraction_summary(results)
    _print_enrichment_summary(results)

    # Print RDF generation summary
    rdf_data = results.get("rdf_generation")
    if rdf_data:
        _print_rdf_generation_summary(rdf_data)

    # Print deployment summary
    deployment_data = results.get("deployment")
    if deployment_data:
        _print_deployment_summary(deployment_data)


def _print_failure_summary(results: "PipelineResults") -> None:
    """Print pipeline failure summary."""
    print("Pipeline failed:", file=sys.stderr)

    error = results.get("error")
    if error:
        print(f"Error: {error}", file=sys.stderr)


def _graph_name_for_rdf_file(
    rdf_file: Path, managed_graph_names: set[str]
) -> str | None:
    """Match an RDF artifact to a managed graph without splitting its name."""
    matching_graphs = [
        graph_name
        for graph_name in managed_graph_names
        if rdf_file.stem == graph_name or rdf_file.stem.startswith(f"{graph_name}_")
    ]
    if matching_graphs:
        return max(matching_graphs, key=len)
    return None


def run_redeploy(args: argparse.Namespace) -> int:
    """Redeploy existing RDF files to the configured backend."""
    from .config import load_config
    from .config.graphs import (
        ENRICHMENT_GRAPH_SOURCE_NAMES,
        GRAPH_CATALOG_PATH,
    )
    from .config.organizations import ORGANIZATION_CATALOG_PATH
    from .deployment import ArtifactDeployer
    from .deployment.factory import create_deployment_handler
    from .utils.logging import setup_logging

    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"Failed to load configuration: {e}", file=sys.stderr)
        return 1

    if getattr(args, "debug", False):
        config.logging.level = "DEBUG"

    setup_logging(config.logging)
    logger = logging.getLogger(__name__)

    try:
        handler = create_deployment_handler(config.deployment)
    except Exception as e:
        print(f"Failed to initialize deployment handler: {e}", file=sys.stderr)
        return 1

    if handler is None:
        print("No deployment backend is configured.", file=sys.stderr)
        return 1

    rdf_dir = Path(args.rdf_dir)
    if not rdf_dir.exists():
        print(f"RDF directory not found: {rdf_dir}", file=sys.stderr)
        return 1

    curated_paths = {
        GRAPH_CATALOG_PATH.resolve(),
        ORGANIZATION_CATALOG_PATH.resolve(),
    }
    rdf_files = sorted(
        path
        for pattern in ("*.nt", "*.ttl")
        for path in rdf_dir.rglob(pattern)
        if path.resolve() not in curated_paths
    )
    if not rdf_files:
        print(f"No supported RDF files found in {rdf_dir}", file=sys.stderr)
        return 1

    managed_graph_names = {
        *(source.name for source in config.data_sources),
        *ENRICHMENT_GRAPH_SOURCE_NAMES,
    }

    # Group files by their longest matching managed graph name.
    files_by_graph: dict[str, list[Path]] = {}
    for f in rdf_files:
        graph_name = _graph_name_for_rdf_file(f, managed_graph_names)
        if graph_name is None:
            print(
                f"Could not determine a managed graph for RDF file: {f}",
                file=sys.stderr,
            )
            return 1
        files_by_graph.setdefault(graph_name, []).append(f)

    multi_file_graphs = {
        graph_name: files
        for graph_name, files in files_by_graph.items()
        if len(files) != 1
    }
    if multi_file_graphs:
        graph_names = ", ".join(sorted(multi_file_graphs))
        print(
            "Redeployment requires exactly one full-snapshot RDF file per graph; "
            f"found multiple files for: {graph_names}",
            file=sys.stderr,
        )
        return 1

    # Build the list of files to deploy
    files_to_deploy = sorted(
        ((graph, f) for graph, files in files_by_graph.items() for f in files),
        key=lambda item: (
            item[0] not in ENRICHMENT_GRAPH_SOURCE_NAMES,
            item[0],
            item[1],
        ),
    )

    print(
        f"Found {len(files_to_deploy)} generated file(s) to deploy "
        f"across {len(files_by_graph)} graph(s), plus the graph and organization catalogs"
    )

    report = ArtifactDeployer(handler).deploy_files(
        [(rdf_file, graph_name) for graph_name, rdf_file in files_to_deploy]
    )
    for outcome in report.outcomes:
        target = outcome.target
        logger.info("Deployed %s (graph: %s)", target.path, target.graph_name)
        graph_uri = config.deployment.graph_template.replace(
            "{SOURCE}", target.graph_name
        )
        status = "OK" if outcome.success else "FAILED"
        print(f"  Replacing {target.path} -> {graph_uri} ... {status}")

    print(
        f"\nRedeployment complete: {report.files_deployed} succeeded, "
        f"{report.total_files - report.files_deployed} failed."
    )
    return 0 if report.success else 1


def run_pipeline(args: argparse.Namespace) -> int:
    """Run the pipeline."""
    from .config import load_config
    from .pipeline import Pipeline

    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"Failed to load configuration: {e}", file=sys.stderr)
        return 1

    if getattr(args, "debug", False):
        config.logging.level = "DEBUG"

    try:
        with Pipeline(config) as pipeline:
            results = pipeline.run(
                skip_download=getattr(args, "skip_download", False),
                skip_extraction=getattr(args, "skip_extraction", False),
                skip_enrichment=getattr(args, "skip_enrichment", False),
                skip_deployment=getattr(args, "skip_deployment", False),
                force_regenerate=getattr(args, "force_regenerate", False),
            )
    except KeyboardInterrupt:
        print(
            "Pipeline interrupted; completed document-extraction checkpoints "
            "were preserved.",
            file=sys.stderr,
        )
        return 130
    except Exception as e:
        print(f"Pipeline execution failed: {e}", file=sys.stderr)
        return 1

    success = results["success"]

    if success:
        _print_success_summary(results)
        return 0
    else:
        _print_failure_summary(results)
        return 1


def run_flush_stage_results(args: argparse.Namespace) -> int:
    """Delete recomputable stage state without touching identity tables."""

    if not getattr(args, "yes", False):
        print(
            "Refusing to flush stage results without --yes; identity data is never "
            "deleted by this command.",
            file=sys.stderr,
        )
        return 1

    from dotenv import load_dotenv

    from .persistence import PostgresDatabase, PostgresStageResultStore

    load_dotenv()
    try:
        with PostgresDatabase.from_environment() as database:
            deleted = PostgresStageResultStore(database.pool).clear()
    except Exception as exc:
        print(f"Failed to flush stage results: {exc}", file=sys.stderr)
        return 1

    print(f"Deleted {deleted} recomputable stage result(s); identities were preserved.")
    return 0


def main() -> int:
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    handlers = {
        "flush-stage-results": run_flush_stage_results,
        "run": run_pipeline,
        "redeploy": run_redeploy,
    }

    handler = handlers.get(args.command)
    if handler:
        return handler(args)
    else:
        print(f"Unknown command: {args.command}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
