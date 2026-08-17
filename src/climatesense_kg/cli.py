"""Command-line interface."""

import argparse
import logging
from pathlib import Path
import sys

from . import __version__


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
        "flush-processing-results",
        help="Delete recomputable extraction and enrichment results",
    )
    flush_parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirm deletion of all persisted processing results",
    )

    audit_parser = subparsers.add_parser(
        "audit-duplicates",
        help="Rebuild exact near-duplicate claim-review candidates",
    )
    audit_parser.add_argument(
        "--config", "-c", type=str, required=True, help="Configuration file path"
    )
    return parser


def _print_pipeline_summary(summary: object) -> None:
    """Print the compact result of one successful pipeline run."""

    from .pipeline import PipelineSummary

    if not isinstance(summary, PipelineSummary):
        raise TypeError("Expected a PipelineSummary")
    status = "with degraded coverage" if summary.degraded else "successfully"
    print(f"Pipeline completed {status} in {summary.duration_seconds:.2f}s.")
    print(f"Processed {summary.reviews} claim reviews.")
    if summary.extraction:
        stage = summary.extraction
        print(
            "Document extraction: "
            f"eligible={stage.eligible}, cached={stage.cached}, "
            f"fetched={stage.succeeded}, retryable={stage.retryable_failures}, "
            f"permanent={stage.permanent_failures}, missing={stage.missing}"
        )
    for stage in summary.enrichments:
        print(
            f"Enrichment {stage.name}: eligible={stage.eligible}, "
            f"cached={stage.cached}, succeeded={stage.succeeded}, "
            f"retryable={stage.retryable_failures}, "
            f"permanent={stage.permanent_failures}, missing={stage.missing}"
        )
    if summary.export:
        print(
            f"RDF export: {len(summary.export.artifacts)} files, "
            f"{summary.export.total_file_size} bytes total"
        )
        for artifact in summary.export.artifacts:
            print(
                f"  - {artifact.graph_name}: {artifact.path} "
                f"({artifact.items} reviews, {artifact.failed_items} failed)"
            )
    if summary.deployment:
        print(
            f"Deployment: {summary.deployment.files_deployed}/"
            f"{summary.deployment.total_files} files"
        )
        if summary.deployment.skipped_graphs:
            print(
                "Preserved incomplete graphs: "
                + ", ".join(summary.deployment.skipped_graphs)
            )


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
        for pattern in ("*.nt.gz", "*.ttl")
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
            summary = pipeline.run(
                cached_sources_only=getattr(args, "skip_download", False),
                offline_extraction=getattr(args, "skip_extraction", False),
                offline_enrichment=getattr(args, "skip_enrichment", False),
                skip_deployment=getattr(args, "skip_deployment", False),
                force=getattr(args, "force_regenerate", False),
            )
    except KeyboardInterrupt:
        print(
            "Pipeline interrupted; committed database results were preserved.",
            file=sys.stderr,
        )
        return 130
    except Exception as e:
        print(f"Pipeline execution failed: {e}", file=sys.stderr)
        return 1

    if summary.success:
        _print_pipeline_summary(summary)
        return 0
    print(f"Pipeline failed: {summary.error or 'unknown error'}", file=sys.stderr)
    return 1


def run_flush_processing_results(args: argparse.Namespace) -> int:
    """Delete recomputable processing state without touching identity tables."""

    if not getattr(args, "yes", False):
        print(
            "Refusing to flush processing results without --yes; identity data is "
            "never deleted by this command.",
            file=sys.stderr,
        )
        return 1

    from dotenv import load_dotenv

    from .database import Database
    from .enrichment import clear_processing_results

    load_dotenv()
    try:
        with Database.from_environment() as database:
            deleted = clear_processing_results(database.pool)
    except Exception as exc:
        print(f"Failed to flush processing results: {exc}", file=sys.stderr)
        return 1

    print(
        f"Deleted {deleted} recomputable processing result(s); identities were "
        "preserved."
    )
    return 0


def run_duplicate_audit(args: argparse.Namespace) -> int:
    """Rebuild bounded exact near-duplicate candidate evidence."""

    from dotenv import load_dotenv

    from .config import load_config
    from .database import Database
    from .identity import DuplicateAuditor
    from .utils.logging import setup_logging

    try:
        config = load_config(args.config)
    except Exception as exc:
        print(f"Failed to load configuration: {exc}", file=sys.stderr)
        return 1

    setup_logging(config.logging)
    load_dotenv()
    audit = config.duplicate_audit
    try:
        with Database.from_environment() as database:
            report = DuplicateAuditor(
                database.pool,
                similarity_threshold=audit.similarity_threshold,
                minimum_similarity_words=audit.minimum_similarity_words,
                group_batch_size=audit.group_batch_size,
            ).run()
    except Exception as exc:
        print(f"Duplicate audit failed: {exc}", file=sys.stderr)
        return 1

    print(
        "Duplicate audit complete: "
        f"groups={report.groups}, pairs={report.candidate_pairs}, "
        f"eligible={report.eligible_pairs}, matches={report.matches}"
    )
    return 0


def main() -> int:
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    handlers = {
        "audit-duplicates": run_duplicate_audit,
        "flush-processing-results": run_flush_processing_results,
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
