"""Command-line interface."""

import argparse
import faulthandler
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
        "--no-cache-extraction",
        action="store_true",
        help="Ignore stored successes and refetch/recompute all document extractions",
    )
    run_parser.add_argument(
        "--no-cache-enrichment",
        action="store_true",
        help="Ignore stored successes and re-run all enrichment",
    )

    purge_parser = subparsers.add_parser(
        "purge-processing-results",
        help="Delete recomputable extraction and enrichment results",
    )
    purge_parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirm deletion of all persisted processing results",
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
        artifacts = summary.export.published_artifacts
        print(
            f"RDF export: {len(artifacts)} files, "
            f"{summary.export.total_file_size} bytes total"
        )
        for artifact in artifacts:
            print(
                f"  - {artifact.graph_name}: {artifact.path} "
                f"({artifact.items} reviews, {artifact.failed_items} failed)"
            )


def run_pipeline(args: argparse.Namespace) -> int:
    """Run the pipeline."""
    from .config import load_config
    from .pipeline import Pipeline

    if getattr(args, "no_cache_extraction", False) and getattr(
        args, "skip_extraction", False
    ):
        print(
            "--no-cache-extraction and --skip-extraction are mutually exclusive",
            file=sys.stderr,
        )
        return 1
    if getattr(args, "no_cache_enrichment", False) and getattr(
        args, "skip_enrichment", False
    ):
        print(
            "--no-cache-enrichment and --skip-enrichment are mutually exclusive",
            file=sys.stderr,
        )
        return 1

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
                ignore_cache_extraction=getattr(args, "no_cache_extraction", False),
                ignore_cache_enrichment=getattr(args, "no_cache_enrichment", False),
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


def run_purge_processing_results(args: argparse.Namespace) -> int:
    """Delete recomputable processing state without touching identity tables."""

    if not getattr(args, "yes", False):
        print(
            "Refusing to purge processing results without --yes; identity data is "
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
        print(f"Failed to purge processing results: {exc}", file=sys.stderr)
        return 1

    print(
        f"Deleted {deleted} recomputable processing result(s); identities were "
        "preserved."
    )
    return 0


def main() -> int:
    """Main CLI entry point."""
    faulthandler.enable()
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    handlers = {
        "purge-processing-results": run_purge_processing_results,
        "run": run_pipeline,
    }

    handler = handlers.get(args.command)
    if handler:
        return handler(args)
    else:
        print(f"Unknown command: {args.command}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
