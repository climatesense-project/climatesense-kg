"""Command-line interface."""

import argparse
import sys
from typing import TYPE_CHECKING

from . import __version__

if TYPE_CHECKING:
    from .pipeline import (
        DeploymentResults,
        PipelineResults,
        RDFGenerationResults,
    )


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
        "--skip-enrichment",
        action="store_true",
        help=(
            "Skip running enrichers; apply cached enrichment data if available"
        ),
    )
    run_parser.add_argument(
        "--skip-deployment",
        action="store_true",
        help="Skip deployment step (e.g., Virtuoso upload)",
    )
    run_parser.add_argument(
        "--force-regenerate",
        action="store_true",
        help="Force regeneration of RDF for all items, ignoring cache",
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

    if not generated_files:
        print("RDF Generation: No files generated")
        return

    print(f"RDF Generation: {total_files} files generated ({total_size} bytes total)")

    for file_info in generated_files:
        print(
            f"  - {file_info['source']}: {file_info['path']} ({file_info['items']} items, {file_info['file_size']} bytes)"
        )


def _print_deployment_summary(deployment_data: "DeploymentResults") -> None:
    """Print deployment summary in a safe way."""
    success = deployment_data["success"]
    files_deployed = deployment_data["files_deployed"]
    total_files = deployment_data["total_files"]

    status = "Success" if success else "Failed"
    print(f"Deployment: {status} ({files_deployed}/{total_files} files)")


def _print_success_summary(results: "PipelineResults") -> None:
    """Print pipeline success summary."""
    print("Pipeline completed successfully!")
    print(f"Processed {results['total_processed']} claim reviews")

    duration = results.get("duration")
    if duration is not None:
        print(f"Duration: {duration:.2f} seconds")

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
        pipeline = Pipeline(config)
        results = pipeline.run(
            skip_download=getattr(args, "skip_download", False),
            skip_enrichment=getattr(args, "skip_enrichment", False),
            skip_deployment=getattr(args, "skip_deployment", False),
            force_regenerate=getattr(args, "force_regenerate", False),
        )
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


def main() -> int:
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    handlers = {
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
