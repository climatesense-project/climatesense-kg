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
        help=("Skip running enrichers; apply cached enrichment data if available"),
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

    for file_info in generated_files:
        failure_summary = ""
        if file_info["failed_items"]:
            failure_summary = f", {file_info['failed_items']} failed"
        print(
            f"  - {file_info['source']}: {file_info['path']} "
            f"({file_info['items']} items{failure_summary}, "
            f"{file_info['file_size']} bytes)"
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


def run_redeploy(args: argparse.Namespace) -> int:
    """Redeploy existing RDF files to the configured backend."""
    from .config import load_config
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

    rdf_files = sorted(
        path for pattern in ("*.nt", "*.ttl") for path in rdf_dir.rglob(pattern)
    )
    if not rdf_files:
        print(f"No supported RDF files found in {rdf_dir}", file=sys.stderr)
        return 1

    # Group files by source name (prefix before the first '_' in the filename)
    files_by_source: dict[str, list[Path]] = {}
    for f in rdf_files:
        source_name = f.stem.split("_")[0]
        files_by_source.setdefault(source_name, []).append(f)

    # Build the list of files to deploy
    files_to_deploy = [
        (source, f) for source, files in files_by_source.items() for f in files
    ]

    print(
        f"Found {len(files_to_deploy)} file(s) to deploy "
        f"across {len(files_by_source)} source(s)"
    )

    success_count = 0
    failure_count = 0
    for source_name, rdf_file in files_to_deploy:
        logger.info(f"Deploying {rdf_file} (source: {source_name})")
        graph_uri = config.deployment.graph_template.replace("{SOURCE}", source_name)
        print(f"  Deploying {rdf_file} -> {graph_uri} ...", end=" ", flush=True)
        ok = handler.deploy(rdf_file, source_name)
        if ok:
            print("OK")
            success_count += 1
        else:
            print("FAILED")
            failure_count += 1

    print(
        f"\nRedeployment complete: {success_count} succeeded, {failure_count} failed."
    )
    return 0 if failure_count == 0 else 1


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
