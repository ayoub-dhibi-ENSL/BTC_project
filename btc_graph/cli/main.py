"""Main CLI module for btc_graph.

This module implements the command-line interface using argparse with
subcommands for different operations (analyze, plot, info).

Design Notes
------------
- Uses subcommands pattern for extensibility (analyze, plot, info).
- Each subcommand has its own handler function.
- Supports both interactive progress output and quiet mode.
- Integrates with the SnapshotAnalysisPipeline for processing.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Optional

# Package metadata
__version__ = "0.1.0"

# Default paths (relative to working directory)
DEFAULT_DATA_DIR = "data"
DEFAULT_OUTPUT_DIR = "data"
DEFAULT_PLOTS_DIR = "plots"


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser with all subcommands.

    Returns
    -------
    argparse.ArgumentParser
        Configured argument parser.

    Examples
    --------
    >>> parser = create_parser()
    >>> args = parser.parse_args(["analyze", "--resolution", "hour"])
    >>> args.command
    'analyze'
    """
    parser = argparse.ArgumentParser(
        prog="btc-graph",
        description=(
            "Bitcoin Graph Analysis Tool\n\n"
            "Analyze the Bitcoin blockchain as a graph, computing centrality "
            "measures and tracking structural changes over time in response "
            "to major cryptocurrency events."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  btc-graph analyze --resolution hour --sample\n"
            "  btc-graph analyze -r year --input-dir data/snapshots\n"
            "  btc-graph plot --resolution hour\n"
            "  btc-graph info\n"
        ),
    )

    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    # Create subcommands
    subparsers = parser.add_subparsers(
        dest="command",
        title="commands",
        description="Available commands",
        metavar="<command>",
    )

    # === ANALYZE subcommand ===
    analyze_parser = subparsers.add_parser(
        "analyze",
        help="Process snapshot data and compute graph metrics",
        description=(
            "Load transaction snapshots, build graphs, compute centrality "
            "metrics (degrees, triangles, density), and export results to CSV."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    analyze_parser.add_argument(
        "-r",
        "--resolution",
        choices=["hour", "year"],
        default="hour",
        metavar="RES",
        help="Time resolution of snapshots (choices: hour, year) [default: hour]",
    )

    analyze_parser.add_argument(
        "-s",
        "--sample",
        action="store_true",
        help=(
            "Run on a sample subset (20 snapshots for hour, 2 for year). "
            "Useful for testing."
        ),
    )

    analyze_parser.add_argument(
        "-i",
        "--input-dir",
        type=str,
        default=None,
        metavar="DIR",
        help=(
            "Input directory containing snapshot parquet files. "
            "Default: data/orbitaal-snapshot-{resolution}/SNAPSHOT/EDGES"
        ),
    )

    analyze_parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        default=None,
        metavar="DIR",
        help=(
            "Output directory for analysis results. "
            "Default: data/snapshot-{resolution}-analysis"
        ),
    )

    analyze_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Limit processing to first N snapshots (overrides --sample)",
    )

    analyze_parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress progress output",
    )

    analyze_parser.add_argument(
        "--driver-memory",
        type=str,
        default="8g",
        metavar="MEM",
        help="Spark driver memory allocation [default: 8g]",
    )

    # === PLOT subcommand ===
    plot_parser = subparsers.add_parser(
        "plot",
        help="Generate visualizations from analysis results",
        description=(
            "Create plots from previously computed analysis results. "
            "Requires the analyze command to have been run first."
        ),
    )

    plot_parser.add_argument(
        "-r",
        "--resolution",
        choices=["hour", "year"],
        default="hour",
        metavar="RES",
        help="Time resolution of analysis data [default: hour]",
    )

    plot_parser.add_argument(
        "-i",
        "--input-dir",
        type=str,
        default=None,
        metavar="DIR",
        help="Directory containing analysis results",
    )

    plot_parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        default=DEFAULT_PLOTS_DIR,
        metavar="DIR",
        help=f"Directory for output plots [default: {DEFAULT_PLOTS_DIR}]",
    )

    # === INFO subcommand ===
    info_parser = subparsers.add_parser(
        "info",
        help="Display package and environment information",
        description="Show version info, installed dependencies, and Spark configuration.",
    )

    info_parser.add_argument(
        "--check-spark",
        action="store_true",
        help="Verify Spark and GraphFrames configuration",
    )

    return parser


def get_default_input_dir(resolution: str, base_dir: str = DEFAULT_DATA_DIR) -> str:
    """Get the default input directory for a resolution.

    Parameters
    ----------
    resolution : str
        Time resolution ("hour" or "year").
    base_dir : str
        Base data directory.

    Returns
    -------
    str
        Path to the input directory.
    """
    return f"{base_dir}/orbitaal-snapshot-{resolution}/SNAPSHOT/EDGES"


def get_default_output_dir(resolution: str, base_dir: str = DEFAULT_DATA_DIR) -> str:
    """Get the default output directory for a resolution.

    Parameters
    ----------
    resolution : str
        Time resolution ("hour" or "year").
    base_dir : str
        Base data directory.

    Returns
    -------
    str
        Path to the output directory.
    """
    return f"{base_dir}/snapshot-{resolution}-analysis"


def get_sample_limit(resolution: str) -> int:
    """Get the sample limit for a resolution.

    Parameters
    ----------
    resolution : str
        Time resolution ("hour" or "year").

    Returns
    -------
    int
        Number of snapshots to process in sample mode.
    """
    return 20 if resolution == "hour" else 2


def cmd_analyze(args: argparse.Namespace) -> int:
    """Execute the analyze command.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    int
        Exit code (0 for success, non-zero for error).
    """
    from btc_graph.io import create_spark_session, stop_spark_session
    from btc_graph.workflows import SnapshotAnalysisPipeline

    resolution = args.resolution
    input_dir = args.input_dir or get_default_input_dir(resolution)
    output_dir = args.output_dir or get_default_output_dir(resolution)

    # Determine limit
    if args.limit is not None:
        limit = args.limit
    elif args.sample:
        limit = get_sample_limit(resolution)
    else:
        limit = None

    if not args.quiet:
        print(f"Bitcoin Graph Analysis - {resolution} resolution")
        print(f"Input:  {input_dir}")
        print(f"Output: {output_dir}")
        if limit:
            print(f"Limit:  {limit} snapshots")
        print()

    try:
        # Create Spark session
        if not args.quiet:
            print("Starting Spark session...")

        spark = create_spark_session(
            app_name="btc-graph-cli",
            driver_memory=args.driver_memory,
        )

        # Create and run pipeline
        pipeline = SnapshotAnalysisPipeline(spark)

        def progress_callback(i: int, total: int, snapshot_id: str) -> None:
            if not args.quiet:
                print(f"[{i + 1}/{total}] Processed {snapshot_id}")

        if not args.quiet:
            print("Processing snapshots...")

        count = pipeline.run(
            input_dir=input_dir,
            output_dir=output_dir,
            resolution=resolution,
            limit=limit,
            progress_callback=progress_callback,
        )

        if not args.quiet:
            print(f"\nCompleted: {count} snapshots processed")
            print(f"Results saved to: {output_dir}")

        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    finally:
        stop_spark_session()


def cmd_plot(args: argparse.Namespace) -> int:
    """Execute the plot command.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    int
        Exit code (0 for success, non-zero for error).
    """
    from btc_graph.visualization import (
        plot_histogram_evolution,
        plot_kl_divergence,
        plot_metric_timeseries,
    )

    resolution = args.resolution
    input_dir = args.input_dir or get_default_output_dir(resolution)
    output_dir = args.output_dir

    print(f"Plotting - {resolution} resolution")
    print(f"Input:  {input_dir}")
    print(f"Output: {output_dir}")
    print()

    # Check if input directory exists
    if not Path(input_dir).exists():
        print(
            f"Error: Input directory not found: {input_dir}",
            file=sys.stderr,
        )
        print("Run 'btc-graph analyze' first to generate analysis results.")
        return 1

    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    try:
        metrics = ["degrees", "triangles"]

        for metric in metrics:
            print(f"Generating {metric} plots...")

            # Histogram evolution heatmap
            print("  - Evolution heatmap...")
            plot_histogram_evolution(
                input_dir,
                resolution,
                metric=metric,
                output_path=f"{output_dir}/{metric}-evolution-{resolution}_heatmap.pdf",
            )

            # KL divergence plot
            print("  - KL divergence...")
            plot_kl_divergence(
                input_dir,
                resolution,
                metric=metric,
                output_path=f"{output_dir}/{metric}-kl_divergence-{resolution}.pdf",
            )

        # Metric time series
        print("Generating time series plots...")
        plot_metric_timeseries(
            input_dir,
            resolution,
            output_path=f"{output_dir}/metrics-timeseries-{resolution}.pdf",
            metrics=["degrees"],
        )

        print(f"\nPlots saved to: {output_dir}")
        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_info(args: argparse.Namespace) -> int:
    """Execute the info command.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    int
        Exit code (0 for success, non-zero for error).
    """
    print(f"btc-graph version {__version__}")
    print()

    # Python info
    print(f"Python: {sys.version}")
    print()

    # Package versions
    print("Dependencies:")
    try:
        import pyspark

        print(f"  PySpark: {pyspark.__version__}")
    except ImportError:
        print("  PySpark: NOT INSTALLED")

    try:
        import importlib.util

        if importlib.util.find_spec("graphframes") is not None:
            print("  GraphFrames: installed")
        else:
            print("  GraphFrames: NOT INSTALLED")
    except Exception:
        print("  GraphFrames: NOT INSTALLED")

    try:
        import pandas

        print(f"  Pandas: {pandas.__version__}")
    except ImportError:
        print("  Pandas: not installed")

    print()

    # Check Spark configuration
    if args.check_spark:
        print("Spark Configuration Check:")
        try:
            from btc_graph.io import create_spark_session, stop_spark_session

            spark = create_spark_session(
                app_name="btc-graph-info",
                driver_memory="1g",
                master="local[1]",
            )
            print(f"  Spark Version: {spark.version}")
            print(f"  App Name: {spark.conf.get('spark.app.name')}")
            print(f"  Master: {spark.conf.get('spark.master')}")
            print(f"  GraphFrames JAR: {spark.conf.get('spark.jars.packages')}")
            print("  Status: OK ✓")
            stop_spark_session()
        except Exception as e:
            print("  Status: FAILED ✗")
            print(f"  Error: {e}")
            return 1

    return 0


def main(argv: Optional[List[str]] = None) -> int:
    """Main entry point for the CLI.

    Parameters
    ----------
    argv : list of str, optional
        Command-line arguments. If None, uses sys.argv.

    Returns
    -------
    int
        Exit code (0 for success, non-zero for error).

    Examples
    --------
    >>> main(["analyze", "--resolution", "hour", "--sample"])
    >>> main(["info", "--check-spark"])
    """
    parser = create_parser()
    args = parser.parse_args(argv)

    # Dispatch to subcommand handlers
    if args.command == "analyze":
        return cmd_analyze(args)
    elif args.command == "plot":
        return cmd_plot(args)
    elif args.command == "info":
        return cmd_info(args)
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())
