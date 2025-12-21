"""Snapshot analysis pipeline for Bitcoin blockchain graphs.

This module provides the SnapshotAnalysisPipeline class that orchestrates
the complete workflow of loading snapshot data, computing graph metrics,
and exporting results.

Design Notes
------------
- The pipeline follows a functional design where each step is a pure function.
- Results are encapsulated in dataclasses for type safety.
- The pipeline supports both batch processing of multiple snapshots and
  single snapshot analysis.
"""

from __future__ import annotations

import glob
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, List, Optional

from pyspark.sql import functions as F

from btc_graph.core import (
    GraphBuilder,
    compute_degrees,
    compute_density,
    compute_triangle_centralities,
)
from btc_graph.io import CSVExporter, SNAPSHOT_SCHEMA

if TYPE_CHECKING:
    from graphframes import GraphFrame
    from pyspark.sql import DataFrame, SparkSession


@dataclass
class AnalysisResult:
    """Container for snapshot analysis results.

    Attributes
    ----------
    snapshot_id : str
        Identifier for the snapshot (e.g., "hour-000001", "year-00").
    degrees_df : DataFrame
        DataFrame with node degree information (id, inDegree, outDegree, degree).
    triangles_df : DataFrame
        DataFrame with triangle centralities (id, count, clustering_coefficient).
    scalar_df : DataFrame
        DataFrame with graph-level scalar metrics (density, avg_cc, global_cc).
    graph : GraphFrame
        The GraphFrame object used for analysis.
    """

    snapshot_id: str
    degrees_df: "DataFrame"
    triangles_df: "DataFrame"
    scalar_df: "DataFrame"
    graph: "GraphFrame"


class SnapshotAnalysisPipeline:
    """Pipeline for analyzing Bitcoin transaction graph snapshots.

    This class provides a high-level interface for processing transaction
    snapshots, computing graph metrics, and exporting results. It handles
    the complete workflow from raw parquet files to structured CSV outputs.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session for data processing.
    schema : StructType, optional
        Schema for reading parquet files. Defaults to SNAPSHOT_SCHEMA.

    Examples
    --------
    >>> from btc_graph.io import create_spark_session
    >>> from btc_graph.workflows import SnapshotAnalysisPipeline
    >>>
    >>> spark = create_spark_session()
    >>> pipeline = SnapshotAnalysisPipeline(spark)
    >>>
    >>> # Process all snapshots in a directory
    >>> pipeline.run(
    ...     input_dir="data/orbitaal-snapshot-hour/SNAPSHOT/EDGES",
    ...     output_dir="data/snapshot-hour-analysis",
    ...     resolution="hour",
    ... )
    >>>
    >>> # Process a single snapshot
    >>> result = pipeline.analyze_single(
    ...     parquet_path="data/snapshot.parquet",
    ...     snapshot_id="test-001",
    ... )
    >>> print(f"Density: {result.scalar_df.collect()[0]['density']}")

    Notes
    -----
    - Resolution determines the ID formatting: "hour" uses 6 digits, "year" uses 2.
    - Output directories are created automatically.
    - Uses local checkpoints for efficient iterative graph algorithms.
    """

    def __init__(
        self,
        spark: "SparkSession",
        schema=SNAPSHOT_SCHEMA,
    ) -> None:
        """Initialize the pipeline with a Spark session.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session.
        schema : StructType, optional
            Schema for parquet files. Defaults to SNAPSHOT_SCHEMA.
        """
        self.spark = spark
        self.schema = schema

    def discover_snapshots(
        self,
        input_dir: str,
        resolution: str,
        limit: Optional[int] = None,
    ) -> List[str]:
        """Discover parquet files for a given resolution.

        Parameters
        ----------
        input_dir : str
            Base directory containing EDGES subdirectory.
        resolution : str
            Time resolution ("hour" or "year").
        limit : int, optional
            Maximum number of files to return. Useful for testing.

        Returns
        -------
        List[str]
            Sorted list of parquet file paths.

        Examples
        --------
        >>> pipeline = SnapshotAnalysisPipeline(spark)
        >>> files = pipeline.discover_snapshots(
        ...     "data/orbitaal-snapshot-hour/SNAPSHOT/EDGES",
        ...     "hour",
        ...     limit=10,
        ... )
        >>> print(f"Found {len(files)} snapshots")
        """
        pattern = (
            f"{input_dir}/{resolution}/"
            f"orbitaal-snapshot-date-*-file-id-*.snappy.parquet"
        )
        paths = sorted(glob.glob(pattern))

        if limit is not None:
            paths = paths[:limit]

        return paths

    def format_snapshot_id(self, index: int, resolution: str) -> str:
        """Format a snapshot ID based on resolution.

        Parameters
        ----------
        index : int
            Zero-based index of the snapshot.
        resolution : str
            Time resolution ("hour" or "year").

        Returns
        -------
        str
            Formatted snapshot ID like "hour-000001" or "year-00".

        Examples
        --------
        >>> pipeline.format_snapshot_id(5, "hour")
        'hour-000005'
        >>> pipeline.format_snapshot_id(5, "year")
        'year-05'
        """
        if resolution == "year":
            return f"{resolution}-{index:02d}"
        else:  # Default to hour format (6 digits)
            return f"{resolution}-{index:06d}"

    def load_snapshot(self, parquet_path: str) -> "DataFrame":
        """Load a snapshot from a parquet file.

        Parameters
        ----------
        parquet_path : str
            Path to the parquet file.

        Returns
        -------
        DataFrame
            Spark DataFrame with snapshot data.
        """
        return self.spark.read.parquet(
            parquet_path,
            schema=self.schema,
        )

    def build_graph(self, df: "DataFrame") -> "GraphFrame":
        """Convert a DataFrame to a GraphFrame.

        Parameters
        ----------
        df : DataFrame
            DataFrame with SRC_ID and DST_ID columns.

        Returns
        -------
        GraphFrame
            Graph representation of the transaction data.
        """
        return GraphBuilder.from_edges(df, src_col="SRC_ID", dst_col="DST_ID")

    def compute_metrics(
        self,
        graph: "GraphFrame",
        snapshot_id: str,
    ) -> AnalysisResult:
        """Compute all graph metrics for a snapshot.

        Parameters
        ----------
        graph : GraphFrame
            Input graph.
        snapshot_id : str
            Identifier for this snapshot.

        Returns
        -------
        AnalysisResult
            Container with all computed metrics.
        """
        # Compute degrees (in, out, total)
        degrees_df = compute_degrees(graph)

        # Compute triangle centralities (requires total degree)
        degree_only_df = degrees_df.select("id", "degree")
        triangles_df, avg_global_cc_df = compute_triangle_centralities(
            graph,
            degree_only_df,
            return_global_metrics=True,
        )

        # Compute graph density
        density = compute_density(graph)

        # Build scalar centralities DataFrame
        scalar_df = avg_global_cc_df.withColumn("density", F.lit(density))

        return AnalysisResult(
            snapshot_id=snapshot_id,
            degrees_df=degrees_df,
            triangles_df=triangles_df,
            scalar_df=scalar_df,
            graph=graph,
        )

    def analyze_single(
        self,
        parquet_path: str,
        snapshot_id: str,
    ) -> AnalysisResult:
        """Analyze a single snapshot file.

        This is the core analysis method that loads data, builds a graph,
        and computes all metrics for a single snapshot.

        Parameters
        ----------
        parquet_path : str
            Path to the parquet file.
        snapshot_id : str
            Identifier for this snapshot.

        Returns
        -------
        AnalysisResult
            Container with all computed metrics.

        Examples
        --------
        >>> result = pipeline.analyze_single(
        ...     "data/snapshot.parquet",
        ...     "test-001",
        ... )
        >>> result.degrees_df.show()
        >>> print(f"Nodes: {result.graph.vertices.count()}")
        """
        df = self.load_snapshot(parquet_path)
        graph = self.build_graph(df)
        return self.compute_metrics(graph, snapshot_id)

    def export_result(
        self,
        result: AnalysisResult,
        output_dir: str,
    ) -> None:
        """Export analysis results to CSV files.

        Creates subdirectories for degrees, triangles, and scalar metrics
        under the snapshot's output directory.

        Parameters
        ----------
        result : AnalysisResult
            Analysis results to export.
        output_dir : str
            Base output directory.

        Notes
        -----
        Creates directories like:
        - {output_dir}/{snapshot_id}/degrees/
        - {output_dir}/{snapshot_id}/triangles/
        - {output_dir}/{snapshot_id}/scalar/
        """
        base_path = Path(output_dir) / result.snapshot_id
        exporter = CSVExporter()

        # Export degrees
        exporter.save(
            result.degrees_df,
            str(base_path / "degrees"),
        )

        # Export triangle centralities
        exporter.save(
            result.triangles_df,
            str(base_path / "triangles"),
        )

        # Export scalar metrics
        exporter.save(
            result.scalar_df,
            str(base_path / "scalar"),
        )

    def analyze_batch(
        self,
        parquet_paths: List[str],
        resolution: str,
    ) -> Iterator[AnalysisResult]:
        """Analyze multiple snapshots as a generator.

        Yields results one at a time to allow streaming processing
        without holding all results in memory.

        Parameters
        ----------
        parquet_paths : List[str]
            List of parquet file paths.
        resolution : str
            Time resolution for ID formatting.

        Yields
        ------
        AnalysisResult
            Analysis results for each snapshot.

        Examples
        --------
        >>> paths = pipeline.discover_snapshots(input_dir, "hour", limit=10)
        >>> for result in pipeline.analyze_batch(paths, "hour"):
        ...     print(f"Processed {result.snapshot_id}")
        ...     pipeline.export_result(result, output_dir)
        """
        for i, parquet_path in enumerate(parquet_paths):
            snapshot_id = self.format_snapshot_id(i, resolution)
            yield self.analyze_single(parquet_path, snapshot_id)

    def run(
        self,
        input_dir: str,
        output_dir: str,
        resolution: str,
        limit: Optional[int] = None,
        progress_callback=None,
    ) -> int:
        """Run the complete analysis pipeline.

        This is the main entry point for batch processing. It discovers
        snapshots, analyzes each one, and exports results.

        Parameters
        ----------
        input_dir : str
            Directory containing snapshot parquet files.
        output_dir : str
            Directory for output CSV files.
        resolution : str
            Time resolution ("hour" or "year").
        limit : int, optional
            Maximum number of snapshots to process. Useful for testing.
        progress_callback : callable, optional
            Function called after each snapshot with (index, total, snapshot_id).

        Returns
        -------
        int
            Number of snapshots processed.

        Examples
        --------
        >>> def on_progress(i, total, sid):
        ...     print(f"[{i+1}/{total}] Processed {sid}")
        >>>
        >>> count = pipeline.run(
        ...     input_dir="data/orbitaal-snapshot-hour/SNAPSHOT/EDGES",
        ...     output_dir="data/snapshot-hour-analysis",
        ...     resolution="hour",
        ...     limit=20,  # Sample mode
        ...     progress_callback=on_progress,
        ... )
        >>> print(f"Processed {count} snapshots")
        """
        paths = self.discover_snapshots(input_dir, resolution, limit)
        total = len(paths)

        for i, result in enumerate(self.analyze_batch(paths, resolution)):
            self.export_result(result, output_dir)

            if progress_callback is not None:
                progress_callback(i, total, result.snapshot_id)

        return total
