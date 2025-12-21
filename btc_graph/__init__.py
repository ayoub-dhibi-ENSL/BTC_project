"""btc_graph: Bitcoin blockchain structural analysis toolkit.

This package provides tools for analysing the Bitcoin transaction network
as a graph, computing centrality measures, and tracking structural changes
over time in response to major cryptocurrency events.

Subpackages
-----------
io
    Data loading and export utilities for snapshots and analysis results.
core
    Graph construction and centrality/metric computations.
workflows
    High-level analysis pipelines for batch processing.

Quick Start
-----------
>>> from btc_graph import SnapshotPathFinder, GraphBuilder, compute_degrees
>>> from btc_graph.io import SNAPSHOT_SCHEMA, create_spark_session
>>>
>>> # Create Spark session and discover snapshots
>>> spark = create_spark_session()
>>> finder = SnapshotPathFinder(base_path="data")
>>> paths = finder.get_snapshot_paths("hour", sample=True)
>>> edges_df = finder.load_snapshot_with_spark(spark, paths[0])
>>>
>>> # Build graph and compute metrics
>>> graph = GraphBuilder.from_edges(edges_df)
>>> degrees_df = compute_degrees(graph)
>>> degrees_df.show(5)
>>>
>>> # Or use the high-level pipeline
>>> from btc_graph.workflows import SnapshotAnalysisPipeline
>>> pipeline = SnapshotAnalysisPipeline(spark)
>>> pipeline.run("data/input", "data/output", resolution="hour")
"""

# I/O utilities
from .io.exporters import CSVExporter
from .io.loaders import (
    SNAPSHOT_SCHEMA,
    AnalysisResultsFinder,
    NodeTableFinder,
    SnapshotMetadata,
    SnapshotPathFinder,
)
from .io.spark import (
    create_spark_session,
    create_test_spark_session,
    stop_spark_session,
)

# Core graph operations
from .core.graph import GraphBuilder
from .core.centralities import (
    compute_degrees,
    compute_pagerank,
    compute_triangle_centralities,
)
from .core.metrics import compute_density, compute_graph_summary

# Workflows
from .workflows.pipeline import AnalysisResult, SnapshotAnalysisPipeline

__all__ = [
    # I/O - Loaders
    "SnapshotPathFinder",
    "SnapshotMetadata",
    "AnalysisResultsFinder",
    "NodeTableFinder",
    "SNAPSHOT_SCHEMA",
    # I/O - Exporters
    "CSVExporter",
    # I/O - Spark
    "create_spark_session",
    "create_test_spark_session",
    "stop_spark_session",
    # Core - Graph
    "GraphBuilder",
    # Core - Centralities
    "compute_degrees",
    "compute_pagerank",
    "compute_triangle_centralities",
    # Core - Metrics
    "compute_density",
    "compute_graph_summary",
    # Workflows
    "SnapshotAnalysisPipeline",
    "AnalysisResult",
]
