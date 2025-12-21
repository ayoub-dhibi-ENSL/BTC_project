"""I/O helpers for btc_graph package.

This module groups loaders and simple path finders for snapshots, node tables
and existing analysis results. Implementations live in `loaders.py`.
Exporters for saving results live in `exporters.py`.
Spark session factory lives in `spark.py`.
"""

from .exporters import CSVExporter
from .loaders import (
    SNAPSHOT_SCHEMA,
    AnalysisResultsFinder,
    NodeTableFinder,
    SnapshotMetadata,
    SnapshotPathFinder,
)
from .spark import (
    DEFAULT_GRAPHFRAMES_PACKAGE,
    create_spark_session,
    create_test_spark_session,
    stop_spark_session,
)

__all__ = [
    # Loaders
    "SnapshotPathFinder",
    "SnapshotMetadata",
    "AnalysisResultsFinder",
    "NodeTableFinder",
    "SNAPSHOT_SCHEMA",
    # Exporters
    "CSVExporter",
    # Spark factory
    "create_spark_session",
    "create_test_spark_session",
    "stop_spark_session",
    "DEFAULT_GRAPHFRAMES_PACKAGE",
]
