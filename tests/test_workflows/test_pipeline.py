"""Tests for btc_graph.workflows.pipeline module.

These tests verify the SnapshotAnalysisPipeline correctly orchestrates
loading, processing, and exporting of snapshot data.
Tests require PySpark and GraphFrames; they are skipped if not installed.
"""

import tempfile
from pathlib import Path

import pytest

# Check if PySpark and GraphFrames are available
try:
    from pyspark.sql import SparkSession
    from graphframes import GraphFrame  # noqa: F401

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

if SPARK_AVAILABLE:
    from btc_graph.io import SNAPSHOT_SCHEMA
    from btc_graph.workflows.pipeline import AnalysisResult, SnapshotAnalysisPipeline


@pytest.fixture(scope="module")
def spark() -> "SparkSession":
    """Create a test Spark session for the module."""
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark or GraphFrames not installed")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("btc_graph_workflow_test")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config(
            "spark.jars.packages",
            "io.graphframes:graphframes-spark4_2.13:0.9.3",
        )
        .config("spark.graphframes.useLocalCheckpoints", "true")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def pipeline(spark: SparkSession) -> SnapshotAnalysisPipeline:
    """Create a pipeline instance for testing."""
    return SnapshotAnalysisPipeline(spark)


@pytest.fixture
def sample_df(spark: SparkSession):
    """Create a sample transaction DataFrame for testing."""
    data = [
        (1, 2, 1000000, 10.0),
        (1, 3, 2000000, 20.0),
        (2, 3, 1500000, 15.0),
        (3, 4, 500000, 5.0),
        (4, 1, 750000, 7.5),
    ]
    return spark.createDataFrame(data, schema=SNAPSHOT_SCHEMA)


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark/GraphFrames not installed")
class TestSnapshotAnalysisPipeline:
    """Tests for SnapshotAnalysisPipeline class."""

    def test_initialization(
        self,
        spark: SparkSession,
    ) -> None:
        """Pipeline should initialize with Spark session and schema."""
        pipeline = SnapshotAnalysisPipeline(spark)

        assert pipeline.spark is spark
        assert pipeline.schema == SNAPSHOT_SCHEMA

    def test_format_snapshot_id_hour_resolution(
        self,
        pipeline: SnapshotAnalysisPipeline,
    ) -> None:
        """Hour resolution should use 6-digit formatting."""
        assert pipeline.format_snapshot_id(0, "hour") == "hour-000000"
        assert pipeline.format_snapshot_id(5, "hour") == "hour-000005"
        assert pipeline.format_snapshot_id(123, "hour") == "hour-000123"

    def test_format_snapshot_id_year_resolution(
        self,
        pipeline: SnapshotAnalysisPipeline,
    ) -> None:
        """Year resolution should use 2-digit formatting."""
        assert pipeline.format_snapshot_id(0, "year") == "year-00"
        assert pipeline.format_snapshot_id(5, "year") == "year-05"
        assert pipeline.format_snapshot_id(12, "year") == "year-12"

    def test_build_graph_creates_graphframe(
        self,
        pipeline: SnapshotAnalysisPipeline,
        sample_df,
    ) -> None:
        """build_graph should convert DataFrame to GraphFrame."""
        from graphframes import GraphFrame

        graph = pipeline.build_graph(sample_df)

        assert isinstance(graph, GraphFrame)
        assert graph.vertices.count() == 4  # Nodes 1, 2, 3, 4
        assert graph.edges.count() == 5


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark/GraphFrames not installed")
class TestDiscoverSnapshots:
    """Tests for discover_snapshots method."""

    def test_discover_with_limit(
        self,
        pipeline: SnapshotAnalysisPipeline,
    ) -> None:
        """discover_snapshots should respect limit parameter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create directory structure
            edges_dir = Path(tmpdir) / "hour"
            edges_dir.mkdir(parents=True)

            # Create dummy files matching the pattern
            for i in range(5):
                (
                    edges_dir
                    / f"orbitaal-snapshot-date-2016-01-0{i}-file-id-0.snappy.parquet"
                ).touch()

            paths = pipeline.discover_snapshots(tmpdir, "hour", limit=3)

            assert len(paths) == 3

    def test_discover_returns_sorted_paths(
        self,
        pipeline: SnapshotAnalysisPipeline,
    ) -> None:
        """discover_snapshots should return sorted paths."""
        with tempfile.TemporaryDirectory() as tmpdir:
            edges_dir = Path(tmpdir) / "hour"
            edges_dir.mkdir(parents=True)

            # Create files in non-sorted order
            for date in ["03", "01", "02"]:
                (
                    edges_dir
                    / f"orbitaal-snapshot-date-2016-01-{date}-file-id-0.snappy.parquet"
                ).touch()

            paths = pipeline.discover_snapshots(tmpdir, "hour")

            # Should be sorted
            assert "01" in paths[0]
            assert "02" in paths[1]
            assert "03" in paths[2]


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark/GraphFrames not installed")
class TestAnalysisResult:
    """Tests for AnalysisResult dataclass."""

    def test_analysis_result_is_dataclass(self) -> None:
        """AnalysisResult should be a dataclass."""
        import dataclasses

        assert dataclasses.is_dataclass(AnalysisResult)
