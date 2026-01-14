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
class TestComputeMetrics:
    """Tests for compute_metrics method."""

    def test_returns_analysis_result(
        self,
        pipeline: SnapshotAnalysisPipeline,
        sample_df,
    ) -> None:
        """compute_metrics should return an AnalysisResult."""
        graph = pipeline.build_graph(sample_df)

        result = pipeline.compute_metrics(graph, "test-001")

        assert isinstance(result, AnalysisResult)
        assert result.snapshot_id == "test-001"

    def test_degrees_df_has_expected_columns(
        self,
        pipeline: SnapshotAnalysisPipeline,
        sample_df,
    ) -> None:
        """Degrees DataFrame should have id, inDegree, outDegree, degree."""
        graph = pipeline.build_graph(sample_df)

        result = pipeline.compute_metrics(graph, "test-001")

        columns = result.degrees_df.columns
        assert "id" in columns
        assert "inDegree" in columns
        assert "outDegree" in columns
        assert "degree" in columns

    def test_triangles_df_has_expected_columns(
        self,
        pipeline: SnapshotAnalysisPipeline,
        sample_df,
    ) -> None:
        """Triangles DataFrame should have id, triangles_count, lcc."""
        graph = pipeline.build_graph(sample_df)

        result = pipeline.compute_metrics(graph, "test-001")

        columns = result.triangles_df.columns
        assert "id" in columns
        assert "triangles_count" in columns
        assert "lcc" in columns

    def test_scalar_df_has_density(
        self,
        pipeline: SnapshotAnalysisPipeline,
        sample_df,
    ) -> None:
        """Scalar DataFrame should include density metric."""
        graph = pipeline.build_graph(sample_df)

        result = pipeline.compute_metrics(graph, "test-001")

        assert "density" in result.scalar_df.columns
        row = result.scalar_df.collect()[0]
        assert row["density"] > 0


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark/GraphFrames not installed")
class TestExportResult:
    """Tests for export_result method."""

    def test_creates_output_directories(
        self,
        pipeline: SnapshotAnalysisPipeline,
        sample_df,
    ) -> None:
        """export_result should create output directory structure."""
        graph = pipeline.build_graph(sample_df)
        result = pipeline.compute_metrics(graph, "test-001")

        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline.export_result(result, tmpdir)

            # Check directories were created
            snapshot_dir = Path(tmpdir) / "test-001"
            assert (snapshot_dir / "degrees").exists()
            assert (snapshot_dir / "triangles").exists()
            assert (snapshot_dir / "scalar").exists()

    def test_exports_csv_files(
        self,
        pipeline: SnapshotAnalysisPipeline,
        sample_df,
    ) -> None:
        """export_result should write CSV files to output directories."""
        graph = pipeline.build_graph(sample_df)
        result = pipeline.compute_metrics(graph, "test-002")

        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline.export_result(result, tmpdir)

            # Check CSV files exist (Spark writes partitioned files)
            degrees_dir = Path(tmpdir) / "test-002" / "degrees"
            csv_files = list(degrees_dir.glob("*.csv"))
            assert len(csv_files) > 0


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark/GraphFrames not installed")
class TestAnalyzeSingle:
    """Tests for analyze_single method."""

    def test_analyze_single_from_parquet(
        self,
        pipeline: SnapshotAnalysisPipeline,
        sample_df,
    ) -> None:
        """analyze_single should process a parquet file end-to-end."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write sample data as parquet
            parquet_path = f"{tmpdir}/test.parquet"
            sample_df.write.parquet(parquet_path)

            # Analyze
            result = pipeline.analyze_single(parquet_path, "single-test")

            assert result.snapshot_id == "single-test"
            assert result.graph.vertices.count() == 4
            assert result.degrees_df.count() == 4


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark/GraphFrames not installed")
class TestAnalyzeBatch:
    """Tests for analyze_batch method."""

    def test_batch_yields_results(
        self,
        pipeline: SnapshotAnalysisPipeline,
        sample_df,
    ) -> None:
        """analyze_batch should yield results for each file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write multiple parquet files
            paths = []
            for i in range(3):
                path = f"{tmpdir}/snapshot_{i}.parquet"
                sample_df.write.parquet(path)
                paths.append(path)

            # Analyze batch
            results = list(pipeline.analyze_batch(paths, "hour"))

            assert len(results) == 3
            assert results[0].snapshot_id == "hour-000000"
            assert results[1].snapshot_id == "hour-000001"
            assert results[2].snapshot_id == "hour-000002"


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

    def test_analysis_result_fields(
        self,
        pipeline: SnapshotAnalysisPipeline,
        sample_df,
    ) -> None:
        """AnalysisResult should have all expected fields."""
        graph = pipeline.build_graph(sample_df)
        result = pipeline.compute_metrics(graph, "result-test")

        assert hasattr(result, "snapshot_id")
        assert hasattr(result, "degrees_df")
        assert hasattr(result, "triangles_df")
        assert hasattr(result, "scalar_df")
        assert hasattr(result, "graph")

    def test_analysis_result_is_dataclass(self) -> None:
        """AnalysisResult should be a dataclass."""
        import dataclasses

        assert dataclasses.is_dataclass(AnalysisResult)
