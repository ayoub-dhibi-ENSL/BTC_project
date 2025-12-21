"""Tests for btc_graph.core.metrics module.

These tests verify the graph-level metric computations: density and summary.
Tests require PySpark and GraphFrames.
"""

import sys
from pathlib import Path

# Ensure repository root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest

# Check if PySpark and GraphFrames are available
try:
    from pyspark.sql import SparkSession
    from graphframes import GraphFrame  # noqa: F401

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


@pytest.fixture(scope="module")
def spark():
    """Create a local SparkSession for testing with GraphFrames support."""
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark not installed")
    return (
        SparkSession.builder.master("local[1]")
        .appName("btc_graph_test_metrics")
        .config("spark.jars.packages", "io.graphframes:graphframes-spark4_2.13:0.9.3")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.graphframes.useLocalCheckpoints", "true")
        .getOrCreate()
    )


@pytest.fixture
def sparse_graph(spark):
    """Create a sparse graph (low density) for testing.

    Graph: 1 → 2 → 3 → 4 (linear chain)
    4 vertices, 3 edges
    Max edges (directed) = 4 * 3 = 12
    Density = 3 / 12 = 0.25
    """
    from btc_graph.core.graph import GraphBuilder

    edge_data = [
        (1, 2, 100, 1.0),
        (2, 3, 100, 1.0),
        (3, 4, 100, 1.0),
    ]
    edges_df = spark.createDataFrame(
        edge_data, ["SRC_ID", "DST_ID", "VALUE_SATOSHI", "VALUE_USD"]
    )

    return GraphBuilder.from_edges(edges_df)


@pytest.fixture
def dense_graph(spark):
    """Create a dense graph (high density) for testing.

    Graph: Complete directed graph on 3 vertices
    3 vertices, 6 edges (all possible)
    Max edges (directed) = 3 * 2 = 6
    Density = 6 / 6 = 1.0
    """
    from btc_graph.core.graph import GraphBuilder

    # All possible directed edges between 3 nodes
    edge_data = [
        (1, 2, 100, 1.0),
        (1, 3, 100, 1.0),
        (2, 1, 100, 1.0),
        (2, 3, 100, 1.0),
        (3, 1, 100, 1.0),
        (3, 2, 100, 1.0),
    ]
    edges_df = spark.createDataFrame(
        edge_data, ["SRC_ID", "DST_ID", "VALUE_SATOSHI", "VALUE_USD"]
    )

    return GraphBuilder.from_edges(edges_df)


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not installed")
class TestComputeDensity:
    """Test suite for compute_density function."""

    def test_density_sparse_graph(self, sparse_graph):
        """Test density computation on a sparse linear graph."""
        from btc_graph.core.metrics import compute_density

        density = compute_density(sparse_graph)

        # 4 vertices, 3 edges, max = 12, density = 0.25
        assert abs(density - 0.25) < 1e-6

    def test_density_dense_graph(self, dense_graph):
        """Test density computation on a fully connected graph."""
        from btc_graph.core.metrics import compute_density

        density = compute_density(dense_graph)

        # Complete graph: density should be 1.0
        assert abs(density - 1.0) < 1e-6

    def test_density_returns_float(self, sparse_graph):
        """Test that density returns a float value."""
        from btc_graph.core.metrics import compute_density

        density = compute_density(sparse_graph)

        assert isinstance(density, float)

    def test_density_in_valid_range(self, sparse_graph):
        """Test that density is between 0 and 1."""
        from btc_graph.core.metrics import compute_density

        density = compute_density(sparse_graph)

        assert 0.0 <= density <= 1.0


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not installed")
class TestComputeGraphSummary:
    """Test suite for compute_graph_summary function."""

    def test_summary_returns_dict(self, sparse_graph):
        """Test that graph summary returns a dictionary."""
        from btc_graph.core.metrics import compute_graph_summary

        summary = compute_graph_summary(sparse_graph)

        assert isinstance(summary, dict)

    def test_summary_contains_expected_keys(self, sparse_graph):
        """Test that summary contains all expected metric keys."""
        from btc_graph.core.metrics import compute_graph_summary

        summary = compute_graph_summary(sparse_graph)

        expected_keys = {"num_vertices", "num_edges", "density", "avg_degree"}
        assert set(summary.keys()) == expected_keys

    def test_summary_vertex_count(self, sparse_graph):
        """Test that vertex count in summary is correct."""
        from btc_graph.core.metrics import compute_graph_summary

        summary = compute_graph_summary(sparse_graph)

        assert summary["num_vertices"] == 4.0

    def test_summary_edge_count(self, sparse_graph):
        """Test that edge count in summary is correct."""
        from btc_graph.core.metrics import compute_graph_summary

        summary = compute_graph_summary(sparse_graph)

        assert summary["num_edges"] == 3.0

    def test_summary_density_matches_compute_density(self, sparse_graph):
        """Test that summary density matches standalone compute_density."""
        from btc_graph.core.metrics import compute_density, compute_graph_summary

        direct_density = compute_density(sparse_graph)
        summary = compute_graph_summary(sparse_graph)

        assert abs(summary["density"] - direct_density) < 1e-6

    def test_summary_avg_degree(self, sparse_graph):
        """Test that average degree is computed correctly."""
        from btc_graph.core.metrics import compute_graph_summary

        summary = compute_graph_summary(sparse_graph)

        # avg_degree = edges / vertices = 3 / 4 = 0.75
        assert abs(summary["avg_degree"] - 0.75) < 1e-6

    def test_summary_values_are_floats(self, sparse_graph):
        """Test that all summary values are floats."""
        from btc_graph.core.metrics import compute_graph_summary

        summary = compute_graph_summary(sparse_graph)

        for key, value in summary.items():
            assert isinstance(value, float), f"{key} should be float, got {type(value)}"
