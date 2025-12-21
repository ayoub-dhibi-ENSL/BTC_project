"""Tests for btc_graph.core.centralities module.

These tests verify the centrality computation functions: degrees, PageRank,
and triangle-based centralities. Tests require PySpark and GraphFrames.
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
        .appName("btc_graph_test_centralities")
        .config("spark.jars.packages", "io.graphframes:graphframes-spark4_2.13:0.9.3")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.graphframes.useLocalCheckpoints", "true")
        .getOrCreate()
    )


@pytest.fixture
def sample_graph(spark):
    """Create a sample graph for testing centrality computations.

    Graph structure:
        1 → 2 → 3
        ↑   ↓
        └── 4

    This creates a graph with:
    - Node 1: out=1, in=1
    - Node 2: out=2, in=1
    - Node 3: out=0, in=1
    - Node 4: out=1, in=1
    """
    from btc_graph.core.graph import GraphBuilder

    edge_data = [
        (1, 2, 100, 1.0),
        (2, 3, 200, 2.0),
        (2, 4, 150, 1.5),
        (4, 1, 120, 1.2),
    ]
    edges_df = spark.createDataFrame(
        edge_data, ["SRC_ID", "DST_ID", "VALUE_SATOSHI", "VALUE_USD"]
    )

    return GraphBuilder.from_edges(edges_df)


@pytest.fixture
def triangle_graph(spark):
    """Create a graph with triangles for testing clustering coefficients.

    Graph structure (undirected view):
        1 ─── 2
        │ ╲   │
        │   ╲ │
        4 ─── 3

    Edges (directed):
        1→2, 2→3, 3→1 (triangle)
        1→4, 4→3 (additional edges)
    """
    from btc_graph.core.graph import GraphBuilder

    edge_data = [
        (1, 2, 100, 1.0),
        (2, 3, 100, 1.0),
        (3, 1, 100, 1.0),  # Completes triangle 1-2-3
        (1, 4, 100, 1.0),
        (4, 3, 100, 1.0),
    ]
    edges_df = spark.createDataFrame(
        edge_data, ["SRC_ID", "DST_ID", "VALUE_SATOSHI", "VALUE_USD"]
    )

    return GraphBuilder.from_edges(edges_df)


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not installed")
class TestComputeDegrees:
    """Test suite for compute_degrees function."""

    def test_compute_degrees_returns_correct_columns(self, sample_graph):
        """Test that compute_degrees returns DataFrame with expected columns."""
        from btc_graph.core.centralities import compute_degrees

        degrees_df = compute_degrees(sample_graph)

        expected_cols = {"id", "inDegree", "outDegree", "degree"}
        assert set(degrees_df.columns) == expected_cols

    def test_compute_degrees_correct_values(self, sample_graph):
        """Test that degree values are computed correctly."""
        from btc_graph.core.centralities import compute_degrees

        degrees_df = compute_degrees(sample_graph)

        # Convert to dict for easier assertion
        degrees_dict = {
            row["id"]: (row["inDegree"], row["outDegree"], row["degree"])
            for row in degrees_df.collect()
        }

        # Node 1: in=1 (from 4), out=1 (to 2)
        assert degrees_dict[1] == (1.0, 1.0, 2.0)

        # Node 2: in=1 (from 1), out=2 (to 3, 4)
        assert degrees_dict[2] == (1.0, 2.0, 3.0)

        # Node 3: in=1 (from 2), out=0
        assert degrees_dict[3] == (1.0, 0.0, 1.0)

        # Node 4: in=1 (from 2), out=1 (to 1)
        assert degrees_dict[4] == (1.0, 1.0, 2.0)

    def test_compute_degrees_are_double_type(self, sample_graph):
        """Test that degrees are cast to double to prevent overflow."""
        from btc_graph.core.centralities import compute_degrees
        from pyspark.sql.types import DoubleType

        degrees_df = compute_degrees(sample_graph)

        # Check that degree columns are DoubleType
        schema_dict = {f.name: f.dataType for f in degrees_df.schema.fields}
        assert isinstance(schema_dict["inDegree"], DoubleType)
        assert isinstance(schema_dict["outDegree"], DoubleType)
        assert isinstance(schema_dict["degree"], DoubleType)

    def test_compute_degrees_includes_all_nodes(self, sample_graph):
        """Test that all nodes are included, even those with zero in/out degree."""
        from btc_graph.core.centralities import compute_degrees

        degrees_df = compute_degrees(sample_graph)

        # Should have all 4 nodes
        assert degrees_df.count() == 4


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not installed")
class TestComputePageRank:
    """Test suite for compute_pagerank function."""

    def test_compute_pagerank_returns_correct_columns(self, sample_graph):
        """Test that compute_pagerank returns DataFrame with expected columns."""
        from btc_graph.core.centralities import compute_pagerank

        pr_df = compute_pagerank(sample_graph)

        assert set(pr_df.columns) == {"id", "pagerank"}

    def test_compute_pagerank_includes_all_nodes(self, sample_graph):
        """Test that PageRank is computed for all nodes."""
        from btc_graph.core.centralities import compute_pagerank

        pr_df = compute_pagerank(sample_graph)

        assert pr_df.count() == 4

    def test_compute_pagerank_values_are_positive(self, sample_graph):
        """Test that all PageRank values are positive."""
        from btc_graph.core.centralities import compute_pagerank

        pr_df = compute_pagerank(sample_graph)

        min_pr = pr_df.agg({"pagerank": "min"}).collect()[0][0]
        assert min_pr > 0


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not installed")
class TestComputeTriangleCentralities:
    """Test suite for compute_triangle_centralities function."""

    def test_triangle_centralities_returns_correct_columns(self, triangle_graph, spark):
        """Test that compute_triangle_centralities returns expected columns."""
        from btc_graph.core.centralities import (
            compute_degrees,
            compute_triangle_centralities,
        )

        degrees_df = compute_degrees(triangle_graph)
        triangles_df = compute_triangle_centralities(triangle_graph, degrees_df)

        # The result includes all columns from degree_df (via join) plus triangle metrics
        expected_cols = {
            "id",
            "inDegree",
            "outDegree",
            "degree",
            "triangles_count",
            "triangles_max_count",
            "lcc",
        }
        assert set(triangles_df.columns) == expected_cols

    def test_triangle_centralities_with_global_metrics(self, triangle_graph):
        """Test that global metrics are returned when requested."""
        from btc_graph.core.centralities import (
            compute_degrees,
            compute_triangle_centralities,
        )

        degrees_df = compute_degrees(triangle_graph)
        result = compute_triangle_centralities(
            triangle_graph, degrees_df, return_global_metrics=True
        )

        # Should return a tuple of (triangles_df, global_metrics_df)
        assert isinstance(result, tuple)
        assert len(result) == 2

        triangles_df, global_df = result

        # Global metrics should have one row with global_cc and average_cc
        assert global_df.count() == 1
        assert set(global_df.columns) == {"global_cc", "average_cc"}

    def test_triangle_centralities_lcc_range(self, triangle_graph):
        """Test that local clustering coefficient is between 0 and 1."""
        from btc_graph.core.centralities import (
            compute_degrees,
            compute_triangle_centralities,
        )

        degrees_df = compute_degrees(triangle_graph)
        triangles_df = compute_triangle_centralities(triangle_graph, degrees_df)

        # All LCC values should be in [0, 1]
        lcc_values = [row["lcc"] for row in triangles_df.collect()]
        assert all(0 <= lcc <= 1 for lcc in lcc_values)

    def test_triangle_count_non_negative(self, triangle_graph):
        """Test that triangle counts are non-negative."""
        from btc_graph.core.centralities import (
            compute_degrees,
            compute_triangle_centralities,
        )

        degrees_df = compute_degrees(triangle_graph)
        triangles_df = compute_triangle_centralities(triangle_graph, degrees_df)

        min_count = triangles_df.agg({"triangles_count": "min"}).collect()[0][0]
        assert min_count >= 0
