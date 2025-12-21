"""Tests for btc_graph.core.graph module.

These tests verify the GraphBuilder class for constructing GraphFrame objects
from edge DataFrames. Tests require PySpark and GraphFrames.
"""

import sys
from pathlib import Path

# Ensure repository root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest

# Check if PySpark and GraphFrames are available
try:
    from pyspark.sql import SparkSession
    from graphframes import GraphFrame

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


@pytest.fixture(scope="module")
def spark():
    """Create a local SparkSession for testing with GraphFrames support.

    Uses a module-scoped fixture to reuse the same session across tests,
    improving test performance.
    """
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark or GraphFrames not installed")
    return (
        SparkSession.builder.master("local[1]")
        .appName("btc_graph_test_core")
        .config("spark.jars.packages", "io.graphframes:graphframes-spark4_2.13:0.9.3")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.graphframes.useLocalCheckpoints", "true")
        .getOrCreate()
    )


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark/GraphFrames not installed")
class TestGraphBuilder:
    """Test suite for GraphBuilder class."""

    def test_from_edges_creates_valid_graphframe(self, spark):
        """Test that from_edges creates a valid GraphFrame with correct structure."""
        from btc_graph.core.graph import GraphBuilder

        # Create sample edge data matching the Bitcoin snapshot schema
        edge_data = [
            (1, 2, 100000, 1.50),
            (2, 3, 200000, 3.00),
            (3, 1, 150000, 2.25),
            (1, 4, 50000, 0.75),
        ]
        edges_df = spark.createDataFrame(
            edge_data, ["SRC_ID", "DST_ID", "VALUE_SATOSHI", "VALUE_USD"]
        )

        # Build graph
        graph = GraphBuilder.from_edges(edges_df)

        # Verify it's a GraphFrame
        assert isinstance(graph, GraphFrame)

        # Verify vertex count (unique IDs: 1, 2, 3, 4)
        assert graph.vertices.count() == 4

        # Verify edge count
        assert graph.edges.count() == 4

        # Verify vertices have 'id' column
        assert "id" in graph.vertices.columns

        # Verify edges have 'src' and 'dst' columns
        assert "src" in graph.edges.columns
        assert "dst" in graph.edges.columns

    def test_from_edges_preserves_edge_attributes(self, spark):
        """Test that edge attributes (VALUE_SATOSHI, VALUE_USD) are preserved."""
        from btc_graph.core.graph import GraphBuilder

        edge_data = [
            (10, 20, 999999, 15.50),
            (20, 30, 888888, 13.25),
        ]
        edges_df = spark.createDataFrame(
            edge_data, ["SRC_ID", "DST_ID", "VALUE_SATOSHI", "VALUE_USD"]
        )

        graph = GraphBuilder.from_edges(edges_df)

        # Check that edge attributes are preserved
        edge_cols = set(graph.edges.columns)
        assert "VALUE_SATOSHI" in edge_cols
        assert "VALUE_USD" in edge_cols

        # Verify values are correct
        first_edge = graph.edges.filter("src = 10").collect()[0]
        assert first_edge["VALUE_SATOSHI"] == 999999
        assert first_edge["VALUE_USD"] == 15.50

    def test_from_edges_custom_column_names(self, spark):
        """Test that custom source/destination column names work correctly."""
        from btc_graph.core.graph import GraphBuilder

        # Use non-standard column names
        edge_data = [(100, 200), (200, 300)]
        edges_df = spark.createDataFrame(edge_data, ["from_addr", "to_addr"])

        graph = GraphBuilder.from_edges(
            edges_df, src_col="from_addr", dst_col="to_addr"
        )

        assert graph.vertices.count() == 3  # 100, 200, 300
        assert graph.edges.count() == 2

    def test_from_edges_raises_on_missing_column(self, spark):
        """Test that ValueError is raised when required columns are missing."""
        from btc_graph.core.graph import GraphBuilder

        edge_data = [(1, 2), (2, 3)]
        edges_df = spark.createDataFrame(edge_data, ["col_a", "col_b"])

        # Should raise ValueError for missing SRC_ID column
        with pytest.raises(ValueError, match="SRC_ID"):
            GraphBuilder.from_edges(edges_df)

    def test_from_edges_handles_self_loops(self, spark):
        """Test that self-loops (node connected to itself) are handled."""
        from btc_graph.core.graph import GraphBuilder

        edge_data = [
            (1, 2, 100, 1.0),
            (2, 2, 200, 2.0),  # Self-loop
            (2, 3, 300, 3.0),
        ]
        edges_df = spark.createDataFrame(
            edge_data, ["SRC_ID", "DST_ID", "VALUE_SATOSHI", "VALUE_USD"]
        )

        graph = GraphBuilder.from_edges(edges_df)

        # Self-loop should be preserved as an edge
        assert graph.edges.count() == 3
        # Vertex 2 should appear only once in vertices
        assert graph.vertices.count() == 3

    def test_from_edges_handles_duplicate_edges(self, spark):
        """Test that duplicate edges are preserved (multi-graph support)."""
        from btc_graph.core.graph import GraphBuilder

        edge_data = [
            (1, 2, 100, 1.0),
            (1, 2, 200, 2.0),  # Duplicate edge with different values
            (2, 3, 300, 3.0),
        ]
        edges_df = spark.createDataFrame(
            edge_data, ["SRC_ID", "DST_ID", "VALUE_SATOSHI", "VALUE_USD"]
        )

        graph = GraphBuilder.from_edges(edges_df)

        # All edges should be preserved (Bitcoin can have multiple txs between same addresses)
        assert graph.edges.count() == 3
        assert graph.vertices.count() == 3
