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
