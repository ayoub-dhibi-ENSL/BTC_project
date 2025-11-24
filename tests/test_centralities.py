import sys
from pathlib import Path

# Add src directory to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from graphframes import GraphFrame
from centralities import get_degrees, get_triangle_centralities, get_density


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing with GraphFrames support."""
    return (
        SparkSession.builder.appName("test")
        .master("local")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.jars.packages", "io.graphframes:graphframes-spark4_2.13:0.9.3")
        .config("spark.graphframes.useLocalCheckpoints", "true")
        .getOrCreate()
    )


@pytest.fixture
def sample_graph(spark):
    """Create a simple sample graph for testing."""
    # Create vertices: [0, 1, 2, 3]
    vertices_data = [(0,), (1,), (2,)]
    vertices_schema = StructType([StructField("id", IntegerType(), True)])
    vertices = spark.createDataFrame(vertices_data, schema=vertices_schema)

    # Create edges: 0->1, 1->2, 2->3, 0->2 (4 edges total)
    edges_data = [(0, 1), (1, 2)]
    edges_schema = StructType(
        [
            StructField("src", IntegerType(), True),
            StructField("dst", IntegerType(), True),
        ]
    )
    edges = spark.createDataFrame(edges_data, schema=edges_schema)

    return GraphFrame(vertices, edges)


class TestGetDegrees:
    """Test suite for get_degrees function."""

    def test_get_degrees_returns_result(self, sample_graph):
        """Test that get_degrees returns a result."""
        result = get_degrees(sample_graph)
        assert result is not None

    def test_get_degrees_has_columns(self, sample_graph):
        """Test that result has required columns."""
        result = get_degrees(sample_graph)
        assert "id" in result.columns
        assert "degree" in result.columns


class TestGetTriangleCentralities:
    """Test suite for get_triangle_centralities function."""

    def test_get_triangle_centralities_returns_result(self, sample_graph):
        """Test that function returns a result."""
        degree_df = get_degrees(sample_graph)
        result = get_triangle_centralities(sample_graph, degree_df)
        assert result is not None

    def test_get_triangle_centralities_has_columns(self, sample_graph):
        """Test that result has required columns."""
        degree_df = get_degrees(sample_graph)
        result = get_triangle_centralities(sample_graph, degree_df)
        assert "id" in result.columns
        assert "lcc" in result.columns


class TestGetDensity:
    """Test suite for get_density function."""

    def test_get_density_returns_number(self, sample_graph):
        """Test that get_density returns a numeric value."""
        result = get_density(sample_graph)
        assert isinstance(result, (int, float))

    def test_get_density_in_range(self, sample_graph):
        """Test that density is between 0 and 1."""
        result = get_density(sample_graph)
        assert 0.0 <= result <= 1.0
