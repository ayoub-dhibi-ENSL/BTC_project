"""Tests for btc_graph.io.exporters module.

These tests require PySpark; they are skipped if PySpark is not installed.
"""

import sys
from pathlib import Path

# Ensure repository root is on sys.path so `btc_graph` package is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest

# Check if PySpark is available
try:
    from pyspark.sql import SparkSession

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


@pytest.fixture(scope="module")
def spark():
    """Create a local SparkSession for testing."""
    if not PYSPARK_AVAILABLE:
        pytest.skip("PySpark not installed")
    return (
        SparkSession.builder.master("local[1]").appName("btc_graph_test").getOrCreate()
    )


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not installed")
def test_csv_exporter_save(spark, tmp_path: Path):
    """Test CSVExporter.save writes CSV to directory."""
    from btc_graph.io.exporters import CSVExporter

    # Create a simple DataFrame
    data = [(1, 2, 100, 1.5), (2, 3, 200, 2.5)]
    df = spark.createDataFrame(data, ["SRC_ID", "DST_ID", "VALUE_SATOSHI", "VALUE_USD"])

    exporter = CSVExporter(coalesce=1)
    output_dir = tmp_path / "output"
    exporter.save(df, str(output_dir))

    # Verify output directory was created and contains a CSV part file
    assert output_dir.exists()
    csv_files = list(output_dir.glob("part-*.csv"))
    assert len(csv_files) == 1

    # Verify content
    content = csv_files[0].read_text()
    assert "SRC_ID" in content  # header present
    assert "1,2,100,1.5" in content or "1,2,100,1.5" in content.replace("\r", "")


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not installed")
def test_csv_exporter_save_single_file(spark, tmp_path: Path):
    """Test CSVExporter.save_single_file writes a single CSV file."""
    from btc_graph.io.exporters import CSVExporter

    data = [(1, 10), (2, 20)]
    df = spark.createDataFrame(data, ["id", "degree"])

    exporter = CSVExporter()
    output_file = tmp_path / "results.csv"
    exporter.save_single_file(df, str(output_file))

    # Verify file was created (not a directory)
    assert output_file.exists()
    assert output_file.is_file()

    content = output_file.read_text()
    assert "id,degree" in content
    assert "1,10" in content
