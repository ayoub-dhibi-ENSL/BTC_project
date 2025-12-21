import sys
from datetime import datetime
from pathlib import Path

# Ensure repository root is on sys.path so `btc_graph` package is importable during tests
sys.path.insert(0, str(Path.cwd()))

from btc_graph.io.loaders import (
    SnapshotPathFinder,
    NodeTableFinder,
    AnalysisResultsFinder,
)


def make_snapshot_file(
    tmp_path: Path, resolution: str, date_str: str, idx: int = 0
) -> Path:
    # Create directory structure matching the repo convention
    base = tmp_path / f"orbitaal-snapshot-{resolution}" / "SNAPSHOT" / "EDGES"
    base.mkdir(parents=True, exist_ok=True)
    name = f"orbitaal-snapshot-date-{date_str}-file-id-{idx}.parquet"
    p = base / name
    p.write_text("")
    return p


def test_get_snapshot_paths_and_parse_metadata(tmp_path: Path):
    # Arrange
    finder = SnapshotPathFinder(base_path=str(tmp_path))
    # create two snapshot files: one with date only and one with hour
    p1 = make_snapshot_file(tmp_path, "hour", "2016-07-09", 1)
    p2 = make_snapshot_file(tmp_path, "hour", "2016-07-09-18", 2)

    # Act
    paths = finder.get_snapshot_paths("hour")

    # Assert path discovery
    assert len(paths) >= 2
    assert any(
        "orbitaal-snapshot-date-2016-07-09-file-id-1.parquet" in p for p in paths
    )

    # Test metadata parsing for both files
    meta1 = finder.parse_snapshot_metadata(str(p1))
    meta2 = finder.parse_snapshot_metadata(str(p2))

    assert meta1 is not None
    assert isinstance(meta1.datetime, datetime)
    assert meta1.datetime.date() == datetime(2016, 7, 9).date()

    assert meta2 is not None
    assert meta2.datetime.year == 2016 and meta2.datetime.hour == 18


def test_node_table_finder(tmp_path: Path):
    base = tmp_path / "orbitaal-nodetable" / "NODE_TABLE"
    base.mkdir(parents=True)
    f = base / "node-table-0001.parquet"
    f.write_text("")

    finder = NodeTableFinder(base_path=str(tmp_path))
    paths = finder.get_node_table_paths()

    assert len(paths) == 1
    assert paths[0].endswith("node-table-0001.parquet")


def test_analysis_results_finder(tmp_path: Path):
    base = tmp_path / "snapshot-hour-analysis"
    d = base / "hour-000000"
    (d / "degrees").mkdir(parents=True)
    f = d / "degrees" / "part-000.csv"
    f.write_text("id,degree\n1,2\n")

    finder = AnalysisResultsFinder(base_path=str(tmp_path))
    snaps = finder.list_analysis_snapshots("hour")
    assert len(snaps) == 1

    metric_files = finder.list_metric_files(str(d), metric="degrees")
    assert len(metric_files) == 1
    assert metric_files[0].endswith("part-000.csv")
