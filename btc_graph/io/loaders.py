"""I/O utilities: path discovery and lightweight Spark loaders.

Design notes:
- All paths returned are relative paths under the repository (user asked for
  relative-only paths).
- The path-discovery and metadata parsing utilities are pure-Python and
  therefore unit-testable without Spark. Spark-loading helpers are thin
  wrappers that will attempt to load Parquet/CSV if a SparkSession is passed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

# PySpark types only imported at type-checking time so pure-Python tests work
if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame  # noqa: F401
    from pyspark.sql import SparkSession  # noqa: F401

# ---------------------------------------------------------------------------
# Schema definition for snapshot edge Parquet files.
# Providing the schema makes loading the file faster.
# ---------------------------------------------------------------------------
try:
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StructField,
        StructType,
    )

    SNAPSHOT_SCHEMA: "StructType" = StructType(
        [
            StructField("SRC_ID", IntegerType(), True),
            StructField("DST_ID", IntegerType(), True),
            StructField("VALUE_SATOSHI", LongType(), True),
            StructField("VALUE_USD", DoubleType(), True),
        ]
    )
except ImportError:
    # PySpark not installed; schema will be None. Spark loaders will fail at
    # runtime if called, but pure-Python utilities remain usable.
    SNAPSHOT_SCHEMA = None  # type: ignore[assignment,misc]

_SNAPSHOT_FILENAME_RE = re.compile(
    r"orbitaal-snapshot-date-(?P<date>\d{4}-\d{2}-\d{2}(?:-\d{2})?)"  # e.g. 2016-07-09 or 2016-07-09-18
)


@dataclass
class SnapshotMetadata:
    path: str
    datetime: datetime
    resolution: str


class SnapshotPathFinder:
    """Find snapshot files and parse metadata from their filenames.

    Usage:
        finder = SnapshotPathFinder(base_path="data")
        paths = finder.get_snapshot_paths("hour", sample=True)
        meta = finder.parse_snapshot_metadata(paths[0])
    """

    def __init__(self, base_path: str = "data"):
        # Only relative paths supported; preserve as Path for manipulation
        self.base = Path(base_path)

    def get_snapshot_paths(self, resolution: str, sample: bool = False) -> List[str]:
        """Return list of relative file paths for snapshot edge files.

        Parameters
        - resolution: 'hour' or 'year' (keeps existing repo conventions)
        - sample: if True, return a small sample (useful for tests/dev)
        """
        pattern = self.base / f"orbitaal-snapshot-{resolution}" / "SNAPSHOT" / "EDGES"
        if not pattern.exists():
            return []

        files = sorted(pattern.rglob("*.parquet"))

        if sample and resolution == "hour":
            files = files[:20]
        elif sample and resolution == "year":
            files = files[:2]

        # Return paths relative to the configured base when possible; otherwise
        # fall back to absolute path. This makes behaviour predictable when the
        # configured base is inside the repository (typical) while still
        # supporting temporary directories used in tests.
        out: List[str] = []
        base_resolved = self.base.resolve()
        for p in files:
            try:
                out.append(str(p.relative_to(base_resolved)))
            except Exception:
                out.append(str(p))
        return out

    def parse_snapshot_metadata(self, path: str) -> Optional[SnapshotMetadata]:
        """Extract date/time metadata from a snapshot filename.

        Returns None if the pattern cannot be parsed.
        """
        p = Path(path)
        m = _SNAPSHOT_FILENAME_RE.search(p.name)
        if not m:
            return None

        date_str = m.group("date")
        # Accept either YYYY-MM-DD or YYYY-MM-DD-HH
        dt = None
        try:
            if len(date_str.split("-")) == 4:
                dt = datetime.strptime(date_str, "%Y-%m-%d-%H")
            else:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return None

        # Determine resolution from parent folder structure if possible
        resolution = "unknown"
        for part in p.parts:
            if part.startswith("orbitaal-snapshot-"):
                resolution = part.replace("orbitaal-snapshot-", "")
                break

        return SnapshotMetadata(path=str(p), datetime=dt, resolution=resolution)

    # Lightweight Spark wrapper: returns Spark DataFrame using enforced schema.
    def load_snapshot_with_spark(
        self, spark: "SparkSession", path: str
    ) -> "SparkDataFrame":
        """Load Parquet snapshot using an existing SparkSession.

        Uses the strict SNAPSHOT_SCHEMA to speed up loading and ensure correct
        types for SRC_ID, DST_ID, VALUE_SATOSHI, VALUE_USD.
        """
        if spark is None:
            raise ValueError("spark session must be provided to load with Spark")
        if SNAPSHOT_SCHEMA is None:
            raise ImportError(
                "PySpark is not installed; cannot load snapshot with Spark"
            )

        return spark.read.schema(SNAPSHOT_SCHEMA).parquet(path)


class NodeTableFinder:
    """Find node table files inside the nodetable directory.

    The repository contains `data/orbitaal-nodetable/NODE_TABLE/` or similar.
    """

    def __init__(self, base_path: str = "data"):
        self.base = Path(base_path)

    def get_node_table_paths(self) -> List[str]:
        candidate = self.base / "orbitaal-nodetable" / "NODE_TABLE"
        if not candidate.exists():
            return []
        files = sorted(candidate.rglob("*.parquet"))
        out: List[str] = []
        base_resolved = candidate.resolve()
        for p in files:
            try:
                out.append(str(p.relative_to(base_resolved)))
            except Exception:
                out.append(str(p))
        return out

    def load_node_table_with_spark(self, spark, path: str):
        if spark is None:
            raise ValueError("spark session must be provided to load with Spark")
        return spark.read.parquet(path)


class AnalysisResultsFinder:
    """Locate previously computed analysis result CSVs (degrees/triangles/scalar).

    This helps reloading results stored under `data/snapshot-hour-analysis/`.
    """

    def __init__(self, base_path: str = "data"):
        self.base = Path(base_path)

    def list_analysis_snapshots(self, resolution: str) -> List[str]:
        root = self.base / f"snapshot-{resolution}-analysis"
        if not root.exists():
            return []
        # Find directories like hour-000000
        dirs = sorted([p for p in root.iterdir() if p.is_dir()])
        out: List[str] = []
        base_resolved = root.resolve()
        for d in dirs:
            try:
                out.append(str(d.relative_to(base_resolved)))
            except Exception:
                out.append(str(d))
        return out

    def list_metric_files(
        self, snapshot_dir: str, metric: str = "degrees"
    ) -> List[str]:
        p = Path(snapshot_dir)
        metric_dir = p / metric
        if not metric_dir.exists():
            return []
        out: List[str] = []
        base_resolved = metric_dir.resolve()
        for f in sorted(metric_dir.rglob("*.csv")):
            try:
                out.append(str(f.relative_to(base_resolved)))
            except Exception:
                out.append(str(f))
        return out

    def load_csv_with_spark(self, spark, path: str):
        if spark is None:
            raise ValueError("spark session must be provided to load CSV with Spark")
        return spark.read.csv(path, header=True, inferSchema=True)
