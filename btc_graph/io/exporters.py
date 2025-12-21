"""Export utilities for btc_graph analysis results.

This module provides CSV export functionality for Spark DataFrames.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame


class CSVExporter:
    """Export Spark DataFrames to CSV files.

    Usage:
        exporter = CSVExporter()
        exporter.save(df, "data/snapshot-hour-analysis/hour-000000/degrees/")
    """

    def __init__(self, coalesce: int = 1):
        """Initialize CSVExporter.

        Parameters
        ----------
        coalesce : int
            Number of partitions to coalesce to before writing. Default is 1
            so a single CSV file is produced.
        """
        self.coalesce = coalesce

    def save(
        self,
        df: "SparkDataFrame",
        path: str,
        header: bool = True,
        mode: str = "overwrite",
    ) -> None:
        """Save Spark DataFrame to CSV.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            The DataFrame to export.
        path : str
            Destination directory path. Spark will write part files inside.
        header : bool
            Whether to include column headers. Default True.
        mode : str
            Write mode: 'overwrite', 'append', 'ignore', 'error'. Default 'overwrite'.
        """
        if df is None:
            raise ValueError("DataFrame must be provided")

        # Ensure parent directory exists (Spark creates the final dir)
        parent = Path(path).parent
        parent.mkdir(parents=True, exist_ok=True)

        df.coalesce(self.coalesce).write.csv(path, header=header, mode=mode)

    def save_single_file(
        self,
        df: "SparkDataFrame",
        filepath: str,
        header: bool = True,
        mode: str = "overwrite",
    ) -> None:
        """Save DataFrame as a single CSV file (not a directory).

        This is a convenience method that writes to a temp directory, then
        moves the single part file to the desired filepath.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            The DataFrame to export.
        filepath : str
            Destination file path (e.g., "output/results.csv").
        header : bool
            Whether to include column headers. Default True.
        mode : str
            Write mode. Default 'overwrite'.
        """
        import shutil
        import tempfile

        if df is None:
            raise ValueError("DataFrame must be provided")

        # Write to temp directory
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir) / "output"
            df.coalesce(1).write.csv(str(tmp_path), header=header, mode=mode)

            # Find the part file
            part_files = list(tmp_path.glob("part-*.csv"))
            if not part_files:
                raise RuntimeError("No part file found after CSV write")

            # Move to final destination
            dest = Path(filepath)
            dest.parent.mkdir(parents=True, exist_ok=True)

            if mode == "overwrite" and dest.exists():
                dest.unlink()

            shutil.move(str(part_files[0]), str(dest))
