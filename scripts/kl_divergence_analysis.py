#!/usr/bin/env python3
"""
KL Divergence Analysis for Bitcoin Transaction Network.

Computes the Kullback-Leibler divergence D_KL(P_t || P_{t+1}) between consecutive
hourly snapshots to quantify how centrality distributions evolve over time.

Metrics computed:
    - KL divergence for in-degree, out-degree, total degree, clustering coefficient
    - Global graph properties: density, clustering coefficients, network size

Usage:
    # High-resolution analysis (every 100th snapshot -> ~1000 samples)
    python scripts/kl_divergence_analysis.py --sample-rate 100 --output-dir results/kl-analysis-highres

    # Resume from checkpoint
    python scripts/kl_divergence_analysis.py --resume --output-dir results/kl-analysis-highres
"""

import argparse
import gc
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.special import rel_entr

sys.path.insert(0, str(Path(__file__).parent.parent))

from btc_graph.io import create_spark_session, stop_spark_session, SNAPSHOT_SCHEMA
from btc_graph.core import GraphBuilder, compute_degrees, compute_density

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


# ============================================================================
# Constants
# ============================================================================

GENESIS_DATE = datetime(2009, 1, 3, 18, 0, 0)  # Bitcoin genesis block timestamp
NUM_BINS = 100


# ============================================================================
# Data Classes
# ============================================================================


@dataclass
class SnapshotMetrics:
    """
    Container for all metrics extracted from a single snapshot.

    Stores both global graph properties and raw centrality values needed
    for KL divergence computation between consecutive snapshots.
    """

    snapshot_id: str
    hour: int
    date: datetime

    # Global properties
    num_vertices: int
    num_edges: int
    density: float
    global_cc: float
    avg_cc: float

    # Raw values for KL computation (using shared bins between snapshots)
    raw_in_degree: np.ndarray = field(default_factory=lambda: np.array([]))
    raw_out_degree: np.ndarray = field(default_factory=lambda: np.array([]))
    raw_degree: np.ndarray = field(default_factory=lambda: np.array([]))
    raw_clustering: np.ndarray = field(default_factory=lambda: np.array([]))


# ============================================================================
# Utility Functions
# ============================================================================


def hours_to_date(hour: int) -> datetime:
    """Convert hour index to calendar date (relative to genesis block)."""
    return GENESIS_DATE + timedelta(hours=hour)


def extract_file_id(path: str) -> int:
    """Extract numeric file ID from parquet filename for sorting."""
    filename = Path(path).name
    try:
        parts = filename.split("file-id-")
        if len(parts) > 1:
            return int(parts[1].split(".")[0])
    except (ValueError, IndexError):
        pass
    return 0


def format_time(seconds: float) -> str:
    """Format seconds into human-readable string (e.g., '2h 30m')."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


# ============================================================================
# KL Divergence Computation
# ============================================================================


def compute_kl_divergence(
    values_p: np.ndarray,
    values_q: np.ndarray,
    num_bins: int = NUM_BINS,
    use_log_bins: bool = True,
    epsilon: float = 1e-10,
) -> float:
    """
    Compute KL divergence D_KL(P || Q) from raw values using shared bins.

    Creates a common bin grid spanning both distributions to ensure
    we're comparing the same physical quantities.

    Parameters
    ----------
    values_p : np.ndarray
        Raw values for distribution P (snapshot at time t).
    values_q : np.ndarray
        Raw values for distribution Q (snapshot at time t+1).
    num_bins : int
        Number of histogram bins.
    use_log_bins : bool
        Use log-spaced bins (better for power-law distributions like degree).
    epsilon : float
        Small constant to avoid log(0).

    Returns
    -------
    float
        KL divergence D_KL(P || Q).
    """
    # Filter invalid values
    if use_log_bins:
        values_p = values_p[values_p > 0]
        values_q = values_q[values_q > 0]
    else:
        values_p = values_p[(values_p >= 0) & (~np.isnan(values_p))]
        values_q = values_q[(values_q >= 0) & (~np.isnan(values_q))]

    if len(values_p) < 2 or len(values_q) < 2:
        return 0.0

    # Create shared bin edges spanning both distributions
    combined_min = min(values_p.min(), values_q.min())
    combined_max = max(values_p.max(), values_q.max())

    if combined_max <= combined_min:
        return 0.0

    if use_log_bins:
        bins = np.logspace(np.log10(combined_min), np.log10(combined_max), num_bins + 1)
    else:
        bins = np.linspace(combined_min, combined_max, num_bins + 1)

    # Compute histograms on shared bins
    hist_p, _ = np.histogram(values_p, bins=bins)
    hist_q, _ = np.histogram(values_q, bins=bins)

    # Convert to probabilities with smoothing
    p = (hist_p.astype(float) + epsilon) / (hist_p.sum() + epsilon * num_bins)
    q = (hist_q.astype(float) + epsilon) / (hist_q.sum() + epsilon * num_bins)

    # Normalize
    p = p / p.sum()
    q = q / q.sum()

    return np.sum(rel_entr(p, q))


# ============================================================================
# Snapshot Analysis
# ============================================================================


def compute_triangle_metrics(graph, degrees_df) -> Tuple[pd.DataFrame, float, float]:
    """
    Compute triangle counts and clustering coefficients for all vertices.

    Returns
    -------
    triangles_df : pd.DataFrame
        DataFrame with id, triangle_count, clustering_coefficient columns.
    global_cc : float
        Global clustering coefficient (transitivity).
    avg_cc : float
        Average local clustering coefficient.
    """
    from pyspark.sql import functions as F

    triangles = graph.triangleCount()
    combined = triangles.join(degrees_df.select("id", "degree"), on="id", how="left")

    # Local CC = 2 * triangles / (degree * (degree - 1))
    combined = combined.withColumn(
        "clustering_coefficient",
        F.when(
            F.col("degree") > 1,
            (2.0 * F.col("count")) / (F.col("degree") * (F.col("degree") - 1)),
        ).otherwise(0.0),
    )

    # Aggregate statistics
    stats = combined.agg(
        F.avg("clustering_coefficient").alias("avg_cc"),
        F.sum("count").alias("total_triangles"),
    ).collect()[0]

    avg_cc = float(stats["avg_cc"]) if stats["avg_cc"] else 0.0
    total_triangles = int(stats["total_triangles"]) if stats["total_triangles"] else 0

    # Global CC = 3 * triangles / connected_triplets
    triplets = degrees_df.agg(
        F.sum(F.col("degree") * (F.col("degree") - 1) / 2).alias("triplets")
    ).collect()[0]["triplets"]
    triplets = float(triplets) if triplets else 0

    global_cc = (3.0 * total_triangles / triplets) if triplets > 0 else 0.0

    triangles_pdf = combined.select("id", "count", "clustering_coefficient").toPandas()
    triangles_pdf.columns = ["id", "triangle_count", "clustering_coefficient"]

    return triangles_pdf, global_cc, avg_cc


def analyze_snapshot(
    spark, parquet_path: str, snapshot_id: str, hour: int
) -> Optional[SnapshotMetrics]:
    """
    Analyze a single snapshot and extract all metrics.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    parquet_path : str
        Path to the parquet file containing edge data.
    snapshot_id : str
        Identifier for this snapshot (e.g., 'hour-000100').
    hour : int
        Hour index since genesis.

    Returns
    -------
    SnapshotMetrics or None
        Extracted metrics, or None if analysis failed.
    """
    try:
        edges_df = spark.read.parquet(parquet_path, schema=SNAPSHOT_SCHEMA)
        graph = GraphBuilder.from_edges(edges_df, src_col="SRC_ID", dst_col="DST_ID")

        num_vertices = graph.vertices.count()
        num_edges = graph.edges.count()

        if num_vertices < 3:
            return SnapshotMetrics(
                snapshot_id=snapshot_id,
                hour=hour,
                date=hours_to_date(hour),
                num_vertices=num_vertices,
                num_edges=num_edges,
                density=1.0 if num_vertices == 2 else 0.0,
                global_cc=0.0,
                avg_cc=0.0,
            )

        degrees_df = compute_degrees(graph)
        degrees_pdf = degrees_df.toPandas()

        density = compute_density(graph)
        triangles_pdf, global_cc, avg_cc = compute_triangle_metrics(graph, degrees_df)

        return SnapshotMetrics(
            snapshot_id=snapshot_id,
            hour=hour,
            date=hours_to_date(hour),
            num_vertices=num_vertices,
            num_edges=num_edges,
            density=density,
            global_cc=global_cc,
            avg_cc=avg_cc,
            raw_in_degree=degrees_pdf["inDegree"].values.astype(float),
            raw_out_degree=degrees_pdf["outDegree"].values.astype(float),
            raw_degree=degrees_pdf["degree"].values.astype(float),
            raw_clustering=triangles_pdf["clustering_coefficient"].values.astype(float),
        )

    except Exception as e:
        logger.error(f"Error analyzing {snapshot_id}: {e}")
        return None


# ============================================================================
# KL Divergence Analysis
# ============================================================================


def compute_all_kl_divergences(metrics_list: List[SnapshotMetrics]) -> pd.DataFrame:
    """
    Compute KL divergences between all consecutive snapshots.

    For each snapshot at time t, computes D_KL(P_t || P_{t+1}) measuring
    how much the distribution changes to the next time point.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: snapshot_id, hour, date, kl_in_degree,
        kl_out_degree, kl_degree, kl_clustering.
    """
    if len(metrics_list) < 2:
        return pd.DataFrame()

    logger.info(
        "Computing KL divergence D_KL(P_t || P_{t+1}) for consecutive snapshots..."
    )

    results = []
    for i in range(len(metrics_list)):
        m_t = metrics_list[i]

        if i < len(metrics_list) - 1:
            m_next = metrics_list[i + 1]

            kl_in = compute_kl_divergence(
                m_t.raw_in_degree, m_next.raw_in_degree, use_log_bins=True
            )
            kl_out = compute_kl_divergence(
                m_t.raw_out_degree, m_next.raw_out_degree, use_log_bins=True
            )
            kl_deg = compute_kl_divergence(
                m_t.raw_degree, m_next.raw_degree, use_log_bins=True
            )
            kl_cc = compute_kl_divergence(
                m_t.raw_clustering, m_next.raw_clustering, use_log_bins=False
            )
        else:
            kl_in = kl_out = kl_deg = kl_cc = np.nan

        results.append(
            {
                "snapshot_id": m_t.snapshot_id,
                "hour": m_t.hour,
                "date": m_t.date,
                "kl_in_degree": kl_in,
                "kl_out_degree": kl_out,
                "kl_degree": kl_deg,
                "kl_clustering": kl_cc,
            }
        )

    return pd.DataFrame(results)


def compute_kl_correlations(kl_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute rolling correlations between KL divergences of different centralities.

    Returns
    -------
    pd.DataFrame
        DataFrame with correlation values for each pair of centralities.
    """
    pairs = [
        ("kl_in_degree", "kl_out_degree"),
        ("kl_in_degree", "kl_degree"),
        ("kl_in_degree", "kl_clustering"),
        ("kl_out_degree", "kl_degree"),
        ("kl_out_degree", "kl_clustering"),
        ("kl_degree", "kl_clustering"),
    ]

    results = []
    for i in range(len(kl_df)):
        row = {"hour": kl_df.iloc[i]["hour"], "date": kl_df.iloc[i]["date"]}

        if i < 2:
            for c1, c2 in pairs:
                row[f"corr_{c1}_{c2}"] = np.nan
        else:
            window = kl_df.iloc[: i + 1]
            for c1, c2 in pairs:
                row[f"corr_{c1}_{c2}"] = window[c1].corr(window[c2])

        results.append(row)

    return pd.DataFrame(results)


# ============================================================================
# Checkpoint Management
# ============================================================================


def load_checkpoint(output_dir: str) -> Tuple[List[dict], int]:
    """Load checkpoint data if exists."""
    checkpoint_path = os.path.join(output_dir, ".checkpoint.json")
    if os.path.exists(checkpoint_path):
        with open(checkpoint_path) as f:
            data = json.load(f)
            return data.get("processed_metrics", []), data.get("last_index", -1)
    return [], -1


def save_checkpoint(
    output_dir: str, metrics_list: List[SnapshotMetrics], last_index: int
):
    """Save checkpoint for resume capability."""
    checkpoint_path = os.path.join(output_dir, ".checkpoint.json")
    os.makedirs(output_dir, exist_ok=True)

    serialized = []
    for m in metrics_list:
        serialized.append(
            {
                "snapshot_id": m.snapshot_id,
                "hour": m.hour,
                "date": m.date.isoformat(),
                "num_vertices": m.num_vertices,
                "num_edges": m.num_edges,
                "density": m.density,
                "global_cc": m.global_cc,
                "avg_cc": m.avg_cc,
                # Note: raw values are stored for KL computation
                "raw_in_degree": m.raw_in_degree.tolist()
                if len(m.raw_in_degree) > 0
                else [],
                "raw_out_degree": m.raw_out_degree.tolist()
                if len(m.raw_out_degree) > 0
                else [],
                "raw_degree": m.raw_degree.tolist() if len(m.raw_degree) > 0 else [],
                "raw_clustering": m.raw_clustering.tolist()
                if len(m.raw_clustering) > 0
                else [],
            }
        )

    with open(checkpoint_path, "w") as f:
        json.dump(
            {
                "processed_metrics": serialized,
                "last_index": last_index,
                "timestamp": datetime.now().isoformat(),
            },
            f,
        )


def deserialize_metrics(data: List[dict]) -> List[SnapshotMetrics]:
    """Reconstruct SnapshotMetrics objects from checkpoint data."""
    metrics = []
    for d in data:
        metrics.append(
            SnapshotMetrics(
                snapshot_id=d["snapshot_id"],
                hour=d["hour"],
                date=datetime.fromisoformat(d["date"]),
                num_vertices=d["num_vertices"],
                num_edges=d["num_edges"],
                density=d["density"],
                global_cc=d["global_cc"],
                avg_cc=d["avg_cc"],
                raw_in_degree=np.array(d.get("raw_in_degree", [])),
                raw_out_degree=np.array(d.get("raw_out_degree", [])),
                raw_degree=np.array(d.get("raw_degree", [])),
                raw_clustering=np.array(d.get("raw_clustering", [])),
            )
        )
    return metrics


# ============================================================================
# Results Export
# ============================================================================


def save_results(
    metrics_list: List[SnapshotMetrics],
    kl_df: pd.DataFrame,
    corr_df: pd.DataFrame,
    output_dir: str,
):
    """Save all analysis results to CSV and JSON files."""
    os.makedirs(output_dir, exist_ok=True)

    # Global properties
    global_df = pd.DataFrame(
        [
            {
                "snapshot_id": m.snapshot_id,
                "hour": m.hour,
                "date": m.date,
                "num_vertices": m.num_vertices,
                "num_edges": m.num_edges,
                "density": m.density,
                "global_cc": m.global_cc,
                "avg_cc": m.avg_cc,
            }
            for m in metrics_list
        ]
    )
    global_df.to_csv(os.path.join(output_dir, "global_properties.csv"), index=False)
    logger.info(f"Saved global_properties.csv ({len(global_df)} rows)")

    # KL divergences
    kl_df.to_csv(os.path.join(output_dir, "kl_divergences.csv"), index=False)
    logger.info(f"Saved kl_divergences.csv ({len(kl_df)} rows)")

    # Correlations
    corr_df.to_csv(os.path.join(output_dir, "kl_correlations.csv"), index=False)
    logger.info(f"Saved kl_correlations.csv ({len(corr_df)} rows)")

    # Summary
    summary = {
        "total_snapshots": len(metrics_list),
        "date_range": f"{metrics_list[0].date.strftime('%Y-%m-%d')} to {metrics_list[-1].date.strftime('%Y-%m-%d')}",
        "min_vertices": min(m.num_vertices for m in metrics_list),
        "max_vertices": max(m.num_vertices for m in metrics_list),
        "min_edges": min(m.num_edges for m in metrics_list),
        "max_edges": max(m.num_edges for m in metrics_list),
        "mean_density": np.mean([m.density for m in metrics_list]),
        "mean_global_cc": np.mean([m.global_cc for m in metrics_list]),
        "mean_avg_cc": np.mean([m.avg_cc for m in metrics_list]),
    }

    with open(os.path.join(output_dir, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2, default=str)
    logger.info("Saved summary.json")


# ============================================================================
# Main
# ============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="KL Divergence Analysis for Bitcoin Network"
    )
    parser.add_argument(
        "--snapshot-dir",
        type=str,
        default="data/orbitaal-snapshot-hour/SNAPSHOT/EDGES/hour",
        help="Directory containing snapshot parquet files",
    )
    parser.add_argument(
        "--output-dir", type=str, required=True, help="Directory for output results"
    )
    parser.add_argument(
        "--sample-rate",
        type=int,
        default=100,
        help="Process every Nth snapshot (default: 100 for high-res)",
    )
    parser.add_argument(
        "--start", type=int, default=0, help="Start from this snapshot index"
    )
    parser.add_argument(
        "--end", type=int, default=None, help="End at this snapshot index"
    )
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument(
        "--driver-memory", type=str, default="8g", help="Spark driver memory"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Snapshots per batch before cache clearing",
    )

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Bitcoin Network KL Divergence Analysis")
    logger.info("=" * 60)

    # Discover snapshots
    snapshot_dir = Path(args.snapshot_dir)
    if not snapshot_dir.exists():
        logger.error(f"Snapshot directory not found: {snapshot_dir}")
        return

    all_paths = sorted(
        snapshot_dir.glob("*.parquet"), key=lambda p: extract_file_id(str(p))
    )
    all_paths = [str(p) for p in all_paths]
    logger.info(f"Found {len(all_paths):,} total snapshots")

    # Apply sampling and range
    if args.sample_rate > 1:
        all_paths = all_paths[:: args.sample_rate]
        logger.info(
            f"Sampling every {args.sample_rate}th snapshot: {len(all_paths):,} to process"
        )

    if args.end is not None:
        all_paths = all_paths[args.start : args.end]
    else:
        all_paths = all_paths[args.start :]

    logger.info(f"Processing range: {args.start} to {args.start + len(all_paths)}")

    # Load checkpoint
    metrics_list = []
    start_idx = 0
    if args.resume:
        saved_metrics, last_idx = load_checkpoint(args.output_dir)
        if saved_metrics:
            metrics_list = deserialize_metrics(saved_metrics)
            start_idx = last_idx + 1
            logger.info(
                f"Resuming from checkpoint: {len(metrics_list)} snapshots loaded"
            )

    if start_idx >= len(all_paths):
        logger.info("All snapshots already processed!")
    else:
        logger.info(
            f"Creating Spark session with {args.driver_memory} driver memory..."
        )
        spark = create_spark_session(
            app_name="btc-kl-analysis", driver_memory=args.driver_memory
        )

        start_time = time.time()

        try:
            for batch_start in range(start_idx, len(all_paths), args.batch_size):
                batch_end = min(batch_start + args.batch_size, len(all_paths))
                batch_num = batch_start // args.batch_size + 1
                total_batches = (
                    len(all_paths) + args.batch_size - 1
                ) // args.batch_size

                logger.info(f"\n--- Batch {batch_num}/{total_batches} ---")

                for idx in range(batch_start, batch_end):
                    path = all_paths[idx]
                    hour = idx * args.sample_rate
                    snapshot_id = f"hour-{hour:06d}"

                    elapsed = time.time() - start_time
                    processed = len(metrics_list) - (start_idx if args.resume else 0)
                    remaining = len(all_paths) - idx - 1

                    if processed > 0:
                        eta_str = format_time(remaining / (processed / elapsed))
                    else:
                        eta_str = "unknown"

                    logger.info(
                        f"[{idx + 1}/{len(all_paths)}] {snapshot_id} | Elapsed: {format_time(elapsed)} | ETA: {eta_str}"
                    )

                    snapshot_start = time.time()
                    metrics = analyze_snapshot(spark, path, snapshot_id, hour)

                    if metrics is not None:
                        metrics_list.append(metrics)
                        logger.info(
                            f"  -> {metrics.num_vertices:,} vertices, {metrics.num_edges:,} edges, "
                            f"density={metrics.density:.2e}, time={time.time() - snapshot_start:.1f}s"
                        )
                    else:
                        logger.warning("  -> Skipped (analysis failed)")

                logger.info("Clearing Spark cache and saving checkpoint...")
                spark.catalog.clearCache()
                save_checkpoint(args.output_dir, metrics_list, idx)
                gc.collect()

        finally:
            stop_spark_session()

    # Compute final results
    logger.info("\nComputing KL divergences...")
    kl_df = compute_all_kl_divergences(metrics_list)

    logger.info("Computing correlations...")
    corr_df = compute_kl_correlations(kl_df)

    logger.info("\nSaving results...")
    save_results(metrics_list, kl_df, corr_df, args.output_dir)

    total_time = time.time() - start_time if "start_time" in dir() else 0
    logger.info("\n" + "=" * 60)
    logger.info("Analysis Complete!")
    logger.info("=" * 60)
    logger.info(f"Total snapshots: {len(metrics_list)}")
    if metrics_list:
        logger.info(
            f"Date range: {metrics_list[0].date.strftime('%Y-%m-%d')} to {metrics_list[-1].date.strftime('%Y-%m-%d')}"
        )
    logger.info(f"Output: {args.output_dir}")
    logger.info(f"Total time: {format_time(total_time)}")


if __name__ == "__main__":
    main()
