"""Histogram plotting functions for graph metrics.

This module provides functions for creating histogram visualizations
of degree distributions, triangle counts, and clustering coefficients.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

import numpy as np

from .style import (
    ACCENT_COLORS,
    PRIMARY_COLOR,
    apply_btc_style,
)

if TYPE_CHECKING:
    import pandas as pd
    from matplotlib.figure import Figure


def plot_metric_histograms(
    df: "pd.DataFrame",
    output_path: Optional[str] = None,
    columns: Optional[List[str]] = None,
    title_prefix: str = "",
    bins: int = 250,
    log_scale: bool = True,
    density: bool = True,
    show_stats: bool = True,
    figsize: tuple = (12, 8),
) -> "Figure":
    """Create histogram plots for DataFrame columns with statistical markers.

    This function generates a grid of histograms for numeric columns in
    the DataFrame, with optional mean and median lines overlaid.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing numeric columns to plot. Index should be
        node IDs.
    output_path : str, optional
        Path to save the figure. If None, figure is not saved.
    columns : list of str, optional
        Specific columns to plot. If None, plots all numeric columns.
    title_prefix : str, optional
        Prefix for subplot titles (e.g., snapshot ID).
    bins : int, optional
        Number of histogram bins. Default is 250.
    log_scale : bool, optional
        Use logarithmic y-axis. Default is True.
    density : bool, optional
        Normalize histograms to density. Default is True.
    show_stats : bool, optional
        Show mean and median lines. Default is True.
    figsize : tuple, optional
        Figure size (width, height). Default is (12, 8).

    Returns
    -------
    matplotlib.figure.Figure
        The created figure object.

    Examples
    --------
    >>> import pandas as pd
    >>> from btc_graph.visualization import plot_metric_histograms
    >>>
    >>> degrees_df = pd.read_csv("degrees.csv", index_col="id")
    >>> fig = plot_metric_histograms(
    ...     degrees_df,
    ...     output_path="plots/degrees_hist.pdf",
    ...     title_prefix="Snapshot 001",
    ... )
    """
    import matplotlib.pyplot as plt

    apply_btc_style()

    # Select columns to plot
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns.tolist()

    n_cols = min(2, len(columns))
    n_rows = (len(columns) + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=figsize)

    # Handle single subplot case
    if len(columns) == 1:
        axes = np.array([axes])
    axes = axes.flatten()

    for i, col in enumerate(columns):
        ax = axes[i]
        data = df[col].dropna()

        # Filter out non-positive values for log scale
        if log_scale:
            data = data[data > 0]

        if len(data) == 0:
            ax.text(0.5, 0.5, "No data", ha="center", va="center")
            ax.set_title(_get_column_title(col, title_prefix))
            continue

        # Plot histogram
        ax.hist(
            data,
            bins=bins,
            color=ACCENT_COLORS["fill"],
            edgecolor=PRIMARY_COLOR,
            alpha=0.7,
            density=density,
            log=log_scale,
        )

        # Add statistical markers
        if show_stats:
            mean_val = data.mean()
            median_val = data.median()

            ax.axvline(
                mean_val,
                color=ACCENT_COLORS["mean"],
                linestyle="--",
                linewidth=2,
                label=f"Mean = {mean_val:.2e}",
            )
            ax.axvline(
                median_val,
                color=ACCENT_COLORS["median"],
                linestyle="-.",
                linewidth=2,
                label=f"Median = {median_val:.2e}",
            )
            ax.legend(loc="best")

        # Set labels
        ax.set_title(_get_column_title(col, title_prefix), pad=10)
        ax.set_xlabel("Value", fontweight="bold")
        ax.set_ylabel("Density" if density else "Count", fontweight="bold")
        ax.grid(True, linestyle=":", alpha=0.6)

    # Remove unused subplots
    for j in range(len(columns), len(axes)):
        fig.delaxes(axes[j])

    plt.tight_layout()

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, bbox_inches="tight", dpi=300)

    return fig


def _get_column_title(col: str, prefix: str = "") -> str:
    """Generate a readable title for a column.

    Parameters
    ----------
    col : str
        Column name.
    prefix : str
        Optional prefix.

    Returns
    -------
    str
        Human-readable title.
    """
    # Column name mappings
    titles = {
        "lcc": "Local Clustering Coefficient",
        "triangles_count": "Triangle Count (Node Included)",
        "triangles_max_count": "Max Possible Triangles",
        "degree": "Total Degree",
        "inDegree": "In-Degree",
        "outDegree": "Out-Degree",
        "pagerank": "PageRank",
    }

    title = titles.get(col, f"{col}")

    if prefix:
        title = f"{prefix} - {title}"

    return title


def plot_snapshot_histograms(
    analysis_dir: str,
    snapshot_id: str,
    output_dir: str,
    metrics: Optional[List[str]] = None,
) -> List["Figure"]:
    """Plot histograms for a specific snapshot from analysis results.

    This function reads CSV files from a snapshot analysis directory
    and creates histogram plots for the specified metrics.

    Parameters
    ----------
    analysis_dir : str
        Base directory containing snapshot analysis results.
    snapshot_id : str
        Snapshot identifier (e.g., "hour-000001").
    output_dir : str
        Directory to save output figures.
    metrics : list of str, optional
        Metrics to plot ("degrees", "triangles"). Default is both.

    Returns
    -------
    list of Figure
        List of created figure objects.

    Examples
    --------
    >>> from btc_graph.visualization import plot_snapshot_histograms
    >>> figs = plot_snapshot_histograms(
    ...     analysis_dir="data/snapshot-hour-analysis",
    ...     snapshot_id="hour-000005",
    ...     output_dir="plots",
    ... )
    """
    import pandas as pd

    if metrics is None:
        metrics = ["degrees", "triangles"]

    figures = []
    snapshot_path = Path(analysis_dir) / snapshot_id

    for metric in metrics:
        metric_dir = snapshot_path / metric

        # Find CSV file
        csv_files = list(metric_dir.glob("*.csv"))
        if not csv_files:
            print(f"No CSV found for {metric} in {snapshot_id}")
            continue

        # Read data
        df = pd.read_csv(csv_files[0], index_col="id")

        # Select columns based on metric
        if metric == "triangles":
            columns = ["triangles_count", "triangles_max_count", "lcc"]
            columns = [c for c in columns if c in df.columns]
        else:
            columns = None  # Use all numeric columns

        # Create output path
        output_path = Path(output_dir) / f"{metric}-{snapshot_id}_histograms.pdf"

        fig = plot_metric_histograms(
            df,
            output_path=str(output_path),
            columns=columns,
            title_prefix=snapshot_id,
        )
        figures.append(fig)

    return figures


def plot_degree_distribution(
    df: "pd.DataFrame",
    output_path: Optional[str] = None,
    log_log: bool = True,
    fit_power_law: bool = False,
) -> "Figure":
    """Plot degree distribution with optional power-law fit.

    This function creates a degree distribution plot, optionally on
    log-log scale to reveal power-law behaviour typical of scale-free
    networks like Bitcoin.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame with degree information. Must have 'degree' column.
    output_path : str, optional
        Path to save the figure.
    log_log : bool, optional
        Use log-log scale. Default is True.
    fit_power_law : bool, optional
        Fit and plot power-law distribution. Default is False.

    Returns
    -------
    matplotlib.figure.Figure
        The created figure.

    Examples
    --------
    >>> fig = plot_degree_distribution(degrees_df, log_log=True)
    """
    import matplotlib.pyplot as plt

    apply_btc_style()

    fig, ax = plt.subplots(figsize=(10, 6))

    # Get degree data
    degrees = df["degree"].dropna()
    degrees = degrees[degrees > 0]

    # Compute degree frequency
    unique_degrees, counts = np.unique(degrees, return_counts=True)
    freq = counts / counts.sum()

    # Plot
    ax.scatter(
        unique_degrees,
        freq,
        alpha=0.6,
        color=PRIMARY_COLOR,
        s=20,
        label="Observed",
    )

    if log_log:
        ax.set_xscale("log")
        ax.set_yscale("log")

    ax.set_xlabel("Degree (k)", fontweight="bold")
    ax.set_ylabel("P(k)", fontweight="bold")
    ax.set_title("Degree Distribution", fontweight="bold")
    ax.grid(True, linestyle=":", alpha=0.6)
    ax.legend()

    plt.tight_layout()

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, bbox_inches="tight", dpi=300)

    return fig
