"""Time evolution visualization functions for graph metrics.

This module provides functions for visualizing how graph metrics
evolve over time, including heatmaps and divergence plots.
"""

from __future__ import annotations

import glob
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Tuple

import numpy as np

from .style import (
    ACCENT_COLORS,
    EVENT_COLOR,
    PRIMARY_COLOR,
    apply_btc_style,
)

if TYPE_CHECKING:
    from matplotlib.figure import Figure


@dataclass
class CryptoEvent:
    """Represents a major cryptocurrency event for marking on plots.

    Attributes
    ----------
    index : int
        Snapshot index where the event occurred.
    name : str
        Short name/label for the event.
    description : str, optional
        Longer description of the event.
    """

    index: int
    name: str
    description: str = ""


# Pre-defined major crypto events (placeholder - customize for your data)
DEFAULT_EVENTS: List[CryptoEvent] = [
    CryptoEvent(5, "Event 1", "Placeholder event 1"),
    CryptoEvent(10, "Event 2", "Placeholder event 2"),
    CryptoEvent(15, "Event 3", "Placeholder event 3"),
]


def load_metric_series(
    analysis_dir: str,
    resolution: str,
    metric: str = "degrees",
    column: Optional[str] = None,
    limit: Optional[int] = None,
) -> Tuple[List[np.ndarray], List[str]]:
    """Load metric data from all snapshots in an analysis directory.

    This function reads CSV files from multiple snapshot directories
    and returns the data as a list of arrays for time series analysis.

    Parameters
    ----------
    analysis_dir : str
        Base directory containing snapshot analysis results.
    resolution : str
        Time resolution ("hour" or "year").
    metric : str, optional
        Metric to load ("degrees" or "triangles"). Default is "degrees".
    column : str, optional
        Specific column to extract. If None, uses first numeric column.
    limit : int, optional
        Maximum number of snapshots to load.

    Returns
    -------
    data : list of ndarray
        List of arrays, one per snapshot.
    labels : list of str
        Snapshot labels/IDs.

    Examples
    --------
    >>> data, labels = load_metric_series(
    ...     "data/snapshot-hour-analysis",
    ...     "hour",
    ...     metric="degrees",
    ...     limit=20,
    ... )
    >>> print(f"Loaded {len(data)} snapshots")
    """
    import pandas as pd

    # Find all snapshot directories
    pattern = f"{analysis_dir}/{resolution}-*/{metric}/*.csv"
    csv_files = sorted(glob.glob(pattern))

    if limit:
        csv_files = csv_files[:limit]

    data = []
    labels = []

    for csv_path in csv_files:
        try:
            df = pd.read_csv(csv_path, index_col="id")

            # Select column
            if column and column in df.columns:
                values = df[column].dropna().values
            else:
                # Use first numeric column
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                if len(numeric_cols) > 0:
                    values = df[numeric_cols[0]].dropna().values
                else:
                    continue

            # Filter positive values for log scale compatibility
            values = values[values > 0]

            if len(values) > 0:
                data.append(values)
                # Extract snapshot ID from path
                snapshot_id = Path(csv_path).parent.parent.name
                labels.append(snapshot_id)

        except Exception as e:
            print(f"Error loading {csv_path}: {e}")
            continue

    return data, labels


def plot_histogram_evolution(
    analysis_dir: str,
    resolution: str,
    metric: str = "degrees",
    output_path: Optional[str] = None,
    events: Optional[List[CryptoEvent]] = None,
    n_bins: int = 50,
    figsize: Tuple[float, float] = (16, 8),
    limit: Optional[int] = None,
) -> "Figure":
    """Plot evolution of histogram distributions as a 2D heatmap.

    Creates a heatmap where the x-axis represents time (snapshots),
    the y-axis represents bin ranges (log scale), and color intensity
    represents frequency. Major events can be marked with vertical lines.

    Parameters
    ----------
    analysis_dir : str
        Directory containing snapshot analysis results.
    resolution : str
        Time resolution ("hour" or "year").
    metric : str, optional
        Metric to plot ("degrees" or "triangles"). Default is "degrees".
    output_path : str, optional
        Path to save the figure.
    events : list of CryptoEvent, optional
        Major events to mark on the plot. If None, uses DEFAULT_EVENTS.
    n_bins : int, optional
        Number of histogram bins. Default is 50.
    figsize : tuple, optional
        Figure size. Default is (16, 8).
    limit : int, optional
        Maximum number of snapshots to include.

    Returns
    -------
    matplotlib.figure.Figure
        The created figure.

    Examples
    --------
    >>> from btc_graph.visualization import plot_histogram_evolution
    >>> fig = plot_histogram_evolution(
    ...     "data/snapshot-hour-analysis",
    ...     "hour",
    ...     metric="degrees",
    ...     output_path="plots/evolution_heatmap.pdf",
    ... )
    """
    import matplotlib.pyplot as plt
    from matplotlib import colors

    apply_btc_style()

    # Load data
    all_data, labels = load_metric_series(analysis_dir, resolution, metric, limit=limit)

    if not all_data:
        print(f"No data found for metric: {metric}")
        fig, ax = plt.subplots(figsize=figsize)
        ax.text(0.5, 0.5, "No data available", ha="center", va="center")
        return fig

    # Compute common bins (log scale)
    min_val = max(min(d.min() for d in all_data), 1e-10)
    max_val = max(d.max() for d in all_data)
    bins = np.logspace(np.log10(min_val), np.log10(max_val), n_bins)

    # Build heatmap matrix
    heatmap_data = np.zeros((len(bins) - 1, len(all_data)))
    for j, data in enumerate(all_data):
        hist, _ = np.histogram(data, bins=bins)
        heatmap_data[:, j] = hist

    # Create figure
    fig, ax = plt.subplots(figsize=figsize)

    # Plot heatmap with log normalization
    positive_data = heatmap_data[heatmap_data > 0]
    if len(positive_data) == 0:
        ax.text(0.5, 0.5, "No positive histogram values", ha="center", va="center")
        return fig

    im = ax.imshow(
        heatmap_data,
        aspect="auto",
        cmap=ACCENT_COLORS["heatmap"],
        norm=colors.LogNorm(vmin=positive_data.min(), vmax=heatmap_data.max()),
        interpolation="bilinear",
        origin="lower",
    )

    # Labels and title
    ax.set_xlabel("Snapshot Index (Time →)", fontweight="bold")
    ax.set_ylabel("Value Range (log scale)", fontweight="bold")
    ax.set_title(
        f"Evolution of {metric.capitalize()} Distribution Over Time ({resolution})",
        fontweight="bold",
        pad=20,
    )

    # X-axis ticks
    n_xticks = min(11, len(all_data))
    x_ticks = np.linspace(0, len(all_data) - 1, n_xticks)
    ax.set_xticks(x_ticks)
    ax.set_xticklabels([f"{int(i)}" for i in x_ticks])

    # Y-axis ticks (bin values)
    n_yticks = 6
    y_ticks = np.linspace(0, len(bins) - 2, n_yticks)
    y_labels = [f"{bins[int(i)]:.1e}" for i in y_ticks]
    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_labels)

    # Colorbar
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label("Frequency (log scale)", fontweight="bold")

    # Mark events
    if events is None:
        events = DEFAULT_EVENTS

    _add_event_markers(ax, events, len(all_data), len(bins) - 2)

    plt.tight_layout()

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, bbox_inches="tight", dpi=300)

    return fig


def plot_kl_divergence(
    analysis_dir: str,
    resolution: str,
    metric: str = "degrees",
    output_path: Optional[str] = None,
    events: Optional[List[CryptoEvent]] = None,
    n_bins: int = 50,
    figsize: Tuple[float, float] = (14, 6),
    limit: Optional[int] = None,
) -> "Figure":
    """Plot Kullback-Leibler divergence between consecutive histograms.

    Computes and plots the KL divergence between distribution at time t
    and time t+1, revealing structural changes in the network over time.
    Spikes in KL divergence may indicate significant events.

    Parameters
    ----------
    analysis_dir : str
        Directory containing snapshot analysis results.
    resolution : str
        Time resolution ("hour" or "year").
    metric : str, optional
        Metric to analyze. Default is "degrees".
    output_path : str, optional
        Path to save the figure.
    events : list of CryptoEvent, optional
        Major events to mark on the plot.
    n_bins : int, optional
        Number of histogram bins. Default is 50.
    figsize : tuple, optional
        Figure size. Default is (14, 6).
    limit : int, optional
        Maximum number of snapshots to include.

    Returns
    -------
    matplotlib.figure.Figure
        The created figure.

    Notes
    -----
    KL divergence is computed as:

    .. math::

        D_{KL}(P || Q) = \\sum_i P(i) \\log\\frac{P(i)}{Q(i)}

    where P is the distribution at time t and Q at time t+1.

    Examples
    --------
    >>> fig = plot_kl_divergence(
    ...     "data/snapshot-hour-analysis",
    ...     "hour",
    ...     output_path="plots/kl_divergence.pdf",
    ... )
    """
    import matplotlib.pyplot as plt

    apply_btc_style()

    # Load data
    all_data, labels = load_metric_series(analysis_dir, resolution, metric, limit=limit)

    if len(all_data) < 2:
        print(f"Need at least 2 snapshots. Found {len(all_data)}")
        fig, ax = plt.subplots(figsize=figsize)
        ax.text(0.5, 0.5, "Insufficient data", ha="center", va="center")
        return fig

    # Compute common bins
    min_val = max(min(d.min() for d in all_data), 1e-10)
    max_val = max(d.max() for d in all_data)
    bins = np.logspace(np.log10(min_val), np.log10(max_val), n_bins)

    # Compute normalized histograms
    histograms = []
    for data in all_data:
        hist, _ = np.histogram(data, bins=bins)
        hist_norm = hist / hist.sum()
        histograms.append(hist_norm)

    # Compute KL divergence between consecutive histograms
    kl_divergences = []
    eps = 1e-10  # Small value to avoid log(0)

    for i in range(len(histograms) - 1):
        p = histograms[i] + eps
        q = histograms[i + 1] + eps

        # Renormalize after adding epsilon
        p = p / p.sum()
        q = q / q.sum()

        # KL divergence: sum(p * log(p/q))
        kl_div = np.sum(p * np.log(p / q))
        kl_divergences.append(kl_div)

    # Create figure
    fig, ax = plt.subplots(figsize=figsize)

    x_indices = np.arange(len(kl_divergences))

    # Plot KL divergence line
    ax.plot(
        x_indices,
        kl_divergences,
        marker="o",
        linewidth=2,
        markersize=4,
        color=PRIMARY_COLOR,
        label="KL Divergence",
    )
    ax.fill_between(
        x_indices,
        kl_divergences,
        alpha=0.3,
        color=ACCENT_COLORS["fill"],
    )

    # Labels and title
    ax.set_xlabel("Snapshot Index (Time →)", fontweight="bold")
    ax.set_ylabel(r"$D_{KL}(P \| Q)$ (nats)", fontweight="bold")
    ax.set_title(
        f"KL Divergence Evolution ({metric.capitalize()}, {resolution})",
        fontweight="bold",
        pad=20,
    )

    ax.grid(True, linestyle=":", alpha=0.6)

    # Mark events
    if events is None:
        events = DEFAULT_EVENTS

    y_max = max(kl_divergences) if kl_divergences else 1
    _add_event_markers(ax, events, len(kl_divergences), y_max, y_offset=0.95)

    ax.legend(loc="best")
    plt.tight_layout()

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, bbox_inches="tight", dpi=300)

    return fig


def _add_event_markers(
    ax,
    events: List[CryptoEvent],
    max_x: int,
    y_position: float,
    y_offset: float = 1.0,
) -> None:
    """Add vertical event markers to an axis.

    Parameters
    ----------
    ax : matplotlib.axes.Axes
        The axes to add markers to.
    events : list of CryptoEvent
        Events to mark.
    max_x : int
        Maximum x value (for filtering events).
    y_position : float
        Y coordinate for labels.
    y_offset : float
        Multiplier for y position of text.
    """
    for event in events:
        if event.index >= max_x:
            continue

        ax.axvline(
            x=event.index,
            color=EVENT_COLOR,
            linestyle="--",
            linewidth=2,
            alpha=0.8,
        )

        ax.text(
            event.index,
            y_position * y_offset,
            event.name,
            rotation=45,
            fontsize=10,
            color=EVENT_COLOR,
            ha="center",
            va="bottom",
            bbox=dict(
                boxstyle="round,pad=0.3",
                facecolor="white",
                alpha=0.8,
                edgecolor=EVENT_COLOR,
                linewidth=1.5,
            ),
        )


def plot_metric_timeseries(
    analysis_dir: str,
    resolution: str,
    output_path: Optional[str] = None,
    metrics: Optional[List[str]] = None,
    stat: str = "mean",
    events: Optional[List[CryptoEvent]] = None,
    figsize: Tuple[float, float] = (14, 6),
    limit: Optional[int] = None,
) -> "Figure":
    """Plot time series of aggregate statistics for metrics.

    Creates a line plot showing how mean/median values of metrics
    change over time across snapshots.

    Parameters
    ----------
    analysis_dir : str
        Directory containing snapshot analysis results.
    resolution : str
        Time resolution ("hour" or "year").
    output_path : str, optional
        Path to save the figure.
    metrics : list of str, optional
        Metrics to plot. Default is ["degrees"].
    stat : str, optional
        Statistic to compute ("mean", "median", "std"). Default is "mean".
    events : list of CryptoEvent, optional
        Events to mark on the plot.
    figsize : tuple, optional
        Figure size. Default is (14, 6).
    limit : int, optional
        Maximum number of snapshots.

    Returns
    -------
    matplotlib.figure.Figure
        The created figure.
    """
    import matplotlib.pyplot as plt

    apply_btc_style()

    if metrics is None:
        metrics = ["degrees"]

    fig, ax = plt.subplots(figsize=figsize)

    for metric in metrics:
        all_data, labels = load_metric_series(
            analysis_dir, resolution, metric, limit=limit
        )

        if not all_data:
            continue

        # Compute statistic for each snapshot
        if stat == "mean":
            values = [d.mean() for d in all_data]
        elif stat == "median":
            values = [np.median(d) for d in all_data]
        elif stat == "std":
            values = [d.std() for d in all_data]
        else:
            values = [d.mean() for d in all_data]

        ax.plot(
            range(len(values)),
            values,
            marker="o",
            markersize=4,
            linewidth=2,
            label=f"{metric} ({stat})",
        )

    ax.set_xlabel("Snapshot Index (Time →)", fontweight="bold")
    ax.set_ylabel(f"{stat.capitalize()} Value", fontweight="bold")
    ax.set_title(
        f"Metric Evolution Over Time ({resolution})",
        fontweight="bold",
        pad=20,
    )

    ax.grid(True, linestyle=":", alpha=0.6)
    ax.legend(loc="best")

    # Mark events
    if events is None:
        events = DEFAULT_EVENTS

    y_min, y_max = ax.get_ylim()
    _add_event_markers(ax, events, len(all_data), y_max, y_offset=0.95)

    plt.tight_layout()

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, bbox_inches="tight", dpi=300)

    return fig
