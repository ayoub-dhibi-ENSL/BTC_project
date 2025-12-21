"""Visualization module for Bitcoin blockchain graph analysis.

This module provides plotting functions for visualizing graph metrics,
including histograms, time evolution heatmaps, and divergence plots.

Submodules
----------
style
    Plot styling constants and configuration utilities.
histograms
    Histogram plotting for degree and triangle distributions.
evolution
    Time series and evolution plots (heatmaps, KL divergence).

Typical Usage
-------------
>>> from btc_graph.visualization import (
...     plot_metric_histograms,
...     plot_histogram_evolution,
...     plot_kl_divergence,
...     apply_btc_style,
... )
>>>
>>> # Apply consistent styling
>>> apply_btc_style()
>>>
>>> # Plot histograms for a single snapshot
>>> plot_metric_histograms(
...     degrees_df,
...     output_path="plots/degrees_hist.pdf",
... )
>>>
>>> # Plot evolution over time
>>> plot_histogram_evolution(
...     input_dir="data/snapshot-hour-analysis",
...     resolution="hour",
...     metric="degrees",
...     output_path="plots/evolution.pdf",
... )
"""

from .style import (
    ACCENT_COLORS,
    EVENT_COLOR,
    PLOT_STYLE,
    PRIMARY_COLOR,
    SECONDARY_COLOR,
    apply_btc_style,
)
from .histograms import (
    plot_metric_histograms,
    plot_snapshot_histograms,
)
from .evolution import (
    plot_histogram_evolution,
    plot_kl_divergence,
    plot_metric_timeseries,
    load_metric_series,
)

__all__ = [
    # Style
    "PLOT_STYLE",
    "PRIMARY_COLOR",
    "SECONDARY_COLOR",
    "EVENT_COLOR",
    "ACCENT_COLORS",
    "apply_btc_style",
    # Histograms
    "plot_metric_histograms",
    "plot_snapshot_histograms",
    # Evolution
    "plot_histogram_evolution",
    "plot_kl_divergence",
    "plot_metric_timeseries",
    "load_metric_series",
]
