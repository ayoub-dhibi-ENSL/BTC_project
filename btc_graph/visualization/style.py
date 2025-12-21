"""Plot styling constants and configuration for btc_graph visualizations.

This module provides a unified style configuration for all plots in the
btc_graph package, ensuring consistent appearance across all visualizations.

Design Notes
------------
- LaTeX rendering is optional and can be disabled for environments without TeX.
- All colors use a consistent palette designed for accessibility and print.
- Style can be applied globally or per-figure.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

# =============================================================================
# Color Palette
# =============================================================================

#: Primary color for main plot elements (lines, bars)
PRIMARY_COLOR: str = "#1f77b4"

#: Secondary color for comparison elements
SECONDARY_COLOR: str = "#ff7f0e"

#: Color for marking events on plots
EVENT_COLOR: str = "#00bfff"

#: Accent colors for specific use cases
ACCENT_COLORS: Dict[str, str] = {
    "mean": "#d62728",  # Red for mean lines
    "median": "#2ca02c",  # Green for median lines
    "positive": "#ff9896",  # Light red for positive values
    "fill": "#aec7e8",  # Light blue for fill areas
    "heatmap": "Blues",  # Colormap for heatmaps
}

# =============================================================================
# Plot Style Configuration
# =============================================================================

#: Default style settings for matplotlib plots
PLOT_STYLE: Dict[str, Any] = {
    # Figure
    "figure.figsize": (14, 6),
    "figure.dpi": 100,
    "figure.facecolor": "white",
    # Font
    "font.size": 10,
    "font.family": "serif",
    # Axes
    "axes.labelsize": 12,
    "axes.titlesize": 14,
    "axes.linewidth": 1.5,
    "axes.labelweight": "bold",
    "axes.titleweight": "bold",
    "axes.grid": True,
    "axes.axisbelow": True,
    # Ticks
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    # Legend
    "legend.fontsize": 11,
    "legend.framealpha": 0.8,
    # Grid
    "grid.linewidth": 0.7,
    "grid.alpha": 0.6,
    "grid.linestyle": ":",
    # Lines
    "lines.linewidth": 2,
    "lines.markersize": 6,
    # Patches (bars, histograms)
    "patch.linewidth": 1.5,
}

#: LaTeX-enabled style (requires TeX installation)
LATEX_STYLE: Dict[str, Any] = {
    "text.usetex": True,
    "font.family": "serif",
    "font.serif": ["Computer Modern"],
}


def apply_btc_style(use_latex: bool = False) -> None:
    """Apply the btc_graph plotting style to matplotlib.

    This function updates matplotlib's rcParams to use the btc_graph
    style configuration, ensuring consistent appearance across all plots.

    Parameters
    ----------
    use_latex : bool, optional
        If True, enable LaTeX rendering for text. Requires a TeX
        installation. Default is False.

    Examples
    --------
    >>> from btc_graph.visualization import apply_btc_style
    >>> apply_btc_style()  # Standard style
    >>> apply_btc_style(use_latex=True)  # With LaTeX rendering

    Notes
    -----
    - LaTeX rendering provides publication-quality math formatting but
      requires pdflatex or similar to be installed.
    - Call this function once at the start of your script or notebook.
    """
    import matplotlib.pyplot as plt

    # Apply base style
    plt.rcParams.update(PLOT_STYLE)

    # Optionally enable LaTeX
    if use_latex:
        try:
            plt.rcParams.update(LATEX_STYLE)
        except Exception:
            # LaTeX not available, fall back to standard rendering
            pass


def get_color_palette(n_colors: int = 10) -> List[str]:
    """Get a list of distinct colors for plotting multiple series.

    Parameters
    ----------
    n_colors : int, optional
        Number of colors needed. Default is 10.

    Returns
    -------
    List[str]
        List of hex color codes.

    Examples
    --------
    >>> colors = get_color_palette(5)
    >>> for i, color in enumerate(colors):
    ...     plt.plot(data[i], color=color)
    """
    # Extended color palette for multiple series
    palette = [
        "#1f77b4",  # Blue
        "#ff7f0e",  # Orange
        "#2ca02c",  # Green
        "#d62728",  # Red
        "#9467bd",  # Purple
        "#8c564b",  # Brown
        "#e377c2",  # Pink
        "#7f7f7f",  # Gray
        "#bcbd22",  # Olive
        "#17becf",  # Cyan
    ]

    # Cycle through palette if more colors needed
    return [palette[i % len(palette)] for i in range(n_colors)]


def create_figure(
    nrows: int = 1,
    ncols: int = 1,
    figsize: Optional[Tuple[float, float]] = None,
    **kwargs,
) -> Tuple:
    """Create a figure with btc_graph styling applied.

    Convenience function that applies styling and creates a figure
    in one step.

    Parameters
    ----------
    nrows : int, optional
        Number of subplot rows. Default is 1.
    ncols : int, optional
        Number of subplot columns. Default is 1.
    figsize : tuple of float, optional
        Figure size (width, height) in inches. If None, uses default.
    **kwargs
        Additional arguments passed to plt.subplots().

    Returns
    -------
    fig : matplotlib.figure.Figure
        The created figure.
    axes : matplotlib.axes.Axes or array of Axes
        The subplot axes.

    Examples
    --------
    >>> from btc_graph.visualization.style import create_figure
    >>> fig, ax = create_figure()
    >>> ax.plot(x, y)
    >>> fig.savefig("plot.pdf")
    """
    import matplotlib.pyplot as plt

    apply_btc_style()

    if figsize is None:
        figsize = PLOT_STYLE["figure.figsize"]

    return plt.subplots(nrows, ncols, figsize=figsize, **kwargs)
