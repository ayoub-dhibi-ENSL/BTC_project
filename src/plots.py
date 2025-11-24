import pandas as pd
import matplotlib.pyplot as plt
import glob
import numpy as np
from matplotlib import colors

# Enable LaTeX rendering
plt.rcParams["text.usetex"] = True
plt.rcParams["font.family"] = "serif"
plt.rcParams["font.serif"] = ["Computer Modern"]

# Unified style configuration
PLOT_STYLE = {
    "figure.figsize": (14, 6),
    "font.size": 10,
    "axes.labelsize": 12,
    "axes.titlesize": 14,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 11,
    "axes.linewidth": 1.5,
    "grid.linewidth": 0.7,
    "lines.linewidth": 2,
    "lines.markersize": 6,
    "patch.linewidth": 1.5,
    "axes.labelweight": "bold",
    "axes.titleweight": "bold",
}

plt.rcParams.update(PLOT_STYLE)

# Color palette
PRIMARY_COLOR = "#1f77b4"
SECONDARY_COLOR = "#ff7f0e"
EVENT_COLOR = "#00bfff"
ACCENT_COLORS = {
    "mean": "#d62728",
    "median": "#2ca02c",
    "positive": "#ff9896",
    "fill": "#aec7e8",
    "heatmap": "Blues",
}


def hist_plots(resolution, id):
    """
    Generate and save histogram plots for degrees and triangles data.

    This function reads CSV files containing degrees and triangles data for a given
    resolution and snapshot id, creates histograms for the relevant columns, and saves
    the resulting figures to PDF files.

    Parameters
    ----------
    resolution : str
        The temporal resolution of the data (e.g., "year" or "hour").
    id : str
        Snapshot identifier (zero-padded string) used in the output filename.

    Returns
    -------
    None
        The function saves histogram plots as PDF files and does not return a value.
    """

    # file_path_scalar = glob.glob(
    #     f"../data/snapshot-year-analysis/{year}-{id}/scalar/*.csv"
    # )[0]
    file_path_triangles = glob.glob(
        f"../data/snapshot-{resolution}-analysis/{resolution}-{id}/triangles/*.csv"
    )[0]  # returns a list so [0] returns a string
    file_path_degrees = glob.glob(
        f"../data/snapshot-{resolution}-analysis/{resolution}-{id}/degrees/*.csv"
    )[0]

    # scalar_centralities_df = pd.read_csv(file_path_scalar)
    all_degrees_df = pd.read_csv(file_path_degrees, index_col="id")
    triangles_df = pd.read_csv(file_path_triangles, index_col="id")[
        ["triangles_count", "triangles_max_count", "lcc"]
    ]

    # # remove isolated nodes
    # triangles_df = triangles_df[triangles_df != 0]
    # triangles_df = triangles_df[triangles_df < 5000]
    # all_degrees_df = all_degrees_df[all_degrees_df != 0]
    # all_degrees_df = all_degrees_df[all_degrees_df < 5000]
    make_save_hist("degrees", all_degrees_df, resolution, id)
    make_save_hist("triangles", triangles_df, resolution, id)


def make_save_hist(name, df, resolution, id):
    """
    Create and save histogram plots for DataFrame columns with mean/median markers.

    This function builds a grid of histogram plots for numeric columns in `df`,
    overlays vertical lines for the mean and median of each column, and saves
    the resulting figure to a PDF file named using `name`, `resolution`, and `id`.

    Parameters
    ----------
    name : str
        A short label used in the output filename (e.g., "degrees" or "triangles").
    df : pandas.DataFrame
        DataFrame containing one or more numeric columns to plot. Columns should be
        indexed by node id (index_col="id") in the caller; non-numeric columns will
        be ignored by the plotting logic.
    resolution : int or str
        Resolution identifier (e.g., year or hour) used in the output filename and
        for contextual labeling.
    id : int or str
        Snapshot identifier (zero-padded string) used in the output filename.

    Returns
    -------
    None
        The function saves the figure as a PDF to ../plots/{name}-{resolution}-{id}_histograms_with_mean_median.pdf
        and does not return a value.

    Notes
    -----
    - Histograms use a logarithmic y-scale (log=True) and 250 bins by default.
    - Mean is plotted as a red dashed line and median as a green dash-dot line.
    """
    numeric_cols = df.columns

    n_cols = 2
    n_rows = (len(numeric_cols) + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(12, 4 * n_rows))
    axes = axes.flatten()

    # Plot histograms
    for i, col in enumerate(numeric_cols):
        ax = axes[i]
        data = df[col].dropna()

        ax.hist(
            data,
            bins=250,
            color=ACCENT_COLORS["fill"],
            edgecolor=PRIMARY_COLOR,
            alpha=0.7,
            density=True,
            log=True,
        )

        mean_val = data.mean()
        median_val = data.median()

        ax.axvline(
            mean_val,
            color=ACCENT_COLORS["mean"],
            linestyle="--",
            linewidth=2,
            label=f"$\\mu = {mean_val:.2e}$",
        )
        ax.axvline(
            median_val,
            color=ACCENT_COLORS["median"],
            linestyle="-.",
            linewidth=2,
            label=f"$\\mathrm{{med}} = {median_val:.2e}$",
        )

        if col == "lcc":
            title = r"Histogram of Local Clustering Coefficient"
        elif col == "triangles_count":
            title = r"Histogram of Triangles (Node Included)"
        elif col == "triangles_max_count":
            title = r"Histogram of Triangles (Node Could Include)"
        else:
            title = f"Histogram of {col}"

        ax.set_title(title, pad=10)
        ax.legend(loc="best")
        ax.grid(True, linestyle=":", alpha=0.6)
        ax.set_ylabel(r"Density", fontweight="bold")
        ax.set_xlabel(r"Value", fontweight="bold")

    # Remove unused subplots (if any)
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    # Save the figure to a file.
    plt.tight_layout()
    plt.savefig(
        f"../plots/{name}-{resolution}-{id}_histograms_with_mean_median.pdf",
        bbox_inches="tight",
    )
    plt.close()


def make_plots(resolution):
    """
    Generate and save plots for processed data snapshots at a given resolution.

    This function locates all CSV files created by the `process_function` and corresponding to the specified resolution,
    counts the number of files, and iterates through each file to
    generate and save plots using the `hist_plots` function.

    Parameters
    ----------
    resolution : str
        The temporal resolution of the data snapshots. Supported values are "year" and "hour".

    Notes
    -----
    - The function expects the data files to be organized in a specific directory structure.
    - The `hist_plots` function is called for each snapshot, passing the resolution and a
      zero-padded identifier based on the snapshot index.
    """
    paths_parquet = glob.glob(
        f"../data/orbitaal-snapshot-{resolution}/SNAPSHOT/EDGES/{resolution}/orbitaal-snapshot-date-*-file-id-*.snappy.parquet"
    )
    snapshots_count = len(paths_parquet)

    for i in range(snapshots_count):
        if resolution == "year":
            id = f"{i:02d}"
        elif resolution == "hour":
            id = f"{i:06d}"

        hist_plots(resolution, id)


def plot_histogram_evolution(resolution, metric="degrees"):
    """
    Plot the evolution of histogram distributions through time as a 2D heatmap.

    This function creates a heatmap where the x-axis represents time (snapshots),
    the y-axis represents bin ranges, and color intensity represents frequency.
    Major events can be marked with vertical dashed lines and labels.

    Parameters
    ----------
    resolution : str
        The temporal resolution of the data (e.g., "year" or "hour").
    metric : str, optional
        The metric to plot ("degrees" or "triangles"). Default is "degrees".

    Returns
    -------
    None
        The function saves the figure as a PDF and does not return a value.

    Notes
    -----
    - Major events are defined as a dummy list and should be edited by the user.
    - Events are marked with vertical dashed lines and diagonal labels.
    """
    major_events = [
        (5, r"Event 1"),
        (10, r"Event 2"),
        (15, r"Event 3"),
    ]

    paths_parquet = glob.glob(
        f"../data/orbitaal-snapshot-{resolution}/SNAPSHOT/EDGES/{resolution}/orbitaal-snapshot-date-*-file-id-*.snappy.parquet"
    )
    snapshots_count = len(paths_parquet)

    # Collect histogram data across all snapshots
    all_data = []
    snapshot_labels = []

    for i in range(snapshots_count):
        if resolution == "year":
            id = f"{i:02d}"
        elif resolution == "hour":
            id = f"{i:06d}"

        try:
            if metric == "degrees":
                file_path = glob.glob(
                    f"../data/snapshot-{resolution}-analysis/{resolution}-{id}/degrees/*.csv"
                )[0]
                df = pd.read_csv(file_path, index_col="id")
            elif metric == "triangles":
                file_path = glob.glob(
                    f"../data/snapshot-{resolution}-analysis/{resolution}-{id}/triangles/*.csv"
                )[0]
                df = pd.read_csv(file_path, index_col="id")[
                    ["triangles_count", "triangles_max_count", "lcc"]
                ]

            # Get first numeric column and filter out zeros/negative values for log scale
            data = df.iloc[:, 0].dropna()
            data = data[data > 0]  # Keep only positive values for log scale
            if len(data) > 0:
                all_data.append(data.values)
                snapshot_labels.append(f"Snapshot {i}")
        except (IndexError, FileNotFoundError):
            continue

    if not all_data:
        print(f"No data found for metric: {metric}")
        return

    # Create 2D histogram data for heatmap
    min_val = min([d.min() for d in all_data])
    max_val = max([d.max() for d in all_data])

    # Ensure bins are valid for log scale
    if min_val <= 0:
        min_val = 1e-10

    bins = np.logspace(np.log10(min_val), np.log10(max_val), 50)
    heatmap_data = np.zeros((len(bins) - 1, len(all_data)))

    for j, data in enumerate(all_data):
        hist, _ = np.histogram(data, bins=bins)
        heatmap_data[:, j] = hist

    fig, ax = plt.subplots(figsize=(16, 8))

    # Plot heatmap with logarithmic normalization
    positive_data = heatmap_data[heatmap_data > 0]
    if len(positive_data) == 0:
        print("No positive histogram values found. Skipping visualization.")
        plt.close()
        return

    im = ax.imshow(
        heatmap_data,
        aspect="auto",
        cmap=ACCENT_COLORS["heatmap"],
        norm=colors.LogNorm(vmin=positive_data.min(), vmax=heatmap_data.max()),
        interpolation="bilinear",
        origin="lower",
    )

    # Set labels and title
    ax.set_xlabel(r"Snapshot Index (Time $\rightarrow$)", fontweight="bold")
    ax.set_ylabel(r"Value Range (log scale)", fontweight="bold")
    title = rf"Evolution of $\mathrm{{{metric.capitalize()}}}$ Distribution Over Time ({resolution})"
    ax.set_title(title, pad=20)

    # Set x-axis ticks
    x_ticks = np.linspace(0, len(all_data) - 1, min(11, len(all_data)))
    ax.set_xticks(x_ticks)
    ax.set_xticklabels([f"{int(i)}" for i in x_ticks], fontsize=10)

    # Set y-axis ticks with bin labels
    y_ticks = np.linspace(0, len(bins) - 2, 6)
    y_labels = [f"{bins[int(i)]:.1e}" for i in y_ticks]
    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_labels, fontsize=9)

    # Add colorbar
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label(r"Frequency (log scale)", fontweight="bold")

    # Add major events as vertical lines with labels
    for event_idx, event_name in major_events:
        if event_idx < len(all_data):
            ax.axvline(
                x=event_idx, color=EVENT_COLOR, linestyle="--", linewidth=2, alpha=0.8
            )

            y_max = len(bins) - 2
            ax.text(
                event_idx,
                y_max,
                event_name,
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

    plt.tight_layout()
    plt.savefig(
        f"../plots/histogram_evolution/{metric}-evolution-{resolution}_heatmap.pdf",
        bbox_inches="tight",
        dpi=300,
    )
    plt.close()


def plot_kl_divergence_evolution(resolution, metric="degrees"):
    """
    Compute and plot the Kullback-Leibler divergence between consecutive histograms over time.

    This function computes the KL divergence between histogram at time t and time t+1,
    using normalized probability distributions. Major events can be marked with vertical
    dashed lines and labels.

    Parameters
    ----------
    resolution : str
        The temporal resolution of the data (e.g., "year" or "hour").
    metric : str, optional
        The metric to plot ("degrees" or "triangles"). Default is "degrees".

    Returns
    -------
    None
        The function saves the figure as a PDF and does not return a value.

    Notes
    -----
    - KL divergence is computed using the formula $D_{KL}(P \\| Q) = \\sum_i P(i) \\log(P(i)/Q(i))$.
    - Histograms are normalized to form valid probability distributions.
    - Major events are marked with vertical dashed lines and labels.
    """
    major_events = [
        (5, r"Event 1"),
        (10, r"Event 2"),
        (15, r"Event 3"),
    ]

    paths_parquet = glob.glob(
        f"../data/orbitaal-snapshot-{resolution}/SNAPSHOT/EDGES/{resolution}/orbitaal-snapshot-date-*-file-id-*.snappy.parquet"
    )
    snapshots_count = len(paths_parquet)

    all_data = []
    snapshot_labels = []

    for i in range(snapshots_count):
        if resolution == "year":
            id = f"{i:02d}"
        elif resolution == "hour":
            id = f"{i:06d}"

        try:
            if metric == "degrees":
                file_path = glob.glob(
                    f"../data/snapshot-{resolution}-analysis/{resolution}-{id}/degrees/*.csv"
                )[0]
                df = pd.read_csv(file_path, index_col="id")
            elif metric == "triangles":
                file_path = glob.glob(
                    f"../data/snapshot-{resolution}-analysis/{resolution}-{id}/triangles/*.csv"
                )[0]
                df = pd.read_csv(file_path, index_col="id")[
                    ["triangles_count", "triangles_max_count", "lcc"]
                ]

            data = df.iloc[:, 0].dropna()
            data = data[data > 0]
            if len(data) > 0:
                all_data.append(data.values)
                snapshot_labels.append(f"Snapshot {i}")
        except (IndexError, FileNotFoundError):
            continue

    if len(all_data) < 2:
        print(
            f"Need at least 2 snapshots to compute KL divergence. Found {len(all_data)}"
        )
        return

    # Create 2D histogram data with common bins
    min_val = min([d.min() for d in all_data])
    max_val = max([d.max() for d in all_data])

    if min_val <= 0:
        min_val = 1e-10

    bins = np.logspace(np.log10(min_val), np.log10(max_val), 50)

    # Compute normalized histograms
    histograms = []
    for data in all_data:
        hist, _ = np.histogram(data, bins=bins)
        # Normalize to form a probability distribution
        hist_normalized = hist / hist.sum()
        histograms.append(hist_normalized)

    # Compute KL divergence between consecutive histograms
    kl_divergences = []
    for i in range(len(histograms) - 1):
        # Add small epsilon to avoid log(0)
        eps = 1e-10
        p = histograms[i] + eps
        q = histograms[i + 1] + eps

        # Normalize again after adding epsilon
        p = p / p.sum()
        q = q / q.sum()

        # Compute KL divergence: sum(p * log(p/q))
        kl_div = np.sum(p * np.log(p / q))
        kl_divergences.append(kl_div)

    fig, ax = plt.subplots(figsize=(14, 6))

    # Plot KL divergence
    x_indices = np.arange(len(kl_divergences))
    ax.plot(
        x_indices,
        kl_divergences,
        marker="o",
        linewidth=2,
        markersize=6,
        color=PRIMARY_COLOR,
        label=r"KL Divergence",
    )
    ax.fill_between(x_indices, kl_divergences, alpha=0.3, color=ACCENT_COLORS["fill"])

    ax.set_xlabel(r"Snapshot Index (Time $\rightarrow$)", fontweight="bold")
    ax.set_ylabel(r"$D_{\mathrm{KL}}(P \| Q)$ (nats)", fontweight="bold")
    title = rf"KL Divergence Evolution Over Time ($\mathrm{{{metric.capitalize()}}}$, {resolution})"
    ax.set_title(title, pad=20)

    ax.grid(True, linestyle=":", alpha=0.6)

    # Add major events as vertical lines with labels
    for event_idx, event_name in major_events:
        if event_idx < len(kl_divergences):
            ax.axvline(
                x=event_idx, color=EVENT_COLOR, linestyle="--", linewidth=2, alpha=0.8
            )

            y_max = max(kl_divergences)
            ax.text(
                event_idx,
                y_max * 0.95,
                event_name,
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

    ax.legend(fontsize=11, loc="best")
    plt.tight_layout()
    plt.savefig(
        f"../plots/histogram_evolution/{metric}-kl_divergence-{resolution}.pdf",
        bbox_inches="tight",
        dpi=300,
    )
    plt.close()
