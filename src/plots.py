import pandas as pd
import matplotlib.pyplot as plt
import glob


def hist_plots(resolution, id):
    """
    Generates and saves histograms for triangle-related metrics from a CSV file for a given year and id.
    The function reads triangle metrics (`triangles_count`, `triangles_max_count`, `lcc`) from a CSV file,
    creates histograms for each metric, and overlays mean and median values on each plot. The resulting
    figure is saved as a PDF file.
    Parameters
    ----------
    resolution : int or str
        The resolution (year or hour) corresponding to the dataset to be analyzed.
    id : int or str
        The identifier for the dataset snapshot.
    Returns
    -------
    None
        The function saves the histogram plots to a PDF file and does not return any value.
    Notes
    -----
    - The CSV file is expected to be located at
      "../data/snapshot-year-analysis/{year}-{id}/triangles/*.csv".
    - The output PDF is saved to "../plots/{year}-{id}_histograms_with_mean_median.pdf".
    - Requires pandas, matplotlib, and glob libraries.
    """
    # file_path_scalar = glob.glob(
    #     f"../data/snapshot-year-analysis/{year}-{id}/scalar/*.csv"
    # )[0]
    file_path_triangles = glob.glob(
        f"../data/snapshot-year-analysis/{resolution}-{id}/triangles/*.csv"
    )[0]  # returns a list so [0] returns a string
    file_path_degrees = glob.glob(
        f"../data/snapshot-year-analysis/{resolution}-{id}/degrees/*.csv"
    )[0]

    # scalar_centralities_df = pd.read_csv(file_path_scalar)
    all_degrees_df = pd.read_csv(file_path_degrees, index_col="id")
    triangles_df = pd.read_csv(file_path_triangles, index_col="id")[
        ["triangles_count", "triangles_max_count", "lcc"]
    ]

    # remove isolated nodes
    triangles_df = triangles_df[triangles_df != 0]
    triangles_df = triangles_df[triangles_df < 5000]
    all_degrees_df = all_degrees_df[all_degrees_df != 0]
    all_degrees_df = all_degrees_df[all_degrees_df < 5000]
    make_save_hist("degrees", all_degrees_df, resolution, id)
    make_save_hist("triangles", triangles_df, resolution, id)


def make_save_hist(name, df, resolution, id):
    # List numeric columns (optional: to skip non-numeric ones)
    numeric_cols = df.columns

    # Set up subplots
    n_cols = 2  # adjust layout (e.g., 2 histograms per row)
    n_rows = (len(numeric_cols) + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(12, 4 * n_rows))
    axes = axes.flatten()

    # Plot histograms
    for i, col in enumerate(numeric_cols):
        ax = axes[i]
        data = df[col].dropna()

        ax.hist(data, bins=250, color="skyblue", edgecolor="black", alpha=0.7, log=True)

        mean_val = data.mean()
        median_val = data.median()

        ax.axvline(
            mean_val,
            color="red",
            linestyle="--",
            linewidth=2,
            label=f"Mean = {mean_val:.2e}",
        )
        ax.axvline(
            median_val,
            color="green",
            linestyle="-.",
            linewidth=2,
            label=f"Median = {median_val:.2e}",
        )

        # ax.set_xscale("log")
        # ax.set_yscale("log")

        if col == "lcc":
            title = "Histogram of local clustering coefficient"

        elif col == "triangles_count":
            title = "Histogram of number of triangles the node is included in."

        elif col == "triangles_max_count":
            title = "Histogram of number of triangles the node could be included in."

        elif col == "triangles_max_count":
            title = "Histogram of number of triangles the node could be included in."

        else:
            title = f"Histogram of {col}"

        ax.set_title(title)
        ax.legend()
        ax.grid(True, linestyle=":", alpha=0.6)

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
