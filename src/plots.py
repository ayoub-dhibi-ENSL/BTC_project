import pandas as pd
import matplotlib.pyplot as plt
import glob


def hist_plots(year, id):
    # file_path_scalar = glob.glob(
    #     f"../data/snapshot-year-analysis/{year}-{id}/scalar/*.csv"
    # )[0]
    file_path_triangles = glob.glob(
        f"../data/snapshot-year-analysis/{year}-{id}/triangles/*.csv"
    )[0]
    # file_path_degrees = glob.glob(
    #     f"../data/snapshot-year-analysis/{year}-{id}/degrees/*.csv"
    # )[0]

    # scalar_centralities_df = pd.read_csv(file_path_scalar)
    # all_degrees_df = pd.read_csv(file_path_degrees, index_col="id")
    triangles_df = pd.read_csv(file_path_triangles, index_col="id")[
        ["triangles_count", "triangles_max_count", "lcc"]
    ]

    # Assuming your DataFrame is called df
    # Example: df = pd.read_csv("your_data.csv")

    # List numeric columns (optional: to skip non-numeric ones)
    numeric_cols = triangles_df.columns

    # Set up subplots
    n_cols = 2  # adjust layout (e.g., 2 histograms per row)
    n_rows = (len(numeric_cols) + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(12, 4 * n_rows))
    axes = axes.flatten()

    # Plot histograms
    for i, col in enumerate(numeric_cols):
        ax = axes[i]
        data = triangles_df[col].dropna()

        ax.hist(data, bins=50, color="skyblue", edgecolor="black", alpha=0.7)

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

        ax.set_title(f"Histogram of {col}")
        ax.legend()
        ax.grid(True, linestyle=":", alpha=0.6)

    # Remove unused subplots (if any)
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    # Save the figure to a file
    plt.tight_layout()
    plt.savefig(
        f"../plots/{year}-{id}_histograms_with_mean_median.pdf", bbox_inches="tight"
    )
    plt.close()


for i in range(1, 7):
    year = 2008 + i
    id = f"{i:02d}"
    print(year)
    print(id)
    hist_plots(year, id)
