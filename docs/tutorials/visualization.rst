=============
Visualization
=============

This tutorial covers how to create publication-ready visualizations from
your blockchain analysis results.

Overview
--------

btc_graph provides several visualization functions:

- **Histogram Evolution** - 3D waterfall plots showing distribution changes
- **KL Divergence** - Measure distribution differences between snapshots
- **Time Series** - Track scalar metrics over time
- **Degree Distributions** - Plot power-law distributions

Prerequisites
-------------

Ensure you have analysis results from the pipeline. If not, run:

.. code-block:: bash

   btc-graph analyze \
       --snapshot-dir data/orbitaal-snapshot-hour/SNAPSHOT/EDGES \
       --output-dir output/for-viz \
       --max-snapshots 10

Setup
-----

.. code-block:: python

   import matplotlib.pyplot as plt
   from pathlib import Path
   
   from btc_graph.visualization import (
       plot_histogram_evolution,
       plot_kl_divergence,
       plot_metric_timeseries,
       plot_degree_distribution,
       plot_metric_histograms,
       apply_style,
   )
   
   # Apply consistent styling
   apply_style()
   
   # Paths
   results_dir = "output/for-viz"
   plots_dir = Path("plots/tutorial")
   plots_dir.mkdir(parents=True, exist_ok=True)

Histogram Evolution
-------------------

The histogram evolution plot shows how degree distributions change over time
using a 3D waterfall visualization.

.. code-block:: python

   # In-degree evolution
   plot_histogram_evolution(
       results_dir=results_dir,
       metric="in_degree",
       output_path=plots_dir / "in_degree_evolution.png",
       log_scale=True,
       num_bins=50,
       title="In-Degree Distribution Evolution"
   )

**Parameters:**

- ``metric`` - One of: ``in_degree``, ``out_degree``, ``pagerank``, ``triangle_count``
- ``log_scale`` - Use logarithmic scale for values (recommended for power-law)
- ``num_bins`` - Number of histogram bins
- ``cmap`` - Matplotlib colormap (default: ``viridis``)

**Comparing In-degree and Out-degree:**

.. code-block:: python

   fig, axes = plt.subplots(1, 2, figsize=(16, 6), subplot_kw={'projection': '3d'})
   
   plot_histogram_evolution(
       results_dir=results_dir,
       metric="in_degree",
       ax=axes[0],
       log_scale=True
   )
   axes[0].set_title("In-Degree")
   
   plot_histogram_evolution(
       results_dir=results_dir,
       metric="out_degree",
       ax=axes[1],
       log_scale=True
   )
   axes[1].set_title("Out-Degree")
   
   plt.tight_layout()
   plt.savefig(plots_dir / "degree_comparison.png", dpi=300)

KL Divergence
-------------

Kullback-Leibler divergence measures how much consecutive distributions differ.
Spikes indicate significant structural changes.

.. code-block:: python

   # KL divergence for in-degree
   plot_kl_divergence(
       results_dir=results_dir,
       metric="in_degree",
       output_path=plots_dir / "kl_divergence.png",
       title="KL Divergence: In-Degree Distribution"
   )

**Multi-metric comparison:**

.. code-block:: python

   fig, axes = plt.subplots(2, 2, figsize=(14, 10))
   
   metrics = ["in_degree", "out_degree", "pagerank", "triangle_count"]
   
   for ax, metric in zip(axes.flat, metrics):
       plot_kl_divergence(
           results_dir=results_dir,
           metric=metric,
           ax=ax,
           title=f"KL Divergence: {metric.replace('_', ' ').title()}"
       )
   
   plt.tight_layout()
   plt.savefig(plots_dir / "kl_all_metrics.png", dpi=300)

Time Series
-----------

Track how scalar metrics evolve over time:

.. code-block:: python

   # Graph size metrics
   plot_metric_timeseries(
       results_dir=results_dir,
       metrics=["vertex_count", "edge_count"],
       output_path=plots_dir / "size_timeseries.png",
       title="Graph Size Over Time"
   )
   
   # Density and clustering
   plot_metric_timeseries(
       results_dir=results_dir,
       metrics=["density"],
       output_path=plots_dir / "density_timeseries.png",
       title="Graph Density Over Time"
   )

**Custom styling:**

.. code-block:: python

   fig, ax = plt.subplots(figsize=(12, 6))
   
   plot_metric_timeseries(
       results_dir=results_dir,
       metrics=["vertex_count", "edge_count"],
       ax=ax,
       colors=["#e41a1c", "#377eb8"],
       linestyle="--",
       marker="s"
   )
   
   ax.set_xlabel("Snapshot", fontsize=12)
   ax.set_ylabel("Count", fontsize=12)
   ax.legend(fontsize=10)
   ax.grid(True, alpha=0.3)
   
   plt.tight_layout()
   plt.savefig(plots_dir / "custom_timeseries.png", dpi=300)

Degree Distributions
--------------------

Plot degree distributions with power-law fitting:

.. code-block:: python

   import pandas as pd
   
   # Load degree data
   degrees_df = pd.read_csv(f"{results_dir}/hour-000000/degrees/part-00000.csv")
   
   # Plot in-degree distribution
   plot_degree_distribution(
       degrees=degrees_df["in_degree"].values,
       output_path=plots_dir / "in_degree_dist.png",
       log_log=True,
       fit_power_law=True,
       title="In-Degree Distribution"
   )

**Side-by-side comparison:**

.. code-block:: python

   fig, axes = plt.subplots(1, 2, figsize=(14, 5))
   
   plot_degree_distribution(
       degrees=degrees_df["in_degree"].values,
       ax=axes[0],
       log_log=True,
       title="In-Degree"
   )
   
   plot_degree_distribution(
       degrees=degrees_df["out_degree"].values,
       ax=axes[1],
       log_log=True,
       title="Out-Degree"
   )
   
   plt.tight_layout()
   plt.savefig(plots_dir / "degree_distributions.png", dpi=300)

Combined Histograms
-------------------

Create histograms for multiple metrics:

.. code-block:: python

   plot_metric_histograms(
       results_dir=results_dir,
       snapshot_id="hour-000000",
       metrics=["in_degree", "out_degree", "pagerank"],
       output_path=plots_dir / "metric_histograms.png",
       log_scale=True,
       num_bins=50
   )

Publication-Ready Figures
-------------------------

For publication, use high DPI and adjust fonts:

.. code-block:: python

   import matplotlib as mpl
   
   # Publication settings
   mpl.rcParams.update({
       'font.size': 14,
       'axes.labelsize': 16,
       'axes.titlesize': 18,
       'xtick.labelsize': 12,
       'ytick.labelsize': 12,
       'legend.fontsize': 12,
       'figure.dpi': 300,
       'savefig.dpi': 300,
       'savefig.bbox': 'tight',
       'savefig.pad_inches': 0.1,
   })
   
   # Create figure
   fig, ax = plt.subplots(figsize=(8, 6))
   
   plot_kl_divergence(
       results_dir=results_dir,
       metric="in_degree",
       ax=ax,
       title="Structural Change Detection"
   )
   
   ax.set_xlabel("Time (hours)")
   ax.set_ylabel(r"$D_{KL}(P_t || P_{t-1})$")  # LaTeX formatting
   
   plt.savefig(
       plots_dir / "publication_kl.pdf",
       format="pdf",
       bbox_inches="tight"
   )

Complete Example
----------------

.. code-block:: python

   #!/usr/bin/env python
   """Generate all visualization plots."""
   
   from pathlib import Path
   import matplotlib.pyplot as plt
   
   from btc_graph.visualization import (
       plot_histogram_evolution,
       plot_kl_divergence,
       plot_metric_timeseries,
       apply_style,
   )
   
   def main():
       apply_style()
       
       results_dir = "output/analysis"
       plots_dir = Path("plots/final")
       plots_dir.mkdir(parents=True, exist_ok=True)
       
       # 1. Histogram evolution
       for metric in ["in_degree", "out_degree", "pagerank"]:
           plot_histogram_evolution(
               results_dir=results_dir,
               metric=metric,
               output_path=plots_dir / f"{metric}_evolution.png",
               log_scale=True
           )
           print(f"Created {metric}_evolution.png")
       
       # 2. KL divergence comparison
       fig, axes = plt.subplots(2, 2, figsize=(14, 10))
       for ax, metric in zip(axes.flat, ["in_degree", "out_degree", "pagerank", "triangle_count"]):
           plot_kl_divergence(
               results_dir=results_dir,
               metric=metric,
               ax=ax
           )
       plt.tight_layout()
       plt.savefig(plots_dir / "kl_comparison.png", dpi=300)
       print("Created kl_comparison.png")
       
       # 3. Time series
       plot_metric_timeseries(
           results_dir=results_dir,
           metrics=["vertex_count", "edge_count", "density"],
           output_path=plots_dir / "metrics_timeseries.png"
       )
       print("Created metrics_timeseries.png")
       
       print(f"\nAll plots saved to {plots_dir}")
   
   if __name__ == "__main__":
       main()

Next Steps
----------

- :doc:`custom_pipelines` - Build custom analysis workflows
- :doc:`../api/visualization` - Full API reference
- :doc:`../examples/index` - More examples
