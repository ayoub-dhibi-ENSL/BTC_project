=====================
Visualization Gallery
=====================

This gallery showcases various visualization capabilities of btc_graph.

Setup
-----

.. code-block:: python

   import matplotlib.pyplot as plt
   import numpy as np
   import pandas as pd
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
   
   # Setup paths
   results_dir = "output/analysis"
   plots_dir = Path("plots/gallery")
   plots_dir.mkdir(parents=True, exist_ok=True)

Degree Distribution (Power Law)
-------------------------------

Bitcoin transaction networks typically exhibit power-law degree distributions.

.. code-block:: python

   """
   Plot in-degree and out-degree distributions on log-log scale.
   """
   # Load degree data
   degrees_df = pd.read_csv(f"{results_dir}/hour-000000/degrees/part-00000.csv")
   
   fig, axes = plt.subplots(1, 2, figsize=(14, 5))
   
   # In-degree distribution
   ax = axes[0]
   in_degrees = degrees_df["in_degree"].values
   in_degrees = in_degrees[in_degrees > 0]  # Remove zeros for log scale
   
   # Log-binned histogram
   bins = np.logspace(0, np.log10(in_degrees.max()), 50)
   ax.hist(in_degrees, bins=bins, density=True, alpha=0.7, color='steelblue')
   ax.set_xscale('log')
   ax.set_yscale('log')
   ax.set_xlabel('In-Degree $k_{in}$')
   ax.set_ylabel('$P(k_{in})$')
   ax.set_title('In-Degree Distribution')
   ax.grid(True, alpha=0.3)
   
   # Out-degree distribution
   ax = axes[1]
   out_degrees = degrees_df["out_degree"].values
   out_degrees = out_degrees[out_degrees > 0]
   
   bins = np.logspace(0, np.log10(out_degrees.max()), 50)
   ax.hist(out_degrees, bins=bins, density=True, alpha=0.7, color='coral')
   ax.set_xscale('log')
   ax.set_yscale('log')
   ax.set_xlabel('Out-Degree $k_{out}$')
   ax.set_ylabel('$P(k_{out})$')
   ax.set_title('Out-Degree Distribution')
   ax.grid(True, alpha=0.3)
   
   plt.tight_layout()
   plt.savefig(plots_dir / "degree_distributions.png", dpi=300)
   plt.show()

Histogram Evolution (3D Waterfall)
----------------------------------

Visualize how distributions change over time.

.. code-block:: python

   """
   Create 3D waterfall plot of in-degree evolution.
   """
   fig = plt.figure(figsize=(12, 8))
   ax = fig.add_subplot(111, projection='3d')
   
   plot_histogram_evolution(
       results_dir=results_dir,
       metric="in_degree",
       ax=ax,
       log_scale=True,
       num_bins=50,
       cmap='viridis'
   )
   
   ax.set_xlabel('Log(In-Degree)')
   ax.set_ylabel('Time (snapshots)')
   ax.set_zlabel('Frequency')
   ax.set_title('In-Degree Distribution Evolution')
   
   # Adjust view angle
   ax.view_init(elev=25, azim=45)
   
   plt.tight_layout()
   plt.savefig(plots_dir / "in_degree_evolution_3d.png", dpi=300)
   plt.show()

KL Divergence Heatmap
---------------------

Compare all pairs of snapshots.

.. code-block:: python

   """
   Create KL divergence heatmap between all snapshot pairs.
   """
   from btc_graph.io import AnalysisResultsFinder
   from scipy.special import rel_entr
   
   finder = AnalysisResultsFinder(results_dir)
   result_dirs = sorted(finder.find_all())[:10]  # First 10 for clarity
   
   # Compute pairwise KL divergence
   n = len(result_dirs)
   kl_matrix = np.zeros((n, n))
   
   distributions = []
   for rdir in result_dirs:
       df = pd.read_csv(f"{rdir}/degrees/part-00000.csv")
       degrees = df["in_degree"].values
       hist, _ = np.histogram(degrees, bins=50, density=True)
       hist = hist + 1e-10  # Smoothing
       hist = hist / hist.sum()
       distributions.append(hist)
   
   for i in range(n):
       for j in range(n):
           kl_matrix[i, j] = np.sum(rel_entr(distributions[i], distributions[j]))
   
   # Plot heatmap
   fig, ax = plt.subplots(figsize=(10, 8))
   
   im = ax.imshow(kl_matrix, cmap='YlOrRd', aspect='auto')
   
   # Labels
   labels = [Path(d).name for d in result_dirs]
   ax.set_xticks(range(n))
   ax.set_yticks(range(n))
   ax.set_xticklabels(labels, rotation=45, ha='right')
   ax.set_yticklabels(labels)
   
   ax.set_xlabel('Snapshot')
   ax.set_ylabel('Snapshot')
   ax.set_title('KL Divergence: In-Degree Distribution')
   
   # Colorbar
   cbar = plt.colorbar(im, ax=ax)
   cbar.set_label('KL Divergence')
   
   plt.tight_layout()
   plt.savefig(plots_dir / "kl_heatmap.png", dpi=300)
   plt.show()

Multi-Metric Dashboard
----------------------

Create a dashboard with multiple metrics.

.. code-block:: python

   """
   Create a comprehensive dashboard of network metrics.
   """
   fig = plt.figure(figsize=(16, 12))
   
   # Layout: 2x2 grid
   gs = fig.add_gridspec(2, 2, hspace=0.3, wspace=0.3)
   
   # 1. Vertex and Edge counts
   ax1 = fig.add_subplot(gs[0, 0])
   plot_metric_timeseries(
       results_dir=results_dir,
       metrics=["vertex_count", "edge_count"],
       ax=ax1,
       colors=['#e41a1c', '#377eb8']
   )
   ax1.set_title('Network Size Over Time')
   ax1.legend(['Vertices', 'Edges'])
   
   # 2. Density evolution
   ax2 = fig.add_subplot(gs[0, 1])
   plot_metric_timeseries(
       results_dir=results_dir,
       metrics=["density"],
       ax=ax2,
       colors=['#4daf4a']
   )
   ax2.set_title('Network Density Over Time')
   ax2.set_ylabel('Density')
   
   # 3. KL divergence
   ax3 = fig.add_subplot(gs[1, 0])
   plot_kl_divergence(
       results_dir=results_dir,
       metric="in_degree",
       ax=ax3
   )
   ax3.set_title('Structural Change (KL Divergence)')
   
   # 4. Degree distribution (final snapshot)
   ax4 = fig.add_subplot(gs[1, 1])
   degrees_df = pd.read_csv(f"{result_dirs[-1]}/degrees/part-00000.csv")
   
   in_deg = degrees_df["in_degree"].values
   in_deg = in_deg[in_deg > 0]
   bins = np.logspace(0, np.log10(in_deg.max()), 30)
   ax4.hist(in_deg, bins=bins, density=True, alpha=0.7, label='In-degree')
   
   out_deg = degrees_df["out_degree"].values
   out_deg = out_deg[out_deg > 0]
   bins = np.logspace(0, np.log10(out_deg.max()), 30)
   ax4.hist(out_deg, bins=bins, density=True, alpha=0.7, label='Out-degree')
   
   ax4.set_xscale('log')
   ax4.set_yscale('log')
   ax4.set_xlabel('Degree')
   ax4.set_ylabel('Probability')
   ax4.set_title('Final Degree Distribution')
   ax4.legend()
   ax4.grid(True, alpha=0.3)
   
   plt.suptitle('Bitcoin Transaction Network Analysis Dashboard', fontsize=14, y=1.02)
   plt.savefig(plots_dir / "dashboard.png", dpi=300, bbox_inches='tight')
   plt.show()

Publication Figure
------------------

Create a publication-ready figure with proper formatting.

.. code-block:: python

   """
   Create publication-quality figure.
   """
   import matplotlib as mpl
   
   # Publication settings
   mpl.rcParams.update({
       'font.family': 'serif',
       'font.size': 11,
       'axes.labelsize': 12,
       'axes.titlesize': 12,
       'xtick.labelsize': 10,
       'ytick.labelsize': 10,
       'legend.fontsize': 10,
       'figure.dpi': 300,
       'savefig.dpi': 300,
       'savefig.format': 'pdf',
   })
   
   fig, axes = plt.subplots(1, 3, figsize=(12, 3.5))
   
   # Panel A: Degree distribution
   ax = axes[0]
   degrees_df = pd.read_csv(f"{result_dirs[-1]}/degrees/part-00000.csv")
   in_deg = degrees_df["in_degree"].values
   in_deg = in_deg[in_deg > 0]
   
   bins = np.logspace(0, np.log10(in_deg.max()), 30)
   ax.hist(in_deg, bins=bins, density=True, alpha=0.8, color='#1f77b4', edgecolor='black', linewidth=0.5)
   ax.set_xscale('log')
   ax.set_yscale('log')
   ax.set_xlabel(r'In-degree $k$')
   ax.set_ylabel(r'$P(k)$')
   ax.set_title('(a) Degree Distribution')
   ax.text(0.05, 0.95, 'A', transform=ax.transAxes, fontsize=14, fontweight='bold', va='top')
   
   # Panel B: Size evolution
   ax = axes[1]
   plot_metric_timeseries(
       results_dir=results_dir,
       metrics=["vertex_count"],
       ax=ax,
       colors=['#2ca02c']
   )
   ax.set_xlabel('Time (hours)')
   ax.set_ylabel('Number of vertices')
   ax.set_title('(b) Network Growth')
   ax.text(0.05, 0.95, 'B', transform=ax.transAxes, fontsize=14, fontweight='bold', va='top')
   
   # Panel C: KL divergence
   ax = axes[2]
   plot_kl_divergence(
       results_dir=results_dir,
       metric="in_degree",
       ax=ax
   )
   ax.set_xlabel('Time (hours)')
   ax.set_ylabel(r'$D_{KL}$')
   ax.set_title('(c) Structural Change')
   ax.text(0.05, 0.95, 'C', transform=ax.transAxes, fontsize=14, fontweight='bold', va='top')
   
   plt.tight_layout()
   plt.savefig(plots_dir / "publication_figure.pdf", bbox_inches='tight')
   plt.savefig(plots_dir / "publication_figure.png", dpi=600, bbox_inches='tight')
   plt.show()

Animated Evolution
------------------

Create an animated GIF of distribution evolution.

.. code-block:: python

   """
   Create animated GIF of degree distribution evolution.
   """
   from matplotlib.animation import FuncAnimation
   from matplotlib.animation import PillowWriter
   
   finder = AnalysisResultsFinder(results_dir)
   result_dirs = sorted(finder.find_all())[:20]
   
   # Setup figure
   fig, ax = plt.subplots(figsize=(8, 6))
   
   def animate(frame):
       ax.clear()
       
       # Load data for this frame
       df = pd.read_csv(f"{result_dirs[frame]}/degrees/part-00000.csv")
       degrees = df["in_degree"].values
       degrees = degrees[degrees > 0]
       
       # Plot histogram
       bins = np.logspace(0, 6, 50)
       ax.hist(degrees, bins=bins, density=True, alpha=0.7, color='steelblue')
       
       ax.set_xscale('log')
       ax.set_yscale('log')
       ax.set_xlim(1, 1e6)
       ax.set_ylim(1e-8, 1)
       ax.set_xlabel('In-Degree')
       ax.set_ylabel('Probability')
       ax.set_title(f'In-Degree Distribution - {Path(result_dirs[frame]).name}')
       ax.grid(True, alpha=0.3)
       
       return ax,
   
   anim = FuncAnimation(
       fig, animate,
       frames=len(result_dirs),
       interval=500,  # 500ms per frame
       blit=True
   )
   
   # Save as GIF
   writer = PillowWriter(fps=2)
   anim.save(plots_dir / "evolution_animation.gif", writer=writer)
   plt.close()
   print("Animation saved!")

Custom Color Schemes
--------------------

.. code-block:: python

   """
   Use custom color schemes for different purposes.
   """
   # Define color schemes
   color_schemes = {
       'academic': ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'],
       'pastel': ['#aec7e8', '#ffbb78', '#98df8a', '#ff9896'],
       'dark': ['#1a1a2e', '#16213e', '#0f3460', '#e94560'],
       'nature': ['#264653', '#2a9d8f', '#e9c46a', '#f4a261'],
   }
   
   fig, axes = plt.subplots(2, 2, figsize=(14, 10))
   
   for ax, (scheme_name, colors) in zip(axes.flat, color_schemes.items()):
       plot_metric_timeseries(
           results_dir=results_dir,
           metrics=["vertex_count", "edge_count"],
           ax=ax,
           colors=colors[:2]
       )
       ax.set_title(f'Color Scheme: {scheme_name.title()}')
       ax.legend(['Vertices', 'Edges'])
   
   plt.tight_layout()
   plt.savefig(plots_dir / "color_schemes.png", dpi=300)
   plt.show()
