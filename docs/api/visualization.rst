==========================
btc_graph.visualization
==========================

The ``btc_graph.visualization`` module provides plotting and visualization
utilities for analysis results.

Style Configuration
-------------------

.. automodule:: btc_graph.visualization.style
   :members:
   :undoc-members:
   :show-inheritance:

Histograms
----------

.. automodule:: btc_graph.visualization.histograms
   :members:
   :undoc-members:
   :show-inheritance:

Evolution Plots
---------------

.. automodule:: btc_graph.visualization.evolution
   :members:
   :undoc-members:
   :show-inheritance:

Example Gallery
---------------

Histogram Evolution
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from btc_graph.visualization import plot_histogram_evolution
   
   plot_histogram_evolution(
       results_dir="output/analysis",
       metric="in_degree",
       output_path="plots/in_degree_evolution.png",
       log_scale=True,
       num_bins=50
   )

KL Divergence
^^^^^^^^^^^^^

.. code-block:: python

   from btc_graph.visualization import plot_kl_divergence
   
   plot_kl_divergence(
       results_dir="output/analysis",
       metric="in_degree",
       output_path="plots/kl_divergence.png"
   )

Time Series
^^^^^^^^^^^

.. code-block:: python

   from btc_graph.visualization import plot_metric_timeseries
   
   plot_metric_timeseries(
       results_dir="output/analysis",
       metrics=["vertex_count", "edge_count", "density"],
       output_path="plots/metrics.png"
   )
