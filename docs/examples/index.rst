========
Examples
========

This section contains complete, runnable examples demonstrating 
various use cases for btc_graph.

.. toctree::
   :maxdepth: 1
   :caption: Examples

   basic_usage
   batch_analysis
   kl_divergence
   visualization_gallery

Basic Usage
-----------

Simple examples to get started:

- Loading snapshot data
- Building transaction graphs
- Computing basic metrics

:doc:`basic_usage`

Batch Analysis
--------------

Process multiple snapshots efficiently:

- Using the analysis pipeline
- Parallel processing considerations
- Memory management

:doc:`batch_analysis`

KL Divergence Analysis
----------------------

Analyze how centrality distributions evolve over time:

- Computing :math:`D_{KL}(P_t \| P_{t+1})` between consecutive snapshots
- Tracking global network properties
- Correlation analysis between centralities
- Filtering by date range

:doc:`kl_divergence`

Visualization Gallery
---------------------

Collection of visualization examples:

- Degree distribution plots
- Temporal evolution charts
- Publication-ready figures

:doc:`visualization_gallery`
