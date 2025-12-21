=================
btc_graph.cli
=================

The ``btc_graph.cli`` module provides the command-line interface.

Command Line Interface
----------------------

The main entry point is the ``btc-graph`` command:

.. code-block:: bash

   btc-graph <command> [options]

Commands
--------

analyze
^^^^^^^

Run the snapshot analysis pipeline:

.. code-block:: bash

   btc-graph analyze --snapshot-dir <path> --output-dir <path> [options]

**Options:**

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Option
     - Description
   * - ``--snapshot-dir``
     - Path to directory containing snapshot files (required)
   * - ``--output-dir``
     - Path to output directory for results (required)
   * - ``--max-snapshots``
     - Maximum number of snapshots to process (default: all)
   * - ``--pagerank-iterations``
     - Number of PageRank iterations (default: 10)
   * - ``--pagerank-reset-prob``
     - PageRank reset probability (default: 0.15)
   * - ``--driver-memory``
     - Spark driver memory (default: 4g)
   * - ``--verbose``
     - Enable verbose output

**Example:**

.. code-block:: bash

   btc-graph analyze \
       --snapshot-dir data/orbitaal-snapshot-hour/SNAPSHOT/EDGES \
       --output-dir output/hourly-analysis \
       --max-snapshots 20 \
       --driver-memory 8g \
       --verbose

plot
^^^^

Generate visualizations from analysis results:

.. code-block:: bash

   btc-graph plot <subcommand> --results-dir <path> [options]

**Subcommands:**

- ``evolution`` - Plot histogram evolution (3D waterfall)
- ``timeseries`` - Plot metric time series
- ``kl`` - Plot KL divergence between snapshots
- ``histogram`` - Plot single histogram

**Example:**

.. code-block:: bash

   # Histogram evolution
   btc-graph plot evolution \
       --results-dir output/analysis \
       --metric in_degree \
       --output plots/in_degree_evolution.png \
       --log-scale
   
   # Time series
   btc-graph plot timeseries \
       --results-dir output/analysis \
       --metrics vertex_count edge_count density \
       --output plots/metrics.png
   
   # KL divergence
   btc-graph plot kl \
       --results-dir output/analysis \
       --metric pagerank \
       --output plots/pagerank_kl.png

info
^^^^

Display package information:

.. code-block:: bash

   btc-graph info

**Output:**

.. code-block:: text

   btc_graph - Bitcoin Blockchain Graph Analysis
   =============================================
   Version: 0.1.0
   
   Available Commands:
     analyze   - Run snapshot analysis pipeline
     plot      - Generate visualizations from analysis results
     info      - Display package information
   
   For more information, see: https://github.com/yourusername/btc_graph

API Reference
-------------

.. automodule:: btc_graph.cli.main
   :members:
   :undoc-members:
   :show-inheritance:
