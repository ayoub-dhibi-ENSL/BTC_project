==========
Quickstart
==========

This guide will help you get started with btc_graph in 5 minutes.

Setup
-----

First, ensure you have btc_graph installed (see :doc:`installation`).

.. code-block:: bash

   conda activate BTC_project

Basic Analysis
--------------

The simplest way to analyze blockchain snapshots is using the CLI:

.. code-block:: bash

   btc-graph analyze \
       --snapshot-dir data/orbitaal-snapshot-hour/SNAPSHOT/EDGES \
       --output-dir output/my-analysis \
       --max-snapshots 5

This will:

1. Load transaction snapshots from the specified directory
2. Build a graph for each snapshot
3. Compute centrality metrics (degrees, PageRank, triangles)
4. Save results to the output directory

Python API
----------

For more control, use the Python API directly:

.. code-block:: python

   from btc_graph import (
       SnapshotAnalysisPipeline,
       create_spark_session,
       stop_spark_session
   )
   
   # Create Spark session
   spark = create_spark_session("my-analysis", driver_memory="4g")
   
   try:
       # Configure pipeline
       pipeline = SnapshotAnalysisPipeline(spark)
       
       # Run analysis
       pipeline.run(
           input_dir="data/orbitaal-snapshot-hour/SNAPSHOT/EDGES",
           output_dir="output/my-analysis",
           resolution="hour",
           max_snapshots=5
       )
   
   finally:
       stop_spark_session(spark)

KL Divergence Analysis
----------------------

Analyze how centrality distributions evolve between consecutive snapshots:

.. code-block:: bash

   # Run high-resolution KL divergence analysis (every 100th snapshot = 1,048 samples)
   conda run -n BTC_project python scripts/kl_divergence_analysis.py \
       --sample-rate 100 \
       --output-dir results/kl-analysis-highres

   # Generate plots with event markers (filtered from 2012)
   python scripts/plot_kl_with_events.py \
       --input-dir results/kl-analysis-highres \
       --output-dir plots/kl-analysis-highres \
       --start-year 2012

This computes:

- **KL Divergence**: :math:`D_{KL}(P_t \| P_{t+1})` for in-degree, out-degree, degree, and clustering coefficient
- **Global Properties**: Network size, density, and clustering coefficient over time
- **Event Impact**: Vertical markers and zoomed views around major crypto events
- **Impact Statistics**: Before/after comparison of KL divergences for each event

Output files in ``results/kl-analysis-highres/``:

- ``kl_divergences.csv`` - KL divergence values per snapshot
- ``global_properties.csv`` - Network metrics per snapshot
- ``kl_correlations.csv`` - Correlation evolution
- ``summary.json`` - Analysis metadata

Event Impact Analysis
---------------------

The ``plot_kl_with_events.py`` script analyzes how major crypto events affected network structure:

.. code-block:: bash

   python scripts/plot_kl_with_events.py \
       --input-dir results/kl-analysis-highres \
       --output-dir plots/kl-events \
       --start-year 2012

**Events Analyzed:**

1. **Bitcoin Halving #1** (2012-11-28) - Block reward 50→25 BTC
2. **Silk Road Shutdown** (2013-10-02) - FBI seized darknet marketplace
3. **Mt. Gox Collapse** (2014-02-24) - Exchange hack, $450M lost
4. **Bitfinex Hack** (2016-08-02) - $72M stolen
5. **Bitcoin Halving #2** (2016-07-09) - Block reward 25→12.5 BTC
6. **Bitcoin Halving #3** (2020-05-11) - Block reward 12.5→6.25 BTC

**Generated outputs:**

- ``kl_evolution_with_events.png`` - Full timeline with event markers
- ``global_properties_with_events.png`` - Network metrics with events
- ``event_zoom_*.png`` - Zoomed views (±4 weeks around each event)
- ``events_impact_summary.png`` - Comparative bar chart
- ``events_statistics.csv`` - Detailed before/after statistics

Visualization
-------------

Generate plots from analysis results:

.. code-block:: bash

   # Plot degree distribution evolution
   btc-graph plot evolution \
       --results-dir output/my-analysis \
       --metric in_degree \
       --output plots/in_degree_evolution.png

Or use Python:

.. code-block:: python

   from btc_graph.visualization import (
       plot_histogram_evolution,
       plot_metric_timeseries,
   )
   
   # Histogram evolution (3D waterfall plot)
   plot_histogram_evolution(
       results_dir="output/my-analysis",
       metric="in_degree",
       output_path="plots/in_degree_evolution.png",
       log_scale=True
   )
   
   # Time series of scalar metrics
   plot_metric_timeseries(
       results_dir="output/my-analysis",
       metrics=["vertex_count", "edge_count", "density"],
       output_path="plots/metrics_over_time.png"
   )

Working with Results
--------------------

Load and explore analysis results:

.. code-block:: python

   import pandas as pd
   
   # Load KL divergence results
   kl_df = pd.read_csv("results/kl-analysis/kl_divergences.csv")
   kl_df['date'] = pd.to_datetime(kl_df['date'])
   
   # Filter from 2012 onwards
   kl_df = kl_df[kl_df['date'] >= '2012-01-01']
   
   # Plot evolution
   import matplotlib.pyplot as plt
   plt.figure(figsize=(12, 6))
   plt.plot(kl_df['date'], kl_df['kl_degree'], label='Degree')
   plt.xlabel('Date')
   plt.ylabel('KL Divergence')
   plt.title('Degree Distribution Change Over Time')
   plt.legend()
   plt.savefig('plots/kl_custom.png')

Next Steps
----------

- :doc:`tutorials/basic_analysis` - In-depth analysis tutorial
- :doc:`tutorials/visualization` - Advanced visualization techniques
- :doc:`api/index` - Complete API reference
- :doc:`examples/index` - More examples
