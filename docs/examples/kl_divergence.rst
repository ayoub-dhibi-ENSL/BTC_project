=====================
KL Divergence Analysis
=====================

This example demonstrates how to analyze the evolution of centrality 
distributions using KL divergence between consecutive snapshots, and 
quantify the impact of major cryptocurrency events on network structure.

Overview
--------

The KL divergence :math:`D_{KL}(P_t \| P_{t+1})` measures how much the 
distribution at time :math:`t` differs from the distribution at time 
:math:`t+1`. A higher value indicates more change in the distribution.

Running the Analysis
--------------------

The KL divergence analysis is performed using a dedicated script:

.. code-block:: bash

   # Run high-resolution analysis (every 100th snapshot = 1,048 samples)
   conda run -n BTC_project python scripts/kl_divergence_analysis.py \
       --sample-rate 100 \
       --output-dir results/kl-analysis-highres

   # Resume from checkpoint if interrupted
   python scripts/kl_divergence_analysis.py \
       --resume \
       --output-dir results/kl-analysis-highres

   # Lower resolution for faster iteration (every 500th = 210 samples)
   python scripts/kl_divergence_analysis.py \
       --sample-rate 500 \
       --output-dir results/kl-analysis

Output Files
------------

The analysis generates several CSV files in the output directory:

**kl_divergences.csv**

Contains KL divergence values for each centrality:

.. code-block:: text

   snapshot_id,hour,date,kl_in_degree,kl_out_degree,kl_degree,kl_clustering
   hour-000000,0,2009-01-03 18:00:00,0.0,0.0,0.0,0.0
   hour-000500,500,2009-01-24 14:00:00,0.125,0.089,0.156,0.042
   ...

**global_properties.csv**

Network-level metrics per snapshot:

.. code-block:: text

   snapshot_id,hour,date,num_vertices,num_edges,density,global_cc,avg_cc
   hour-000000,0,2009-01-03 18:00:00,2,1,1.0,0.0,0.0
   hour-000500,500,2009-01-24 14:00:00,6,5,0.167,0.0,0.0
   ...

**kl_correlations.csv**

Rolling correlations between KL divergences:

.. code-block:: text

   hour,date,corr_kl_in_degree_kl_out_degree,corr_kl_in_degree_kl_degree,...
   ...

Generating Plots
----------------

Generate visualizations from the analysis results:

.. code-block:: bash

   # Basic plots (full date range 2009-2020)
   python scripts/plot_kl_results.py \
       --input-dir results/kl-analysis-highres \
       --output-dir plots/kl-analysis

   # Plots with event markers (recommended)
   python scripts/plot_kl_with_events.py \
       --input-dir results/kl-analysis-highres \
       --output-dir plots/kl-events \
       --start-year 2012

Event Impact Analysis
---------------------

The ``plot_kl_with_events.py`` script analyzes 6 major cryptocurrency events:

.. list-table:: Crypto Events Analyzed
   :header-rows: 1
   :widths: 30 20 50

   * - Event
     - Date
     - Description
   * - Bitcoin Halving #1
     - 2012-11-28
     - Block reward reduced from 50 to 25 BTC
   * - Silk Road Shutdown
     - 2013-10-02
     - FBI seized major darknet marketplace
   * - Mt. Gox Collapse
     - 2014-02-24
     - Largest exchange hack, $450M lost
   * - Bitfinex Hack
     - 2016-08-02
     - $72M stolen from exchange
   * - Bitcoin Halving #2
     - 2016-07-09
     - Block reward reduced from 25 to 12.5 BTC
   * - Bitcoin Halving #3
     - 2020-05-11
     - Block reward reduced from 12.5 to 6.25 BTC

**Key Findings from High-Resolution Analysis (1,048 snapshots):**

.. list-table:: Event Impact on KL Divergence
   :header-rows: 1
   :widths: 25 25 25 25

   * - Event
     - Degree KL Change
     - Clustering KL Change
     - Interpretation
   * - **Bitfinex Hack**
     - **-70%**
     - **-76%**
     - Major stabilization after hack
   * - Mt. Gox
     - -26%
     - +14%
     - Degree stabilized, clustering disrupted
   * - Halving #1
     - -47%
     - -46%
     - Both distributions stabilized
   * - Halving #3
     - +39%
     - +92%
     - Significant structural disruption

Generated Plots
---------------

The event analysis script generates:

1. **kl_evolution_with_events.png** - Full timeline with vertical event markers
2. **global_properties_with_events.png** - Network size, density, CC with events
3. **event_zoom_*.png** - Zoomed views (±4 weeks) around each event:

   - ``event_zoom_halving_1.png``
   - ``event_zoom_silk_road.png``
   - ``event_zoom_mt_gox.png``
   - ``event_zoom_bitfinex.png``
   - ``event_zoom_halving_2.png``
   - ``event_zoom_halving_3.png``

4. **events_impact_summary.png** - Comparative bar chart of impacts
5. **events_statistics.csv** - Detailed before/after statistics
6. **events_reference.md** - Event descriptions and dates

Custom Analysis
---------------

You can also work with the results programmatically:

.. code-block:: python

   import pandas as pd
   import matplotlib.pyplot as plt
   
   # Load results
   kl_df = pd.read_csv('results/kl-analysis-highres/kl_divergences.csv')
   kl_df['date'] = pd.to_datetime(kl_df['date'])
   
   # Filter from 2015 onwards
   kl_df = kl_df[kl_df['date'] >= '2015-01-01']
   
   # Plot degree KL divergence
   fig, ax = plt.subplots(figsize=(12, 6))
   ax.plot(kl_df['date'], kl_df['kl_degree'], 'b-', alpha=0.7)
   ax.fill_between(kl_df['date'], kl_df['kl_degree'], alpha=0.3)
   ax.set_xlabel('Date')
   ax.set_ylabel('KL Divergence')
   ax.set_title('Degree Distribution Change: 2015-2020')
   plt.tight_layout()
   plt.savefig('plots/custom_kl.png', dpi=150)

   # Compute statistics
   print(f"Mean KL (degree): {kl_df['kl_degree'].mean():.4f}")
   print(f"Max KL (degree): {kl_df['kl_degree'].max():.4f}")
   print(f"Date of max: {kl_df.loc[kl_df['kl_degree'].idxmax(), 'date']}")

Event-Specific Analysis
-----------------------

To analyze specific events in detail:

.. code-block:: python

   import pandas as pd
   from datetime import timedelta

   # Load data
   kl_df = pd.read_csv('results/kl-analysis-highres/kl_divergences.csv')
   kl_df['date'] = pd.to_datetime(kl_df['date'])

   # Define event
   event_date = pd.Timestamp('2016-08-02')  # Bitfinex Hack
   window = timedelta(weeks=4)

   # Extract window around event
   mask = (kl_df['date'] >= event_date - window) & (kl_df['date'] <= event_date + window)
   event_window = kl_df[mask]

   # Split into before/after
   before = event_window[event_window['date'] < event_date]
   after = event_window[event_window['date'] >= event_date]

   # Compute change
   before_mean = before['kl_degree'].mean()
   after_mean = after['kl_degree'].mean()
   pct_change = 100 * (after_mean - before_mean) / before_mean

   print(f"Before: {before_mean:.4f}")
   print(f"After: {after_mean:.4f}")
   print(f"Change: {pct_change:+.1f}%")

Understanding the Results
-------------------------

**Interpretation of KL Values**

- **KL ≈ 0**: Consecutive distributions are nearly identical
- **KL < 0.1**: Minor changes in distribution shape
- **KL > 0.5**: Significant structural changes in the network
- **KL > 1.0**: Major distributional shifts (often in early network history)

**Early vs. Mature Network**

The Bitcoin network shows different behavior in different periods:

- **2009-2011**: High variability, small network, erratic KL values
- **2012-2016**: Network stabilization, moderate KL values
- **2017-2020**: Mature network, relatively stable distributions

This is why the ``--start-year`` option is useful for filtering out the 
noisy early period.

Technical Details
-----------------

**Shared Bin Grids**

The KL divergence is computed using shared bin grids between consecutive 
snapshots. This ensures that we're comparing the same physical quantities 
rather than just relative positions within each distribution.

**Smoothing**

A small epsilon (1e-10) is added to avoid log(0) issues when computing 
KL divergence. The distributions are re-normalized after smoothing.

**Centralities Analyzed**

- **In-Degree**: Number of incoming transactions to each address
- **Out-Degree**: Number of outgoing transactions from each address  
- **Total Degree**: Sum of in-degree and out-degree
- **Clustering Coefficient**: Local triangle density around each node
