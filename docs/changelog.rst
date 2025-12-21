=========
Changelog
=========

All notable changes to btc_graph will be documented in this file.

The format is based on `Keep a Changelog <https://keepachangelog.com/en/1.0.0/>`_,
and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.

[0.2.0] - 2025-12-20
--------------------

KL Divergence & Event Impact Analysis Release.

Added
^^^^^

**KL Divergence Analysis**

- ``scripts/kl_divergence_analysis.py`` for computing KL divergence between consecutive snapshots
- Proper KL computation using shared bin grids for accurate distribution comparison
- Support for all centrality measures: in-degree, out-degree, total degree, clustering coefficient
- Global graph properties tracking: density, clustering coefficient, network size
- Rolling correlation analysis between KL divergences of different centralities
- Checkpoint/resume capability for long-running analyses
- Batch processing with memory management

**Event Impact Analysis**

- ``scripts/plot_kl_with_events.py`` for event impact visualization
- Analysis of 6 major crypto events:
  
  - Bitcoin Halving #1 (2012-11-28)
  - Silk Road FBI Shutdown (2013-10-02)
  - Mt. Gox Collapse (2014-02-24)
  - Bitfinex Hack (2016-08-02)
  - Bitcoin Halving #2 (2016-07-09)
  - Bitcoin Halving #3 (2020-05-11)

- Zoomed views (±4 weeks) around each event
- Before/after KL divergence statistics
- Event impact summary visualizations
- Color-coded event markers by category (halving, hack, regulatory)

**High-Resolution Analysis**

- Sample rate 100 for 1,048 snapshots (5x improvement over initial 210)
- Better temporal resolution for detecting event impacts
- Key findings:

  - Bitfinex Hack: -70% degree KL, -76% clustering KL
  - Mt. Gox: -26% degree KL, -52% out-degree KL
  - Halving #3: +39% degree KL, +92% clustering KL

**Visualization**

- ``scripts/plot_kl_results.py`` for basic KL divergence visualization
- ``--start-year`` option to filter plots by date (e.g., start from 2012)
- KL evolution panels with event markers
- Global properties evolution plots
- Event zoom plots with ±4 weeks window
- Events impact summary bar charts

**Scalability**

- Successfully analyzed 104,823 hourly snapshots (2009-2020)
- Sample rate option for faster iteration during development
- Efficient memory management with periodic garbage collection

Changed
^^^^^^^

- Updated ``SnapshotAnalysisPipeline`` API for cleaner usage
- Improved documentation with KL divergence and event impact examples
- Updated README with key findings table

[0.1.0] - 2024-XX-XX
--------------------

Initial release.

Added
^^^^^

**Core Module**

- ``GraphBuilder`` class for building GraphFrames from edge DataFrames
- ``compute_degrees()`` for in-degree and out-degree centralities
- ``compute_pagerank()`` for PageRank centrality
- ``compute_triangle_centralities()`` for triangle counts
- ``compute_density()`` for graph density
- ``compute_graph_summary()`` for comprehensive graph metrics

**I/O Module**

- ``SnapshotPathFinder`` for discovering snapshot files
- ``NodeTableFinder`` for finding node table files
- ``AnalysisResultsFinder`` for finding analysis output directories
- ``CSVExporter`` for exporting DataFrames and scalars
- ``create_spark_session()`` with GraphFrames configuration
- ``create_test_spark_session()`` for unit testing
- ``SNAPSHOT_SCHEMA`` and ``NODE_TABLE_SCHEMA`` definitions

**Workflows Module**

- ``SnapshotAnalysisPipeline`` for batch snapshot analysis
- ``AnalysisResult`` dataclass for pipeline outputs

**Visualization Module**

- ``plot_histogram_evolution()`` for 3D waterfall plots
- ``plot_metric_timeseries()`` for scalar metric tracking
- ``plot_degree_distribution()`` for power-law visualization
- ``plot_metric_histograms()`` for multi-metric comparison
- ``apply_style()`` for consistent plot styling

**CLI**

- ``btc-graph analyze`` command for running pipelines
- ``btc-graph plot`` command for generating visualizations
- ``btc-graph info`` command for package information

**Documentation**

- Sphinx documentation with ReadTheDocs theme
- API reference with autodoc
- Quickstart guide
- Tutorials for basic analysis, visualization, and custom pipelines
- Example gallery
- Developer guide

**Testing**

- Comprehensive test suite (144 tests)
- pytest fixtures for Spark sessions
- Coverage reporting

Dependencies
^^^^^^^^^^^^

- PySpark 4.0+
- GraphFrames 0.9.3 (Spark 4 / Scala 2.13)
- matplotlib 3.0+
- pandas 2.0+
- numpy 1.20+
- scipy (for KL divergence computation)

[Unreleased]
------------

Nothing yet.
