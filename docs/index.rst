.. btc_graph documentation master file

==========================================
btc_graph: Bitcoin Blockchain Graph Analysis
==========================================

.. image:: https://img.shields.io/badge/python-3.10+-blue.svg
   :alt: Python Version

.. image:: https://img.shields.io/badge/spark-4.0+-orange.svg
   :alt: Spark Version

.. image:: https://img.shields.io/badge/license-MIT-green.svg
   :alt: License

.. image:: https://img.shields.io/badge/tests-144%20passed-brightgreen.svg
   :alt: Tests

**btc_graph** is a Python package for analyzing the structural properties of 
the Bitcoin blockchain transaction graph in response to major cryptocurrency events.

It provides tools for:

- **Graph Construction**: Build transaction graphs from blockchain snapshots
- **Centrality Analysis**: Compute degree distributions, PageRank, and triangle centralities
- **KL Divergence Analysis**: Track distributional changes :math:`D_{KL}(P_t \| P_{t+1})` between consecutive snapshots
- **Event Impact Analysis**: Quantify how major crypto events affect network structure
- **Temporal Evolution**: Track how graph metrics evolve over time (2009-2020)
- **Global Properties**: Monitor network density, clustering coefficient, and size
- **Visualization**: Generate publication-ready plots with event markers and zoomed views
- **Scalable Processing**: Built on PySpark and GraphFrames for big data (104k+ snapshots)

Key Findings
------------

Our high-resolution analysis of **1,048 hourly snapshots** (2009-2020) reveals:

.. list-table:: Event Impact on Network Structure
   :header-rows: 1
   :widths: 30 20 25 25

   * - Event
     - Date
     - Degree KL Change
     - Clustering KL Change
   * - **Bitfinex Hack**
     - 2016-08-02
     - **-70%**
     - **-76%**
   * - Mt. Gox Collapse
     - 2014-02-24
     - -26%
     - +14%
   * - Halving #1
     - 2012-11-28
     - -47%
     - -46%
   * - Halving #3
     - 2020-05-11
     - +39%
     - +92%

Quick Example
-------------

.. code-block:: python

   from btc_graph import SnapshotAnalysisPipeline, create_spark_session

   # Create Spark session with GraphFrames
   spark = create_spark_session("btc-analysis")
   
   # Run analysis pipeline
   pipeline = SnapshotAnalysisPipeline(spark)
   pipeline.run(
       input_dir="data/orbitaal-snapshot-hour/SNAPSHOT/EDGES",
       output_dir="output/analysis",
       resolution="hour",
       max_snapshots=10
   )

KL Divergence Analysis
----------------------

Analyze how centrality distributions change between consecutive snapshots:

.. code-block:: bash

   # Run high-resolution KL divergence analysis
   python scripts/kl_divergence_analysis.py \
       --sample-rate 100 \
       --output-dir results/kl-analysis-highres

   # Generate plots with event markers (filtered from 2012)
   python scripts/plot_kl_with_events.py \
       --input-dir results/kl-analysis-highres \
       --output-dir plots/kl-analysis-highres \
       --start-year 2012

This computes :math:`D_{KL}(P_t \| P_{t+1})` for each centrality measure, with vertical
markers for major crypto events and zoomed views around each event.

Installation
------------

From source:

.. code-block:: bash

   git clone https://github.com/ayoub-dhibi-ENSL/BTC_project.git
   cd BTC_project
   pip install -e ".[dev]"

Using conda:

.. code-block:: bash

   conda env create -f environment.yml
   conda activate BTC_project
   pip install -e .

Command Line Interface
----------------------

The package provides a CLI for common tasks:

.. code-block:: bash

   # Run snapshot analysis
   btc-graph analyze --snapshot-dir data/snapshots --output-dir output/ --max-snapshots 20
   
   # Generate evolution plots
   btc-graph plot evolution --results-dir output/analysis --metric in_degree
   
   # Show package info
   btc-graph info

User Guide
----------

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   quickstart
   installation

.. toctree::
   :maxdepth: 2
   :caption: Tutorials

   tutorials/basic_analysis
   tutorials/visualization
   tutorials/custom_pipelines

.. toctree::
   :maxdepth: 2
   :caption: Examples

   examples/index

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/index

.. toctree::
   :maxdepth: 1
   :caption: Development

   developer_guide
   changelog

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
