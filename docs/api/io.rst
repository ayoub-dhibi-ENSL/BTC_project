=================
btc_graph.io
=================

The ``btc_graph.io`` module provides functionality for loading blockchain data,
exporting results, and managing Spark sessions.

Spark Session Management
------------------------

.. automodule:: btc_graph.io.spark
   :members:
   :undoc-members:
   :show-inheritance:

Data Loaders
------------

.. autoclass:: btc_graph.io.loaders.SnapshotPathFinder
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: btc_graph.io.loaders.NodeTableFinder
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: btc_graph.io.loaders.AnalysisResultsFinder
   :members:
   :undoc-members:
   :show-inheritance:

Schema Definitions
------------------

.. autodata:: btc_graph.io.loaders.SNAPSHOT_SCHEMA
   :annotation:

Data Exporters
--------------

.. autoclass:: btc_graph.io.exporters.CSVExporter
   :members:
   :undoc-members:
   :show-inheritance:
