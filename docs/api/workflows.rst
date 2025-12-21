======================
btc_graph.workflows
======================

The ``btc_graph.workflows`` module provides high-level analysis pipelines
for processing blockchain snapshots.

Analysis Pipeline
-----------------

.. autoclass:: btc_graph.workflows.pipeline.SnapshotAnalysisPipeline
   :members:
   :undoc-members:
   :show-inheritance:

Data Classes
------------

.. autoclass:: btc_graph.workflows.pipeline.AnalysisResult
   :members:
   :undoc-members:
   :show-inheritance:

Example Usage
-------------

Basic Pipeline
^^^^^^^^^^^^^^

.. code-block:: python

   from btc_graph import SnapshotAnalysisPipeline, create_spark_session
   
   spark = create_spark_session("analysis")
   
   pipeline = SnapshotAnalysisPipeline(
       snapshot_dir="data/snapshots",
       output_dir="output/results",
       spark=spark
   )
   
   results = pipeline.run(max_snapshots=10)

Custom Analysis
^^^^^^^^^^^^^^^

.. code-block:: python

   from btc_graph import SnapshotAnalysisPipeline, create_spark_session
   from btc_graph.io import SnapshotPathFinder
   
   spark = create_spark_session("analysis")
   
   # Find specific snapshots
   finder = SnapshotPathFinder("data/snapshots")
   snapshot_paths = finder.find_all()[:5]  # First 5 only
   
   pipeline = SnapshotAnalysisPipeline(
       snapshot_dir="data/snapshots",
       output_dir="output/results",
       spark=spark,
       pagerank_iterations=20,
       pagerank_reset_prob=0.10
   )
   
   # Analyze individual snapshots
   for path in snapshot_paths:
       result = pipeline.analyze_single(path)
       print(f"Analyzed: {result.snapshot_id}")
