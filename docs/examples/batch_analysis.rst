==============
Batch Analysis
==============

This example shows how to process multiple snapshots efficiently.

Using the Pipeline
------------------

.. code-block:: python

   """
   Example: Run the analysis pipeline on multiple snapshots.
   """
   from btc_graph import (
       SnapshotAnalysisPipeline,
       create_spark_session,
       stop_spark_session,
   )
   
   # Create Spark session with sufficient memory
   spark = create_spark_session(
       app_name="batch-analysis",
       driver_memory="8g"
   )
   
   try:
       # Configure pipeline
       pipeline = SnapshotAnalysisPipeline(
           snapshot_dir="data/orbitaal-snapshot-hour/SNAPSHOT/EDGES",
           output_dir="output/batch-analysis",
           spark=spark,
           pagerank_iterations=10,
           pagerank_reset_prob=0.15
       )
       
       # Run on all snapshots (or limit with max_snapshots)
       results = pipeline.run(max_snapshots=20)
       
       # Summarize results
       print(f"\nProcessed {len(results)} snapshots:")
       print("-" * 50)
       
       for result in results:
           print(
               f"{result.snapshot_id}: "
               f"{result.vertex_count:,} vertices, "
               f"{result.edge_count:,} edges, "
               f"density={result.density:.6f}"
           )
       
       # Compute aggregate statistics
       import numpy as np
       
       vertices = [r.vertex_count for r in results]
       edges = [r.edge_count for r in results]
       
       print(f"\nAggregate Statistics:")
       print(f"  Avg vertices: {np.mean(vertices):,.0f}")
       print(f"  Avg edges: {np.mean(edges):,.0f}")
       print(f"  Vertex growth: {(vertices[-1] - vertices[0]) / vertices[0] * 100:.1f}%")
       
   finally:
       stop_spark_session(spark)

Selective Processing
--------------------

.. code-block:: python

   """
   Example: Process specific snapshots selectively.
   """
   from btc_graph import (
       SnapshotAnalysisPipeline,
       create_spark_session,
       stop_spark_session,
   )
   from btc_graph.io import SnapshotPathFinder
   
   spark = create_spark_session("selective-analysis")
   
   try:
       # Find all snapshots
       finder = SnapshotPathFinder("data/orbitaal-snapshot-hour/SNAPSHOT/EDGES")
       all_paths = finder.find_all()
       
       # Select specific snapshots (e.g., every 5th one)
       selected_paths = all_paths[::5]  # Every 5th snapshot
       print(f"Processing {len(selected_paths)} of {len(all_paths)} snapshots")
       
       # Create pipeline
       pipeline = SnapshotAnalysisPipeline(
           snapshot_dir="data/orbitaal-snapshot-hour/SNAPSHOT/EDGES",
           output_dir="output/selective-analysis",
           spark=spark
       )
       
       # Process selected snapshots
       results = []
       for path in selected_paths:
           result = pipeline.analyze_single(path)
           results.append(result)
           print(f"Processed: {result.snapshot_id}")
       
   finally:
       stop_spark_session(spark)

Progress Tracking
-----------------

.. code-block:: python

   """
   Example: Track progress with tqdm.
   """
   from tqdm import tqdm
   from btc_graph import (
       SnapshotAnalysisPipeline,
       create_spark_session,
       stop_spark_session,
   )
   from btc_graph.io import SnapshotPathFinder
   
   spark = create_spark_session("progress-tracking")
   
   try:
       finder = SnapshotPathFinder("data/orbitaal-snapshot-hour/SNAPSHOT/EDGES")
       paths = finder.find_all()[:10]  # First 10 for demo
       
       pipeline = SnapshotAnalysisPipeline(
           snapshot_dir="data/orbitaal-snapshot-hour/SNAPSHOT/EDGES",
           output_dir="output/progress-demo",
           spark=spark
       )
       
       results = []
       for path in tqdm(paths, desc="Analyzing snapshots"):
           result = pipeline.analyze_single(path)
           results.append(result)
       
       print(f"\nCompleted {len(results)} snapshots")
       
   finally:
       stop_spark_session(spark)

Error Handling
--------------

.. code-block:: python

   """
   Example: Handle errors gracefully during batch processing.
   """
   import logging
   from typing import Optional, List
   
   from btc_graph import (
       SnapshotAnalysisPipeline,
       create_spark_session,
       stop_spark_session,
   )
   from btc_graph.io import SnapshotPathFinder
   from btc_graph.workflows import AnalysisResult
   
   logging.basicConfig(level=logging.INFO)
   logger = logging.getLogger(__name__)
   
   
   def safe_analyze(
       pipeline: SnapshotAnalysisPipeline,
       path: str
   ) -> Optional[AnalysisResult]:
       """Analyze with error handling."""
       try:
           return pipeline.analyze_single(path)
       except Exception as e:
           logger.error(f"Failed to analyze {path}: {e}")
           return None
   
   
   spark = create_spark_session("error-handling")
   
   try:
       finder = SnapshotPathFinder("data/orbitaal-snapshot-hour/SNAPSHOT/EDGES")
       paths = finder.find_all()
       
       pipeline = SnapshotAnalysisPipeline(
           snapshot_dir="data/orbitaal-snapshot-hour/SNAPSHOT/EDGES",
           output_dir="output/error-handling",
           spark=spark
       )
       
       results: List[AnalysisResult] = []
       failed: List[str] = []
       
       for path in paths:
           result = safe_analyze(pipeline, path)
           if result:
               results.append(result)
           else:
               failed.append(path)
       
       print(f"\nSuccessful: {len(results)}")
       print(f"Failed: {len(failed)}")
       
       if failed:
           print("\nFailed snapshots:")
           for path in failed:
               print(f"  - {path}")
       
   finally:
       stop_spark_session(spark)

Memory Management
-----------------

.. code-block:: python

   """
   Example: Manage memory for large-scale processing.
   """
   import gc
   from btc_graph import (
       SnapshotAnalysisPipeline,
       create_spark_session,
       stop_spark_session,
   )
   from btc_graph.io import SnapshotPathFinder
   
   # Configure Spark for large datasets
   spark = create_spark_session(
       app_name="memory-management",
       driver_memory="16g",
       extra_config={
           "spark.sql.shuffle.partitions": "200",
           "spark.memory.fraction": "0.8",
           "spark.memory.storageFraction": "0.3",
       }
   )
   
   try:
       finder = SnapshotPathFinder("data/orbitaal-snapshot-hour/SNAPSHOT/EDGES")
       paths = finder.find_all()
       
       pipeline = SnapshotAnalysisPipeline(
           snapshot_dir="data/orbitaal-snapshot-hour/SNAPSHOT/EDGES",
           output_dir="output/memory-managed",
           spark=spark
       )
       
       # Process in batches to manage memory
       batch_size = 5
       results = []
       
       for i in range(0, len(paths), batch_size):
           batch_paths = paths[i:i + batch_size]
           print(f"\nProcessing batch {i // batch_size + 1}")
           
           for path in batch_paths:
               result = pipeline.analyze_single(path)
               results.append(result)
           
           # Clear Spark cache after each batch
           spark.catalog.clearCache()
           gc.collect()
           print(f"Batch complete. Total processed: {len(results)}")
       
       print(f"\nAll done! Processed {len(results)} snapshots")
       
   finally:
       stop_spark_session(spark)

Resumable Processing
--------------------

.. code-block:: python

   """
   Example: Resume processing from where it left off.
   """
   from pathlib import Path
   from btc_graph import (
       SnapshotAnalysisPipeline,
       create_spark_session,
       stop_spark_session,
   )
   from btc_graph.io import SnapshotPathFinder
   
   
   def get_processed_snapshots(output_dir: str) -> set:
       """Find already processed snapshots."""
       processed = set()
       output_path = Path(output_dir)
       
       if output_path.exists():
           for subdir in output_path.iterdir():
               if subdir.is_dir() and (subdir / "scalar").exists():
                   processed.add(subdir.name)
       
       return processed
   
   
   def extract_snapshot_id(path: str) -> str:
       """Extract snapshot ID from path."""
       return Path(path).name
   
   
   spark = create_spark_session("resumable-processing")
   output_dir = "output/resumable"
   
   try:
       finder = SnapshotPathFinder("data/orbitaal-snapshot-hour/SNAPSHOT/EDGES")
       all_paths = finder.find_all()
       
       # Find already processed
       processed = get_processed_snapshots(output_dir)
       print(f"Already processed: {len(processed)} snapshots")
       
       # Filter to unprocessed
       remaining_paths = [
           p for p in all_paths
           if extract_snapshot_id(p) not in processed
       ]
       print(f"Remaining: {len(remaining_paths)} snapshots")
       
       if remaining_paths:
           pipeline = SnapshotAnalysisPipeline(
               snapshot_dir="data/orbitaal-snapshot-hour/SNAPSHOT/EDGES",
               output_dir=output_dir,
               spark=spark
           )
           
           for path in remaining_paths:
               result = pipeline.analyze_single(path)
               print(f"Processed: {result.snapshot_id}")
       else:
           print("All snapshots already processed!")
       
   finally:
       stop_spark_session(spark)
