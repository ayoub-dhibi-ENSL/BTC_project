==============
Basic Analysis
==============

This tutorial walks through a complete analysis of Bitcoin blockchain 
transaction snapshots using btc_graph.

Overview
--------

We will:

1. Set up the Spark environment
2. Load and explore snapshot data
3. Build transaction graphs
4. Compute centrality metrics
5. Analyze the results

Prerequisites
-------------

- btc_graph installed (see :doc:`../installation`)
- Sample data in ``data/orbitaal-snapshot-hour/``
- At least 4GB of RAM available

Step 1: Setup
-------------

First, import the necessary modules and create a Spark session:

.. code-block:: python

   import os
   from pathlib import Path
   
   from btc_graph import (
       create_spark_session,
       stop_spark_session,
       SnapshotPathFinder,
       GraphBuilder,
   )
   from btc_graph.core import (
       compute_degrees,
       compute_pagerank,
       compute_triangle_centralities,
       compute_graph_summary,
   )
   
   # Create Spark session
   spark = create_spark_session(
       app_name="btc-analysis-tutorial",
       driver_memory="4g"
   )
   
   print(f"Spark version: {spark.version}")

Step 2: Discover Snapshots
--------------------------

Use the ``SnapshotPathFinder`` to locate available snapshots:

.. code-block:: python

   # Find snapshots
   finder = SnapshotPathFinder("data/orbitaal-snapshot-hour/SNAPSHOT/EDGES")
   snapshot_paths = finder.find_all()
   
   print(f"Found {len(snapshot_paths)} snapshots")
   for path in snapshot_paths[:5]:
       print(f"  - {path}")

Output:

.. code-block:: text

   Found 20 snapshots
     - data/orbitaal-snapshot-hour/SNAPSHOT/EDGES/hour=000000
     - data/orbitaal-snapshot-hour/SNAPSHOT/EDGES/hour=000001
     - data/orbitaal-snapshot-hour/SNAPSHOT/EDGES/hour=000002
     - data/orbitaal-snapshot-hour/SNAPSHOT/EDGES/hour=000003
     - data/orbitaal-snapshot-hour/SNAPSHOT/EDGES/hour=000004

Step 3: Load and Explore Data
-----------------------------

Load a single snapshot and examine its structure:

.. code-block:: python

   # Load first snapshot
   builder = GraphBuilder(spark)
   edges_df = builder.load_edges(snapshot_paths[0])
   
   # Show schema
   edges_df.printSchema()
   
   # Show sample data
   edges_df.show(5, truncate=False)
   
   # Count edges
   print(f"Total edges: {edges_df.count()}")

Output:

.. code-block:: text

   root
    |-- src: long (nullable = true)
    |-- dst: long (nullable = true)
    |-- weight: double (nullable = true)
   
   +---+---+------+
   |src|dst|weight|
   +---+---+------+
   |1  |2  |0.5   |
   |1  |3  |1.0   |
   |2  |4  |0.25  |
   |3  |4  |0.75  |
   |4  |5  |1.0   |
   +---+---+------+
   
   Total edges: 12345

Step 4: Build Graph
-------------------

Convert the edge DataFrame into a GraphFrames graph:

.. code-block:: python

   # Build graph
   graph = builder.build_graph(edges_df)
   
   # Examine vertices
   print(f"Vertices: {graph.vertices.count()}")
   graph.vertices.show(5)
   
   # Examine edges
   print(f"Edges: {graph.edges.count()}")

Step 5: Compute Centralities
----------------------------

In-Degree and Out-Degree
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Compute degree centralities
   degrees_df = compute_degrees(graph)
   
   degrees_df.show(10)
   
   # Summary statistics
   degrees_df.describe().show()

PageRank
^^^^^^^^

.. code-block:: python

   # Compute PageRank
   pagerank_df = compute_pagerank(
       graph,
       max_iter=10,
       reset_probability=0.15
   )
   
   # Top 10 by PageRank
   pagerank_df.orderBy("pagerank", ascending=False).show(10)

Triangle Centralities
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Compute triangle counts
   triangles_df = compute_triangle_centralities(graph)
   
   # Nodes with most triangles
   triangles_df.orderBy("triangle_count", ascending=False).show(10)

Step 6: Graph Summary
---------------------

Get an overview of the graph structure:

.. code-block:: python

   summary = compute_graph_summary(graph)
   
   print(f"Vertices: {summary['vertex_count']}")
   print(f"Edges: {summary['edge_count']}")
   print(f"Density: {summary['density']:.6f}")
   print(f"Avg in-degree: {summary['avg_in_degree']:.2f}")
   print(f"Avg out-degree: {summary['avg_out_degree']:.2f}")

Step 7: Save Results
--------------------

Save the computed metrics:

.. code-block:: python

   from btc_graph.io import CSVExporter
   
   output_dir = "output/tutorial_analysis"
   
   exporter = CSVExporter(output_dir)
   
   # Save degree centralities
   exporter.export_dataframe(degrees_df, "degrees")
   
   # Save PageRank
   exporter.export_dataframe(pagerank_df, "pagerank")
   
   # Save triangle counts
   exporter.export_dataframe(triangles_df, "triangles")
   
   # Save scalar metrics
   exporter.export_scalars(summary, "scalar")
   
   print(f"Results saved to {output_dir}")

Step 8: Cleanup
---------------

Always stop the Spark session when done:

.. code-block:: python

   stop_spark_session(spark)

Complete Script
---------------

Here's the complete analysis script:

.. code-block:: python

   #!/usr/bin/env python
   """Complete basic analysis script."""
   
   from btc_graph import (
       create_spark_session,
       stop_spark_session,
       SnapshotPathFinder,
       GraphBuilder,
   )
   from btc_graph.core import (
       compute_degrees,
       compute_pagerank,
       compute_triangle_centralities,
       compute_graph_summary,
   )
   from btc_graph.io import CSVExporter
   
   def main():
       # Setup
       spark = create_spark_session("tutorial-analysis", driver_memory="4g")
       
       try:
           # Find snapshots
           finder = SnapshotPathFinder("data/orbitaal-snapshot-hour/SNAPSHOT/EDGES")
           snapshot_paths = finder.find_all()
           
           # Process first snapshot
           builder = GraphBuilder(spark)
           edges_df = builder.load_edges(snapshot_paths[0])
           graph = builder.build_graph(edges_df)
           
           # Compute metrics
           degrees = compute_degrees(graph)
           pagerank = compute_pagerank(graph, max_iter=10)
           triangles = compute_triangle_centralities(graph)
           summary = compute_graph_summary(graph)
           
           # Export results
           exporter = CSVExporter("output/tutorial")
           exporter.export_dataframe(degrees, "degrees")
           exporter.export_dataframe(pagerank, "pagerank")
           exporter.export_dataframe(triangles, "triangles")
           exporter.export_scalars(summary, "scalar")
           
           print("Analysis complete!")
           print(f"  Vertices: {summary['vertex_count']}")
           print(f"  Edges: {summary['edge_count']}")
           print(f"  Density: {summary['density']:.6f}")
       
       finally:
           stop_spark_session(spark)
   
   if __name__ == "__main__":
       main()

Next Steps
----------

- :doc:`visualization` - Visualize your results
- :doc:`custom_pipelines` - Build custom analysis pipelines
- :doc:`../api/core` - Full API reference for core module
