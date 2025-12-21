===========
Basic Usage
===========

This example demonstrates the fundamental operations in btc_graph.

Loading and Exploring Data
--------------------------

.. code-block:: python

   """
   Example: Load and explore Bitcoin transaction snapshot data.
   """
   from btc_graph import create_spark_session, stop_spark_session
   from btc_graph.io import SnapshotPathFinder, SNAPSHOT_SCHEMA
   
   # Create Spark session
   spark = create_spark_session("basic-example")
   
   # Find available snapshots
   finder = SnapshotPathFinder("data/orbitaal-snapshot-hour/SNAPSHOT/EDGES")
   paths = finder.find_all()
   print(f"Found {len(paths)} snapshots")
   
   # Load a single snapshot
   edges_df = spark.read.csv(
       paths[0],
       schema=SNAPSHOT_SCHEMA,
       header=False
   )
   
   # Explore the data
   print("\nSchema:")
   edges_df.printSchema()
   
   print("\nSample data:")
   edges_df.show(5)
   
   print(f"\nTotal edges: {edges_df.count()}")
   
   # Basic statistics
   print("\nWeight statistics:")
   edges_df.describe("weight").show()
   
   # Cleanup
   stop_spark_session(spark)

Building a Graph
----------------

.. code-block:: python

   """
   Example: Build a GraphFrames graph from transaction data.
   """
   from btc_graph import create_spark_session, stop_spark_session, GraphBuilder
   from btc_graph.io import SnapshotPathFinder
   
   spark = create_spark_session("graph-example")
   
   # Load edges
   finder = SnapshotPathFinder("data/orbitaal-snapshot-hour/SNAPSHOT/EDGES")
   builder = GraphBuilder(spark)
   edges_df = builder.load_edges(finder.find_all()[0])
   
   # Build graph
   graph = builder.build_graph(edges_df)
   
   # Explore graph structure
   print(f"Vertices: {graph.vertices.count()}")
   print(f"Edges: {graph.edges.count()}")
   
   print("\nVertex sample:")
   graph.vertices.show(5)
   
   print("\nEdge sample:")
   graph.edges.show(5)
   
   # Cleanup
   stop_spark_session(spark)

Computing Centralities
----------------------

.. code-block:: python

   """
   Example: Compute various centrality measures.
   """
   from btc_graph import create_spark_session, stop_spark_session, GraphBuilder
   from btc_graph.core import (
       compute_degrees,
       compute_pagerank,
       compute_triangle_centralities,
   )
   from btc_graph.io import SnapshotPathFinder
   
   spark = create_spark_session("centrality-example")
   
   # Build graph
   finder = SnapshotPathFinder("data/orbitaal-snapshot-hour/SNAPSHOT/EDGES")
   builder = GraphBuilder(spark)
   edges_df = builder.load_edges(finder.find_all()[0])
   graph = builder.build_graph(edges_df)
   
   # Degree centrality
   print("=== Degree Centrality ===")
   degrees = compute_degrees(graph)
   degrees.show(10)
   
   # Find nodes with highest in-degree (most popular receivers)
   print("\nTop 10 by in-degree:")
   degrees.orderBy("in_degree", ascending=False).show(10)
   
   # PageRank
   print("\n=== PageRank ===")
   pagerank = compute_pagerank(graph, max_iter=10, reset_probability=0.15)
   
   print("Top 10 by PageRank:")
   pagerank.orderBy("pagerank", ascending=False).show(10)
   
   # Triangle centrality
   print("\n=== Triangle Centrality ===")
   triangles = compute_triangle_centralities(graph)
   
   print("Top 10 by triangle count:")
   triangles.orderBy("triangle_count", ascending=False).show(10)
   
   # Cleanup
   stop_spark_session(spark)

Graph Metrics
-------------

.. code-block:: python

   """
   Example: Compute graph-level metrics.
   """
   from btc_graph import create_spark_session, stop_spark_session, GraphBuilder
   from btc_graph.core import compute_graph_summary, compute_density
   from btc_graph.io import SnapshotPathFinder
   
   spark = create_spark_session("metrics-example")
   
   # Build graph
   finder = SnapshotPathFinder("data/orbitaal-snapshot-hour/SNAPSHOT/EDGES")
   builder = GraphBuilder(spark)
   edges_df = builder.load_edges(finder.find_all()[0])
   graph = builder.build_graph(edges_df)
   
   # Compute density
   density = compute_density(graph)
   print(f"Graph density: {density:.8f}")
   
   # Compute full summary
   summary = compute_graph_summary(graph)
   
   print("\n=== Graph Summary ===")
   for key, value in summary.items():
       if isinstance(value, float):
           print(f"{key}: {value:.6f}")
       else:
           print(f"{key}: {value}")
   
   # Cleanup
   stop_spark_session(spark)

Exporting Results
-----------------

.. code-block:: python

   """
   Example: Export analysis results to CSV.
   """
   from pathlib import Path
   from btc_graph import create_spark_session, stop_spark_session, GraphBuilder
   from btc_graph.core import compute_degrees, compute_graph_summary
   from btc_graph.io import SnapshotPathFinder, CSVExporter
   
   spark = create_spark_session("export-example")
   
   # Build graph and compute metrics
   finder = SnapshotPathFinder("data/orbitaal-snapshot-hour/SNAPSHOT/EDGES")
   builder = GraphBuilder(spark)
   edges_df = builder.load_edges(finder.find_all()[0])
   graph = builder.build_graph(edges_df)
   
   degrees = compute_degrees(graph)
   summary = compute_graph_summary(graph)
   
   # Export
   output_dir = "output/export-example"
   exporter = CSVExporter(output_dir)
   
   # Export DataFrame
   exporter.export_dataframe(degrees, "degrees")
   print(f"Exported degrees to {output_dir}/degrees/")
   
   # Export scalar metrics
   exporter.export_scalars(summary, "scalar")
   print(f"Exported scalars to {output_dir}/scalar/")
   
   # Verify files exist
   for path in Path(output_dir).rglob("*.csv"):
       print(f"  Created: {path}")
   
   # Cleanup
   stop_spark_session(spark)
