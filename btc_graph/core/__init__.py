"""Core graph operations for Bitcoin blockchain analysis.

This subpackage contains the fundamental graph building and analysis components:

- **graph**: GraphFrame construction from edge DataFrames
- **centralities**: Node-level centrality measures (degrees, PageRank, triangles)
- **metrics**: Graph-level scalar metrics (density, clustering coefficients)

Example Usage
-------------
>>> from btc_graph.core import GraphBuilder, compute_degrees, compute_density
>>> from btc_graph.io import SnapshotPathFinder
>>>
>>> # Load snapshot
>>> finder = SnapshotPathFinder(base_path="data")
>>> df = finder.load_snapshot_with_spark(spark, paths[0])
>>>
>>> # Build graph
>>> graph = GraphBuilder.from_edges(df)
>>>
>>> # Compute metrics
>>> degrees_df = compute_degrees(graph)
>>> density = compute_density(graph)
"""

from .centralities import (
    compute_degrees,
    compute_pagerank,
    compute_triangle_centralities,
)
from .graph import GraphBuilder
from .metrics import compute_density

__all__ = [
    "GraphBuilder",
    "compute_degrees",
    "compute_pagerank",
    "compute_triangle_centralities",
    "compute_density",
]
