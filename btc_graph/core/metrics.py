"""Graph-level scalar metrics for Bitcoin transaction networks.

This module provides functions to compute graph-wide metrics that summarise
the overall structure of the network as single scalar values. These metrics
are useful for comparing different snapshots or tracking network evolution
over time.

Available Metrics
-----------------
- **Density**: Ratio of actual edges to maximum possible edges
- **Vertex count**: Number of unique addresses
- **Edge count**: Number of transactions

Design Notes
------------
- All functions accept a GraphFrame and return scalar values or small
  DataFrames.
- These metrics complement the node-level centralities in ``centralities.py``.

Typical Usage
-------------
>>> from btc_graph.core import GraphBuilder, compute_density
>>> graph = GraphBuilder.from_edges(edges_df)
>>> density = compute_density(graph)
>>> print(f"Graph density: {density:.6f}")
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:
    from graphframes import GraphFrame


def compute_density(graph: "GraphFrame") -> float:
    """Compute the density of the graph.

    Graph density is the ratio of the number of actual edges to the maximum
    possible number of edges in a directed graph. It measures how "connected"
    the graph is, with values ranging from 0 (no edges) to 1 (fully connected).

    Parameters
    ----------
    graph : graphframes.GraphFrame
        The input graph.

    Returns
    -------
    float
        The density of the graph, a value between 0 and 1.

        - **0**: No edges (empty graph)
        - **1**: Fully connected (every node connected to every other node)

    Examples
    --------
    >>> from btc_graph.core import GraphBuilder, compute_density
    >>> graph = GraphBuilder.from_edges(edges_df)
    >>> density = compute_density(graph)
    >>> print(f"Density: {density:.8f}")
    Density: 0.00000342

    Notes
    -----
    For a directed graph, the maximum number of edges is:

    .. math::

        E_{max} = V \\times (V - 1)

    where :math:`V` is the number of vertices.

    The density is then:

    .. math::

        \\text{density} = \\frac{E}{E_{max}} = \\frac{E}{V \\times (V - 1)}

    Bitcoin transaction networks are typically very sparse (low density)
    because most addresses only transact with a small number of other
    addresses relative to the total network size.

    See Also
    --------
    compute_graph_summary : Get multiple metrics at once.
    """
    num_edges = graph.edges.count()
    num_vertices = graph.vertices.count()

    # Handle edge cases
    if num_vertices <= 1:
        return 0.0

    # Maximum edges in a directed graph: V * (V - 1)
    max_edges = num_vertices * (num_vertices - 1)

    if max_edges == 0:
        return 0.0

    return num_edges / max_edges


def compute_graph_summary(graph: "GraphFrame") -> Dict[str, float]:
    """Compute a summary of key graph-level metrics.

    This convenience function computes multiple scalar metrics in a single
    call, useful for generating snapshot summaries or tracking network
    evolution over time.

    Parameters
    ----------
    graph : graphframes.GraphFrame
        The input graph.

    Returns
    -------
    dict
        A dictionary containing:

        - ``num_vertices``: Number of unique vertices (addresses)
        - ``num_edges``: Number of edges (transactions)
        - ``density``: Graph density (see :func:`compute_density`)
        - ``avg_degree``: Average degree (2 * edges / vertices for undirected
          interpretation, or edges / vertices for directed)

    Examples
    --------
    >>> from btc_graph.core import GraphBuilder
    >>> from btc_graph.core.metrics import compute_graph_summary
    >>>
    >>> graph = GraphBuilder.from_edges(edges_df)
    >>> summary = compute_graph_summary(graph)
    >>> for metric, value in summary.items():
    ...     print(f"{metric}: {value:,.4f}")
    num_vertices: 125,432.0000
    num_edges: 287,651.0000
    density: 0.0000
    avg_degree: 2.2929

    Notes
    -----
    This function performs two Spark actions (counting vertices and edges),
    so it may be slow for very large graphs. Consider caching the graph
    before calling this function if you plan to compute additional metrics.

    See Also
    --------
    compute_density : Density calculation details.
    """
    num_vertices = graph.vertices.count()
    num_edges = graph.edges.count()

    # Compute density
    if num_vertices <= 1:
        density = 0.0
    else:
        max_edges = num_vertices * (num_vertices - 1)
        density = num_edges / max_edges if max_edges > 0 else 0.0

    # Average degree (directed interpretation: edges per vertex)
    avg_degree = num_edges / num_vertices if num_vertices > 0 else 0.0

    return {
        "num_vertices": float(num_vertices),
        "num_edges": float(num_edges),
        "density": density,
        "avg_degree": avg_degree,
    }
