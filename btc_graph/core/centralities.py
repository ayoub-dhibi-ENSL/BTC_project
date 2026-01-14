"""Node-level centrality measures for Bitcoin transaction graphs.

This module provides functions to compute various centrality metrics for nodes
in a Bitcoin transaction network represented as a GraphFrame. Centrality
measures help identify important or influential nodes in the network.

Available Centralities
----------------------
- **Degree centrality**: In-degree, out-degree, and total degree
- **PageRank**: Importance based on link structure
- **Triangle centralities**: Triangle count and local clustering coefficient

Design Notes
------------
- All functions accept a GraphFrame and return a Spark DataFrame.
- Results include the vertex ``id`` column for easy joining with other data.
- Degrees are cast to ``double`` to prevent integer overflow in large graphs.

Typical Usage
-------------
>>> from btc_graph.core import GraphBuilder, compute_degrees
>>> graph = GraphBuilder.from_edges(edges_df)
>>> degrees_df = compute_degrees(graph)
>>> degrees_df.show(5)
+---+--------+---------+------+
| id|inDegree|outDegree|degree|
+---+--------+---------+------+
|  1|     5.0|      3.0|   8.0|
|  2|     2.0|      7.0|   9.0|
...
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Tuple, Union

if TYPE_CHECKING:
    from graphframes import GraphFrame
    from pyspark.sql import DataFrame as SparkDataFrame


def compute_degrees(graph: "GraphFrame") -> "SparkDataFrame":
    """Compute in-degree, out-degree, and total degree for each node.

    This function calculates degree centralities for all vertices in the graph,
    including nodes that may have zero in-degree or zero out-degree. Degrees
    are cast to ``double`` type to prevent integer overflow when computing
    derived metrics on large graphs.

    Parameters
    ----------
    graph : graphframes.GraphFrame
        The input graph. Must be a valid GraphFrame with vertices and edges.

    Returns
    -------
    pyspark.sql.DataFrame
        A DataFrame with the following columns:

        - ``id``: The vertex identifier
        - ``inDegree``: Number of incoming edges (as double)
        - ``outDegree``: Number of outgoing edges (as double)
        - ``degree``: Total degree (inDegree + outDegree)

        All vertices are included, even those with zero in-degree or out-degree.

    Examples
    --------
    >>> from btc_graph.core import GraphBuilder, compute_degrees
    >>> graph = GraphBuilder.from_edges(edges_df)
    >>> degrees = compute_degrees(graph)
    >>> degrees.orderBy("degree", ascending=False).show(10)

    Notes
    -----
    - The total degree is the sum of in-degree and out-degree.
    - For undirected graph analysis, you may want to use only the ``degree``
      column.
    - Missing degree values (for isolated nodes in one direction) are filled
      with 0.

    See Also
    --------
    compute_triangle_centralities : Triangle-based centrality metrics.
    compute_pagerank : PageRank centrality.
    """
    from pyspark.sql.functions import col

    # Compute in-degrees and cast to double to prevent overflow
    in_degrees = graph.inDegrees.withColumn(
        "inDegree",
        col("inDegree").cast("double"),
    )

    # Compute out-degrees and cast to double to prevent overflow
    out_degrees = graph.outDegrees.withColumn(
        "outDegree",
        col("outDegree").cast("double"),
    )

    # Full outer join to include all nodes, fill missing values with 0
    all_degrees = in_degrees.join(out_degrees, on="id", how="full").na.fill(0)

    # Compute total degree as sum of in and out degrees
    all_degrees = all_degrees.withColumn(
        "degree",
        col("inDegree") + col("outDegree"),
    )

    return all_degrees


def compute_pagerank(
    graph: "GraphFrame",
    reset_probability: float = 0.15,
    max_iterations: int = 20,
) -> "SparkDataFrame":
    """Compute PageRank centrality for each vertex in the graph.

    PageRank measures the importance of each node based on the structure of
    incoming links. Originally developed for ranking web pages, it is useful
    for identifying influential addresses in the Bitcoin network.

    Parameters
    ----------
    graph : graphframes.GraphFrame
        The input graph.
    reset_probability : float, optional
        Probability of resetting to a random vertex at each step (damping
        factor is ``1 - reset_probability``). Default is 0.15.
    max_iterations : int, optional
        Maximum number of iterations. Default is 20.

    Returns
    -------
    pyspark.sql.DataFrame
        A DataFrame with two columns:

        - ``id``: The vertex identifier
        - ``pagerank``: The computed PageRank score

    Examples
    --------
    >>> from btc_graph.core import GraphBuilder, compute_pagerank
    >>> graph = GraphBuilder.from_edges(edges_df)
    >>> pagerank_df = compute_pagerank(graph)
    >>> # Find top 10 most important addresses
    >>> pagerank_df.orderBy("pagerank", ascending=False).show(10)

    Notes
    -----
    - PageRank values sum to approximately the number of vertices.
    - Higher values indicate more "important" nodes in the network.
    - GraphFrames requires exactly one of ``maxIter`` or ``tol`` to be set.
      This function uses ``maxIter`` for deterministic behaviour.

    See Also
    --------
    compute_degrees : Degree-based centrality.
    graphframes.GraphFrame.pageRank : Underlying implementation.
    """
    # Run PageRank algorithm
    # Note: GraphFrames 0.9+ requires exactly ONE of maxIter or tol, not both
    pagerank_graph = graph.pageRank(
        resetProbability=reset_probability,
        maxIter=max_iterations,
    )

    # Extract vertex PageRank scores
    pagerank_df = pagerank_graph.vertices.select("id", "pagerank")

    return pagerank_df


def compute_triangle_centralities(
    graph: "GraphFrame",
    degree_df: "SparkDataFrame",
    return_global_metrics: bool = False,
) -> Union["SparkDataFrame", Tuple["SparkDataFrame", "SparkDataFrame"]]:
    """Compute triangle-based centralities for each vertex.

    This function calculates the triangle count and local clustering coefficient
    (LCC) for each vertex. These metrics measure how well-connected a node's
    neighbours are to each other, revealing the local cohesiveness of the
    network structure.

    Parameters
    ----------
    graph : graphframes.GraphFrame
        The input graph.
    degree_df : pyspark.sql.DataFrame
        DataFrame containing vertex degrees. Must have columns ``id`` and
        ``degree``. Typically obtained from :func:`compute_degrees`.
    return_global_metrics : bool, optional
        If ``True``, also return a DataFrame with global clustering metrics.
        Default is ``False``.

    Returns
    -------
    triangles_df : pyspark.sql.DataFrame
        DataFrame with the following columns:

        - ``id``: The vertex identifier
        - ``triangles_count``: Number of triangles the vertex participates in
        - ``degree``: Total degree of the vertex
        - ``triangles_max_count``: Maximum possible triangles given the degree
        - ``lcc``: Local clustering coefficient (triangles_count / triangles_max_count)

    global_metrics_df : pyspark.sql.DataFrame, optional
        Only returned if ``return_global_metrics=True``. A single-row DataFrame
        with columns:

        - ``global_cc``: Global clustering coefficient
        - ``average_cc``: Average local clustering coefficient

    Examples
    --------
    >>> from btc_graph.core import GraphBuilder, compute_degrees
    >>> from btc_graph.core import compute_triangle_centralities
    >>>
    >>> graph = GraphBuilder.from_edges(edges_df)
    >>> degrees = compute_degrees(graph)
    >>>
    >>> # Get per-node triangle metrics
    >>> triangles = compute_triangle_centralities(graph, degrees)
    >>> triangles.show(5)
    >>>
    >>> # Also get global clustering coefficients
    >>> triangles, global_cc = compute_triangle_centralities(
    ...     graph, degrees, return_global_metrics=True
    ... )
    >>> global_cc.show()

    Notes
    -----
    - **Local Clustering Coefficient (LCC)**: Ratio of actual triangles to
      maximum possible triangles for a vertex. Ranges from 0 to 1.
    - **Global Clustering Coefficient**: Ratio of total triangles to total
      possible triangles across the entire graph.
    - **Average Clustering Coefficient**: Mean of all local clustering
      coefficients.
    - Triangle counting treats the graph as undirected (ignores edge direction).
    - Vertices with degree < 2 have LCC = 0 (no triangles possible).

    See Also
    --------
    compute_degrees : Required to obtain degree_df input.
    graphframes.GraphFrame.triangleCount : Underlying triangle counting.
    """
    from pyspark.sql import functions as F

    # Count triangles for each vertex
    # Note: triangleCount treats the graph as undirected
    # Handle API differences between GraphFrames versions
    try:
        # Newer GraphFrames (Spark 4.x) requires storage_level argument
        from pyspark import StorageLevel

        triangles_raw = graph.triangleCount(StorageLevel.MEMORY_AND_DISK).select(
            "id", "count"
        )
    except TypeError:
        # Older GraphFrames versions don't accept storage_level
        triangles_raw = graph.triangleCount().select("id", "count")
    triangles_df = triangles_raw.withColumnRenamed("count", "triangles_count")

    # Join with degree information
    # Full join ensures all vertices are included
    triangles_df = triangles_df.join(degree_df, on="id", how="full")

    # Calculate maximum possible triangles for each vertex
    # Formula: degree * (degree - 1) / 2 (combinations of 2 from degree neighbours)
    triangles_df = triangles_df.withColumn(
        "triangles_max_count",
        triangles_df["degree"] * (triangles_df["degree"] - 1) / 2,
    )

    # Calculate local clustering coefficient
    # LCC = actual triangles / maximum possible triangles
    # Fill NaN/NULL with 0 (for nodes with degree < 2)
    triangles_df = triangles_df.withColumn(
        "lcc",
        triangles_df["triangles_count"] / triangles_df["triangles_max_count"],
    ).na.fill(0)

    if not return_global_metrics:
        return triangles_df

    # Compute global clustering metrics
    global_metrics_df = triangles_df.select(
        # Global CC: total triangles / total possible triangles
        (
            F.sum(triangles_df["triangles_count"])
            / F.sum(triangles_df["triangles_max_count"])
        ).alias("global_cc"),
        # Average CC: mean of local clustering coefficients
        F.avg(triangles_df["lcc"]).alias("average_cc"),
    )

    return triangles_df, global_metrics_df
