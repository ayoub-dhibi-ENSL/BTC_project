"""Graph construction utilities for Bitcoin transaction networks.

This module provides the :class:`GraphBuilder` class for converting Spark
DataFrames of edges into GraphFrame objects suitable for graph analytics.

Design Notes
------------
- The GraphFrame library requires vertices to have an ``id`` column and edges
  to have ``src`` and ``dst`` columns. This module handles the renaming.
- Vertex extraction is done by unioning source and destination columns, then
  deduplicating to get all unique node IDs.

Typical Usage
-------------
>>> from btc_graph.core.graph import GraphBuilder
>>> # Assuming `edges_df` is a Spark DataFrame with SRC_ID and DST_ID columns
>>> graph = GraphBuilder.from_edges(edges_df, src_col="SRC_ID", dst_col="DST_ID")
>>> print(graph.vertices.count(), graph.edges.count())
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from graphframes import GraphFrame
    from pyspark.sql import DataFrame as SparkDataFrame


class GraphBuilder:
    """Factory class for constructing GraphFrame objects from edge DataFrames.

    This class provides static methods to convert Spark DataFrames representing
    edges (transactions) into GraphFrame objects that can be used with graph
    algorithms such as PageRank, triangle counting, and connected components.

    The Bitcoin transaction network is modelled as a directed graph where:
    - **Nodes** represent wallet addresses (identified by integer IDs)
    - **Edges** represent transactions from source to destination address

    Examples
    --------
    >>> from btc_graph.core.graph import GraphBuilder
    >>> # edges_df has columns: SRC_ID, DST_ID, VALUE_SATOSHI, VALUE_USD
    >>> graph = GraphBuilder.from_edges(edges_df)
    >>> graph.vertices.show(5)
    >>> graph.edges.show(5)

    Notes
    -----
    The class uses static methods since no instance state is required. This
    design allows for easy testing and clear separation of concerns.
    """

    @staticmethod
    def _create_vertices(
        edges_df: "SparkDataFrame",
        src_col: str = "src",
        dst_col: str = "dst",
    ) -> "SparkDataFrame":
        """Extract unique vertex IDs from source and destination columns.

        This internal method unions the source and destination columns, then
        removes duplicates to produce a DataFrame of unique vertex IDs.

        Parameters
        ----------
        edges_df : pyspark.sql.DataFrame
            DataFrame containing edge information with source and destination
            columns already renamed to ``src`` and ``dst``.
        src_col : str, optional
            Name of the source column. Default is ``"src"``.
        dst_col : str, optional
            Name of the destination column. Default is ``"dst"``.

        Returns
        -------
        pyspark.sql.DataFrame
            A DataFrame with a single column ``id`` containing all unique
            vertex identifiers from both source and destination columns.

        Notes
        -----
        This is an internal method called by :meth:`from_edges`. It should
        not typically be called directly by users.
        """
        from pyspark.sql import functions as F

        # Select source IDs and rename to 'id'
        src_vertices = edges_df.select(F.col(src_col).alias("id"))

        # Select destination IDs and rename to 'id'
        dst_vertices = edges_df.select(F.col(dst_col).alias("id"))

        # Union and deduplicate to get all unique vertices
        vertices = src_vertices.union(dst_vertices).distinct()

        return vertices

    @staticmethod
    def from_edges(
        edges_df: "SparkDataFrame",
        src_col: str = "SRC_ID",
        dst_col: str = "DST_ID",
    ) -> "GraphFrame":
        """Construct a GraphFrame from an edge DataFrame.

        Converts a Spark DataFrame containing transaction edges into a
        GraphFrame object. The method automatically extracts unique vertices
        from the source and destination columns and renames columns to match
        GraphFrame's expected schema.

        Parameters
        ----------
        edges_df : pyspark.sql.DataFrame
            Input DataFrame representing edges. Must contain at least the
            source and destination columns specified. Additional columns
            (e.g., ``VALUE_SATOSHI``, ``VALUE_USD``) are preserved as edge
            attributes.
        src_col : str, optional
            Name of the column containing source vertex IDs.
            Default is ``"SRC_ID"`` (matching the Bitcoin snapshot schema).
        dst_col : str, optional
            Name of the column containing destination vertex IDs.
            Default is ``"DST_ID"`` (matching the Bitcoin snapshot schema).

        Returns
        -------
        graphframes.GraphFrame
            A GraphFrame object with:
            - ``vertices``: DataFrame with column ``id`` (unique node IDs)
            - ``edges``: DataFrame with columns ``src``, ``dst``, and any
              additional columns from the input DataFrame

        Raises
        ------
        ValueError
            If ``src_col`` or ``dst_col`` are not found in the DataFrame.

        Examples
        --------
        >>> from btc_graph.io import SnapshotPathFinder, SNAPSHOT_SCHEMA
        >>> from btc_graph.core import GraphBuilder
        >>>
        >>> # Load a snapshot
        >>> finder = SnapshotPathFinder(base_path="data")
        >>> paths = finder.get_snapshot_paths("hour", sample=True)
        >>> edges_df = finder.load_snapshot_with_spark(spark, paths[0])
        >>>
        >>> # Build the graph
        >>> graph = GraphBuilder.from_edges(edges_df)
        >>> print(f"Vertices: {graph.vertices.count()}")
        >>> print(f"Edges: {graph.edges.count()}")

        Notes
        -----
        The GraphFrame library expects:
        - Vertices DataFrame to have an ``id`` column
        - Edges DataFrame to have ``src`` and ``dst`` columns

        This method handles the column renaming automatically based on the
        ``src_col`` and ``dst_col`` parameters.

        See Also
        --------
        graphframes.GraphFrame : The underlying graph representation.
        """
        from graphframes import GraphFrame

        # Validate that required columns exist
        existing_cols = set(edges_df.columns)
        if src_col not in existing_cols:
            raise ValueError(
                f"Source column '{src_col}' not found in DataFrame. "
                f"Available columns: {list(existing_cols)}"
            )
        if dst_col not in existing_cols:
            raise ValueError(
                f"Destination column '{dst_col}' not found in DataFrame. "
                f"Available columns: {list(existing_cols)}"
            )

        # Rename source and destination columns to GraphFrame conventions
        renamed_edges = edges_df.withColumnsRenamed({src_col: "src", dst_col: "dst"})

        # Extract unique vertices from the renamed edges
        vertices = GraphBuilder._create_vertices(renamed_edges, "src", "dst")

        # Construct and return the GraphFrame
        return GraphFrame(vertices, renamed_edges)
