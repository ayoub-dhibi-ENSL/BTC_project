import pyspark.pandas as ps  # type: ignore


def create_degree_col(
    pydf, src_colname, dst_colname, weighted=False, weight_colname=None
):
    """
    Compute the in-degree and out-degree (weighted or unweighted) for nodes in a directed graph using pandas-on-Spark.

    Parameters
    ----------
    pydf : pyspark.pandas.DataFrame
        DataFrame containing the edge list of the graph.
    src_colname : str
        Name of the column representing the source nodes.
    dst_colname : str
        Name of the column representing the destination nodes.
    weighted : bool, optional
        If True, compute weighted degrees using the specified weight column. Default is False.
    weight_colname : str, optional
        Name of the column representing edge weights. Required if `weighted` is True.

    Returns
    -------
    out_degree : pyspark.pandas.Series
        Series containing the out-degree (or weighted out-degree) for each source node.
    in_degree : pyspark.pandas.Series
        Series containing the in-degree (or weighted in-degree) for each destination node.

    Raises
    ------
    ValueError
        If `weighted` is True and `weight_colname` is not provided.
    """
    if not weighted:
        OUT_DEGREE = pydf.groupby(src_colname).size().rename("OUT_DEGREE")
        IN_DEGREE = pydf.groupby(dst_colname).size().rename("IN_DEGREE")

        return OUT_DEGREE, IN_DEGREE
    elif weighted and weight_colname is None:
        raise ValueError(
            "If 'weighted' is True, you must also provide 'weight_colname'."
        )
    else:
        WEIGHTED_OUT_DEGREE = (
            pydf.groupby(src_colname)[weight_colname]
            .sum()
            .rename(f"WEIGHTED_{weight_colname}_OUT_DEGREE")
        )
        WEIGHTED_IN_DEGREE = (
            pydf.groupby(dst_colname)[weight_colname]
            .sum()
            .rename(f"WEIGHTED_{weight_colname}_IN_DEGREE")
        )
        return WEIGHTED_OUT_DEGREE, WEIGHTED_IN_DEGREE


def get_vertices_and_density(pydf, src_colname, dst_colname):
    """
    Compute the density of a graph represented by transaction data.
    Parameters
    ----------
    pydf : polars.DataFrame
        The DataFrame containing transaction data with columns for source and destination IDs.
    src_colname : str
        The name of the column representing source node IDs.
    dst_colname : str
        The name of the column representing destination node IDs.
    Returns
    -------
    IDS : polars.Series
        Unique node IDs present in the transactions.
    number_of_nodes : int
        The total number of unique nodes in the graph.
    graph_density : float
        The density of the graph, calculated as the ratio of the number of edges to the maximum possible number of edges.
    Notes
    -----
    Graph density is defined as the ratio of the number of edges to the number of possible edges in a directed graph.
    """
    IDS = ps.concat(
        [pydf["SRC_ID"], pydf["DST_ID"]]
    ).unique()  # gets the all the IDs that were in a transaction i.e. all the different nodes of the graph

    number_of_nodes = IDS.size
    graph_density = pydf.size / (number_of_nodes * (number_of_nodes - 1))

    return (IDS, number_of_nodes, graph_density)


def get_PageRank(G):
    """
    Computes the PageRank centrality for each vertex in the given graph.

    Wraps the GraphFrame `pageRank` method to calculate PageRank scores
    for all vertices in the input graph and returns the results as a DataFrame
    containing vertex IDs and their corresponding PageRank values.

    Parameters
    ----------
    G : GraphFrame
        The input graph as a GraphFrame object.

    Returns
    -------
    PageRank_df : pyspark.sql.DataFrame
        A DataFrame with two columns:
            - 'id': The vertex identifier.
            - 'pagerank': The computed PageRank score for the vertex.

    Notes
    -----
    - The PageRank algorithm is run with a reset probability of 0.15 and a tolerance of 0.01.
    - This function assumes that the input graph is a valid GraphFrame object.

    """
    PageRankGraph = G.pageRank(resetProbability=0.15, tol=0.01)
    PageRank_df = PageRankGraph.vertices.select("id", "pagerank")
    return PageRank_df
