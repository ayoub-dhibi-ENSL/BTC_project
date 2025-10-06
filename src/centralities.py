from pyspark.sql import functions as F  # type: ignore


def get_degrees(G):
    """
    Calculates the in-degree, out-degree, and total degree for each node in a GraphFrame,
    including nodes with zero in-degree or out-degree.
    Parameters
    ----------
    G : GraphFrame
        The input GraphFrame object representing the graph.
    Returns
    -------
    pyspark.sql.DataFrame
        A DataFrame containing columns 'id', 'inDegree', 'outDegree', and 'degree' for each node.
        Nodes with zero in-degree or out-degree are included with degree values set to 0.
    """
    in_degrees = G.inDegrees.withColumn(
        "inDegree", G.inDegrees["inDegree"].cast("double")
    )
    out_degrees = G.outDegrees.withColumn(
        "outDegree", G.outDegrees["outDegree"].cast("double")
    )  # cast to double to avoid 32-bit int overflow when computing degree based centralities
    all_degrees_df = in_degrees.join(out_degrees, on="id", how="full").na.fill(
        0
    )  # add nodes with 0 degree
    all_degrees_df = all_degrees_df.withColumn(
        "degree", all_degrees_df.inDegree + all_degrees_df.outDegree
    )
    return all_degrees_df


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


def get_triangle_centralities(G, degree_df, return_avg_and_global_cc=False):
    """
    Computes triangle-based centralities (triangle count and local clustering coefficient)
    for each vertex in the given graph.

    Parameters
    ----------
    G : GraphFrame
        The input graph as a GraphFrame object.
    degree_df : pyspark.sql.DataFrame
        DataFrame containing vertex IDs and their degrees (degree = in_degree + out_degree).

    Returns
    -------
    triangles_df : pyspark.sql.DataFrame
        DataFrame containing vertex IDs, triangle counts, and local clustering coefficients (lcc).

    Notes
    -----
    Ignores edge direction; i.e., all edges are treated as undirected.
    """

    triangles_df = (
        G.triangleCount()
        .select("id", "count")
        .withColumnRenamed("count", "triangles_count")
    )  # counts the triangles for each vertex

    triangles_df = triangles_df.join(
        degree_df, on="id", how="full"
    )  # join the degrees column to the triangles counts for further manipulation

    triangles_df = triangles_df.withColumn(
        "triangles_max_count", triangles_df.degree * (triangles_df.degree - 1) / 2
    )  # creates a column with the maximum possible triangles the vertex can be part of given his degree

    triangles_df = triangles_df.withColumn(
        "lcc", triangles_df.triangles_count / triangles_df.triangles_max_count
    ).na.fill(
        0
    )  # creates a column with the local clustering coefficients for each vertex

    if not return_avg_and_global_cc:
        return triangles_df
    else:
        avg_and_global_cc_df = triangles_df.select(
            (
                F.sum(triangles_df["triangles_count"])
                / F.sum(triangles_df["triangles_max_count"])
            ).alias("global_cc"),
            F.avg(triangles_df["lcc"]).alias("average_cc"),
        )  # creates a dataframe with one row and two columns containing gloabl and avg cc
        return triangles_df, avg_and_global_cc_df


def get_density(G):
    number_of_edges = G.edges.count()
    number_of_vertices = G.vertices.count()
    max_number_of_edges = number_of_vertices * (number_of_vertices - 1)
    density = number_of_edges / max_number_of_edges
    return density
