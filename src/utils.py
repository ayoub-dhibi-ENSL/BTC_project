from graphframes import GraphFrame  # type: ignore
from pyspark.sql import functions as F  # type: ignore


def create_vertex_df(df, src_colname, dst_colname, new_vertex_colname):
    """
    Creates a DataFrame containing distinct vertices from source and destination columns.
    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The input DataFrame containing the source and destination columns.
    src_colname : str
        The name of the source column in the DataFrame.
    dst_colname : str
        The name of the destination column in the DataFrame.
    new_vertex_colname : str
        The name to assign to the vertex column in the resulting DataFrame.
    Returns
    -------
    pyspark.sql.DataFrame
        A DataFrame with a single column containing distinct vertex values.
    """
    vertex_df = (
        df.select(F.col(src_colname).alias(new_vertex_colname))
        .union(df.select(F.col(dst_colname)).alias(new_vertex_colname))
        .distinct()
    )
    return vertex_df


def sparkDataframe_to_GraphFrame(df, src_colname, dst_colname):
    """
    Converts a Spark DataFrame representing edges into a GraphFrame object.
    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The input Spark DataFrame containing edge information.
    src_colname : str
        The name of the column in `df` representing the source vertex.
    dst_colname : str
        The name of the column in `df` representing the destination vertex.
    Returns
    -------
    GraphFrame
        A GraphFrame object constructed from the input DataFrame.
    Notes
    -----
    The function renames the source and destination columns to 'src' and 'dst',
    creates a vertices DataFrame, and constructs a GraphFrame using these DataFrames.
    """
    edges_df = df.withColumnsRenamed({src_colname: "src", dst_colname: "dst"})
    vertices_df = create_vertex_df(edges_df, "src", "dst", "id")
    G = GraphFrame(vertices_df, edges_df)
    return G
