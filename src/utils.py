from graphframes import GraphFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


def start_SparkSession():
    """
    Creates and configures a SparkSession for the BTC_project.

    This function initializes a SparkSession with custom settings:
        - Disables ANSI SQL mode for compatibility with legacy SQL features.
        - Loads the GraphFrames package for advanced graph analytics.
        - Enables local checkpoints to optimize iterative graph algorithms.
        - Sets increased driver memory to efficiently process large datasets.

    Returns
    -------
    pyspark.sql.SparkSession
        Configured SparkSession object.

    Notes
    -----
    The SparkSession is the entry point to programming Spark with the Dataset and DataFrame API.
    For more details, see: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.html
    """
    # Initialize SparkSession with custom configurations.
    spark = (
        SparkSession.builder.appName("BTC_project")
        .config(
            "spark.sql.ansi.enabled", "false"
        )  # Disables ANSI SQL mode for compatibility.
        .config(
            "spark.jars.packages", "io.graphframes:graphframes-spark4_2.13:0.9.3"
        )  # Loads the GraphFrames package for graph analytics.
        .config(
            "spark.graphframes.useLocalCheckpoints", "true"
        )  # Enables local checkpoints for efficient graph algorithms.
        .config(
            "spark.driver.memory", "8g"
        )  # Increases driver memory to handle large datasets.
        .getOrCreate()
    )
    return spark


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


# def normalize_cols(df, cols):
#     df2 = df.select((F.col(cols) / F.sum(df[cols])).alias("scaled_column"))
#     df2.show()


def normalize_cols(df, cols):
    """
    Return a DataFrame with normalized column(s).
    - cols may be a string (single column) or list of strings.
    - For each column c in cols a new column named '{c}_scaled' is added with values = c / sum(c).
    """
    if isinstance(cols, str):
        cols = [cols]

    # compute totals for each column (single aggregated call)
    agg_exprs = [F.sum(F.col(c)).alias(f"__sum_{c}") for c in cols]
    totals_row = df.agg(*agg_exprs).collect()[0]

    result = df
    for c in cols:
        total = totals_row[f"__sum_{c}"] or 0.0
        if float(total) == 0.0:
            # avoid division by zero: produce zeros
            result = result.withColumn(f"{c}_scaled", F.lit(0.0))
        else:
            result = result.withColumn(f"{c}_scaled", F.col(c) / F.lit(float(total)))
    return result


def save_to_csv(df, file_path):
    df.coalesce(1).write.csv(file_path, mode="overwrite", header=True)
