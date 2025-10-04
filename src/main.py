# %%
import compute_centralities as cc
import pyspark.pandas as ps  # type: ignore
from pyspark.sql import SparkSession  # type: ignore

# %%
spark = (
    SparkSession.builder.appName("PandasOnSparkExample")
    .config("spark.sql.ansi.enabled", "false")  # disable ANSI mode
    .config(
        "spark.jars.packages", "io.graphframes:graphframes-spark4_2.13:0.9.3"
    )  # enable to use graphframes
    .config(
        "spark.graphframes.useLocalCheckpoints", "true"
    )  # use local checkpoints for the connected component algorithme
    .getOrCreate()
)

# pydf_snapshot_year_2015 = ps.read_parquet(
#     "../data/orbitaal-snapshot-year/SNAPSHOT/EDGES/year/*2015*.parquet"
# )
# pydf_snapshot_year_2015 = pydf_snapshot_year_2015.sample(
#     frac=0.0001, random_state=42
# )  # Sample x% of the data

pydf = ps.read_csv("../data/orbitaal-snapshot-2016_07_09.csv")

# OUT_DEGREE, IN_DEGREE = cc.create_degree_col(pydf, "SRC_ID", "DST_ID")
# WEIGHTED_SATOSHI_OUT_DEGREE, WEIGHTED_SATOSHI_IN_DEGREE = cc.create_degree_col(
#     pydf, "SRC_ID", "DST_ID", weighted=True, weight_colname="VALUE_SATOSHI"
# )
# WEIGHTED_USD_OUT_DEGREE, WEIGHTED_USD_IN_DEGREE = cc.create_degree_col(
#     pydf, "SRC_ID", "DST_ID", weighted=True, weight_colname="VALUE_USD"
# )

# degree_columns = [
#     OUT_DEGREE,
#     IN_DEGREE,
#     WEIGHTED_SATOSHI_OUT_DEGREE,
#     WEIGHTED_SATOSHI_IN_DEGREE,
#     WEIGHTED_USD_OUT_DEGREE,
#     WEIGHTED_USD_IN_DEGREE,
# ]

# for col in degree_columns:
#     col.index.name = None

# result = ps.concat(degree_columns, axis=1).fillna(0)

# result_pd = result.to_pandas()

# for col in result_pd.columns:
#     plt.figure(figsize=(8, 4))
#     plt.hist(result_pd[col], bins=50, color="skyblue", edgecolor="black", log=True)
#     plt.title(f"Histogram of {col}")
#     plt.xlabel(col)
#     plt.ylabel("Frequency (log scale)")
#     plt.tight_layout()
#     plt.show()


IDS, number_of_nodes, graph_density = cc.get_vertices_and_density(
    pydf, "SRC_ID", "DST_ID"
)

G, VERTICES, EDGES = cc.pydf_to_graphframe(pydf, IDS, "SRC_ID", "DST_ID")
# triangles_df = cc.get_triangles(G)

# max_triangles_df =
# Local clustering coefficient

# %%
INDEG = G.inDegrees
# %%
