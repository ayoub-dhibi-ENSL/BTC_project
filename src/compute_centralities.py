# %% Imports
import pyspark.pandas as ps  # type: ignore
from pyspark.sql import SparkSession  # type: ignore
import matplotlib.pyplot as plt  # type: ignore

# %% Creating SparkSession and loading the data
spark = (
    SparkSession.builder.appName("PandasOnSparkExample")
    .config("spark.sql.ansi.enabled", "false")  # disable ANSI mode
    .config("spark.jars.packages", "io.graphframes:graphframes-spark4_2.13:0.9.3")
    .getOrCreate()
)

# pydf_snapshot_year_2015 = ps.read_parquet(
#     "../data/orbitaal-snapshot-year/SNAPSHOT/EDGES/year/*2015*.parquet"
# )
# pydf_snapshot_year_2015 = pydf_snapshot_year_2015.sample(
#     frac=0.0001, random_state=42
# )  # Sample x% of the data

pydf = ps.read_csv("../data/orbitaal-snapshot-2016_07_09.csv")

# %% Creating degree columns
OUT_DEGREE = pydf.groupby("SRC_ID").size().rename("OUT_DEGREE")
IN_DEGREE = pydf.groupby("DST_ID").size().rename("IN_DEGREE")
WEIGHTED_SATOSHI_OUT_DEGREE = (
    pydf.groupby("SRC_ID")["VALUE_SATOSHI"].sum().rename("WEIGHTED_SATOSHI_OUT_DEGREE")
)
WEIGHTED_SATOSHI_IN_DEGREE = (
    pydf.groupby("DST_ID")["VALUE_SATOSHI"].sum().rename("WEIGHTED_SATOSHI_IN_DEGREE")
)
WEIGHTED_USD_OUT_DEGREE = (
    pydf.groupby("SRC_ID")["VALUE_USD"].sum().rename("WEIGHTED_USD_OUT_DEGREE")
)
WEIGHTED_USD_IN_DEGREE = (
    pydf.groupby("DST_ID")["VALUE_USD"].sum().rename("WEIGHTED_USD_IN_DEGREE")
)

degree_columns = [
    OUT_DEGREE,
    IN_DEGREE,
    WEIGHTED_SATOSHI_OUT_DEGREE,
    WEIGHTED_SATOSHI_IN_DEGREE,
    WEIGHTED_USD_OUT_DEGREE,
    WEIGHTED_USD_IN_DEGREE,
]

for col in degree_columns:
    col.index.name = None

result = ps.concat(degree_columns, axis=1).fillna(0)

# %% Plotting degrees distributions
# Convert result to pandas DataFrame for plotting
result_pd = result.to_pandas()

# Plot histograms for each column
for col in result_pd.columns:
    plt.figure(figsize=(8, 4))
    plt.hist(result_pd[col], bins=50, color="skyblue", edgecolor="black", log=True)
    plt.title(f"Histogram of {col}")
    plt.xlabel(col)
    plt.ylabel("Frequency (log scale)")
    plt.tight_layout()
    plt.show()

# %% Getting the vertices identifiers
IDS = ps.concat(
    [pydf["SRC_ID"], pydf["DST_ID"]]
).unique()  # gets the all the IDs that were in a transaction i.e. all the different nodes of the graph

number_of_nodes = IDS.size
graph_density = pydf.size / (number_of_nodes * (number_of_nodes - 1))

# %% Using GraphFrames to compute centralities
from graphframes import *

VERTICES = IDS.to_frame(name="id").to_spark()
EDGES = (
    pydf[["SRC_ID", "DST_ID"]]
    .rename(columns={"SRC_ID": "src", "DST_ID": "dst"})
    .to_spark()
)  # Renaming columns according to graphframes doc
# %% Creating the graph
G = GraphFrame(VERTICES, EDGES)  # type: ignore
PageRankGraph = G.pageRank(resetProbability=0.15, tol=0.01)

# %%
