# %%
from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType  # type: ignore
from utils import sparkDataframe_to_GraphFrame
from centralities import get_degrees, get_triangle_centralities

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

path = "../data/orbitaal-snapshot-2016_07_09.csv"
schema = StructType(
    [
        StructField("SRC_ID", IntegerType(), True),
        StructField("DST_ID", IntegerType(), True),
        StructField("VALUE_SATOSHI", LongType(), True),
        StructField("VALUE_USD", DoubleType(), True),
    ]
)  # providing the schema makes the loading of the file faster
df = spark.read.csv(path, header=True, inferSchema=False, schema=schema)

# %%
G = sparkDataframe_to_GraphFrame(df, "SRC_ID", "DST_ID")

# %%
all_degrees_df = get_degrees(G)
degrees_df = all_degrees_df.select("id", "degree")
test_df, test_cc = get_triangle_centralities(
    G, degrees_df, return_avg_and_global_cc=True
)

# %%
