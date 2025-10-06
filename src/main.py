# %%
from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType  # type: ignore
from pyspark.sql import functions as F  # type: ignore
from utils import sparkDataframe_to_GraphFrame, save_to_csv
from centralities import get_degrees, get_triangle_centralities, get_density

# %%
spark = (
    SparkSession.builder.appName("BTC_project")
    .config("spark.sql.ansi.enabled", "false")  # disable ANSI mode
    .config(
        "spark.jars.packages", "io.graphframes:graphframes-spark4_2.13:0.9.3"
    )  # enable to use graphframes
    .config(
        "spark.graphframes.useLocalCheckpoints", "true"
    )  # use local checkpoints for the connected component algorithme
    .config(
        "spark.driver.memory", "8g"
    )  # increases the driver memory to show() larger data
    .getOrCreate()
)

schema = StructType(
    [
        StructField("SRC_ID", IntegerType(), True),
        StructField("DST_ID", IntegerType(), True),
        StructField("VALUE_SATOSHI", LongType(), True),
        StructField("VALUE_USD", DoubleType(), True),
    ]
)  # providing the schema makes the loading of the file faster

# path_csv = "../data/orbitaal-snapshot-2016_07_09.csv"
# df = spark.read.csv(path_csv, header=True, inferSchema=False, schema=schema)
# %%
for i in range(1, 14):
    year = 2000 + i
    id = f"{i:02d}"
    print(id)
    path_parquet = f"../data/orbitaal-snapshot-year/SNAPSHOT/EDGES/year/orbitaal-snapshot-date-{year}-file-id-{id}.snappy.parquet"
    df = spark.read.parquet(path_parquet, inferSchema=False, schema=schema)

# %%
path_parquet = "../data/orbitaal-snapshot-year/SNAPSHOT/EDGES/year/orbitaal-snapshot-date-2009-file-id-01.snappy.parquet"
df = spark.read.parquet(path_parquet, inferSchema=False, schema=schema)
# %%
G = sparkDataframe_to_GraphFrame(df, "SRC_ID", "DST_ID")

all_degrees_df = get_degrees(G)

degrees_df = all_degrees_df.select("id", "degree")
triangles_df, avg_and_global_cc_df = get_triangle_centralities(
    G, degrees_df, return_avg_and_global_cc=True
)

d = get_density(G)
scalar_centralities_df = avg_and_global_cc_df.withColumn("density", F.lit(d))


# %%
year = "2009"
id = "01"
file_path_triangles = f"../data/snapshot-year-analysis/{year}-{id}/triangles/"
file_path_degrees = f"../data/snapshot-year-analysis/{year}-{id}/degrees/"
file_path_scalar = f"../data/snapshot-year-analysis/{year}-{id}/scalar/"
save_dict = {
    file_path_degrees: all_degrees_df,
    file_path_scalar: scalar_centralities_df,
    file_path_triangles: triangles_df,
}

for file_path in save_dict:
    df = save_dict[file_path]
    save_to_csv(df, file_path)
# %%
