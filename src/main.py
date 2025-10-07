from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType
from pyspark.sql import functions as F
from utils import sparkDataframe_to_GraphFrame, save_to_csv
from centralities import get_degrees, get_triangle_centralities, get_density
from plots import hist_plots
from cli import get_arguments


def start_SparkSession():
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


def process_data(spark, year_start, year_end):
    start = year_start - 2008
    end = year_end - 2008

    schema = StructType(
        [
            StructField("SRC_ID", IntegerType(), True),
            StructField("DST_ID", IntegerType(), True),
            StructField("VALUE_SATOSHI", LongType(), True),
            StructField("VALUE_USD", DoubleType(), True),
        ]
    )  # Providing the schema makes loading the file faster.

    # Loop through each year and corresponding file id to process the data.
    for i in range(start, end):
        # Load the data from a parquet file to a pyspark.sql.DataFrame object.
        year = 2008 + i
        id = f"{i:02d}"
        path_parquet = f"../data/orbitaal-snapshot-year/SNAPSHOT/EDGES/year/orbitaal-snapshot-date-{year}-file-id-{id}.snappy.parquet"
        df = spark.read.parquet(path_parquet, inferSchema=False, schema=schema)

        # Converts the data to a GraphFrame object.
        G = sparkDataframe_to_GraphFrame(df, "SRC_ID", "DST_ID")

        # Get the in/out degrees.
        all_degrees_df = get_degrees(G)

        degrees_df = all_degrees_df.select(
            "id", "degree"
        )  # Gets the total degree (in + out) to compute triangles centralities.
        triangles_df, avg_and_global_cc_df = get_triangle_centralities(
            G, degrees_df, return_avg_and_global_cc=True
        )

        d = get_density(G)
        scalar_centralities_df = avg_and_global_cc_df.withColumn(
            "density", F.lit(d)
        )  # Scalar graph wise centralities are saved in a DataFrame.

        # Saves the processed data in csv files.
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


def make_plots(year_start, year_end):
    start = year_start - 2008
    end = year_end - 2008
    # Save plots of the processed data
    for i in range(start, end):
        year = 2008 + i
        id = f"{i:02d}"
        hist_plots(year, id)


if __name__ == "__main__":
    args = get_arguments()
    year_start = args.year_start
    year_end = args.year_end

    if args.C:
        print("Processing the data ...")
        spark = start_SparkSession()
        process_data(spark, year_start, year_end)

    elif args.P:
        print("Making plots and saving them ...")
        make_plots(year_start, year_end)

    elif args.B:
        print("Processing the data, making the plots and svaing them ...")
        spark = start_SparkSession()
        process_data(spark, year_start, year_end)
        make_plots(year_start, year_end)
