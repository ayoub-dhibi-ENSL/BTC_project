from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType
from pyspark.sql import functions as F
from utils import sparkDataframe_to_GraphFrame, save_to_csv, start_SparkSession
from centralities import get_degrees, get_triangle_centralities, get_density
from plots import make_plots
from cli import get_arguments
import glob


def process_data(spark, resolution, arg_sample=False):
    """
    Processes transaction data from Parquet files, computes graph-based metrics, and saves results as CSV files.
    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        The Spark session used to read Parquet files and perform distributed computations.
    resolution : str
        The time resolution of the data to process. Supported values are "year" and "hour".
    Notes
    -----
    - Loads Parquet files matching the specified resolution.
    - Converts transaction data into a GraphFrame.
    - Computes node degrees, triangle centralities, and graph density.
    - Saves the computed metrics (degrees, triangle centralities, scalar centralities) as CSV files in structured directories.
    """
    schema = StructType(
        [
            StructField("SRC_ID", IntegerType(), True),
            StructField("DST_ID", IntegerType(), True),
            StructField("VALUE_SATOSHI", LongType(), True),
            StructField("VALUE_USD", DoubleType(), True),
        ]
    )  # Providing the schema makes loading the file faster.

    paths_parquet = glob.glob(
        f"../data/orbitaal-snapshot-{resolution}/SNAPSHOT/EDGES/{resolution}/orbitaal-snapshot-date-*-file-id-*.snappy.parquet"
    )  # List of all the paths of the parquet files for the corresponding resolution.

    if arg_sample and resolution == "hour":
        paths_parquet = paths_parquet[:20]
    elif arg_sample and resolution == "year":
        paths_parquet = paths_parquet[:2]

    snapshots_count = len(paths_parquet)
    for i in range(snapshots_count):
        # Load the data from a parquet file to a pyspark.sql.DataFrame object.
        if resolution == "year":
            id = f"{i:02d}"
        elif resolution == "hour":
            id = f"{i:06d}"

        path_parquet = paths_parquet[i]
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
        file_path_triangles = (
            f"../data/snapshot-{resolution}-analysis/{resolution}-{id}/triangles/"
        )
        file_path_degrees = (
            f"../data/snapshot-{resolution}-analysis/{resolution}-{id}/degrees/"
        )
        file_path_scalar = (
            f"../data/snapshot-{resolution}-analysis/{resolution}-{id}/scalar/"
        )

        save_dict = {
            file_path_degrees: all_degrees_df,
            file_path_scalar: scalar_centralities_df,
            file_path_triangles: triangles_df,
        }

        for file_path in save_dict:
            df = save_dict[file_path]
            save_to_csv(df, file_path)


def main():
    print("Bitcoin Graph")
    args = get_arguments()
    resolution = args.resolution
    arg_sample = args.sample

    if args.compute:
        print("Processing the data ...")
        spark = start_SparkSession()
        process_data(spark, resolution, arg_sample)

    elif args.plot:
        print("Making plots and saving them ...")
        make_plots(resolution)

    elif args.both:
        print("Processing the data, making the plots and svaing them ...")
        spark = start_SparkSession()
        process_data(spark, resolution, arg_sample)
        make_plots(resolution)


if __name__ == "__main__":
    main()
