"""Spark session factory for Bitcoin blockchain graph analysis.

This module provides a centralized way to create and configure SparkSession
instances with all the necessary settings for GraphFrames and large-scale
graph processing.

Design Notes
------------
- All Spark/GraphFrames configuration is centralized here to avoid duplication.
- The factory supports both production and testing configurations.
- GraphFrames JAR is automatically loaded via Maven coordinates.

Typical Usage
-------------
>>> from btc_graph.io.spark import create_spark_session
>>> spark = create_spark_session()
>>> # Use spark for loading data and graph operations
>>> df = spark.read.parquet("data/snapshot.parquet")
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from pyspark.sql import SparkSession as SparkSessionType

# Default GraphFrames package for Spark 4.x with Scala 2.13
DEFAULT_GRAPHFRAMES_PACKAGE = "io.graphframes:graphframes-spark4_2.13:0.9.3"

# Default Spark configuration values
DEFAULT_DRIVER_MEMORY = "8g"
DEFAULT_APP_NAME = "BTC_Graph_Analysis"


def create_spark_session(
    app_name: str = DEFAULT_APP_NAME,
    driver_memory: str = DEFAULT_DRIVER_MEMORY,
    graphframes_package: str = DEFAULT_GRAPHFRAMES_PACKAGE,
    use_local_checkpoints: bool = True,
    shuffle_partitions: Optional[int] = None,
    master: Optional[str] = None,
    extra_configs: Optional[dict] = None,
) -> "SparkSessionType":
    """Create and configure a SparkSession for Bitcoin graph analysis.

    This factory function creates a SparkSession with all necessary
    configurations for GraphFrames graph analytics, including JAR loading,
    memory settings, and checkpoint optimization.

    Parameters
    ----------
    app_name : str, optional
        Name of the Spark application. Default is "BTC_Graph_Analysis".
    driver_memory : str, optional
        Amount of memory for the Spark driver. Default is "8g".
        Increase for larger datasets.
    graphframes_package : str, optional
        Maven coordinates for the GraphFrames package.
        Default is for Spark 4.x with Scala 2.13.
    use_local_checkpoints : bool, optional
        Whether to use local checkpoints for iterative graph algorithms.
        Default is True (recommended for PageRank, connected components).
    shuffle_partitions : int, optional
        Number of shuffle partitions. If None, uses Spark's default (200).
        Lower values (e.g., 10-50) can improve performance for smaller datasets.
    master : str, optional
        Spark master URL. If None, uses existing configuration or local mode.
        Examples: "local[*]", "local[4]", "spark://host:7077"
    extra_configs : dict, optional
        Additional Spark configuration key-value pairs.

    Returns
    -------
    pyspark.sql.SparkSession
        Configured SparkSession ready for graph analytics.

    Examples
    --------
    >>> from btc_graph.io.spark import create_spark_session
    >>>
    >>> # Default configuration for production
    >>> spark = create_spark_session()
    >>>
    >>> # Testing configuration with less memory
    >>> spark = create_spark_session(
    ...     app_name="test",
    ...     driver_memory="2g",
    ...     master="local[1]",
    ...     shuffle_partitions=1,
    ... )
    >>>
    >>> # Custom configuration
    >>> spark = create_spark_session(
    ...     driver_memory="16g",
    ...     extra_configs={
    ...         "spark.executor.memory": "8g",
    ...         "spark.sql.adaptive.enabled": "true",
    ...     }
    ... )

    Notes
    -----
    - The function uses ``getOrCreate()`` so it will return an existing
      session if one is already active with compatible configuration.
    - GraphFrames JAR is loaded via ``spark.jars.packages`` which downloads
      from Maven Central on first use.
    - ANSI SQL mode is disabled for compatibility with legacy SQL operations.

    See Also
    --------
    pyspark.sql.SparkSession : The underlying Spark session class.
    """
    from pyspark.sql import SparkSession

    # Start building the session
    builder = SparkSession.builder.appName(app_name)

    # Set master if provided
    if master is not None:
        builder = builder.master(master)

    # Core configurations
    builder = builder.config("spark.driver.memory", driver_memory)

    # GraphFrames package (loads JAR from Maven)
    builder = builder.config("spark.jars.packages", graphframes_package)

    # Disable ANSI SQL mode for compatibility with legacy operations
    builder = builder.config("spark.sql.ansi.enabled", "false")

    # Local checkpoints for iterative graph algorithms (PageRank, etc.)
    if use_local_checkpoints:
        builder = builder.config("spark.graphframes.useLocalCheckpoints", "true")

    # Shuffle partitions (useful for smaller datasets or testing)
    if shuffle_partitions is not None:
        builder = builder.config(
            "spark.sql.shuffle.partitions", str(shuffle_partitions)
        )

    # Apply any extra configurations
    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()


def create_test_spark_session(
    app_name: str = "btc_graph_test",
) -> "SparkSessionType":
    """Create a minimal SparkSession for unit testing.

    This is a convenience function that creates a lightweight Spark session
    suitable for unit tests, with minimal resources and single-threaded
    execution for deterministic behaviour.

    Parameters
    ----------
    app_name : str, optional
        Name of the test application. Default is "btc_graph_test".

    Returns
    -------
    pyspark.sql.SparkSession
        Lightweight SparkSession for testing.

    Examples
    --------
    >>> from btc_graph.io.spark import create_test_spark_session
    >>> spark = create_test_spark_session()
    >>> # Run tests with this session

    Notes
    -----
    - Uses ``local[1]`` master for single-threaded execution.
    - Sets shuffle partitions to 1 for faster test execution.
    - Still loads GraphFrames for testing graph operations.
    """
    return create_spark_session(
        app_name=app_name,
        driver_memory="2g",
        master="local[1]",
        shuffle_partitions=1,
    )


def stop_spark_session() -> None:
    """Stop the active SparkSession if one exists.

    This is useful for cleanup in tests or when switching configurations.

    Examples
    --------
    >>> from btc_graph.io.spark import create_spark_session, stop_spark_session
    >>> spark = create_spark_session()
    >>> # ... do work ...
    >>> stop_spark_session()
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is not None:
        spark.stop()
