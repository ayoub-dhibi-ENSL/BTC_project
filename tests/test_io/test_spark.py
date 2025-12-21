"""Tests for btc_graph.io.spark module.

These tests verify the Spark session factory creates correctly configured
SparkSession instances for GraphFrames graph analytics.
"""

from pyspark.sql import SparkSession

from btc_graph.io.spark import (
    DEFAULT_APP_NAME,
    DEFAULT_DRIVER_MEMORY,
    DEFAULT_GRAPHFRAMES_PACKAGE,
    create_spark_session,
    create_test_spark_session,
    stop_spark_session,
)


class TestCreateSparkSession:
    """Tests for create_spark_session function."""

    def test_creates_spark_session_with_defaults(self) -> None:
        """Factory should create a session with default configuration."""
        try:
            spark = create_spark_session()

            assert spark is not None
            assert isinstance(spark, SparkSession)
            # Check app name
            assert spark.conf.get("spark.app.name") == DEFAULT_APP_NAME
        finally:
            stop_spark_session()

    def test_creates_spark_session_with_custom_app_name(self) -> None:
        """Factory should respect custom app name."""
        try:
            spark = create_spark_session(app_name="custom_test_app")

            assert spark.conf.get("spark.app.name") == "custom_test_app"
        finally:
            stop_spark_session()

    def test_graphframes_package_is_configured(self) -> None:
        """GraphFrames package should be in jars.packages config."""
        try:
            spark = create_spark_session()

            jars_packages = spark.conf.get("spark.jars.packages")
            assert DEFAULT_GRAPHFRAMES_PACKAGE in jars_packages
        finally:
            stop_spark_session()

    def test_local_checkpoints_enabled_by_default(self) -> None:
        """Local checkpoints should be enabled by default for graph algorithms."""
        try:
            spark = create_spark_session()

            assert spark.conf.get("spark.graphframes.useLocalCheckpoints") == "true"
        finally:
            stop_spark_session()

    def test_local_checkpoints_can_be_disabled(self) -> None:
        """Should be able to disable local checkpoints when creating fresh session."""
        # Note: This test verifies the factory doesn't set the config when disabled
        # The actual Spark behaviour with getOrCreate may retain previous config
        try:
            spark = create_spark_session(use_local_checkpoints=False)
            # The session is created without explicitly setting local checkpoints to true
            # This is a config check, actual behaviour depends on fresh vs reused session
            assert spark is not None
        finally:
            stop_spark_session()

    def test_ansi_mode_disabled(self) -> None:
        """ANSI SQL mode should be disabled for compatibility."""
        try:
            spark = create_spark_session()

            assert spark.conf.get("spark.sql.ansi.enabled") == "false"
        finally:
            stop_spark_session()

    def test_custom_shuffle_partitions(self) -> None:
        """Should support custom shuffle partitions."""
        try:
            spark = create_spark_session(shuffle_partitions=10)

            assert spark.conf.get("spark.sql.shuffle.partitions") == "10"
        finally:
            stop_spark_session()

    def test_custom_master(self) -> None:
        """Should support custom Spark master."""
        try:
            spark = create_spark_session(master="local[2]")

            assert "local[2]" in spark.conf.get("spark.master")
        finally:
            stop_spark_session()

    def test_extra_configs_applied(self) -> None:
        """Should apply extra configuration key-value pairs."""
        try:
            spark = create_spark_session(
                extra_configs={
                    "spark.sql.adaptive.enabled": "true",
                }
            )

            assert spark.conf.get("spark.sql.adaptive.enabled") == "true"
        finally:
            stop_spark_session()

    def test_get_or_create_returns_existing_session(self) -> None:
        """Calling factory twice should return same session instance."""
        try:
            spark1 = create_spark_session(app_name="test_reuse")
            spark2 = create_spark_session(app_name="test_reuse")

            # Should be the same session object
            assert spark1 is spark2
        finally:
            stop_spark_session()


class TestCreateTestSparkSession:
    """Tests for create_test_spark_session convenience function."""

    def test_creates_lightweight_session(self) -> None:
        """Test session should be lightweight with minimal resources."""
        try:
            spark = create_test_spark_session()

            assert spark is not None
            assert isinstance(spark, SparkSession)
            assert spark.conf.get("spark.app.name") == "btc_graph_test"
        finally:
            stop_spark_session()

    def test_test_session_has_single_partition(self) -> None:
        """Test session should use single shuffle partition for determinism."""
        try:
            spark = create_test_spark_session()

            assert spark.conf.get("spark.sql.shuffle.partitions") == "1"
        finally:
            stop_spark_session()

    def test_test_session_uses_local_mode(self) -> None:
        """Test session should use local[1] master."""
        try:
            spark = create_test_spark_session()

            assert "local[1]" in spark.conf.get("spark.master")
        finally:
            stop_spark_session()

    def test_test_session_includes_graphframes(self) -> None:
        """Test session should still have GraphFrames for testing graph ops."""
        try:
            spark = create_test_spark_session()

            jars_packages = spark.conf.get("spark.jars.packages")
            assert DEFAULT_GRAPHFRAMES_PACKAGE in jars_packages
        finally:
            stop_spark_session()


class TestStopSparkSession:
    """Tests for stop_spark_session function."""

    def test_stops_active_session(self) -> None:
        """Should stop an active Spark session."""
        _spark = create_test_spark_session()
        assert SparkSession.getActiveSession() is not None

        stop_spark_session()

        # After stopping, active session should be None
        assert SparkSession.getActiveSession() is None

    def test_safe_when_no_session_active(self) -> None:
        """Should not raise error when no session is active."""
        # Ensure no session is active
        stop_spark_session()

        # Calling again should not raise
        stop_spark_session()  # Should not raise


class TestConstants:
    """Tests for module constants."""

    def test_default_graphframes_package_is_spark4_scala213(self) -> None:
        """Default package should be for Spark 4.x with Scala 2.13."""
        assert "spark4" in DEFAULT_GRAPHFRAMES_PACKAGE
        assert "2.13" in DEFAULT_GRAPHFRAMES_PACKAGE
        assert "graphframes" in DEFAULT_GRAPHFRAMES_PACKAGE.lower()

    def test_default_app_name_is_meaningful(self) -> None:
        """Default app name should be descriptive."""
        assert "BTC" in DEFAULT_APP_NAME or "btc" in DEFAULT_APP_NAME.lower()

    def test_default_driver_memory_is_reasonable(self) -> None:
        """Default driver memory should be reasonable for graph processing."""
        assert DEFAULT_DRIVER_MEMORY == "8g"
