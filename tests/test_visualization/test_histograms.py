"""Tests for btc_graph.visualization.histograms module."""

import tempfile
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def sample_degrees_df() -> pd.DataFrame:
    """Create a sample degrees DataFrame for testing."""
    np.random.seed(42)
    n = 1000
    return pd.DataFrame(
        {
            "inDegree": np.random.power(2.5, n) * 100,
            "outDegree": np.random.power(2.5, n) * 100,
            "degree": np.random.power(2.5, n) * 200,
        },
        index=pd.RangeIndex(n, name="id"),
    )


@pytest.fixture
def sample_triangles_df() -> pd.DataFrame:
    """Create a sample triangles DataFrame for testing."""
    np.random.seed(42)
    n = 1000
    return pd.DataFrame(
        {
            "triangles_count": np.random.poisson(5, n),
            "triangles_max_count": np.random.poisson(20, n),
            "lcc": np.random.uniform(0, 1, n),
        },
        index=pd.RangeIndex(n, name="id"),
    )


class TestPlotMetricHistograms:
    """Tests for plot_metric_histograms function."""

    def test_creates_figure(self, sample_degrees_df: pd.DataFrame) -> None:
        """Should create a matplotlib figure."""
        from btc_graph.visualization.histograms import plot_metric_histograms

        fig = plot_metric_histograms(sample_degrees_df)

        assert fig is not None
        assert isinstance(fig, plt.Figure)

        plt.close(fig)

    def test_saves_to_file(self, sample_degrees_df: pd.DataFrame) -> None:
        """Should save figure to specified path."""
        from btc_graph.visualization.histograms import plot_metric_histograms

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_hist.pdf"

            fig = plot_metric_histograms(
                sample_degrees_df,
                output_path=str(output_path),
            )

            assert output_path.exists()

            plt.close(fig)

    def test_plots_specific_columns(self, sample_degrees_df: pd.DataFrame) -> None:
        """Should plot only specified columns."""
        from btc_graph.visualization.histograms import plot_metric_histograms

        fig = plot_metric_histograms(
            sample_degrees_df,
            columns=["degree"],
        )

        # Should have only 1 subplot
        assert len(fig.axes) == 1

        plt.close(fig)

    def test_shows_statistics(self, sample_degrees_df: pd.DataFrame) -> None:
        """Should show mean and median lines when show_stats=True."""
        from btc_graph.visualization.histograms import plot_metric_histograms

        fig = plot_metric_histograms(
            sample_degrees_df,
            columns=["degree"],
            show_stats=True,
        )

        # Check axes has legend (with mean/median)
        ax = fig.axes[0]
        legend = ax.get_legend()
        assert legend is not None

        plt.close(fig)

    def test_handles_triangles_data(self, sample_triangles_df: pd.DataFrame) -> None:
        """Should handle triangle metrics correctly."""
        from btc_graph.visualization.histograms import plot_metric_histograms

        fig = plot_metric_histograms(sample_triangles_df)

        assert fig is not None

        plt.close(fig)


class TestPlotDegreeDistribution:
    """Tests for plot_degree_distribution function."""

    def test_creates_figure(self, sample_degrees_df: pd.DataFrame) -> None:
        """Should create a degree distribution plot."""
        from btc_graph.visualization.histograms import plot_degree_distribution

        fig = plot_degree_distribution(sample_degrees_df)

        assert fig is not None

        plt.close(fig)

    def test_log_log_scale(self, sample_degrees_df: pd.DataFrame) -> None:
        """Should use log-log scale by default."""
        from btc_graph.visualization.histograms import plot_degree_distribution

        fig = plot_degree_distribution(sample_degrees_df, log_log=True)

        ax = fig.axes[0]
        assert ax.get_xscale() == "log"
        assert ax.get_yscale() == "log"

        plt.close(fig)

    def test_linear_scale(self, sample_degrees_df: pd.DataFrame) -> None:
        """Should support linear scale."""
        from btc_graph.visualization.histograms import plot_degree_distribution

        fig = plot_degree_distribution(sample_degrees_df, log_log=False)

        ax = fig.axes[0]
        assert ax.get_xscale() == "linear"

        plt.close(fig)


class TestGetColumnTitle:
    """Tests for _get_column_title helper function."""

    def test_known_column_names(self) -> None:
        """Should return readable titles for known columns."""
        from btc_graph.visualization.histograms import _get_column_title

        assert "Clustering" in _get_column_title("lcc")
        assert "Degree" in _get_column_title("degree")
        assert "PageRank" in _get_column_title("pagerank")

    def test_unknown_column_names(self) -> None:
        """Should return column name for unknown columns."""
        from btc_graph.visualization.histograms import _get_column_title

        assert "custom_metric" in _get_column_title("custom_metric")

    def test_with_prefix(self) -> None:
        """Should include prefix in title."""
        from btc_graph.visualization.histograms import _get_column_title

        title = _get_column_title("degree", prefix="Snapshot 001")
        assert "Snapshot 001" in title
        assert "Degree" in title
