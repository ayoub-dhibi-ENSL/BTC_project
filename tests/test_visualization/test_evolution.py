"""Tests for btc_graph.visualization.evolution module."""

import tempfile
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def sample_analysis_dir():
    """Create a temporary analysis directory with sample data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)

        # Create 5 snapshot directories with degrees data
        for i in range(5):
            snapshot_id = f"hour-{i:06d}"
            degrees_dir = base / snapshot_id / "degrees"
            degrees_dir.mkdir(parents=True)

            # Generate random degree data
            np.random.seed(42 + i)
            n = 500 + i * 100  # Varying sizes
            df = pd.DataFrame(
                {
                    "inDegree": np.random.power(2.5, n) * 100,
                    "outDegree": np.random.power(2.5, n) * 100,
                    "degree": np.random.power(2.5, n) * 200,
                },
                index=pd.RangeIndex(n, name="id"),
            )
            df.to_csv(degrees_dir / "part-00000.csv")

        yield str(base)


class TestCryptoEvent:
    """Tests for CryptoEvent dataclass."""

    def test_create_event(self) -> None:
        """Should create an event with required fields."""
        from btc_graph.visualization.evolution import CryptoEvent

        event = CryptoEvent(index=5, name="Test Event")

        assert event.index == 5
        assert event.name == "Test Event"
        assert event.description == ""

    def test_create_event_with_description(self) -> None:
        """Should support optional description."""
        from btc_graph.visualization.evolution import CryptoEvent

        event = CryptoEvent(
            index=10,
            name="Major Event",
            description="Something significant happened",
        )

        assert event.description == "Something significant happened"


class TestLoadMetricSeries:
    """Tests for load_metric_series function."""

    def test_loads_data_from_directory(self, sample_analysis_dir: str) -> None:
        """Should load metric data from analysis directory."""
        from btc_graph.visualization.evolution import load_metric_series

        data, labels = load_metric_series(
            sample_analysis_dir,
            resolution="hour",
            metric="degrees",
        )

        assert len(data) == 5
        assert len(labels) == 5

    def test_respects_limit(self, sample_analysis_dir: str) -> None:
        """Should respect the limit parameter."""
        from btc_graph.visualization.evolution import load_metric_series

        data, labels = load_metric_series(
            sample_analysis_dir,
            resolution="hour",
            metric="degrees",
            limit=3,
        )

        assert len(data) == 3

    def test_returns_positive_values(self, sample_analysis_dir: str) -> None:
        """Should filter out non-positive values."""
        from btc_graph.visualization.evolution import load_metric_series

        data, labels = load_metric_series(
            sample_analysis_dir,
            resolution="hour",
            metric="degrees",
        )

        for d in data:
            assert np.all(d > 0)


class TestPlotHistogramEvolution:
    """Tests for plot_histogram_evolution function."""

    def test_creates_figure(self, sample_analysis_dir: str) -> None:
        """Should create a heatmap figure."""
        from btc_graph.visualization.evolution import plot_histogram_evolution

        fig = plot_histogram_evolution(
            sample_analysis_dir,
            resolution="hour",
            metric="degrees",
        )

        assert fig is not None
        assert isinstance(fig, plt.Figure)

        plt.close(fig)

    def test_saves_to_file(self, sample_analysis_dir: str) -> None:
        """Should save figure to specified path."""
        from btc_graph.visualization.evolution import plot_histogram_evolution

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "evolution.pdf"

            fig = plot_histogram_evolution(
                sample_analysis_dir,
                resolution="hour",
                output_path=str(output_path),
            )

            assert output_path.exists()

            plt.close(fig)

    def test_respects_limit(self, sample_analysis_dir: str) -> None:
        """Should process only limited snapshots."""
        from btc_graph.visualization.evolution import plot_histogram_evolution

        fig = plot_histogram_evolution(
            sample_analysis_dir,
            resolution="hour",
            limit=3,
        )

        assert fig is not None

        plt.close(fig)


class TestPlotKLDivergence:
    """Tests for plot_kl_divergence function."""

    def test_creates_figure(self, sample_analysis_dir: str) -> None:
        """Should create a KL divergence plot."""
        from btc_graph.visualization.evolution import plot_kl_divergence

        fig = plot_kl_divergence(
            sample_analysis_dir,
            resolution="hour",
            metric="degrees",
        )

        assert fig is not None
        assert isinstance(fig, plt.Figure)

        plt.close(fig)

    def test_saves_to_file(self, sample_analysis_dir: str) -> None:
        """Should save figure to specified path."""
        from btc_graph.visualization.evolution import plot_kl_divergence

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "kl_div.pdf"

            fig = plot_kl_divergence(
                sample_analysis_dir,
                resolution="hour",
                output_path=str(output_path),
            )

            assert output_path.exists()

            plt.close(fig)

    def test_has_legend(self, sample_analysis_dir: str) -> None:
        """Should include a legend."""
        from btc_graph.visualization.evolution import plot_kl_divergence

        fig = plot_kl_divergence(
            sample_analysis_dir,
            resolution="hour",
        )

        ax = fig.axes[0]
        legend = ax.get_legend()
        assert legend is not None

        plt.close(fig)


class TestPlotMetricTimeseries:
    """Tests for plot_metric_timeseries function."""

    def test_creates_figure(self, sample_analysis_dir: str) -> None:
        """Should create a time series plot."""
        from btc_graph.visualization.evolution import plot_metric_timeseries

        fig = plot_metric_timeseries(
            sample_analysis_dir,
            resolution="hour",
        )

        assert fig is not None

        plt.close(fig)

    def test_supports_different_stats(self, sample_analysis_dir: str) -> None:
        """Should support different statistics."""
        from btc_graph.visualization.evolution import plot_metric_timeseries

        for stat in ["mean", "median", "std"]:
            fig = plot_metric_timeseries(
                sample_analysis_dir,
                resolution="hour",
                stat=stat,
            )

            assert fig is not None

            plt.close(fig)
