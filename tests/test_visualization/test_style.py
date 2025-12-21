"""Tests for btc_graph.visualization.style module."""


class TestPlotStyle:
    """Tests for plot style constants and configuration."""

    def test_primary_color_is_hex(self) -> None:
        """Primary color should be a valid hex color."""
        from btc_graph.visualization.style import PRIMARY_COLOR

        assert PRIMARY_COLOR.startswith("#")
        assert len(PRIMARY_COLOR) == 7

    def test_accent_colors_has_required_keys(self) -> None:
        """Accent colors should have required keys."""
        from btc_graph.visualization.style import ACCENT_COLORS

        required_keys = ["mean", "median", "fill", "heatmap"]
        for key in required_keys:
            assert key in ACCENT_COLORS

    def test_plot_style_has_figure_settings(self) -> None:
        """Plot style should include figure settings."""
        from btc_graph.visualization.style import PLOT_STYLE

        assert "figure.figsize" in PLOT_STYLE
        assert "font.size" in PLOT_STYLE


class TestApplyBtcStyle:
    """Tests for apply_btc_style function."""

    def test_apply_style_updates_rcparams(self) -> None:
        """apply_btc_style should update matplotlib rcParams."""
        import matplotlib.pyplot as plt

        from btc_graph.visualization.style import PLOT_STYLE, apply_btc_style

        apply_btc_style()

        # Check some settings were applied
        assert plt.rcParams["font.size"] == PLOT_STYLE["font.size"]

    def test_apply_style_without_latex(self) -> None:
        """Should work without LaTeX enabled."""
        from btc_graph.visualization.style import apply_btc_style

        # Should not raise
        apply_btc_style(use_latex=False)


class TestGetColorPalette:
    """Tests for get_color_palette function."""

    def test_returns_requested_number_of_colors(self) -> None:
        """Should return the requested number of colors."""
        from btc_graph.visualization.style import get_color_palette

        colors = get_color_palette(5)
        assert len(colors) == 5

    def test_colors_are_hex_strings(self) -> None:
        """All colors should be hex strings."""
        from btc_graph.visualization.style import get_color_palette

        colors = get_color_palette(10)
        for color in colors:
            assert color.startswith("#")

    def test_cycles_for_large_requests(self) -> None:
        """Should cycle through colors for large requests."""
        from btc_graph.visualization.style import get_color_palette

        colors = get_color_palette(20)
        assert len(colors) == 20


class TestCreateFigure:
    """Tests for create_figure function."""

    def test_creates_figure_and_axes(self) -> None:
        """Should create a figure and axes."""
        import matplotlib.pyplot as plt

        from btc_graph.visualization.style import create_figure

        fig, ax = create_figure()

        assert fig is not None
        assert ax is not None

        plt.close(fig)

    def test_creates_subplots(self) -> None:
        """Should create multiple subplots."""
        import matplotlib.pyplot as plt

        from btc_graph.visualization.style import create_figure

        fig, axes = create_figure(nrows=2, ncols=2)

        assert axes.shape == (2, 2)

        plt.close(fig)

    def test_respects_figsize(self) -> None:
        """Should use provided figsize."""
        import matplotlib.pyplot as plt

        from btc_graph.visualization.style import create_figure

        fig, ax = create_figure(figsize=(10, 5))

        assert fig.get_figwidth() == 10
        assert fig.get_figheight() == 5

        plt.close(fig)
