"""Tests for btc_graph.cli.main module.

These tests verify the CLI argument parsing and command dispatch logic.
"""

import pytest

from btc_graph.cli.main import (
    __version__,
    create_parser,
    get_default_input_dir,
    get_default_output_dir,
    get_sample_limit,
    main,
)


class TestCreateParser:
    """Tests for create_parser function."""

    def test_parser_has_version(self) -> None:
        """Parser should support --version flag."""
        parser = create_parser()
        # Version flag causes SystemExit
        with pytest.raises(SystemExit):
            parser.parse_args(["--version"])

    def test_parser_has_help(self) -> None:
        """Parser should support --help flag."""
        parser = create_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--help"])

    def test_no_command_returns_none(self) -> None:
        """No subcommand should set command to None."""
        parser = create_parser()
        args = parser.parse_args([])
        assert args.command is None


class TestAnalyzeCommand:
    """Tests for the analyze subcommand."""

    def test_analyze_default_resolution(self) -> None:
        """Default resolution should be 'hour'."""
        parser = create_parser()
        args = parser.parse_args(["analyze"])
        assert args.resolution == "hour"

    def test_analyze_hour_resolution(self) -> None:
        """Should accept 'hour' resolution."""
        parser = create_parser()
        args = parser.parse_args(["analyze", "-r", "hour"])
        assert args.resolution == "hour"

    def test_analyze_year_resolution(self) -> None:
        """Should accept 'year' resolution."""
        parser = create_parser()
        args = parser.parse_args(["analyze", "--resolution", "year"])
        assert args.resolution == "year"

    def test_analyze_invalid_resolution(self) -> None:
        """Should reject invalid resolution."""
        parser = create_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["analyze", "-r", "month"])

    def test_analyze_sample_flag(self) -> None:
        """Should parse --sample flag."""
        parser = create_parser()
        args = parser.parse_args(["analyze", "--sample"])
        assert args.sample is True

    def test_analyze_sample_short_flag(self) -> None:
        """Should parse -s flag."""
        parser = create_parser()
        args = parser.parse_args(["analyze", "-s"])
        assert args.sample is True

    def test_analyze_no_sample_by_default(self) -> None:
        """Sample should be False by default."""
        parser = create_parser()
        args = parser.parse_args(["analyze"])
        assert args.sample is False

    def test_analyze_input_dir(self) -> None:
        """Should parse --input-dir option."""
        parser = create_parser()
        args = parser.parse_args(["analyze", "-i", "/path/to/input"])
        assert args.input_dir == "/path/to/input"

    def test_analyze_output_dir(self) -> None:
        """Should parse --output-dir option."""
        parser = create_parser()
        args = parser.parse_args(["analyze", "-o", "/path/to/output"])
        assert args.output_dir == "/path/to/output"

    def test_analyze_limit(self) -> None:
        """Should parse --limit option."""
        parser = create_parser()
        args = parser.parse_args(["analyze", "--limit", "10"])
        assert args.limit == 10

    def test_analyze_quiet_flag(self) -> None:
        """Should parse --quiet flag."""
        parser = create_parser()
        args = parser.parse_args(["analyze", "-q"])
        assert args.quiet is True

    def test_analyze_driver_memory(self) -> None:
        """Should parse --driver-memory option."""
        parser = create_parser()
        args = parser.parse_args(["analyze", "--driver-memory", "16g"])
        assert args.driver_memory == "16g"

    def test_analyze_driver_memory_default(self) -> None:
        """Driver memory default should be 8g."""
        parser = create_parser()
        args = parser.parse_args(["analyze"])
        assert args.driver_memory == "8g"


class TestPlotCommand:
    """Tests for the plot subcommand."""

    def test_plot_default_resolution(self) -> None:
        """Default resolution should be 'hour'."""
        parser = create_parser()
        args = parser.parse_args(["plot"])
        assert args.resolution == "hour"

    def test_plot_year_resolution(self) -> None:
        """Should accept 'year' resolution."""
        parser = create_parser()
        args = parser.parse_args(["plot", "-r", "year"])
        assert args.resolution == "year"

    def test_plot_input_dir(self) -> None:
        """Should parse --input-dir option."""
        parser = create_parser()
        args = parser.parse_args(["plot", "-i", "/path/to/results"])
        assert args.input_dir == "/path/to/results"

    def test_plot_output_dir(self) -> None:
        """Should parse --output-dir option."""
        parser = create_parser()
        args = parser.parse_args(["plot", "-o", "/path/to/plots"])
        assert args.output_dir == "/path/to/plots"

    def test_plot_default_output_dir(self) -> None:
        """Default output dir should be 'plots'."""
        parser = create_parser()
        args = parser.parse_args(["plot"])
        assert args.output_dir == "plots"


class TestInfoCommand:
    """Tests for the info subcommand."""

    def test_info_command(self) -> None:
        """Should parse info command."""
        parser = create_parser()
        args = parser.parse_args(["info"])
        assert args.command == "info"

    def test_info_check_spark_flag(self) -> None:
        """Should parse --check-spark flag."""
        parser = create_parser()
        args = parser.parse_args(["info", "--check-spark"])
        assert args.check_spark is True

    def test_info_no_check_spark_by_default(self) -> None:
        """check_spark should be False by default."""
        parser = create_parser()
        args = parser.parse_args(["info"])
        assert args.check_spark is False


class TestHelperFunctions:
    """Tests for helper functions."""

    def test_get_default_input_dir_hour(self) -> None:
        """Should return correct path for hour resolution."""
        path = get_default_input_dir("hour")
        assert "orbitaal-snapshot-hour" in path
        assert "EDGES" in path

    def test_get_default_input_dir_year(self) -> None:
        """Should return correct path for year resolution."""
        path = get_default_input_dir("year")
        assert "orbitaal-snapshot-year" in path

    def test_get_default_input_dir_custom_base(self) -> None:
        """Should respect custom base directory."""
        path = get_default_input_dir("hour", base_dir="/custom")
        assert path.startswith("/custom")

    def test_get_default_output_dir_hour(self) -> None:
        """Should return correct path for hour resolution."""
        path = get_default_output_dir("hour")
        assert "snapshot-hour-analysis" in path

    def test_get_default_output_dir_year(self) -> None:
        """Should return correct path for year resolution."""
        path = get_default_output_dir("year")
        assert "snapshot-year-analysis" in path

    def test_get_sample_limit_hour(self) -> None:
        """Hour resolution sample should be 20."""
        assert get_sample_limit("hour") == 20

    def test_get_sample_limit_year(self) -> None:
        """Year resolution sample should be 2."""
        assert get_sample_limit("year") == 2


class TestMain:
    """Tests for main entry point."""

    def test_main_no_args_returns_zero(self) -> None:
        """Main with no args should return 0 and print help."""
        # Note: This just prints help, doesn't run any command
        result = main([])
        assert result == 0

    def test_main_info_returns_zero(self) -> None:
        """Main with 'info' should return 0."""
        result = main(["info"])
        assert result == 0


class TestVersion:
    """Tests for version constant."""

    def test_version_is_string(self) -> None:
        """Version should be a string."""
        assert isinstance(__version__, str)

    def test_version_format(self) -> None:
        """Version should follow semver-like format."""
        parts = __version__.split(".")
        assert len(parts) >= 2
        # First two parts should be numeric
        assert parts[0].isdigit()
        assert parts[1].isdigit()
