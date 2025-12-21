"""Command-line interface for btc_graph.

This module provides the CLI entry point for running Bitcoin blockchain
graph analysis from the terminal.

Usage
-----
After installing the package, run:

    $ btc-graph --help
    $ btc-graph analyze --resolution hour --sample
    $ btc-graph info

Or run as a module:

    $ python -m btc_graph.cli --help
"""

from .main import create_parser, main

__all__ = ["create_parser", "main"]
