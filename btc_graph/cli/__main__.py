"""Allow running btc_graph.cli as a module.

Usage:
    python -m btc_graph.cli --help
    python -m btc_graph.cli analyze --resolution hour --sample
"""

import sys

from .main import main

if __name__ == "__main__":
    sys.exit(main())
