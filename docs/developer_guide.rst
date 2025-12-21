===============
Developer Guide
===============

This guide covers development practices, testing, and contributing to btc_graph.

Development Setup
-----------------

Clone and Setup
^^^^^^^^^^^^^^^

.. code-block:: bash

   # Clone repository
   git clone https://github.com/yourusername/btc_graph.git
   cd btc_graph
   
   # Create conda environment
   conda env create -f environment.yml
   conda activate BTC_project
   
   # Install in development mode
   pip install -e ".[dev]"

Project Structure
^^^^^^^^^^^^^^^^^

.. code-block:: text

   btc_graph/
   ├── btc_graph/              # Main package
   │   ├── __init__.py         # Package exports
   │   ├── cli/                # Command-line interface
   │   │   ├── __init__.py
   │   │   └── main.py
   │   ├── core/               # Graph algorithms
   │   │   ├── __init__.py
   │   │   ├── centralities.py
   │   │   ├── graph.py
   │   │   └── metrics.py
   │   ├── io/                 # Data I/O
   │   │   ├── __init__.py
   │   │   ├── exporters.py
   │   │   ├── loaders.py
   │   │   └── spark.py
   │   ├── visualization/      # Plotting
   │   │   ├── __init__.py
   │   │   ├── evolution.py
   │   │   ├── histograms.py
   │   │   └── style.py
   │   └── workflows/          # Pipelines
   │       ├── __init__.py
   │       └── pipeline.py
   ├── tests/                  # Test suite
   ├── docs/                   # Documentation
   ├── pyproject.toml          # Package configuration
   └── environment.yml         # Conda environment

Testing
-------

Running Tests
^^^^^^^^^^^^^

.. code-block:: bash

   # Run all tests
   pytest tests/ -v
   
   # Run with coverage
   pytest tests/ --cov=btc_graph --cov-report=html
   
   # Run specific test module
   pytest tests/test_core/test_centralities.py -v
   
   # Run tests matching pattern
   pytest tests/ -k "test_degree" -v

Writing Tests
^^^^^^^^^^^^^

Tests use pytest with fixtures for Spark sessions:

.. code-block:: python

   # tests/conftest.py
   import pytest
   from btc_graph import create_test_spark_session, stop_spark_session
   
   
   @pytest.fixture(scope="module")
   def spark():
       """Create a Spark session for testing."""
       spark = create_test_spark_session()
       yield spark
       stop_spark_session(spark)
   
   
   @pytest.fixture
   def sample_edges(spark):
       """Create sample edge data."""
       data = [
           (1, 2, 1.0),
           (1, 3, 0.5),
           (2, 3, 0.75),
           (3, 4, 1.0),
       ]
       return spark.createDataFrame(data, ["src", "dst", "weight"])

Example test:

.. code-block:: python

   # tests/test_core/test_centralities.py
   import pytest
   from btc_graph.core import compute_degrees, GraphBuilder
   
   
   def test_compute_degrees(spark, sample_edges):
       """Test degree computation."""
       builder = GraphBuilder(spark)
       graph = builder.build_graph(sample_edges)
       
       degrees = compute_degrees(graph)
       
       assert degrees.count() > 0
       assert "in_degree" in degrees.columns
       assert "out_degree" in degrees.columns

Test Categories
^^^^^^^^^^^^^^^

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Directory
     - Description
   * - ``test_io/``
     - Data loading, export, Spark session tests
   * - ``test_core/``
     - Graph building, centrality, metrics tests
   * - ``test_workflows/``
     - Pipeline tests
   * - ``test_visualization/``
     - Plotting tests
   * - ``test_cli/``
     - CLI command tests

Code Style
----------

Formatting
^^^^^^^^^^

Use black for code formatting:

.. code-block:: bash

   # Format all files
   black btc_graph/ tests/
   
   # Check formatting
   black --check btc_graph/ tests/

Linting
^^^^^^^

Use ruff for linting:

.. code-block:: bash

   # Run linter
   ruff check btc_graph/ tests/
   
   # Auto-fix issues
   ruff check --fix btc_graph/ tests/

Type Hints
^^^^^^^^^^

Use type hints for function signatures:

.. code-block:: python

   from typing import List, Dict, Optional
   from pyspark.sql import DataFrame
   
   
   def compute_degrees(graph: "GraphFrame") -> DataFrame:
       """
       Compute degree centralities.
       
       Parameters
       ----------
       graph : GraphFrame
           Input graph
       
       Returns
       -------
       DataFrame
           DataFrame with columns: id, in_degree, out_degree
       """
       ...

Docstrings
^^^^^^^^^^

Use NumPy-style docstrings:

.. code-block:: python

   def analyze_snapshot(
       snapshot_path: str,
       output_dir: str,
       pagerank_iterations: int = 10
   ) -> AnalysisResult:
       """
       Analyze a single blockchain snapshot.
       
       This function loads a snapshot, builds a graph, computes
       centrality metrics, and exports the results.
       
       Parameters
       ----------
       snapshot_path : str
           Path to the snapshot directory
       output_dir : str
           Directory for output files
       pagerank_iterations : int, optional
           Number of PageRank iterations (default: 10)
       
       Returns
       -------
       AnalysisResult
           Object containing analysis results
       
       Raises
       ------
       FileNotFoundError
           If snapshot_path does not exist
       ValueError
           If snapshot data is invalid
       
       Examples
       --------
       >>> result = analyze_snapshot("data/snapshot-001", "output/")
       >>> print(result.vertex_count)
       1000
       
       See Also
       --------
       SnapshotAnalysisPipeline : For batch processing
       """
       ...

Documentation
-------------

Building Docs
^^^^^^^^^^^^^

.. code-block:: bash

   cd docs/
   
   # Install requirements
   pip install -r requirements.txt
   
   # Build HTML
   make html
   
   # Live preview
   make livehtml

Adding Documentation
^^^^^^^^^^^^^^^^^^^^

1. Add RST files in appropriate directories
2. Update toctree in index.rst
3. Use autodoc for API documentation:

.. code-block:: rst

   .. autofunction:: btc_graph.core.compute_degrees

   .. autoclass:: btc_graph.workflows.SnapshotAnalysisPipeline
      :members:
      :undoc-members:

Release Process
---------------

Version Bumping
^^^^^^^^^^^^^^^

Update version in:

- ``pyproject.toml``
- ``btc_graph/__init__.py``
- ``docs/conf.py``

Creating a Release
^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   # Ensure tests pass
   pytest tests/ -v
   
   # Build package
   python -m build
   
   # Check package
   twine check dist/*
   
   # Upload to PyPI
   twine upload dist/*

Contributing
------------

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Run the test suite
5. Submit a pull request

Pull Request Checklist
^^^^^^^^^^^^^^^^^^^^^^

- [ ] Tests pass locally
- [ ] Code is formatted with black
- [ ] Linting passes with ruff
- [ ] Docstrings are complete
- [ ] Documentation is updated
- [ ] Changelog is updated
