============
Installation
============

This guide covers the installation of btc_graph and its dependencies.

Requirements
------------

- Python 3.10+
- Apache Spark 4.0+
- Java 17+ (for Spark)

Using Conda (Recommended)
-------------------------

The recommended way to install btc_graph is using conda to manage dependencies:

.. code-block:: bash

   # Clone the repository
   git clone https://github.com/yourusername/btc_graph.git
   cd btc_graph
   
   # Create conda environment
   conda env create -f environment.yml
   conda activate BTC_project
   
   # Install package in development mode
   pip install -e ".[dev]"

Using pip
---------

If you prefer pip, ensure you have Spark and Java installed first:

.. code-block:: bash

   # Install package
   pip install -e .
   
   # For development (includes testing and docs dependencies)
   pip install -e ".[dev]"

Verifying Installation
----------------------

Verify the installation by running:

.. code-block:: bash

   # Check CLI is available
   btc-graph info
   
   # Run tests
   pytest tests/ -v

You should see output like:

.. code-block:: text

   btc_graph - Bitcoin Blockchain Graph Analysis
   =============================================
   Version: 0.1.0
   
   Available Commands:
     analyze   - Run snapshot analysis pipeline
     plot      - Generate visualizations from analysis results
     info      - Display package information

GraphFrames Setup
-----------------

btc_graph uses GraphFrames for graph algorithms. The Spark session factory 
automatically downloads the required JAR:

.. code-block:: python

   from btc_graph import create_spark_session
   
   # This automatically configures GraphFrames
   spark = create_spark_session("my-app")

If you need to specify a custom GraphFrames version:

.. code-block:: python

   from btc_graph.io import create_spark_session
   
   spark = create_spark_session(
       app_name="my-app",
       graphframes_version="0.9.3",
       scala_version="2.13"
   )

Troubleshooting
---------------

Common Issues
^^^^^^^^^^^^^

**Java not found**

Ensure JAVA_HOME is set:

.. code-block:: bash

   export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
   export PATH=$JAVA_HOME/bin:$PATH

**Spark driver memory errors**

Increase driver memory:

.. code-block:: python

   spark = create_spark_session(
       app_name="my-app",
       driver_memory="8g"
   )

**GraphFrames JAR download fails**

Check your network connection or download manually from Maven Central.
