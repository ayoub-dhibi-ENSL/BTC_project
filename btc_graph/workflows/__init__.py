"""Workflows for Bitcoin blockchain graph analysis.

This module provides high-level pipeline classes that orchestrate the loading,
processing, and exporting of snapshot data. It ties together the IO and Core
layers into reusable analysis workflows.

Typical Usage
-------------
>>> from btc_graph.workflows import SnapshotAnalysisPipeline
>>> from btc_graph.io import create_spark_session
>>>
>>> spark = create_spark_session()
>>> pipeline = SnapshotAnalysisPipeline(spark)
>>> pipeline.run(
...     input_dir="data/orbitaal-snapshot-hour/SNAPSHOT/EDGES",
...     output_dir="data/snapshot-hour-analysis",
...     resolution="hour",
... )
"""

from .pipeline import (
    AnalysisResult,
    SnapshotAnalysisPipeline,
)

__all__ = [
    "SnapshotAnalysisPipeline",
    "AnalysisResult",
]
