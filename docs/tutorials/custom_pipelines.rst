================
Custom Pipelines
================

This tutorial shows how to build custom analysis pipelines for specific
research questions.

Overview
--------

While the built-in ``SnapshotAnalysisPipeline`` covers common use cases,
you may need custom pipelines for:

- Specific time windows or event analysis
- Custom centrality measures
- Comparative analysis across datasets
- Integration with external data sources

Building Blocks
---------------

btc_graph provides composable building blocks:

.. code-block:: python

   # Data access
   from btc_graph.io import (
       SnapshotPathFinder,
       CSVExporter,
       create_spark_session,
   )
   
   # Graph construction
   from btc_graph.core import GraphBuilder
   
   # Centrality computations
   from btc_graph.core import (
       compute_degrees,
       compute_pagerank,
       compute_triangle_centralities,
       compute_density,
       compute_graph_summary,
   )

Example: Event Analysis Pipeline
--------------------------------

Analyze the blockchain around a specific event (e.g., Mt. Gox collapse):

.. code-block:: python

   from datetime import datetime
   from dataclasses import dataclass
   from typing import List, Dict, Any
   import pandas as pd
   
   from btc_graph import create_spark_session, stop_spark_session
   from btc_graph.io import SnapshotPathFinder, CSVExporter
   from btc_graph.core import (
       GraphBuilder,
       compute_degrees,
       compute_pagerank,
       compute_graph_summary,
   )
   
   
   @dataclass
   class EventAnalysisResult:
       """Results from event analysis."""
       event_name: str
       before_summary: Dict[str, Any]
       after_summary: Dict[str, Any]
       change_metrics: Dict[str, float]
   
   
   class EventAnalysisPipeline:
       """Analyze blockchain structure before/after events."""
       
       def __init__(
           self,
           snapshot_dir: str,
           output_dir: str,
           spark=None
       ):
           self.snapshot_dir = snapshot_dir
           self.output_dir = output_dir
           self.spark = spark or create_spark_session("event-analysis")
           self.builder = GraphBuilder(self.spark)
           self.exporter = CSVExporter(output_dir)
           self.finder = SnapshotPathFinder(snapshot_dir)
       
       def analyze_event(
           self,
           event_name: str,
           before_snapshots: List[int],
           after_snapshots: List[int]
       ) -> EventAnalysisResult:
           """
           Analyze blockchain before and after an event.
           
           Parameters
           ----------
           event_name : str
               Name of the event being analyzed
           before_snapshots : List[int]
               Indices of snapshots before the event
           after_snapshots : List[int]
               Indices of snapshots after the event
           
           Returns
           -------
           EventAnalysisResult
               Comparison metrics
           """
           all_paths = self.finder.find_all()
           
           # Analyze before period
           before_summary = self._analyze_period(
               [all_paths[i] for i in before_snapshots],
               f"{event_name}_before"
           )
           
           # Analyze after period
           after_summary = self._analyze_period(
               [all_paths[i] for i in after_snapshots],
               f"{event_name}_after"
           )
           
           # Compute changes
           change_metrics = self._compute_changes(before_summary, after_summary)
           
           return EventAnalysisResult(
               event_name=event_name,
               before_summary=before_summary,
               after_summary=after_summary,
               change_metrics=change_metrics
           )
       
       def _analyze_period(
           self,
           snapshot_paths: List[str],
           period_name: str
       ) -> Dict[str, Any]:
           """Analyze a time period (multiple snapshots)."""
           summaries = []
           
           for path in snapshot_paths:
               edges = self.builder.load_edges(path)
               graph = self.builder.build_graph(edges)
               summary = compute_graph_summary(graph)
               summaries.append(summary)
           
           # Aggregate summaries
           df = pd.DataFrame(summaries)
           return {
               'avg_vertices': df['vertex_count'].mean(),
               'avg_edges': df['edge_count'].mean(),
               'avg_density': df['density'].mean(),
               'std_vertices': df['vertex_count'].std(),
               'std_edges': df['edge_count'].std(),
           }
       
       def _compute_changes(
           self,
           before: Dict[str, Any],
           after: Dict[str, Any]
       ) -> Dict[str, float]:
           """Compute percentage changes."""
           return {
               'vertex_change_pct': (
                   (after['avg_vertices'] - before['avg_vertices'])
                   / before['avg_vertices'] * 100
               ),
               'edge_change_pct': (
                   (after['avg_edges'] - before['avg_edges'])
                   / before['avg_edges'] * 100
               ),
               'density_change_pct': (
                   (after['avg_density'] - before['avg_density'])
                   / before['avg_density'] * 100
               ),
           }

Usage:

.. code-block:: python

   pipeline = EventAnalysisPipeline(
       snapshot_dir="data/orbitaal-snapshot-hour/SNAPSHOT/EDGES",
       output_dir="output/event-analysis"
   )
   
   result = pipeline.analyze_event(
       event_name="test_event",
       before_snapshots=[0, 1, 2, 3, 4],
       after_snapshots=[5, 6, 7, 8, 9]
   )
   
   print(f"Event: {result.event_name}")
   print(f"Vertex change: {result.change_metrics['vertex_change_pct']:.2f}%")
   print(f"Edge change: {result.change_metrics['edge_change_pct']:.2f}%")

Example: Comparative Pipeline
-----------------------------

Compare different datasets or time periods:

.. code-block:: python

   from typing import Tuple
   import numpy as np
   from scipy import stats
   
   
   class ComparativePipeline:
       """Compare graph properties across datasets."""
       
       def __init__(self, spark=None):
           self.spark = spark or create_spark_session("comparative")
           self.builder = GraphBuilder(self.spark)
       
       def compare_degree_distributions(
           self,
           path_a: str,
           path_b: str
       ) -> Dict[str, float]:
           """
           Compare degree distributions using statistical tests.
           
           Returns
           -------
           Dict with KS statistic, p-value, and KL divergence
           """
           # Load and compute degrees
           graph_a = self.builder.build_graph(self.builder.load_edges(path_a))
           graph_b = self.builder.build_graph(self.builder.load_edges(path_b))
           
           degrees_a = compute_degrees(graph_a).toPandas()['in_degree'].values
           degrees_b = compute_degrees(graph_b).toPandas()['in_degree'].values
           
           # Kolmogorov-Smirnov test
           ks_stat, ks_pvalue = stats.ks_2samp(degrees_a, degrees_b)
           
           # KL divergence (binned)
           kl_div = self._compute_kl_divergence(degrees_a, degrees_b)
           
           return {
               'ks_statistic': ks_stat,
               'ks_pvalue': ks_pvalue,
               'kl_divergence': kl_div,
               'mean_diff': np.mean(degrees_a) - np.mean(degrees_b),
               'std_diff': np.std(degrees_a) - np.std(degrees_b),
           }
       
       def _compute_kl_divergence(
           self,
           dist_a: np.ndarray,
           dist_b: np.ndarray,
           num_bins: int = 50
       ) -> float:
           """Compute KL divergence between distributions."""
           # Create common bins
           all_values = np.concatenate([dist_a, dist_b])
           bins = np.logspace(
               np.log10(max(1, all_values.min())),
               np.log10(all_values.max() + 1),
               num_bins
           )
           
           # Compute histograms
           hist_a, _ = np.histogram(dist_a, bins=bins, density=True)
           hist_b, _ = np.histogram(dist_b, bins=bins, density=True)
           
           # Add smoothing
           eps = 1e-10
           hist_a = hist_a + eps
           hist_b = hist_b + eps
           
           # Normalize
           hist_a = hist_a / hist_a.sum()
           hist_b = hist_b / hist_b.sum()
           
           # KL divergence
           return np.sum(hist_a * np.log(hist_a / hist_b))

Example: Streaming Pipeline
---------------------------

Process snapshots incrementally for real-time analysis:

.. code-block:: python

   from collections import deque
   import time
   
   
   class StreamingPipeline:
       """Process snapshots as they arrive."""
       
       def __init__(
           self,
           watch_dir: str,
           output_dir: str,
           window_size: int = 10,
           spark=None
       ):
           self.watch_dir = watch_dir
           self.output_dir = output_dir
           self.window_size = window_size
           self.spark = spark or create_spark_session("streaming")
           self.builder = GraphBuilder(self.spark)
           self.exporter = CSVExporter(output_dir)
           
           # Rolling window of metrics
           self.metric_window = deque(maxlen=window_size)
           self.processed = set()
       
       def process_new(self) -> List[Dict[str, Any]]:
           """Process any new snapshots."""
           finder = SnapshotPathFinder(self.watch_dir)
           all_paths = set(finder.find_all())
           
           new_paths = all_paths - self.processed
           results = []
           
           for path in sorted(new_paths):
               result = self._process_snapshot(path)
               results.append(result)
               self.metric_window.append(result)
               self.processed.add(path)
           
           return results
       
       def _process_snapshot(self, path: str) -> Dict[str, Any]:
           """Process a single snapshot."""
           edges = self.builder.load_edges(path)
           graph = self.builder.build_graph(edges)
           summary = compute_graph_summary(graph)
           
           # Detect anomalies
           if len(self.metric_window) > 0:
               avg_vertices = np.mean([m['vertex_count'] for m in self.metric_window])
               if summary['vertex_count'] > avg_vertices * 1.5:
                   summary['anomaly'] = 'vertex_spike'
               elif summary['vertex_count'] < avg_vertices * 0.5:
                   summary['anomaly'] = 'vertex_drop'
               else:
                   summary['anomaly'] = None
           
           return summary
       
       def run_continuous(self, interval: int = 60):
           """Run continuous processing."""
           print(f"Watching {self.watch_dir} for new snapshots...")
           
           while True:
               results = self.process_new()
               for r in results:
                   if r.get('anomaly'):
                       print(f"ANOMALY DETECTED: {r['anomaly']}")
                   print(f"Processed: vertices={r['vertex_count']}, edges={r['edge_count']}")
               
               time.sleep(interval)

Extending the Built-in Pipeline
-------------------------------

You can extend ``SnapshotAnalysisPipeline`` for custom behavior:

.. code-block:: python

   from btc_graph.workflows import SnapshotAnalysisPipeline, AnalysisResult
   
   
   class CustomPipeline(SnapshotAnalysisPipeline):
       """Extended pipeline with custom metrics."""
       
       def __init__(self, *args, **kwargs):
           self.custom_metric_fn = kwargs.pop('custom_metric_fn', None)
           super().__init__(*args, **kwargs)
       
       def analyze_single(self, snapshot_path: str) -> AnalysisResult:
           """Override to add custom metrics."""
           # Call parent implementation
           result = super().analyze_single(snapshot_path)
           
           # Add custom metric
           if self.custom_metric_fn:
               custom_value = self.custom_metric_fn(result)
               # Store custom metric
               self._save_custom_metric(result.snapshot_id, custom_value)
           
           return result
       
       def _save_custom_metric(self, snapshot_id: str, value: Any):
           """Save custom metric to output."""
           import json
           output_path = f"{self.output_dir}/{snapshot_id}/custom_metric.json"
           with open(output_path, 'w') as f:
               json.dump({'value': value}, f)

Best Practices
--------------

1. **Memory Management**

   .. code-block:: python

      # Checkpoint large DataFrames to avoid recomputation
      degrees_df.checkpoint()
      
      # Unpersist when done
      degrees_df.unpersist()

2. **Error Handling**

   .. code-block:: python

      from typing import Optional
      
      def safe_analyze(self, path: str) -> Optional[AnalysisResult]:
          try:
              return self.analyze_single(path)
          except Exception as e:
              self.logger.error(f"Failed to analyze {path}: {e}")
              return None

3. **Progress Tracking**

   .. code-block:: python

      from tqdm import tqdm
      
      results = []
      for path in tqdm(snapshot_paths, desc="Analyzing"):
          result = pipeline.analyze_single(path)
          results.append(result)

4. **Parallel Processing**

   .. code-block:: python

      # Spark handles parallelism within each snapshot
      # For multiple snapshots, process sequentially to avoid memory issues
      # Or use separate Spark applications for true parallelism

Next Steps
----------

- :doc:`../api/workflows` - Workflow API reference
- :doc:`../api/core` - Core module API
- :doc:`../examples/index` - Complete examples
