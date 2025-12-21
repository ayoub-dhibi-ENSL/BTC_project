<p align="center">
    <img src="images/logo.png" alt="BTC Project Logo" width="200" height="200">
</p>
<p align="right"><b style="font-size:2em;">v0.2.0</b></p>

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Spark](https://img.shields.io/badge/spark-4.0+-orange.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Tests](https://img.shields.io/badge/tests-144%20passed-brightgreen.svg)

---

# btc_graph

**btc_graph** is a Python package for analyzing the structural properties of the Bitcoin blockchain transaction graph in response to major cryptocurrency events.

Built on PySpark and GraphFrames for scalable big data processing.

---

## Key Findings

Our high-resolution analysis of **~1,048 hourly snapshots** (2009-2020) reveals how major crypto events impacted the Bitcoin network structure:

| Event | Date | Degree KL Change | Clustering KL Change |
|-------|------|------------------|---------------------|
| **Bitfinex Hack** | 2016-08-02 | **-70%** | **-76%** |
| **Mt. Gox Collapse** | 2014-02-24 | -26% | +14% |
| **Halving #1** | 2012-11-28 | -47% | -46% |
| **Silk Road Shutdown** | 2013-10-02 | -1% | -2% |
| **Halving #3** | 2020-05-11 | +39% | +92% |

*Negative change = distributions stabilized; Positive = more structural disruption*

---

## Features

- **Graph Construction**: Build transaction graphs from blockchain snapshots
- **Centrality Analysis**: Compute degree distributions, PageRank, and triangle centralities
- **KL Divergence Analysis**: Track distributional changes $D_{KL}(P_t \| P_{t+1})$ between consecutive snapshots
- **Event Impact Analysis**: Quantify how major crypto events affect network structure
- **Temporal Evolution**: Track how graph metrics evolve over time (2009-2020)
- **Global Properties**: Monitor network density, clustering coefficient, and size
- **Visualization**: Generate publication-ready plots with event markers and zoomed views
- **Scalable Processing**: Built on PySpark and GraphFrames for big data (104k+ snapshots)
- **CLI Interface**: Easy-to-use command-line tools

---

## Quick Start

### Basic Pipeline Usage

```python
from btc_graph import SnapshotAnalysisPipeline, create_spark_session

# Create Spark session with GraphFrames
spark = create_spark_session("btc-analysis")

# Run analysis pipeline
pipeline = SnapshotAnalysisPipeline(spark)
pipeline.run(
    input_dir="data/orbitaal-snapshot-hour/SNAPSHOT/EDGES",
    output_dir="output/analysis",
    resolution="hour",
    max_snapshots=10
)
```

### KL Divergence Analysis

Analyze how centrality distributions change over time:

```bash
# Run high-resolution KL divergence analysis (every 100th snapshot = ~1,048 samples)
conda run -n BTC_project python scripts/kl_divergence_analysis.py \
    --sample-rate 100 \
    --output-dir results/kl-analysis-highres

# Generate plots with event markers (filtered from 2012)
python scripts/plot_kl_with_events.py \
    --input-dir results/kl-analysis-highres \
    --output-dir plots/kl-analysis-highres \
    --start-year 2012
```

This computes $D_{KL}(P_t \| P_{t+1})$ for each centrality measure, showing how distributions evolve between consecutive snapshots, with vertical markers for major crypto events.

### Event Impact Analysis

The `plot_kl_with_events.py` script analyzes 6 major cryptocurrency events:

1. **Bitcoin Halving #1** (2012-11-28) - Block reward reduced 50→25 BTC
2. **Silk Road FBI Shutdown** (2013-10-02) - Major darknet marketplace seized
3. **Mt. Gox Hack** (2014-02-24) - Largest exchange collapse ($450M lost)
4. **Bitfinex Hack** (2016-08-02) - $72M stolen from exchange
5. **Bitcoin Halving #2** (2016-07-09) - Block reward reduced 25→12.5 BTC
6. **Bitcoin Halving #3** (2020-05-11) - Block reward reduced 12.5→6.25 BTC

For each event, the script generates:
- Full timeline plots with event markers
- Zoomed plots (±4 weeks around each event)
- Impact statistics (before/after KL divergence changes)

---

## Installation

### Using Conda (Recommended)

```bash
# Clone the repository
git clone https://github.com/your-username/btc_graph.git
cd btc_graph

# Create conda environment
conda env create -f environment.yml
conda activate BTC_project

# Install package
pip install -e ".[dev]"
```

### Using pip

```bash
pip install -e .
```

---

## Command Line Interface

The package provides a CLI for common tasks:

```bash
# Run snapshot analysis
btc-graph analyze \
    --snapshot-dir data/orbitaal-snapshot-hour/SNAPSHOT/EDGES \
    --output-dir output/analysis \
    --max-snapshots 20

# Generate evolution plots
btc-graph plot evolution \
    --results-dir output/analysis \
    --metric in_degree \
    --output plots/in_degree_evolution.png

# Show package info
btc-graph info
```

---

## Data Source

The blockchain data used in this project is sourced from [ORBITAAL](https://zenodo.org/records/12581515), which provides comprehensive datasets for Bitcoin transactions and addresses. More details about the methodology can be found on [arXiv](https://arxiv.org/html/2408.14147v1).

### Setting Up Data

Download and extract the snapshots from ORBITAAL:
- `orbitaal-snapshot-year.tar.gz` (23.1 GB) - Yearly snapshots
- `orbitaal-snapshot-hour.tar.gz` (26.9 GB) - Hourly snapshots

Extract to the `data/` directory.

---

## Package Structure

```
btc_graph/                  # Core package
├── io/                     # Data I/O
│   ├── loaders.py          # SnapshotPathFinder, schemas
│   ├── exporters.py        # CSVExporter
│   └── spark.py            # Spark session factory
├── core/                   # Graph algorithms
│   ├── graph.py            # GraphBuilder
│   ├── centralities.py     # Degree, PageRank, triangles
│   └── metrics.py          # Density, summaries
├── workflows/              # High-level pipelines
│   └── pipeline.py         # SnapshotAnalysisPipeline
├── visualization/          # Plotting utilities
│   ├── histograms.py       # Histogram plots
│   ├── evolution.py        # Temporal evolution plots
│   └── style.py            # Plot styling
└── cli/                    # Command-line interface
    └── main.py             # CLI entry point

scripts/                    # Analysis scripts
├── kl_divergence_analysis.py   # KL divergence computation (main analysis)
└── plot_kl_with_events.py      # Event impact visualization
```

---

## Analysis Outputs

### KL Divergence Results (`results/kl-analysis-highres/`)

| File | Description |
|------|-------------|
| `kl_divergences.csv` | $D_{KL}(P_t \| P_{t+1})$ for in-degree, out-degree, degree, clustering |
| `global_properties.csv` | Network size, density, clustering coefficient per snapshot |
| `kl_correlations.csv` | Rolling correlations between KL divergences |
| `summary.json` | Analysis metadata and statistics |

### Generated Plots (`plots/kl-analysis-highres/`)

| Plot | Description |
|------|-------------|
| `kl_evolution_with_events.png` | KL divergence evolution with event markers |
| `global_properties_with_events.png` | Network metrics with event markers |
| `event_zoom_*.png` | Zoomed views (±4 weeks) around each crypto event |
| `events_impact_summary.png` | Bar chart comparing event impacts |
| `events_statistics.csv` | Before/after KL statistics for each event |
| `events_reference.md` | Event descriptions and dates |
| `kl_correlations.png` | Rolling correlations over time |
| `kl_correlation_heatmap.png` | Correlation heatmap between KL measures |

---

## Documentation

Full documentation is available at [Read the Docs](https://btc-graph.readthedocs.io/) (coming soon).

To build documentation locally:

```bash
cd docs/
pip install -r requirements.txt
make html
```

---

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=btc_graph --cov-report=html
```

Current test coverage: 144 tests passing.

---

## Roadmap

- [x] Build GraphFrame API/Wrapper
- [x] Build data pipeline (from .parquet to plots)
- [x] Implement KL divergence for snapshot comparison
- [x] Package as installable Python module
- [x] Comprehensive documentation
- [x] Scale analysis to full dataset (104,823 hourly snapshots)
- [x] KL divergence evolution analysis with consecutive snapshot comparison
- [x] Global graph properties tracking (density, clustering, network size)
- [x] Correlation analysis between centrality measures
- [x] High-resolution temporal analysis (~1,048 snapshots)
- [x] Event impact analysis with zoomed visualizations
- [x] Quantified impact of 6 major crypto events on network structure
- [ ] Implement ML pipeline to fit/extrapolate dynamic changes
- [ ] Graph Neural Network integration
- [ ] Anomaly detection based on KL divergence spikes

---

## Support

If you need help or have questions, feel free to contact: [ayoub.dhibi@ens-lyon.fr](mailto:ayoub.dhibi@ens-lyon.fr)

---

## License

This project is licensed under the [MIT License](https://choosealicense.com/licenses/mit/).
