<p align="center">
    <img src="images/logo.png" alt="BTC Project Logo" width="200" height="200">
</p>
<p align="right"><b style="font-size:2em;">v0.0.1</b></p>

---

Bitcoin Graph is an ongoing open-source initiative designed to help users analyze and understand the Bitcoin blockchain using graph theory, machine learning, and statistical physics. The project provides tools to model the blockchain as a dynamic graph, detect signals related to major cryptocurrency events, and compare Bitcoin price movements to established stochastic models.

---

### What Does This Project Do?

- **Blockchain Graph Modeling:** Converts Bitcoin blockchain data into a dynamic graph structure, allowing users to visualize and analyze connections between transactions and addresses.
- **Network Analysis:** Applies network theory to study the structural properties of the blockchain graph, such as centrality, clustering, and connectivity.
- **Event Detection:** Uses machine learning and graph neural networks (GNNs) to identify patterns and signals associated with significant events (e.g., hacks, regulatory changes).
- **Price Dynamics Comparison:** Compares Bitcoin price movements to stochastic models like Langevin/Black–Scholes, helping users understand market behavior under various conditions.
- **Scientific Software Practices:** Implements modular code, version control, and reproducibility to ensure reliability and ease of collaboration.

---

### Data Source

The blockchain data used in this project is sourced from [ORBITAAL](https://zenodo.org/records/12581515), which provides comprehensive datasets for Bitcoin transactions and addresses. More details about the methodology used by the authors of the dataset can be found on [Arxiv](https://arxiv.org/html/2408.14147v1).
 
---

### Installation

1. **Clone the Repository:**
    ```bash
    git clone https://github.com/your-username/BTC_project.git
    cd BTC_project
    ```

2. **Install Dependencies:**
    If you want to replicate my setup, make sure you have [Python](https://www.python.org/) 3.8+ and [conda](https://docs.conda.io/projects/conda/en/stable/index.html) 24+ installed. Then, in your BTC_project directory, run:
    ```bash
    conda env create -f environment.yml
    ```

3. **Set Up Data:**
    The current version of the project uses the yearly snapshots `orbitaal-snapshot-year.tar.gz` (23.1 GB) from [ORBITAAL](https://zenodo.org/records/12581515). Download and extract the data in the `data/` directory, you should get 13 parquet files under `/data/orbitaal-snapshot-year/SNAPSHOT/EDGES/year/`.
    

---

### Timeline
- **Previous steps:**
    - Build a GraphFrame API/Wrapper
    - Build data pipeline (from .parquet to plots)

- **Current Step:**  
    - Computing graph centralities (= features for GNN)

- **Upcoming Steps:**  
    - Explore the correlations/mutual information between the centralities
    - Use graph embeddings to reduce dimensionnality
    - Build event detection module
    - Integrate price dynamics comparison
    - Build visualization module w/ **streamlit**
    - Finalize documentation and reproducibility features  
---

### Usage
To use the command-line interface, run:

```bash
python3 main.py [-h] [-r] (-c | -p | -b)
```

Key options (choose one):

- `-c`, `--compute`  
    Process the data from ../data/ and save to CSVs.

- `-p`, `--plot`  
    Make plots from the CSVs in ../data/ and save them in ../plots/.

- `-b`, `--both`  
    Process the data from ../data/ and save to CSVs, then make the plots from the CSVs and save them.

Optional:

- `-r`, `--resolution (hour | year)`  
    Set the resolution of the snapshots (default: year).

**Examples:**

Process data for yearly snapshots:
```bash
python3 main.py -c -r year
```

Plot from existing CSVs for hourly snapshots:
```bash
python3 main.py -p -r hour
```

Process and plot in one step:
```bash
python3 main.py -b -r year
```

For more options and details, use the help flag:
```bash
python3 main.py --help
```
---

### Support

If you need help or have questions, feel free to contact: [ayoub.dhibi@ens-lyon.fr](mailto:ayoub.dhibi@ens-lyon.fr)

---

### License

This project is licensed under the [MIT License](https://choosealicense.com/licenses/mit/).
