# ΛXYM Research — Medicaid Fraud Detection Pipeline

Benchmarking ΛXYM's unified data platform against a traditional multi-database stack for Medicaid fraud detection.

## Pipeline Steps

| Step | Title | Status |
|------|-------|--------|
| 1 | Data Ingestion | **Active** |
| 2 | Querying | Pending |
| 3 | Graph Traversal | Pending |
| 4 | Embedding Generation | Pending |
| 5 | RAG Integration | Pending |
| 6 | Full Pipeline | Pending |

## Quick Start

### Prerequisites

- Python 3.11+
- ~15 GB free disk space
- Cloud database accounts:
  - [MotherDuck](https://motherduck.com/) (hosted DuckDB)
  - [Neon](https://neon.tech/) (serverless PostgreSQL with pgvector)

### Setup & Run

```bash
git clone <repo-url> axym-research
cd axym-research
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Configure cloud credentials
cp .env.example .env
# Edit .env with your MotherDuck token and Neon DSN

make setup      # Verify connections + download data (~2.9 GB)
make benchmark  # Run all ingestion benchmarks
make notebook   # Open Jupyter notebook with results
```

### Targets

| Command | Description |
|---------|-------------|
| `make setup` | Verify cloud connections, download data |
| `make check-connections` | Test connectivity to MotherDuck and Neon |
| `make benchmark` | Run all ingestion scripts, save results to `results/` |
| `make notebook` | Launch JupyterLab with the Step 1 notebook |
| `make clean` | Remove downloaded data files |

## Graph Database Evaluation

Dedicated graph databases (Neo4j AuraDB, TigerGraph Savanna) were evaluated and rejected due to cost at full dataset scale (1.81M nodes, 227M+ relationships). Instead, graph capabilities are demonstrated using PostgreSQL graph tables on the same Neon instance. See [`docs/graph_database_cost_analysis.md`](docs/graph_database_cost_analysis.md) for details.

## Data Source

[CMS Medicaid Provider Utilization & Spending](https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-medicaid-opioid-prescribing-rates/medicaid-spending-by-drug) — aggregated provider-level claims data from HHS/CMS.

- **Format**: Parquet (2.9 GB)
- **Records**: ~89M rows
- **Columns**: Provider NPIs, HCPCS codes, claim dates, beneficiary counts, payment amounts

## Project Structure

```
axym-research/
├── config/settings.py          # Central configuration
├── lib/                        # Shared library code
│   ├── metrics.py              # Benchmark framework
│   ├── connections.py          # DB connection factories
│   └── report.py               # Chart/table rendering
├── scripts/                    # Ingestion scripts
│   ├── download.py             # Data download + verification
│   ├── ingest_postgres.py      # Parquet → PostgreSQL/Neon
│   ├── ingest_duckdb.py        # Parquet → DuckDB/MotherDuck
│   ├── ingest_graph.py          # medicaid_claims → graph tables (PostgreSQL)
│   └── ingest_axym.py          # ΛXYM placeholder
├── notebooks/
│   └── step_1_data_ingestion.ipynb
├── data/                       # Downloaded files (.gitignored)
└── results/                    # Benchmark JSON (committed)
```
