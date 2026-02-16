# AXYM Research — Medicaid Fraud Detection Pipeline

Benchmarking AXYM's unified data platform against a traditional multi-database stack for Medicaid fraud detection.

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

- Docker & Docker Compose
- Python 3.11+
- ~15 GB free disk space
- ~8 GB RAM

### Setup & Run

```bash
git clone <repo-url> axym-research
cd axym-research
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
make setup      # Start databases + download data (~2.9 GB)
make benchmark  # Run all ingestion benchmarks
make notebook   # Open Jupyter notebook with results
```

### Targets

| Command | Description |
|---------|-------------|
| `make setup` | Start Docker containers, wait for health, download data |
| `make benchmark` | Run all ingestion scripts, save results to `results/` |
| `make notebook` | Launch JupyterLab with the Step 1 notebook |
| `make clean` | Remove data files and Docker volumes |

## Data Source

[CMS Medicaid Provider Utilization & Spending](https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-medicaid-opioid-prescribing-rates/medicaid-spending-by-drug) — aggregated provider-level claims data from HHS/CMS.

- **Format**: Parquet (2.9 GB)
- **Records**: ~89M rows
- **Columns**: Provider NPIs, HCPCS codes, claim dates, beneficiary counts, payment amounts

## Project Structure

```
axym-research/
├── config/settings.py          # Central configuration
├── infra/                      # Database initialization
│   ├── postgres/init.sql
│   └── neo4j/neo4j.conf
├── lib/                        # Shared library code
│   ├── metrics.py              # Benchmark framework
│   ├── connections.py          # DB connection factories
│   └── report.py               # Chart/table rendering
├── scripts/                    # Ingestion scripts
│   ├── download.py             # Data download + verification
│   ├── ingest_postgres.py      # Parquet → PostgreSQL
│   ├── ingest_duckdb.py        # Parquet → DuckDB
│   ├── ingest_neo4j.py         # Parquet → Neo4j (graph ETL)
│   └── ingest_axym.py          # AXYM placeholder
├── notebooks/
│   └── step_1_data_ingestion.ipynb
├── data/                       # Downloaded files (.gitignored)
└── results/                    # Benchmark JSON (committed)
```
