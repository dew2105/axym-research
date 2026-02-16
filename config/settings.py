"""Central configuration for AXYM Research benchmarks."""

import os
from pathlib import Path

# Project root
ROOT_DIR = Path(__file__).resolve().parent.parent

# Data directories
DATA_DIR = ROOT_DIR / os.getenv("DATA_DIR", "data")
RESULTS_DIR = ROOT_DIR / os.getenv("RESULTS_DIR", "results")

# Data source URLs
PARQUET_URL = (
    "https://stopendataprod.blob.core.windows.net/datasets/"
    "medicaid-provider-spending/2026-02-09/medicaid-provider-spending.parquet"
)
CSV_ZIP_URL = (
    "https://stopendataprod.blob.core.windows.net/datasets/"
    "medicaid-provider-spending/2026-02-09/medicaid-provider-spending.csv.zip"
)

# SHA256 checksums for verification
PARQUET_SHA256 = "a998e5ae11a391f1eb0d8464b3866a3ee7fe18aa13e56d411c50e72e3a0e35c7"
CSV_ZIP_SHA256 = "0816f7b67234e24e65ab3be533195ca21780628baae173c894f2a6a6436b19dc"

# File paths
PARQUET_PATH = DATA_DIR / "medicaid-provider-spending.parquet"
DUCKDB_PATH = DATA_DIR / "medicaid_claims.duckdb"
NEO4J_IMPORT_DIR = DATA_DIR / "neo4j_import"

# PostgreSQL
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "axym_research")
POSTGRES_USER = os.getenv("POSTGRES_USER", "axym")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "axym_research")
POSTGRES_DSN = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Neo4j
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "axym_research")

# Benchmark settings
BENCHMARK_RUNS = int(os.getenv("BENCHMARK_RUNS", "3"))

# Neo4j import volume path (inside the container)
NEO4J_CONTAINER_IMPORT = "/var/lib/neo4j/import"
NEO4J_CONTAINER_NAME = "axym-neo4j"
