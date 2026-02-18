"""Central configuration for AXYM Research benchmarks."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

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

# MotherDuck (hosted DuckDB)
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN", "")
MOTHERDUCK_DB = os.getenv("MOTHERDUCK_DB", "axym_research")

# PostgreSQL / Neon (connection string from env, must include sslmode=require)
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://axym:axym_research@localhost:5432/axym_research",
)
# Benchmark settings
BENCHMARK_RUNS = int(os.getenv("BENCHMARK_RUNS", "3"))
