#!/usr/bin/env python3
"""Ingest Medicaid claims Parquet data into DuckDB via MotherDuck."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import PARQUET_PATH, RESULTS_DIR
from lib.connections import get_duckdb_connection
from lib.metrics import run_with_metrics

TABLE_NAME = "medicaid_claims"


def ingest() -> dict:
    """Load Parquet into MotherDuck with a single SQL statement."""
    conn = get_duckdb_connection()
    try:
        # Drop existing table for clean benchmark
        conn.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")

        conn.execute(f"""
            CREATE TABLE {TABLE_NAME} AS
            SELECT * FROM read_parquet('{PARQUET_PATH}')
        """)

        row_count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]

        # Create indexes to match PostgreSQL for fair comparison
        conn.execute(f"CREATE INDEX idx_billing_npi ON {TABLE_NAME}(BILLING_PROVIDER_NPI_NUM)")
        conn.execute(f"CREATE INDEX idx_servicing_npi ON {TABLE_NAME}(SERVICING_PROVIDER_NPI_NUM)")
        conn.execute(f"CREATE INDEX idx_hcpcs ON {TABLE_NAME}(HCPCS_CODE)")
        conn.execute(f"CREATE INDEX idx_claim_month ON {TABLE_NAME}(CLAIM_FROM_MONTH)")

        # Estimate disk usage from MotherDuck metadata
        try:
            result = conn.execute(
                "SELECT estimated_size FROM duckdb_tables() "
                f"WHERE table_name = '{TABLE_NAME}'"
            ).fetchone()
            disk_bytes = result[0] if result else 0
        except Exception:
            disk_bytes = 0
    finally:
        conn.close()

    return {
        "row_count": row_count,
        "disk_bytes": disk_bytes,
        "metadata": {"table": TABLE_NAME, "source": str(PARQUET_PATH)},
    }


def main():
    print("=" * 60)
    print("ΛXYM Research — DuckDB/MotherDuck Ingestion")
    print("=" * 60)

    if not PARQUET_PATH.exists():
        print(f"Error: Parquet file not found at {PARQUET_PATH}")
        print("Run `python scripts/download.py` first.")
        sys.exit(1)

    result = run_with_metrics("DuckDB", ingest)

    output_path = RESULTS_DIR / "ingest_duckdb.json"
    result.save(output_path)
    print(f"\nResult saved to {output_path}")
    print(f"  Rows:      {result.row_count:,}")
    print(f"  Wall time: {result.wall_time_seconds:.1f}s")
    print(f"  Disk:      {result.disk_mb:,.0f} MB")
    print(f"  Rows/sec:  {result.rows_per_second:,.0f}")

    if result.error:
        print(f"  ERROR: {result.error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
