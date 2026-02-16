#!/usr/bin/env python3
"""Ingest Medicaid claims Parquet data into PostgreSQL via COPY protocol."""

import sys
from io import StringIO
from pathlib import Path

import pyarrow.parquet as pq

from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import PARQUET_PATH, RESULTS_DIR
from lib.connections import get_postgres_connection
from lib.metrics import BenchmarkResult, run_with_metrics

BATCH_SIZE = 100_000
TABLE_NAME = "medicaid_claims"

# Column mapping: Parquet column name → PostgreSQL column name
COLUMN_MAP = {
    "Billing_Provider_NPI": "billing_provider_npi",
    "Servicing_Provider_NPI": "servicing_provider_npi",
    "HCPCS_Code": "hcpcs_code",
    "Claim_From_Month": "claim_from_month",
    "Total_Unique_Beneficiaries": "total_unique_beneficiaries",
    "Total_Claims": "total_claims",
    "Total_Paid": "total_paid",
}

PG_COLUMNS = list(COLUMN_MAP.values())


def _truncate_table(conn):
    """Clear existing data for a clean benchmark run."""
    conn.execute(f"TRUNCATE TABLE {TABLE_NAME}")
    conn.commit()


def _get_table_size(conn) -> int:
    """Get total relation size in bytes (table + indexes)."""
    cur = conn.execute(
        "SELECT pg_total_relation_size(%s)",
        (TABLE_NAME,),
    )
    return cur.fetchone()[0]


def ingest() -> dict:
    """Load Parquet into PostgreSQL using COPY protocol."""
    parquet_file = pq.ParquetFile(PARQUET_PATH)
    total_rows = parquet_file.metadata.num_rows

    conn = get_postgres_connection()
    try:
        _truncate_table(conn)

        columns_str = ", ".join(PG_COLUMNS)
        loaded = 0

        with tqdm(total=total_rows, desc="PostgreSQL COPY", unit="rows") as pbar:
            for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
                df = batch.to_pandas()
                df = df.rename(columns=COLUMN_MAP)
                df = df[PG_COLUMNS]

                # Convert to CSV in memory for COPY
                buf = StringIO()
                df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
                buf.seek(0)

                with conn.cursor() as cur:
                    with cur.copy(f"COPY {TABLE_NAME} ({columns_str}) FROM STDIN") as copy:
                        for line in buf:
                            copy.write(line)
                conn.commit()

                loaded += len(df)
                pbar.update(len(df))

        # Update statistics for query planner
        conn.execute(f"ANALYZE {TABLE_NAME}")
        conn.commit()

        disk_bytes = _get_table_size(conn)
    finally:
        conn.close()

    return {
        "row_count": loaded,
        "disk_bytes": disk_bytes,
        "metadata": {"batch_size": BATCH_SIZE, "table": TABLE_NAME},
    }


def main():
    print("=" * 60)
    print("AXYM Research — PostgreSQL Ingestion")
    print("=" * 60)

    if not PARQUET_PATH.exists():
        print(f"Error: Parquet file not found at {PARQUET_PATH}")
        print("Run `python scripts/download.py` first.")
        sys.exit(1)

    result = run_with_metrics("PostgreSQL", ingest)

    output_path = RESULTS_DIR / "ingest_postgres.json"
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
