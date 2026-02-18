#!/usr/bin/env python3
"""Ingest Medicaid claims Parquet data into PostgreSQL/Neon via COPY protocol."""

import json
import sys
import time
from io import StringIO
from pathlib import Path

import pyarrow.parquet as pq

from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import PARQUET_PATH, RESULTS_DIR
from lib.connections import get_postgres_connection
from lib.metrics import BenchmarkResult, run_with_metrics

BATCH_SIZE = 10_000
TABLE_NAME = "medicaid_claims"
CHECKPOINT_INTERVAL = 10_000  # checkpoint every batch (~10K rows)
CHECKPOINT_PATH = RESULTS_DIR / "ingest_postgres_checkpoint.json"

# Columns match between Parquet (uppercase) and PostgreSQL (lowercase — case insensitive)
PG_COLUMNS = [
    "billing_provider_npi_num",
    "servicing_provider_npi_num",
    "hcpcs_code",
    "claim_from_month",
    "total_unique_beneficiaries",
    "total_claims",
    "total_paid",
]


def _write_checkpoint(rows_loaded, total_rows, t_start, status="running"):
    elapsed = time.time() - t_start
    CHECKPOINT_PATH.write_text(json.dumps({
        "rows_loaded": rows_loaded,
        "total_rows": total_rows,
        "elapsed_seconds": round(elapsed, 1),
        "rows_per_second": round(rows_loaded / elapsed) if elapsed > 0 else 0,
        "pct_complete": round(100 * rows_loaded / total_rows, 2) if total_rows > 0 else 0,
        "status": status,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }, indent=2) + "\n")


def _ensure_schema(conn):
    """Create extension, table, and indexes if they don't exist.

    Replaces the Docker entrypoint init.sql for Neon.
    """
    conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            billing_provider_npi_num    VARCHAR(10),
            servicing_provider_npi_num  VARCHAR(10),
            hcpcs_code                  VARCHAR(10),
            claim_from_month            VARCHAR(10),
            total_unique_beneficiaries  BIGINT,
            total_claims                BIGINT,
            total_paid                  DOUBLE PRECISION
        )
    """)
    conn.commit()


def _create_indexes(conn):
    """Create B-tree indexes after bulk loading to avoid write amplification."""
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_billing_npi ON {TABLE_NAME}(billing_provider_npi_num)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_servicing_npi ON {TABLE_NAME}(servicing_provider_npi_num)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_hcpcs ON {TABLE_NAME}(hcpcs_code)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_claim_month ON {TABLE_NAME}(claim_from_month)")
    conn.commit()


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
    t_start = time.time()
    last_checkpoint_rows = 0
    loaded = 0

    try:
        _ensure_schema(conn)
        _truncate_table(conn)

        columns_str = ", ".join(PG_COLUMNS)

        with tqdm(total=total_rows, desc="PostgreSQL COPY", unit="rows") as pbar:
            for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
                df = batch.to_pandas()
                df.columns = [c.lower() for c in df.columns]

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

                if loaded - last_checkpoint_rows >= CHECKPOINT_INTERVAL:
                    _write_checkpoint(loaded, total_rows, t_start)
                    last_checkpoint_rows = loaded

        # Build indexes now that all data is loaded (much faster than incremental)
        _create_indexes(conn)

        # Update statistics for query planner
        conn.execute(f"ANALYZE {TABLE_NAME}")
        conn.commit()

        disk_bytes = _get_table_size(conn)
        _write_checkpoint(loaded, total_rows, t_start, status="complete")
    except Exception:
        # Try to get actual committed row count from DB
        try:
            err_conn = get_postgres_connection()
            cur = err_conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            loaded = cur.fetchone()[0]
            err_conn.close()
        except Exception:
            pass  # loaded stays at whatever it was
        _write_checkpoint(loaded, total_rows, t_start, status="error")
        raise
    finally:
        conn.close()

    return {
        "row_count": loaded,
        "disk_bytes": disk_bytes,
        "metadata": {"batch_size": BATCH_SIZE, "table": TABLE_NAME},
    }


def main():
    print("=" * 60)
    print("ΛXYM Research — PostgreSQL/Neon Ingestion")
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
