#!/usr/bin/env python3
"""ΛXYM platform ingestion — placeholder with clean interface.

The ΛXYM CLI is still being built. This module provides the interface contract
that will be implemented once the CLI is available.

Interface:
    Input:  Path to Parquet file
    Output: BenchmarkResult with row_count, disk_bytes, wall_time, etc.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import PARQUET_PATH, RESULTS_DIR
from lib.metrics import BenchmarkResult, run_with_metrics


class AXYMIngestionNotAvailable(Exception):
    """Raised when the ΛXYM CLI is not yet available."""

    def __init__(self):
        super().__init__(
            "ΛXYM CLI is not yet available. "
            "To implement this ingestion:\n"
            "  1. Install the ΛXYM CLI\n"
            "  2. Update check_axym_available() to detect the CLI\n"
            "  3. Implement ingest() to call: axym ingest <parquet_path>\n"
            "  4. Capture row_count and disk_bytes from CLI output"
        )


def check_axym_available() -> bool:
    """Check if the ΛXYM CLI is installed and available."""
    # TODO: Implement CLI detection when available
    # import shutil
    # return shutil.which("axym") is not None
    return False


def ingest() -> dict:
    """Ingest Parquet data into ΛXYM.

    Returns dict with row_count, disk_bytes, metadata for BenchmarkResult.
    Raises AXYMIngestionNotAvailable if CLI is not installed.
    """
    if not check_axym_available():
        raise AXYMIngestionNotAvailable()

    # TODO: Implement when ΛXYM CLI is available
    # Expected implementation:
    #   result = subprocess.run(["axym", "ingest", str(PARQUET_PATH)], ...)
    #   parse row_count, disk_bytes from result.stdout
    #   return {"row_count": ..., "disk_bytes": ..., "metadata": {...}}


def main():
    print("=" * 60)
    print("ΛXYM Research — ΛXYM Ingestion")
    print("=" * 60)

    if not check_axym_available():
        print("ΛXYM CLI is not yet available.")
        print("Creating placeholder result...")

        result = BenchmarkResult(
            name="ΛXYM",
            error="ΛXYM CLI not yet available",
            metadata={"status": "pending", "reason": "CLI under development"},
        )
    else:
        result = run_with_metrics("ΛXYM", ingest)

    output_path = RESULTS_DIR / "ingest_axym.json"
    result.save(output_path)
    print(f"Result saved to {output_path}")


if __name__ == "__main__":
    main()
