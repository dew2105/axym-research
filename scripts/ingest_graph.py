#!/usr/bin/env python3
"""Ingest Medicaid claims into PostgreSQL graph tables (nodes + edges).

Graph model (same as the original Neo4j design):
    graph_providers  (npi VARCHAR(10) PRIMARY KEY)
    graph_procedures (hcpcs_code VARCHAR(10) PRIMARY KEY)
    graph_billed_for (provider_npi, hcpcs_code, month, claims, paid, beneficiaries)
    graph_referred_to (from_npi, to_npi, month, hcpcs_code, claims, paid)

All data comes from the existing `medicaid_claims` table via INSERT...SELECT
(server-side, no data transfer to client).  Full dataset — no sampling.

This is intentionally "hacky": the point is to demonstrate the impedance
mismatch and ETL overhead of building graph structure on top of a relational
database, compared to a purpose-built graph DB or a unified platform.

Dedicated graph databases were evaluated and rejected due to cost:
  - Neo4j AuraDB Professional: ~$5.76/hr for 64 GB ($4,160/month)
  - TigerGraph Savanna TG-1:   ~$4.00/hr for 64 GB ($2,920/month)
  - Neo4j AuraDB Free: 200K node / 400K rel limit (requires 99.8% sampling)
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import PARQUET_PATH, RESULTS_DIR
from lib.connections import get_postgres_connection
from lib.metrics import run_with_metrics

GRAPH_TABLES = [
    "graph_providers",
    "graph_procedures",
    "graph_billed_for",
    "graph_referred_to",
]


def _exec(conn, sql: str) -> None:
    """Execute a SQL statement."""
    conn.execute(sql)


def _scalar(conn, sql: str):
    """Execute a SQL statement and return the first column of the first row."""
    cur = conn.execute(sql)
    return cur.fetchone()[0]


def ingest() -> dict:
    """Build PostgreSQL graph tables from medicaid_claims with per-phase timing."""
    conn = get_postgres_connection()
    timings = {}

    try:
        # Phase 0: Drop existing graph tables
        t0 = time.perf_counter()
        for tbl in reversed(GRAPH_TABLES):
            _exec(conn, f"DROP TABLE IF EXISTS {tbl} CASCADE")
        conn.commit()
        timings["drop_tables_seconds"] = round(time.perf_counter() - t0, 3)

        # Phase 1: Create graph tables + indexes
        t0 = time.perf_counter()

        _exec(conn, """
            CREATE TABLE graph_providers (
                npi VARCHAR(10) PRIMARY KEY
            )
        """)

        _exec(conn, """
            CREATE TABLE graph_procedures (
                hcpcs_code VARCHAR(10) PRIMARY KEY
            )
        """)

        _exec(conn, """
            CREATE TABLE graph_billed_for (
                provider_npi VARCHAR(10) NOT NULL,
                hcpcs_code   VARCHAR(10) NOT NULL,
                month        TEXT,
                claims       INTEGER,
                paid         NUMERIC,
                beneficiaries INTEGER
            )
        """)

        _exec(conn, """
            CREATE TABLE graph_referred_to (
                from_npi  VARCHAR(10) NOT NULL,
                to_npi    VARCHAR(10) NOT NULL,
                month     TEXT,
                hcpcs_code VARCHAR(10),
                claims    INTEGER,
                paid      NUMERIC
            )
        """)

        # Indexes for graph traversal patterns
        _exec(conn, "CREATE INDEX idx_billed_for_provider ON graph_billed_for (provider_npi)")
        _exec(conn, "CREATE INDEX idx_billed_for_hcpcs ON graph_billed_for (hcpcs_code)")
        _exec(conn, "CREATE INDEX idx_referred_to_from ON graph_referred_to (from_npi)")
        _exec(conn, "CREATE INDEX idx_referred_to_to ON graph_referred_to (to_npi)")

        conn.commit()
        timings["create_tables_seconds"] = round(time.perf_counter() - t0, 3)

        # Phase 2: Populate graph_providers (union of billing + servicing NPIs)
        t0 = time.perf_counter()
        _exec(conn, """
            INSERT INTO graph_providers (npi)
            SELECT DISTINCT npi FROM (
                SELECT billing_provider_npi_num AS npi FROM medicaid_claims
                WHERE billing_provider_npi_num IS NOT NULL AND billing_provider_npi_num != ''
                UNION
                SELECT servicing_provider_npi_num AS npi FROM medicaid_claims
                WHERE servicing_provider_npi_num IS NOT NULL AND servicing_provider_npi_num != ''
            ) AS all_npis
        """)
        conn.commit()
        timings["populate_providers_seconds"] = round(time.perf_counter() - t0, 3)

        # Phase 3: Populate graph_procedures
        t0 = time.perf_counter()
        _exec(conn, """
            INSERT INTO graph_procedures (hcpcs_code)
            SELECT DISTINCT hcpcs_code FROM medicaid_claims
            WHERE hcpcs_code IS NOT NULL AND hcpcs_code != ''
        """)
        conn.commit()
        timings["populate_procedures_seconds"] = round(time.perf_counter() - t0, 3)

        # Phase 4: Populate graph_billed_for (full dataset)
        t0 = time.perf_counter()
        _exec(conn, """
            INSERT INTO graph_billed_for (provider_npi, hcpcs_code, month, claims, paid, beneficiaries)
            SELECT
                billing_provider_npi_num,
                hcpcs_code,
                claim_from_month::text,
                total_claims,
                total_paid,
                total_unique_beneficiaries
            FROM medicaid_claims
            WHERE billing_provider_npi_num IS NOT NULL AND billing_provider_npi_num != ''
              AND hcpcs_code IS NOT NULL AND hcpcs_code != ''
        """)
        conn.commit()
        timings["populate_billed_for_seconds"] = round(time.perf_counter() - t0, 3)

        # Phase 5: Populate graph_referred_to (where billing != servicing)
        t0 = time.perf_counter()
        _exec(conn, """
            INSERT INTO graph_referred_to (from_npi, to_npi, month, hcpcs_code, claims, paid)
            SELECT
                billing_provider_npi_num,
                servicing_provider_npi_num,
                claim_from_month::text,
                hcpcs_code,
                total_claims,
                total_paid
            FROM medicaid_claims
            WHERE billing_provider_npi_num IS NOT NULL AND billing_provider_npi_num != ''
              AND servicing_provider_npi_num IS NOT NULL AND servicing_provider_npi_num != ''
              AND billing_provider_npi_num != servicing_provider_npi_num
        """)
        conn.commit()
        timings["populate_referred_to_seconds"] = round(time.perf_counter() - t0, 3)

        # Phase 6: ANALYZE all graph tables
        t0 = time.perf_counter()
        # ANALYZE requires autocommit or being outside a transaction
        conn.autocommit = True
        for tbl in GRAPH_TABLES:
            _exec(conn, f"ANALYZE {tbl}")
        conn.autocommit = False
        timings["analyze_seconds"] = round(time.perf_counter() - t0, 3)

        # Collect counts
        provider_count = _scalar(conn, "SELECT COUNT(*) FROM graph_providers")
        procedure_count = _scalar(conn, "SELECT COUNT(*) FROM graph_procedures")
        billed_for_count = _scalar(conn, "SELECT COUNT(*) FROM graph_billed_for")
        referred_to_count = _scalar(conn, "SELECT COUNT(*) FROM graph_referred_to")

        total_nodes = provider_count + procedure_count
        total_relationships = billed_for_count + referred_to_count

        # Disk measurement: sum of pg_total_relation_size across all graph tables
        disk_bytes = _scalar(conn, """
            SELECT SUM(pg_total_relation_size(tablename::regclass))
            FROM (VALUES ('graph_providers'), ('graph_procedures'),
                         ('graph_billed_for'), ('graph_referred_to')) AS t(tablename)
        """)

        # ETL overhead = time spent transforming tabular → graph structure
        etl_overhead = sum(
            timings[k] for k in [
                "populate_providers_seconds",
                "populate_procedures_seconds",
                "populate_billed_for_seconds",
                "populate_referred_to_seconds",
            ]
        )
        timings["etl_overhead_seconds"] = round(etl_overhead, 3)

    finally:
        conn.close()

    return {
        "row_count": total_nodes + total_relationships,
        "disk_bytes": int(disk_bytes or 0),
        "metadata": {
            "timings": timings,
            "counts": {
                "total_nodes": total_nodes,
                "total_relationships": total_relationships,
                "provider_nodes": provider_count,
                "procedure_nodes": procedure_count,
                "billed_for_relationships": billed_for_count,
                "referred_to_relationships": referred_to_count,
            },
            "provider_count": provider_count,
            "procedure_count": procedure_count,
            "billed_for_count": billed_for_count,
            "referred_to_count": referred_to_count,
            "sampling": "none — full dataset",
            "method": "INSERT INTO ... SELECT from medicaid_claims (server-side)",
            "cost_note": (
                "Dedicated graph DBs rejected due to cost. "
                "Neo4j AuraDB Pro: ~$5.76/hr (64 GB). "
                "TigerGraph Savanna: ~$4/hr (64 GB). "
                "PostgreSQL graph tables: $0 incremental (reuses Neon instance)."
            ),
        },
    }


def main():
    print("=" * 60)
    print("AXYM Research — Graph (PostgreSQL) Ingestion")
    print("=" * 60)

    print("  Method: INSERT...SELECT from medicaid_claims → graph tables")
    print("  Sampling: none (full dataset)")

    result = run_with_metrics("Graph (PostgreSQL)", ingest)

    output_path = RESULTS_DIR / "ingest_graph.json"
    result.save(output_path)
    print(f"\nResult saved to {output_path}")
    print(f"  Nodes+Rels: {result.row_count:,}")
    print(f"  Wall time:  {result.wall_time_seconds:.1f}s")
    print(f"  Disk:       {result.disk_mb:,.0f} MB")

    if result.metadata.get("timings"):
        print("\n  ETL Breakdown:")
        for phase, secs in result.metadata["timings"].items():
            print(f"    {phase}: {secs:.1f}s")

    if result.metadata.get("counts"):
        print("\n  Graph Counts:")
        for k, v in result.metadata["counts"].items():
            print(f"    {k}: {v:,}")

    if result.error:
        print(f"\n  ERROR: {result.error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
