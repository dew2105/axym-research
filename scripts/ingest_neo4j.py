#!/usr/bin/env python3
"""Ingest Medicaid claims Parquet data into Neo4j as a property graph.

Graph model:
    (:Provider {npi})  — unique NPIs (billing + servicing)
    (:Procedure {hcpcs_code})  — unique HCPCS codes
    [:BILLED_FOR {month, claims, paid, beneficiaries}]  — provider → procedure
    [:REFERRED_TO {month, hcpcs_code, claims, paid}]  — billing → servicing provider

ETL phases are timed separately to show the impedance mismatch overhead.
"""

import csv
import subprocess
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import (
    NEO4J_CONTAINER_NAME,
    NEO4J_IMPORT_DIR,
    PARQUET_PATH,
    RESULTS_DIR,
)
from lib.connections import get_neo4j_driver
from lib.metrics import run_with_metrics

BATCH_SIZE = 500_000
COMMIT_SIZE = 10_000


def _clear_database(driver):
    """Drop all nodes and relationships."""
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        # Drop indexes/constraints if they exist
        for idx in ["provider_npi_idx", "procedure_hcpcs_idx"]:
            try:
                session.run(f"DROP INDEX {idx} IF EXISTS")
            except Exception:
                pass


def _extract_providers(parquet_path: Path) -> set[str]:
    """Extract unique provider NPIs from Parquet."""
    pf = pq.ParquetFile(parquet_path)
    npis = set()
    for batch in tqdm(pf.iter_batches(batch_size=BATCH_SIZE, columns=["Billing_Provider_NPI", "Servicing_Provider_NPI"]),
                      desc="Extract providers"):
        npis.update(batch.column("Billing_Provider_NPI").to_pylist())
        npis.update(batch.column("Servicing_Provider_NPI").to_pylist())
    # Remove None/empty
    npis.discard(None)
    npis.discard("")
    return npis


def _extract_procedures(parquet_path: Path) -> set[str]:
    """Extract unique HCPCS codes from Parquet."""
    pf = pq.ParquetFile(parquet_path)
    codes = set()
    for batch in tqdm(pf.iter_batches(batch_size=BATCH_SIZE, columns=["HCPCS_Code"]),
                      desc="Extract procedures"):
        codes.update(batch.column("HCPCS_Code").to_pylist())
    codes.discard(None)
    codes.discard("")
    return codes


def _write_node_csvs(providers: set[str], procedures: set[str], import_dir: Path):
    """Write node CSV files for LOAD CSV."""
    import_dir.mkdir(parents=True, exist_ok=True)

    # Providers
    provider_path = import_dir / "providers.csv"
    with open(provider_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["npi"])
        for npi in sorted(providers):
            writer.writerow([npi])

    # Procedures
    procedure_path = import_dir / "procedures.csv"
    with open(procedure_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["hcpcs_code"])
        for code in sorted(procedures):
            writer.writerow([code])

    return provider_path, procedure_path


def _write_relationship_csvs(parquet_path: Path, import_dir: Path) -> tuple[Path, Path]:
    """Write relationship CSV files for LOAD CSV."""
    billed_for_path = import_dir / "billed_for.csv"
    referred_to_path = import_dir / "referred_to.csv"

    pf = pq.ParquetFile(parquet_path)

    with (
        open(billed_for_path, "w", newline="") as bf_file,
        open(referred_to_path, "w", newline="") as rt_file,
    ):
        bf_writer = csv.writer(bf_file)
        rt_writer = csv.writer(rt_file)
        bf_writer.writerow(["billing_npi", "hcpcs_code", "month", "claims", "paid", "beneficiaries"])
        rt_writer.writerow(["billing_npi", "servicing_npi", "month", "hcpcs_code", "claims", "paid"])

        for batch in tqdm(pf.iter_batches(batch_size=BATCH_SIZE), desc="Extract relationships"):
            df = batch.to_pandas()
            for _, row in df.iterrows():
                billing = row["Billing_Provider_NPI"]
                servicing = row["Servicing_Provider_NPI"]
                hcpcs = row["HCPCS_Code"]
                month = str(row["Claim_From_Month"])
                claims = row.get("Total_Claims", "")
                paid = row.get("Total_Paid", "")
                beneficiaries = row.get("Total_Unique_Beneficiaries", "")

                if billing and hcpcs:
                    bf_writer.writerow([billing, hcpcs, month, claims, paid, beneficiaries])
                if billing and servicing and billing != servicing:
                    rt_writer.writerow([billing, servicing, month, hcpcs, claims, paid])

    return billed_for_path, referred_to_path


def _copy_csvs_to_neo4j(import_dir: Path):
    """Copy CSV files to Neo4j's import volume via docker cp."""
    csv_files = list(import_dir.glob("*.csv"))
    for csv_file in csv_files:
        subprocess.run(
            ["docker", "cp", str(csv_file), f"{NEO4J_CONTAINER_NAME}:/var/lib/neo4j/import/{csv_file.name}"],
            check=True,
            capture_output=True,
        )


def _load_csv_into_neo4j(driver):
    """Load CSV files into Neo4j using LOAD CSV with periodic commits."""
    with driver.session() as session:
        # Create indexes first
        session.run("CREATE INDEX provider_npi_idx IF NOT EXISTS FOR (p:Provider) ON (p.npi)")
        session.run("CREATE INDEX procedure_hcpcs_idx IF NOT EXISTS FOR (p:Procedure) ON (p.hcpcs_code)")

        # Load Provider nodes
        print("Loading Provider nodes...")
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///providers.csv' AS row
            CALL {
                WITH row
                MERGE (p:Provider {npi: row.npi})
            } IN TRANSACTIONS OF 10000 ROWS
        """)

        # Load Procedure nodes
        print("Loading Procedure nodes...")
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///procedures.csv' AS row
            CALL {
                WITH row
                MERGE (p:Procedure {hcpcs_code: row.hcpcs_code})
            } IN TRANSACTIONS OF 10000 ROWS
        """)

        # Load BILLED_FOR relationships
        print("Loading BILLED_FOR relationships...")
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///billed_for.csv' AS row
            CALL {
                WITH row
                MATCH (provider:Provider {npi: row.billing_npi})
                MATCH (procedure:Procedure {hcpcs_code: row.hcpcs_code})
                CREATE (provider)-[:BILLED_FOR {
                    month: row.month,
                    claims: toInteger(row.claims),
                    paid: toFloat(row.paid),
                    beneficiaries: toInteger(row.beneficiaries)
                }]->(procedure)
            } IN TRANSACTIONS OF 10000 ROWS
        """)

        # Load REFERRED_TO relationships
        print("Loading REFERRED_TO relationships...")
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///referred_to.csv' AS row
            CALL {
                WITH row
                MATCH (billing:Provider {npi: row.billing_npi})
                MATCH (servicing:Provider {npi: row.servicing_npi})
                CREATE (billing)-[:REFERRED_TO {
                    month: row.month,
                    hcpcs_code: row.hcpcs_code,
                    claims: toInteger(row.claims),
                    paid: toFloat(row.paid)
                }]->(servicing)
            } IN TRANSACTIONS OF 10000 ROWS
        """)


def _get_counts(driver) -> dict:
    """Get node and relationship counts."""
    with driver.session() as session:
        nodes = session.run("MATCH (n) RETURN count(n) AS cnt").single()["cnt"]
        rels = session.run("MATCH ()-[r]->() RETURN count(r) AS cnt").single()["cnt"]
        providers = session.run("MATCH (p:Provider) RETURN count(p) AS cnt").single()["cnt"]
        procedures = session.run("MATCH (p:Procedure) RETURN count(p) AS cnt").single()["cnt"]
    return {
        "total_nodes": nodes,
        "total_relationships": rels,
        "provider_nodes": providers,
        "procedure_nodes": procedures,
    }


def _get_neo4j_disk_bytes() -> int:
    """Get Neo4j data directory size via docker exec."""
    try:
        result = subprocess.run(
            ["docker", "exec", NEO4J_CONTAINER_NAME, "du", "-sb", "/data"],
            capture_output=True, text=True, check=True,
        )
        return int(result.stdout.split()[0])
    except Exception:
        return 0


def ingest() -> dict:
    """Full Neo4j ETL pipeline with per-phase timing."""
    driver = get_neo4j_driver()
    timings = {}

    try:
        # Phase 0: Clear
        t0 = time.perf_counter()
        _clear_database(driver)
        timings["clear_seconds"] = round(time.perf_counter() - t0, 3)

        # Phase 1: Extract providers
        t0 = time.perf_counter()
        providers = _extract_providers(PARQUET_PATH)
        timings["extract_providers_seconds"] = round(time.perf_counter() - t0, 3)

        # Phase 2: Extract procedures
        t0 = time.perf_counter()
        procedures = _extract_procedures(PARQUET_PATH)
        timings["extract_procedures_seconds"] = round(time.perf_counter() - t0, 3)

        # Phase 3: Write node CSVs
        t0 = time.perf_counter()
        _write_node_csvs(providers, procedures, NEO4J_IMPORT_DIR)
        timings["write_node_csvs_seconds"] = round(time.perf_counter() - t0, 3)

        # Phase 4: Write relationship CSVs
        t0 = time.perf_counter()
        _write_relationship_csvs(PARQUET_PATH, NEO4J_IMPORT_DIR)
        timings["write_relationship_csvs_seconds"] = round(time.perf_counter() - t0, 3)

        # Phase 5: Copy CSVs to Neo4j import volume
        t0 = time.perf_counter()
        _copy_csvs_to_neo4j(NEO4J_IMPORT_DIR)
        timings["copy_csvs_seconds"] = round(time.perf_counter() - t0, 3)

        # Phase 6: LOAD CSV into Neo4j
        t0 = time.perf_counter()
        _load_csv_into_neo4j(driver)
        timings["load_csv_seconds"] = round(time.perf_counter() - t0, 3)

        # Get counts and disk usage
        counts = _get_counts(driver)
        disk_bytes = _get_neo4j_disk_bytes()

        etl_time = (
            timings["extract_providers_seconds"]
            + timings["extract_procedures_seconds"]
            + timings["write_node_csvs_seconds"]
            + timings["write_relationship_csvs_seconds"]
        )
        timings["etl_overhead_seconds"] = round(etl_time, 3)

    finally:
        driver.close()

    return {
        "row_count": counts["total_nodes"] + counts["total_relationships"],
        "disk_bytes": disk_bytes,
        "metadata": {
            "timings": timings,
            "counts": counts,
            "provider_count": len(providers),
            "procedure_count": len(procedures),
        },
    }


def main():
    print("=" * 60)
    print("AXYM Research — Neo4j Ingestion")
    print("=" * 60)

    if not PARQUET_PATH.exists():
        print(f"Error: Parquet file not found at {PARQUET_PATH}")
        print("Run `python scripts/download.py` first.")
        sys.exit(1)

    result = run_with_metrics("Neo4j", ingest)

    output_path = RESULTS_DIR / "ingest_neo4j.json"
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
