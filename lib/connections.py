"""Database connection factories and connectivity checks."""

import os

import duckdb
import psycopg

from config.settings import (
    MOTHERDUCK_DB,
    POSTGRES_DSN,
)


def get_postgres_connection() -> psycopg.Connection:
    """Return a psycopg3 connection to PostgreSQL."""
    return psycopg.connect(POSTGRES_DSN, autocommit=False)


def get_duckdb_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection via MotherDuck."""
    if not os.getenv("MOTHERDUCK_TOKEN"):
        raise RuntimeError(
            "MOTHERDUCK_TOKEN environment variable is not set. "
            "Set it in your .env file or export it in your shell."
        )
    return duckdb.connect(f"md:{MOTHERDUCK_DB}", read_only=read_only)


def verify_connections() -> dict[str, bool]:
    """Lightweight connectivity check for hosted services.

    Returns a dict mapping service name to reachability (True/False).
    """
    status: dict[str, bool] = {}

    # MotherDuck / DuckDB
    try:
        conn = get_duckdb_connection()
        conn.execute("SELECT 1")
        conn.close()
        status["MotherDuck"] = True
    except Exception as exc:
        print(f"  MotherDuck: FAILED — {exc}")
        status["MotherDuck"] = False

    # Neon / PostgreSQL
    try:
        conn = get_postgres_connection()
        conn.execute("SELECT 1")
        conn.close()
        status["Neon"] = True
    except Exception as exc:
        print(f"  Neon: FAILED — {exc}")
        status["Neon"] = False

    return status
