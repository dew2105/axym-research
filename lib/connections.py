"""Database connection factories and health-check waiters."""

import time

import duckdb
import psycopg
from neo4j import GraphDatabase

from config.settings import (
    DUCKDB_PATH,
    NEO4J_PASSWORD,
    NEO4J_URI,
    NEO4J_USER,
    POSTGRES_DSN,
)


def get_postgres_connection() -> psycopg.Connection:
    """Return a psycopg3 connection to PostgreSQL."""
    return psycopg.connect(POSTGRES_DSN, autocommit=False)


def get_duckdb_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection (file-backed)."""
    return duckdb.connect(str(DUCKDB_PATH), read_only=read_only)


def get_neo4j_driver():
    """Return a Neo4j driver instance."""
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def wait_for_postgres(timeout: int = 60) -> bool:
    """Poll PostgreSQL until it accepts connections or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conn = get_postgres_connection()
            conn.execute("SELECT 1")
            conn.close()
            return True
        except Exception:
            time.sleep(2)
    return False


def wait_for_neo4j(timeout: int = 120) -> bool:
    """Poll Neo4j until it accepts connections or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            driver = get_neo4j_driver()
            driver.verify_connectivity()
            driver.close()
            return True
        except Exception:
            time.sleep(3)
    return False
