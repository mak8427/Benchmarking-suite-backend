from __future__ import annotations

import os
import time

import duckdb


def setup_duckdb_with_postgres(*, password: str, logger) -> duckdb.DuckDBPyConnection:
    """Create DuckDB connection, install/load postgres extension, and attach Postgres."""

    logger.info("Initializing DuckDB connection...")
    db_init_start = time.perf_counter()
    con = duckdb.connect()
    logger.info("⏱️  DuckDB connection created in %.3f seconds", time.perf_counter() - db_init_start)

    logger.info("Installing and loading PostgreSQL extension...")
    pg_setup_start = time.perf_counter()
    con.execute("INSTALL postgres;")
    con.execute("LOAD postgres;")
    logger.info("⏱️  PostgreSQL extension setup took %.3f seconds", time.perf_counter() - pg_setup_start)

    host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    port = os.getenv("POSTGRES_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB", "postgres")
    user = os.getenv("POSTGRES_USER", "postgres")
    conn_str = f"host={host} port={port} dbname={dbname} user={user} password={password}"

    logger.info("Attaching to PostgreSQL database at %s:%s db=%s user=%s", host, port, dbname, user)
    attach_start = time.perf_counter()
    con.execute(f"ATTACH '{conn_str}' AS pg (TYPE postgres);")
    logger.info("⏱️  PostgreSQL attachment took %.3f seconds", time.perf_counter() - attach_start)
    return con
