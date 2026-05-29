"""Normalized PostgreSQL tables for user-facing Grafana dashboards."""

from __future__ import annotations

from pathlib import Path
import os

import duckdb
import polars as pl
import psycopg
from psycopg import errors

from api_backend.db import get_storage_object, mark_storage_object_processed


def infer_owner_metadata(file_label: str) -> dict[str, str]:
    """Infer owner and object metadata for a processed file label.

    Remote S3 labels use the full object key, which begins with the backend user
    id. Local files fall back to an `unknown` owner so the pipeline still works.
    """
    object_key = file_label
    owner_user_id = file_label.split("/", 1)[0] if "/" in file_label else "unknown"
    stored = get_storage_object(object_key) if "/" in file_label else None
    return {
        "owner_user_id": stored.get("user_id", owner_user_id) if stored else owner_user_id,
        "owner_username": stored.get("username", "unknown") if stored else "unknown",
        "object_key": object_key,
        "original_filename": stored.get("original_filename", Path(file_label).name) if stored else Path(file_label).name,
    }


def ensure_normalized_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create normalized dashboard tables and row-level security policies."""
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pg.public.benchmark_jobs (
            object_key TEXT PRIMARY KEY,
            owner_user_id TEXT NOT NULL,
            owner_username TEXT NOT NULL,
            original_filename TEXT NOT NULL,
            processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            sample_count BIGINT NOT NULL,
            max_power_w DOUBLE PRECISION,
            total_energy_j DOUBLE PRECISION,
            max_elapsed_time_s DOUBLE PRECISION
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pg.public.benchmark_samples (
            object_key TEXT NOT NULL,
            owner_user_id TEXT NOT NULL,
            elapsed_time DOUBLE PRECISION NOT NULL,
            epoch_time BIGINT,
            node_power DOUBLE PRECISION,
            energy_used_j DOUBLE PRECISION,
            energy_increment_j DOUBLE PRECISION,
            cpu_utilization DOUBLE PRECISION
        );
        """
    )
    apply_postgres_security()


def apply_postgres_security() -> None:
    """Apply RLS policies and grants using a direct PostgreSQL connection."""
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        return
    with psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=password,
        autocommit=True,
    ) as connection:
        connection.execute("ALTER TABLE public.benchmark_jobs ENABLE ROW LEVEL SECURITY")
        connection.execute("ALTER TABLE public.benchmark_samples ENABLE ROW LEVEL SECURITY")
        for policy, table_name in (
            ("benchmark_jobs_owner", "benchmark_jobs"),
            ("benchmark_samples_owner", "benchmark_samples"),
        ):
            try:
                connection.execute(
                    f"CREATE POLICY {policy} ON public.{table_name} "
                    "USING (owner_user_id = current_setting('app.user_id', true))"
                )
            except errors.DuplicateObject:
                pass
        roles = connection.execute("SELECT rolname FROM pg_roles WHERE rolname LIKE 'bench_user_%'").fetchall()
        for (role,) in roles:
            safe_role = '"' + role.replace('"', '""') + '"'
            connection.execute(f"GRANT USAGE ON SCHEMA public TO {safe_role}")
            connection.execute(f"GRANT SELECT ON public.benchmark_jobs TO {safe_role}")
            connection.execute(f"GRANT SELECT ON public.benchmark_samples TO {safe_role}")


def _first_existing(df: pl.DataFrame, candidates: list[str]) -> str | None:
    for column in candidates:
        if column in df.columns:
            return column
    return None


def build_dashboard_samples(df: pl.DataFrame, metadata: dict[str, str]) -> pl.DataFrame:
    """Convert a processed dataframe into stable dashboard sample rows."""
    elapsed = _first_existing(df, ["ElapsedTime"])
    node_power = _first_existing(df, ["NodePower", "Energy__NodePower", "Node__NodePower"])
    energy_used = _first_existing(df, ["Energy_used_J"])
    energy_increment = _first_existing(df, ["Energy_Increment_J"])
    epoch_time = _first_existing(df, ["EpochTime", "Energy__EpochTime", "Node__EpochTime"])
    cpu_utilization = _first_existing(df, ["CPUUtilization", "Node__CPUUtilization"])

    if not elapsed:
        return pl.DataFrame()

    expressions = [
        pl.lit(metadata["object_key"]).alias("object_key"),
        pl.lit(metadata["owner_user_id"]).alias("owner_user_id"),
        pl.col(elapsed).cast(pl.Float64).alias("elapsed_time"),
        pl.col(epoch_time).cast(pl.Int64).alias("epoch_time") if epoch_time else pl.lit(None).cast(pl.Int64).alias("epoch_time"),
        pl.col(node_power).cast(pl.Float64).alias("node_power") if node_power else pl.lit(None).cast(pl.Float64).alias("node_power"),
        pl.col(energy_used).cast(pl.Float64).alias("energy_used_j") if energy_used else pl.lit(None).cast(pl.Float64).alias("energy_used_j"),
        pl.col(energy_increment).cast(pl.Float64).alias("energy_increment_j")
        if energy_increment
        else pl.lit(None).cast(pl.Float64).alias("energy_increment_j"),
        pl.col(cpu_utilization).cast(pl.Float64).alias("cpu_utilization")
        if cpu_utilization
        else pl.lit(None).cast(pl.Float64).alias("cpu_utilization"),
    ]
    return df.select(expressions)


def write_dashboard_tables(
    con: duckdb.DuckDBPyConnection,
    df: pl.DataFrame,
    *,
    file_label: str,
    logger,
) -> None:
    """Write normalized job and sample rows for Grafana dashboards."""
    metadata = infer_owner_metadata(file_label)
    samples = build_dashboard_samples(df, metadata)
    if samples.is_empty():
        logger.warning("Skipping normalized dashboard rows for %s: no sample columns", file_label)
        return

    ensure_normalized_schema(con)
    summary = samples.select(
        pl.len().cast(pl.Int64).alias("sample_count"),
        pl.col("node_power").max().alias("max_power_w"),
        pl.col("energy_used_j").max().alias("total_energy_j"),
        pl.col("elapsed_time").max().alias("max_elapsed_time_s"),
    ).to_dicts()[0]
    jobs = pl.DataFrame(
        {
            "object_key": [metadata["object_key"]],
            "owner_user_id": [metadata["owner_user_id"]],
            "owner_username": [metadata["owner_username"]],
            "original_filename": [metadata["original_filename"]],
            "sample_count": [summary["sample_count"]],
            "max_power_w": [summary["max_power_w"]],
            "total_energy_j": [summary["total_energy_j"]],
            "max_elapsed_time_s": [summary["max_elapsed_time_s"]],
        }
    )

    con.register("dashboard_jobs", jobs)
    con.register("dashboard_samples", samples)
    con.execute("DELETE FROM pg.public.benchmark_samples WHERE object_key = ?", [metadata["object_key"]])
    con.execute("DELETE FROM pg.public.benchmark_jobs WHERE object_key = ?", [metadata["object_key"]])
    con.execute(
        """
        INSERT INTO pg.public.benchmark_jobs
        (object_key, owner_user_id, owner_username, original_filename, sample_count, max_power_w, total_energy_j, max_elapsed_time_s)
        SELECT object_key, owner_user_id, owner_username, original_filename, sample_count, max_power_w, total_energy_j, max_elapsed_time_s
        FROM dashboard_jobs;
        """
    )
    con.execute(
        """
        INSERT INTO pg.public.benchmark_samples
        (object_key, owner_user_id, elapsed_time, epoch_time, node_power, energy_used_j, energy_increment_j, cpu_utilization)
        SELECT object_key, owner_user_id, elapsed_time, epoch_time, node_power, energy_used_j, energy_increment_j, cpu_utilization
        FROM dashboard_samples;
        """
    )
    mark_storage_object_processed(metadata["object_key"])
