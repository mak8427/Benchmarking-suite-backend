#!/usr/bin/env python3
"""Backfill normalized electricity price and cost columns from SMARD data."""

from __future__ import annotations

import argparse
import os
import shlex
import sys
from pathlib import Path

import polars as pl
import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis_module.connectors.normalized import prepare_postgres_normalized_schema
from analysis_module.pipeline_core.pricing_fetch import fetch_smard_prices


class Logger:
    def info(self, message, *args) -> None:
        print(message % args if args else message)

    def warning(self, message, *args) -> None:
        print("warning: " + (message % args if args else message))


def load_env_file(path: Path) -> None:
    """Load simple KEY=VALUE or fish `set -x KEY VALUE` env files."""
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = shlex.split(line)
        if len(parts) >= 4 and parts[0] == "set" and parts[1] == "-x":
            os.environ.setdefault(parts[2], " ".join(parts[3:]))
        elif "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(
                key.strip(), shlex.split(value.strip())[0] if value.strip() else ""
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--filter-id", type=int, default=4169)
    parser.add_argument("--region", default="DE-LU")
    parser.add_argument("--resolution", default="quarterhour")
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def connect():
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.environ["POSTGRES_PASSWORD"],
        autocommit=False,
    )


def _sample_frame(connection) -> pl.DataFrame:
    rows = connection.execute(
        """
        select object_key, elapsed_time, epoch_time, energy_used_j
        from benchmark_samples
        where epoch_time is not null
          and energy_used_j is not null
          and (price_eur_per_mwh is null or cumulative_cost_eur is null)
        order by epoch_time, object_key, elapsed_time
        """
    ).fetchall()
    if not rows:
        return pl.DataFrame(
            schema={
                "object_key": pl.Utf8,
                "elapsed_time": pl.Float64,
                "epoch_time": pl.Int64,
                "energy_used_j": pl.Float64,
            }
        )
    return pl.DataFrame(
        rows,
        schema=["object_key", "elapsed_time", "epoch_time", "energy_used_j"],
        orient="row",
    ).with_columns(
        pl.col("object_key").cast(pl.Utf8),
        pl.col("elapsed_time").cast(pl.Float64),
        pl.col("epoch_time").cast(pl.Int64),
        pl.col("energy_used_j").cast(pl.Float64),
    )


def _priced_samples(samples: pl.DataFrame, prices: pl.DataFrame) -> pl.DataFrame:
    joined = samples.sort("epoch_time").join_asof(
        prices.sort("EpochTime"),
        left_on="epoch_time",
        right_on="EpochTime",
        strategy="nearest",
    )
    return joined.with_columns(
        pl.col("Price_EUR_per_MWh").interpolate().alias("price_eur_per_mwh"),
        (
            (pl.col("energy_used_j") / 3_600_000_000.0)
            * pl.col("Price_EUR_per_MWh")
        ).alias("cumulative_cost_eur"),
    ).select(
        "object_key",
        "elapsed_time",
        "price_eur_per_mwh",
        "cumulative_cost_eur",
    )


def _update_samples(connection, priced: pl.DataFrame, batch_size: int) -> int:
    rows = priced.to_dicts()
    updated = 0
    with connection.cursor() as cursor:
        for offset in range(0, len(rows), batch_size):
            batch = rows[offset : offset + batch_size]
            cursor.executemany(
                """
                update benchmark_samples
                   set price_eur_per_mwh = %s,
                       cumulative_cost_eur = %s
                 where object_key = %s and elapsed_time = %s
                """,
                [
                    (
                        row["price_eur_per_mwh"],
                        row["cumulative_cost_eur"],
                        row["object_key"],
                        row["elapsed_time"],
                    )
                    for row in batch
                ],
            )
            updated += len(batch)
    return updated


def _update_jobs(connection) -> int:
    cursor = connection.execute(
        """
        with rollup as (
            select object_key,
                   max(cumulative_cost_eur) as total_cost_eur,
                   avg(price_eur_per_mwh) as mean_price_eur_per_mwh
              from benchmark_samples
             where cumulative_cost_eur is not null
                or price_eur_per_mwh is not null
             group by object_key
        )
        update benchmark_jobs j
           set total_cost_eur = r.total_cost_eur,
               mean_price_eur_per_mwh = r.mean_price_eur_per_mwh
          from rollup r
         where j.object_key = r.object_key
           and (j.total_cost_eur is null or j.mean_price_eur_per_mwh is null)
        """
    )
    return cursor.rowcount if cursor.rowcount is not None else 0


def main() -> int:
    args = parse_args()
    load_env_file(Path(args.env_file))
    prepare_postgres_normalized_schema()
    logger = Logger()

    with connect() as connection:
        samples = _sample_frame(connection)
        if samples.is_empty():
            print("samples_to_price=0 updated_samples=0 updated_jobs=0")
            return 0
        prices = fetch_smard_prices(
            samples["epoch_time"],
            filter_id=args.filter_id,
            region=args.region,
            resolution=args.resolution,
            logger=logger,
        )
        if prices is None or prices.is_empty():
            print(f"samples_to_price={samples.height} updated_samples=0 updated_jobs=0")
            return 1
        priced = _priced_samples(samples, prices)
        if args.dry_run:
            connection.rollback()
            print(
                f"samples_to_price={samples.height} price_points={prices.height} updated_samples=0 updated_jobs=0"
            )
            return 0
        updated_samples = _update_samples(connection, priced, args.batch_size)
        updated_jobs = _update_jobs(connection)
        connection.commit()
        print(
            f"samples_to_price={samples.height} price_points={prices.height} updated_samples={updated_samples} updated_jobs={updated_jobs}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
