#!/usr/bin/env python3
"""Backfill LIKWID samples from historical Slurm stderr files."""

from __future__ import annotations

import argparse
import os
import shlex
import sys
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis_module.connectors.normalized import prepare_postgres_normalized_schema
from analysis_module.processing.likwid import match_elapsed_times, parse_likwid_stderr


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
    parser.add_argument(
        "jobs_root", type=Path, help="Root containing benchmark/job_*/slurm-*.err files"
    )
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--username", help="Restrict matching to one backend username")
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


def find_energy_match(connection, job_id: str, benchmark: str, username: str | None):
    filters = ["job_id = %s", "coalesce(benchmark_name, 'unknown') = %s"]
    params: list[str] = [job_id, benchmark]
    if username:
        filters.append("owner_username = %s")
        params.append(username)
    where = " and ".join(filters)
    return connection.execute(
        f"""
        select object_key, owner_user_id, owner_username, original_filename,
               compute_node, benchmark_name
        from benchmark_jobs
        where {where}
        order by case when original_filename like %s then 0 else 1 end,
                 coalesce(max_elapsed_time_s, 0) desc
        limit 1
        """,
        [*params, "%_0_%"],
    ).fetchone()


def energy_elapsed_values(connection, object_key: str) -> list[float]:
    rows = connection.execute(
        "select elapsed_time from benchmark_samples where object_key = %s",
        [object_key],
    ).fetchall()
    return [float(row[0]) for row in rows if row[0] is not None]


def insert_rows(
    connection, *, source: Path, match, job_id: str, rows: list[dict]
) -> None:
    connection.execute(
        "delete from benchmark_likwid_samples where source_object_key = %s and h5_object_key = %s",
        [str(source), match[0]],
    )
    payload = []
    for sample_index, row in enumerate(rows):
        payload.append(
            [
                str(source),
                match[0],
                match[1],
                match[2],
                source.name,
                job_id,
                match[4],
                match[5],
                "stderr",
                sample_index,
                row.get("likwid_group_id"),
                row.get("metrics_count"),
                row.get("cpu_count"),
                row.get("elapsed_time_s"),
                row.get("runtime_rdtsc_s"),
                row.get("runtime_unhalted_s"),
                row.get("clock_mhz"),
                row.get("cpi"),
                row.get("dp_mflops"),
                row.get("avx_dp_mflops"),
                row.get("avx512_dp_mflops"),
                row.get("packed_muops_s"),
                row.get("scalar_muops_s"),
                row.get("vectorization_ratio_pct"),
                row.get("matched_energy_elapsed_time_s"),
                row.get("matched_energy_delta_s"),
            ]
        )
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            insert into benchmark_likwid_samples
            (source_object_key, h5_object_key, owner_user_id, owner_username,
             original_filename, job_id, compute_node, benchmark_name, source_kind,
             sample_index, likwid_group_id, metrics_count, cpu_count, elapsed_time_s,
             runtime_rdtsc_s, runtime_unhalted_s, clock_mhz, cpi, dp_mflops,
             avx_dp_mflops, avx512_dp_mflops, packed_muops_s, scalar_muops_s,
             vectorization_ratio_pct, matched_energy_elapsed_time_s,
             matched_energy_delta_s)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            payload,
        )


def main() -> int:
    args = parse_args()
    load_env_file(Path(args.env_file))
    prepare_postgres_normalized_schema()
    parsed = matched = inserted = unmatched = 0
    with connect() as connection:
        for err_path in sorted(args.jobs_root.glob("*/job_*/slurm-*.err")):
            benchmark = err_path.parents[1].name
            job_id = err_path.parent.name.removeprefix("job_")
            result = parse_likwid_stderr(err_path.read_text(errors="replace"))
            if not result.rows:
                continue
            parsed += 1
            match = find_energy_match(connection, job_id, benchmark, args.username)
            if not match:
                unmatched += 1
                continue
            matched += 1
            rows = match_elapsed_times(
                result.rows, energy_elapsed_values(connection, match[0])
            )
            if not args.dry_run:
                insert_rows(
                    connection, source=err_path, match=match, job_id=job_id, rows=rows
                )
                inserted += len(rows)
        if args.dry_run:
            connection.rollback()
        else:
            connection.commit()
    print(
        f"parsed_files={parsed} matched_files={matched} unmatched_files={unmatched} inserted_rows={inserted}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
