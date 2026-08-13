"""Run pending S3 HDF5 analysis safely from a host-side systemd timer."""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable

from api_backend.storage.minio_client import ADMIN_MINIO, BUCKET


LOGGER = logging.getLogger("benchmark-analysis-worker")
PipelineRunner = Callable[..., subprocess.CompletedProcess]


def pending_hdf5_by_user(db_path: Path) -> dict[str, set[str]]:
    """Return unprocessed HDF5 object keys grouped by backend user ID."""
    grouped: defaultdict[str, set[str]] = defaultdict(set)
    with sqlite3.connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT user_id, object_key
            FROM storage_objects
            WHERE state != 'processed' AND lower(object_key) LIKE '%.h5'
            ORDER BY user_id, object_key
            """
        ).fetchall()
    for user_id, object_key in rows:
        grouped[str(user_id)].add(str(object_key))
    return dict(grouped)


def uploaded_pending_keys(
    client: Any, bucket: str, user_id: str, pending_keys: set[str]
) -> set[str]:
    """Return pending keys that are now visible in the user's S3 prefix."""
    visible = {
        obj.object_name
        for obj in client.list_objects(
            bucket, prefix=f"{user_id}/", recursive=True
        )
    }
    return pending_keys & visible


def mark_uploaded(db_path: Path, object_keys: set[str]) -> None:
    """Mark S3-visible objects uploaded without overwriting processed rows."""
    if not object_keys:
        return
    with sqlite3.connect(db_path) as connection:
        connection.executemany(
            """
            UPDATE storage_objects
            SET state = 'uploaded', uploaded_at = unixepoch('subsec')
            WHERE object_key = ? AND state != 'processed'
            """,
            [(key,) for key in sorted(object_keys)],
        )
        connection.commit()


def launch_pipeline(
    user_id: str,
    *,
    repository: Path,
    runner: PipelineRunner = subprocess.run,
) -> int:
    """Run the existing pipeline for one complete backend-user prefix."""
    environment = os.environ.copy()
    environment["S3_SYNC"] = "1"
    environment["S3_OBJECT_PREFIX"] = user_id
    command = [
        sys.executable,
        "-m",
        "analysis_module.pipeline_runner",
        "--allow-missing-source",
    ]
    LOGGER.info("Starting analysis for backend user %s", user_id)
    result = runner(
        command,
        cwd=repository,
        env=environment,
        check=False,
    )
    if result.returncode:
        LOGGER.error(
            "Analysis failed for backend user %s with exit code %d",
            user_id,
            result.returncode,
        )
    else:
        LOGGER.info("Analysis completed for backend user %s", user_id)
    return int(result.returncode)


def run_once(
    *,
    db_path: Path,
    repository: Path,
    client: Any = ADMIN_MINIO,
    bucket: str = BUCKET,
    runner: PipelineRunner = subprocess.run,
) -> int:
    """Analyze each user prefix containing at least one newly visible HDF5."""
    pending = pending_hdf5_by_user(db_path)
    if not pending:
        LOGGER.info("No pending HDF5 objects")
        return 0

    failures = 0
    for user_id, pending_keys in pending.items():
        try:
            visible = uploaded_pending_keys(client, bucket, user_id, pending_keys)
        except Exception:  # noqa: BLE001
            LOGGER.exception("Unable to inspect S3 prefix for backend user %s", user_id)
            failures += 1
            continue
        if not visible:
            LOGGER.info(
                "No completed S3 upload yet for %d pending object(s) owned by %s",
                len(pending_keys),
                user_id,
            )
            continue
        mark_uploaded(db_path, visible)
        failures += bool(
            launch_pipeline(user_id, repository=repository, runner=runner)
        )
    return 1 if failures else 0


def build_parser() -> argparse.ArgumentParser:
    """Build the worker command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        type=Path,
        default=Path(os.getenv("AUTH_DB_PATH", "auth.db")),
        help="SQLite authentication/storage database path.",
    )
    parser.add_argument(
        "--repository",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Backend repository root used as the pipeline working directory.",
    )
    return parser


def main() -> int:
    """Run one pending-object scan for systemd."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = build_parser().parse_args()
    if not args.database.is_file():
        LOGGER.error("SQLite database does not exist: %s", args.database)
        return 2
    return run_once(
        db_path=args.database.resolve(),
        repository=args.repository.resolve(),
    )


if __name__ == "__main__":
    raise SystemExit(main())
