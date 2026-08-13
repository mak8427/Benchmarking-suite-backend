"""Tests for the host-side pending HDF5 analysis worker."""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace

from analysis_module import host_analysis_worker


class StorageStub:
    """Return configured object names through the storage wrapper interface."""

    def __init__(self, names):
        self.names = names
        self.prefixes = []

    def list_objects(self, bucket, prefix, recursive):
        self.prefixes.append((bucket, prefix, recursive))
        return [SimpleNamespace(object_name=name) for name in self.names]


def _database(tmp_path, rows):
    path = tmp_path / "auth.db"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE storage_objects (
                object_key TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                state TEXT NOT NULL,
                uploaded_at REAL
            )
            """
        )
        connection.executemany(
            "INSERT INTO storage_objects(object_key, user_id, state) VALUES (?, ?, ?)",
            rows,
        )
    return path


def test_worker_runs_only_for_pending_objects_visible_in_s3(tmp_path) -> None:
    """A completed PUT should launch one prefix-scoped pipeline invocation."""
    database = _database(
        tmp_path,
        [
            ("user-1/new.h5", "user-1", "presigned"),
            ("user-1/old.h5", "user-1", "processed"),
            ("user-2/missing.h5", "user-2", "presigned"),
        ],
    )
    storage = StorageStub(["user-1/new.h5", "user-1/old.h5"])
    calls = []

    def runner(command, cwd, env, check):
        calls.append((command, cwd, env, check))
        return SimpleNamespace(returncode=0)

    result = host_analysis_worker.run_once(
        db_path=database,
        repository=tmp_path,
        client=storage,
        bucket="bench",
        runner=runner,
    )

    assert result == 0
    assert len(calls) == 1
    command, cwd, environment, check = calls[0]
    assert command[1:] == [
        "-m",
        "analysis_module.pipeline_runner",
        "--allow-missing-source",
    ]
    assert cwd == tmp_path
    assert environment["S3_SYNC"] == "1"
    assert environment["S3_OBJECT_PREFIX"] == "user-1"
    assert check is False
    with sqlite3.connect(database) as connection:
        states = dict(connection.execute("SELECT object_key, state FROM storage_objects"))
    assert states["user-1/new.h5"] == "uploaded"
    assert states["user-1/old.h5"] == "processed"
    assert states["user-2/missing.h5"] == "presigned"


def test_worker_returns_failure_when_pipeline_fails(tmp_path) -> None:
    """A pipeline error should make the oneshot systemd service fail."""
    database = _database(
        tmp_path,
        [("user-1/new.h5", "user-1", "presigned")],
    )

    def runner(*_args, **_kwargs):
        return SimpleNamespace(returncode=7)

    result = host_analysis_worker.run_once(
        db_path=database,
        repository=tmp_path,
        client=StorageStub(["user-1/new.h5"]),
        bucket="bench",
        runner=runner,
    )

    assert result == 1


def test_worker_does_nothing_without_pending_hdf5(tmp_path) -> None:
    """Processed objects should not trigger S3 listing or pipeline work."""
    database = _database(
        tmp_path,
        [("user-1/old.h5", "user-1", "processed")],
    )
    storage = StorageStub([])

    result = host_analysis_worker.run_once(
        db_path=database,
        repository=tmp_path,
        client=storage,
        bucket="bench",
    )

    assert result == 0
    assert storage.prefixes == []
