"""Fast end-to-end checks for HDF5 analysis and listener wiring."""

from __future__ import annotations

import numpy as np
import h5py
import pytest

from fastapi import BackgroundTasks

from api_backend.db import get_storage_object, record_storage_object
from analysis_module import minio_listener
from analysis_module.connectors.normalized import (
    build_dashboard_samples,
    derive_job_metadata,
    infer_owner_metadata,
)
from analysis_module.pipeline_core.config import PipelineConfig, PriceSettings
from analysis_module.processing.h5_processing import h5_to_dataframe
from analysis_module.utils.common import validate_h5_file


class SilentLogger:
    def info(self, *_args, **_kwargs) -> None:
        pass

    def warning(self, *_args, **_kwargs) -> None:
        pass

    def error(self, *_args, **_kwargs) -> None:
        pass

    def exception(self, *_args, **_kwargs) -> None:
        pass


def _pipeline_config(base):
    return PipelineConfig(
        base,
        base,
        base / "out",
        base / "stats",
        base / "summaries",
        base / "prices",
        base / "analysis.log",
        False,
        PriceSettings(4169, "DE-LU", "quarterhour"),
        True,
    )


def _write_minimal_slurm_h5(path) -> None:
    dtype = np.dtype(
        [
            ("ElapsedTime", "i8"),
            ("EpochTime", "i8"),
            ("NodePower", "f8"),
            ("CPUUtilization", "f8"),
        ]
    )
    rows = np.array(
        [
            (0, 1_700_000_000, 10.0, 20.0),
            (1, 1_700_000_001, 20.0, 30.0),
            (2, 1_700_000_002, 30.0, 40.0),
        ],
        dtype=dtype,
    )
    with h5py.File(path, "w") as handle:
        handle.create_dataset("Node", data=rows)


def test_hdf5_processing_preserves_owner_and_benchmark_metadata(tmp_path) -> None:
    """A real HDF5 file should become dashboard samples with backend metadata."""
    h5_path = tmp_path / "14038010_batch_agq001.h5"
    _write_minimal_slurm_h5(h5_path)
    object_key = "user-1/14038010_batch_agq001.h5"
    record_storage_object(
        object_key=object_key,
        user_id="user-1",
        username="alice",
        original_filename=h5_path.name,
        benchmark_name="coremark_mini",
        state="uploaded",
    )

    logger = SilentLogger()
    assert validate_h5_file(h5_path, logger=logger) is True

    frame = h5_to_dataframe(
        h5_path,
        _pipeline_config(tmp_path),
        logger=logger,
        display_name=object_key,
    )
    metadata = infer_owner_metadata(object_key)
    samples = build_dashboard_samples(frame, metadata)

    assert metadata["owner_user_id"] == "user-1"
    assert metadata["owner_username"] == "alice"
    assert metadata["benchmark_name"] == "coremark_mini"
    assert derive_job_metadata(metadata["original_filename"]) == {
        "job_id": "14038010",
        "compute_node": "agq001",
    }
    assert samples.height == 3
    assert samples["energy_used_j"][-1] > 0
    assert get_storage_object(object_key)["state"] == "uploaded"


def test_listener_launches_current_pipeline_entrypoint(monkeypatch) -> None:
    """MinIO notifications should launch the current module entry point."""
    calls = []

    def fake_run(command, capture_output, text, check):
        calls.append(command)

        class Result:
            returncode = 0
            stdout = "created"
            stderr = ""

        return Result()

    monkeypatch.setattr(minio_listener.subprocess, "run", fake_run)
    monkeypatch.setattr(minio_listener.time, "time", lambda: 123456)

    minio_listener.launch_job("benchmarking-suite", "user-1/14038010_batch_agq001.h5")

    command = calls[0]
    shell = command[-1]
    assert command[:3] == ["kubectl", "create", "job"]
    assert "duckdb-123456" in command
    assert "python -m analysis_module.pipeline_runner" in shell
    assert "S3_SYNC=1" in shell
    assert "S3_BUCKET=benchmarking-suite" in shell
    assert "S3_OBJECT_PREFIX=user-1" in shell


@pytest.mark.anyio
async def test_listener_schedules_analysis_artifacts(monkeypatch) -> None:
    """Webhook parsing should schedule HDF5 and LIKWID analysis artifacts."""
    scheduled = []

    def fake_add_task(fn, *args):
        scheduled.append((fn, args))

    class Request:
        async def json(self):
            return {
                "Records": [
                    {"s3": {"bucket": {"name": "b"}, "object": {"key": "u/a.h5"}}},
                    {"s3": {"bucket": {"name": "b"}, "object": {"key": "u/a.out"}}},
                    {"s3": {"bucket": {"name": "b"}, "object": {"key": "u/a.err"}}},
                    {"s3": {"bucket": {"name": "b"}, "object": {"key": "u/a.csv"}}},
                    {"s3": {"bucket": {"name": "b"}, "object": {"key": "u/b.h5"}}},
                ]
            }

    tasks = BackgroundTasks()
    monkeypatch.setattr(tasks, "add_task", fake_add_task)

    result = await minio_listener.minio_event(Request(), tasks)

    assert result == {"ok": True, "scheduled": 4}
    assert [args for _fn, args in scheduled] == [
        ("b", "u/a.h5"),
        ("b", "u/a.err"),
        ("b", "u/a.csv"),
        ("b", "u/b.h5"),
    ]
