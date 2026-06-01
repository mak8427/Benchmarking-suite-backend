"""Tests for normalized dashboard row generation."""

from __future__ import annotations

import polars as pl

from api_backend.db import record_storage_object
from analysis_module.connectors.normalized import (
    build_dashboard_samples,
    derive_job_metadata,
    infer_owner_metadata,
    prepare_postgres_normalized_schema,
)
from analysis_module.pipeline_core.energy_profile import compute_energy_profile


class SilentLogger:
    def info(self, *_args, **_kwargs) -> None:
        pass

    def warning(self, *_args, **_kwargs) -> None:
        pass


def test_infer_owner_metadata_uses_recorded_storage_object() -> None:
    """Remote object labels should resolve to backend ownership metadata."""
    key = "user-1/file.h5"
    record_storage_object(
        object_key=key,
        user_id="user-1",
        username="alice",
        original_filename="file.h5",
        benchmark_name="stream_triad",
    )

    metadata = infer_owner_metadata(key)

    assert metadata == {
        "owner_user_id": "user-1",
        "owner_username": "alice",
        "object_key": key,
        "original_filename": "file.h5",
        "benchmark_name": "stream_triad",
    }


def test_build_dashboard_samples_extracts_common_metrics() -> None:
    """Processed frames should become stable dashboard sample rows."""
    frame = pl.DataFrame(
        {
            "ElapsedTime": [0, 1],
            "Node__EpochTime": [100, 101],
            "NodePower": [10.0, 20.0],
            "Energy_used_J": [0.0, 20.0],
            "Energy_Increment_J": [0.0, 20.0],
            "Node__CPUUtilization": [30.0, 40.0],
        }
    )
    metadata = {"object_key": "u/file.h5", "owner_user_id": "u"}

    samples = build_dashboard_samples(frame, metadata)

    assert samples.columns == [
        "object_key",
        "owner_user_id",
        "elapsed_time",
        "epoch_time",
        "node_power",
        "energy_used_j",
        "energy_increment_j",
        "cpu_utilization",
    ]
    assert samples.to_dicts()[1]["energy_used_j"] == 20.0


def test_derive_job_metadata_from_hdf5_filename() -> None:
    """Job id and compute node should come from the uploaded filename."""
    assert derive_job_metadata("14001040_0_agq007.h5") == {
        "job_id": "14001040",
        "compute_node": "agq007",
    }


def test_compute_energy_profile_handles_missing_cumulative_energy() -> None:
    """Files with null cumulative energy should not crash normalization."""
    frame = pl.DataFrame({"ElapsedTime": [0.0, 1.0], "Energy": [None, None]})

    output, metrics = compute_energy_profile(frame, "job-1", "node-1", logger=SilentLogger())

    assert output.height == 2
    assert metrics is not None
    assert metrics["energy_to_solution_j"] is None


def test_prepare_postgres_normalized_schema_noops_without_password(monkeypatch) -> None:
    """The pre-attach migration hook should be safe in local test runs."""
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)

    prepare_postgres_normalized_schema()
