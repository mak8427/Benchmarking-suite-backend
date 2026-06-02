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
from api_backend.grafana import GrafanaProvisioner, GrafanaSettings


class SilentLogger:
    def info(self, *_args, **_kwargs) -> None:
        pass

    def warning(self, *_args, **_kwargs) -> None:
        pass


class FakeGrafanaResponse:
    def raise_for_status(self) -> None:
        pass


class FakeGrafanaClient:
    def __init__(self) -> None:
        self.requests = []

    def request(self, method, url, **kwargs):
        self.requests.append({"method": method, "url": url, **kwargs})
        return FakeGrafanaResponse()


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


def test_grafana_dashboard_groups_node_metrics_by_canonical_jobs() -> None:
    """Provisioned dashboards should compare nodes without counting duplicate HDF5 views."""
    client = FakeGrafanaClient()
    provisioner = GrafanaProvisioner(
        settings=GrafanaSettings(
            url="http://grafana.local",
            admin_user="admin",
            admin_password="secret",
            postgres_host="postgres",
            postgres_port="5432",
            postgres_db="benchmarks",
            postgres_admin_user="postgres",
            postgres_admin_password="postgres",
        ),
        client=client,
    )

    provisioner.ensure_dashboard(user_id="user-1", org_id=7)

    request = client.requests[0]
    dashboard = request["json"]["dashboard"]
    panel_titles = {panel["title"] for panel in dashboard["panels"]}
    sql = "\n".join(target["rawSql"] for panel in dashboard["panels"] for target in panel.get("targets", []))

    assert request["method"] == "POST"
    assert request["headers"] == {"X-Grafana-Org-Id": "7"}
    variable = dashboard["templating"]["list"][0]
    assert variable["type"] == "query"
    assert variable["includeAll"] is True
    assert variable["refresh"] == 2
    assert "select distinct coalesce(benchmark_name, 'unknown')" in variable["query"]
    assert "Mean Energy by Compute Node" in panel_titles
    assert "Mean Power by Compute Node" in panel_titles
    assert "Mean Elapsed Time by Compute Node" in panel_titles
    assert "partition by coalesce(job_id, object_key), coalesce(benchmark_name, 'unknown')" in sql
    assert "$__timeFilter(coalesce(measured_at, processed_at))" in sql
    assert "coalesce(benchmark_name, 'unknown') = '${benchmark:raw}'" in sql
