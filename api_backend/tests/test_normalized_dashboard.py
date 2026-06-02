"""Tests for normalized dashboard row generation."""

from __future__ import annotations

import os

import polars as pl
import pytest

from api_backend.db import create_user, list_users, record_storage_object
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


def _dashboard() -> dict:
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
    return client.requests[0]["json"]["dashboard"]


def _panel_sql(dashboard: dict, title: str) -> str:
    for panel in dashboard["panels"]:
        if panel["title"] == title:
            return panel["targets"][0]["rawSql"]
    raise AssertionError(f"panel not found: {title}")


def _render_trace_sql(sql: str, *, benchmark: str = "%", job_trace: str = "%") -> str:
    return (
        sql.replace(
            "$__unixEpochFilter(s.epoch_time)",
            "(s.epoch_time >= 1700000000 AND s.epoch_time <= 1700000100)",
        )
        .replace("${benchmark:raw}", benchmark)
        .replace("${job_trace:raw}", job_trace)
    )


def _render_dashboard_sql(sql: str, *, benchmark: str = "%") -> str:
    return (
        sql.replace(
            "$__timeFilter(coalesce(j.measured_at, j.processed_at))",
            "coalesce(j.measured_at, j.processed_at) between timestamp '2025-01-01' and timestamp '2027-01-01'",
        )
        .replace(
            "$__timeFilter(coalesce(measured_at, processed_at))",
            "coalesce(measured_at, processed_at) between timestamp '2025-01-01' and timestamp '2027-01-01'",
        )
        .replace("${benchmark:raw}", benchmark)
        .replace("${job_trace:raw}", "%")
    )


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
            "Price_EUR_per_MWh": [80.0, 90.0],
            "Cumulative_cost_EUR": [0.0, 0.0005],
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
        "price_eur_per_mwh",
        "cumulative_cost_eur",
        "cpu_utilization",
    ]
    assert samples.to_dicts()[1]["energy_used_j"] == 20.0
    assert samples.to_dicts()[1]["price_eur_per_mwh"] == 90.0
    assert samples.to_dicts()[1]["cumulative_cost_eur"] == 0.0005


def test_derive_job_metadata_from_hdf5_filename() -> None:
    """Job id and compute node should come from the uploaded filename."""
    assert derive_job_metadata("14001040_0_agq007.h5") == {
        "job_id": "14001040",
        "compute_node": "agq007",
    }


def test_compute_energy_profile_handles_missing_cumulative_energy() -> None:
    """Files with null cumulative energy should not crash normalization."""
    frame = pl.DataFrame({"ElapsedTime": [0.0, 1.0], "Energy": [None, None]})

    output, metrics = compute_energy_profile(
        frame, "job-1", "node-1", logger=SilentLogger()
    )

    assert output.height == 2
    assert metrics is not None
    assert metrics["energy_to_solution_j"] is None


def test_prepare_postgres_normalized_schema_noops_without_password(monkeypatch) -> None:
    """The pre-attach migration hook should be safe in local test runs."""
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)

    prepare_postgres_normalized_schema()


def test_list_users_orders_by_creation_time() -> None:
    """Maintenance scripts should be able to target every backend user."""
    first = create_user("dashboard_user_a", "hash")
    second = create_user("dashboard_user_b", "hash")

    users = list_users()

    assert [first["username"], second["username"]] == [
        user["username"] for user in users[-2:]
    ]


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
    panels_by_title = {panel["title"]: panel for panel in dashboard["panels"]}
    sql = "\n".join(
        target["rawSql"]
        for panel in dashboard["panels"]
        for target in panel.get("targets", [])
    )

    assert request["method"] == "POST"
    assert request["headers"] == {"X-Grafana-Org-Id": "7"}
    variables = {
        variable["name"]: variable for variable in dashboard["templating"]["list"]
    }
    benchmark_variable = variables["benchmark"]
    assert benchmark_variable["type"] == "query"
    assert benchmark_variable["includeAll"] is True
    assert benchmark_variable["refresh"] == 2
    assert (
        "select distinct coalesce(benchmark_name, 'unknown')"
        in benchmark_variable["query"]
    )
    job_trace_variable = variables["job_trace"]
    assert job_trace_variable["label"] == "Job Trace"
    assert job_trace_variable["includeAll"] is True
    assert (
        "select distinct job_id::text as job_id from benchmark_jobs"
        in job_trace_variable["query"]
    )
    assert "Mean Energy by Compute Node" in panel_titles
    assert "Mean Power by Compute Node" in panel_titles
    assert "Mean Elapsed Time by Compute Node" in panel_titles
    assert "Electricity Cost" in panel_titles
    assert "LIKWID DP FLOP/s by Compute Node" in panel_titles
    assert "LIKWID DP MFLOP/s per Watt" in panel_titles
    assert "LIKWID Vectorization by Compute Node" in panel_titles
    assert "LIKWID CPI by Compute Node" in panel_titles
    assert "LIKWID Cost per GFLOP" in panel_titles
    assert "LIKWID Node Efficiency Summary" in panel_titles
    assert (
        "partition by coalesce(job_id, object_key), coalesce(benchmark_name, 'unknown')"
        in sql
    )
    assert "$__timeFilter(coalesce(measured_at, processed_at))" in sql
    assert "coalesce(benchmark_name, 'unknown') = '${benchmark:raw}'" in sql
    node_power_sql = panels_by_title["Node Power"]["targets"][0]["rawSql"]
    cumulative_energy_sql = panels_by_title["Cumulative Energy"]["targets"][0]["rawSql"]
    summary_sql = "\n".join(
        target["rawSql"]
        for title, panel in panels_by_title.items()
        if title not in {"Node Power", "Cumulative Energy"}
        for target in panel.get("targets", [])
    )
    assert "${job_trace:raw}" in node_power_sql
    assert "${job_trace:raw}" in cumulative_energy_sql
    assert "$__unixEpochFilter(s.epoch_time)" in node_power_sql
    assert "$__unixEpochFilter(s.epoch_time)" in cumulative_energy_sql
    assert "j.job_id::text = '${job_trace:raw}'" in node_power_sql
    assert "j.job_id::text = '${job_trace:raw}'" in cumulative_energy_sql
    assert "${job_trace:raw}" not in summary_sql
    assert "sum(total_cost_eur)" in panels_by_title["Electricity Cost"]["targets"][0]["rawSql"]
    assert "total_cost_eur" in panels_by_title["Recent Jobs"]["targets"][0]["rawSql"]
    assert "mean_cost_eur" in panels_by_title["Compute Node Benchmark Summary"]["targets"][0]["rawSql"]
    assert "benchmark_likwid_samples" in panels_by_title["LIKWID Node Efficiency Summary"]["targets"][0]["rawSql"]
    assert "dp_mflops / nullif(j.mean_power_w, 0)" in panels_by_title["LIKWID DP MFLOP/s per Watt"]["targets"][0]["rawSql"]
    assert "j.total_cost_eur / nullif" in panels_by_title["LIKWID Cost per GFLOP"]["targets"][0]["rawSql"]


def test_trace_panel_sql_renders_text_safe_job_filter() -> None:
    """Trace SQL should render without bigint casts for job ids."""
    dashboard = _dashboard()
    for title in ("Node Power", "Cumulative Energy"):
        rendered = _render_trace_sql(
            _panel_sql(dashboard, title),
            benchmark="coremark_mini",
            job_trace="14038010",
        )
        assert "$__" not in rendered
        assert "${" not in rendered
        assert "s.epoch_time >= 1700000000" in rendered
        assert "j.job_id::text = '14038010'" in rendered
        assert "to_timestamp(s.epoch_time) as time" in rendered


def test_likwid_dashboard_sql_renders_expected_efficiency_metrics() -> None:
    """LIKWID panels should render concrete node-efficiency SQL."""
    dashboard = _dashboard()
    for title in (
        "LIKWID DP FLOP/s by Compute Node",
        "LIKWID DP MFLOP/s per Watt",
        "LIKWID Vectorization by Compute Node",
        "LIKWID CPI by Compute Node",
        "LIKWID Cost per GFLOP",
        "LIKWID Node Efficiency Summary",
    ):
        rendered = _render_dashboard_sql(
            _panel_sql(dashboard, title), benchmark="coremark_mini"
        )
        assert "$__" not in rendered
        assert "${" not in rendered
        assert "benchmark_likwid_samples" in rendered
        assert "coremark_mini" in rendered


@pytest.mark.skipif(
    not os.getenv("POSTGRES_PASSWORD"),
    reason="Postgres SQL validation requires POSTGRES_* environment variables.",
)
def test_trace_panel_sql_explains_on_postgres() -> None:
    """Rendered trace queries should be accepted by PostgreSQL."""
    import psycopg

    dashboard = _dashboard()
    queries = [
        _render_trace_sql(_panel_sql(dashboard, "Node Power"), job_trace="14038010"),
        _render_trace_sql(
            _panel_sql(dashboard, "Cumulative Energy"), job_trace="14038010"
        ),
    ]
    with psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.environ["POSTGRES_PASSWORD"],
    ) as connection:
        with connection.transaction():
            connection.execute(
                "CREATE TEMP TABLE benchmark_jobs (object_key text, original_filename text, "
                "benchmark_name text, job_id text, measured_at timestamptz, processed_at timestamptz, "
                "mean_power_w double precision, total_cost_eur double precision)"
            )
            connection.execute(
                "CREATE TEMP TABLE benchmark_samples (object_key text, epoch_time bigint, "
                "node_power double precision, energy_used_j double precision)"
            )
            connection.execute(
                "CREATE TEMP TABLE benchmark_likwid_samples (h5_object_key text, job_id text, "
                "compute_node text, benchmark_name text, dp_mflops double precision, "
                "vectorization_ratio_pct double precision, cpi double precision, clock_mhz double precision, "
                "elapsed_time_s double precision)"
            )
            for query in queries:
                connection.execute(f"EXPLAIN {query}").fetchall()
