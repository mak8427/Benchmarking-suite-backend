"""Grafana provisioning helpers for backend users.

The backend remains the source of identity. Grafana is provisioned with a user,
an isolated organization, a Postgres datasource, and a starter dashboard. Users
should reach Grafana through auth-proxy headers rather than managing a second
Grafana password.
"""

from __future__ import annotations

import os
import secrets
from hashlib import sha256
from dataclasses import dataclass
from typing import Any

import httpx
import psycopg

try:
    from api_backend.db import get_or_create_user_workspace, set_user_workspace_grafana_org
except ModuleNotFoundError:  # pragma: no cover - supports running from api_backend/
    from db import get_or_create_user_workspace, set_user_workspace_grafana_org  # type: ignore[no-redef]


def _bar_chart_field_config(unit: str) -> dict[str, Any]:
    """Return bar chart field config with value labels forced on."""
    return {
        "defaults": {
            "unit": unit,
            "custom": {"showValue": "always"},
        },
        "overrides": [],
    }


def _bar_chart_options() -> dict[str, Any]:
    """Return bar chart options that show values across Grafana versions."""
    return {
        "orientation": "auto",
        "showValue": "always",
        "text": {"valueSize": 12},
        "xField": "compute_node",
    }


@dataclass(frozen=True)
class GrafanaSettings:
    """Runtime settings for Grafana provisioning."""

    url: str
    admin_user: str | None
    admin_password: str | None
    postgres_host: str
    postgres_port: str
    postgres_db: str
    postgres_admin_user: str
    postgres_admin_password: str | None


def resolve_grafana_settings() -> GrafanaSettings:
    """Resolve Grafana and Postgres settings from environment variables."""
    return GrafanaSettings(
        url=os.getenv("GRAFANA_URL", "http://127.0.0.1:3000").rstrip("/"),
        admin_user=os.getenv("GRAFANA_ADMIN_USER"),
        admin_password=os.getenv("GRAFANA_ADMIN_PASSWORD"),
        postgres_host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        postgres_port=os.getenv("POSTGRES_PORT", "5432"),
        postgres_db=os.getenv("POSTGRES_DB", "postgres"),
        postgres_admin_user=os.getenv("POSTGRES_USER", "postgres"),
        postgres_admin_password=os.getenv("POSTGRES_PASSWORD"),
    )


class GrafanaProvisioner:
    """Small Grafana HTTP API client used by backend registration hooks."""

    def __init__(self, settings: GrafanaSettings | None = None, client: httpx.Client | None = None) -> None:
        self.settings = settings or resolve_grafana_settings()
        self.client = client or httpx.Client(timeout=15.0)

    @staticmethod
    def datasource_uid(user_id: str) -> str:
        """Return a stable Grafana-safe datasource UID for a backend user."""
        return f"benchpg-{sha256(user_id.encode('utf-8')).hexdigest()[:20]}"

    @property
    def enabled(self) -> bool:
        """Return whether Grafana admin credentials are configured."""
        return bool(self.settings.admin_user and self.settings.admin_password)

    def _request(self, method: str, path: str, *, org_id: int | None = None, **kwargs: Any) -> httpx.Response:
        headers = kwargs.pop("headers", {})
        if org_id is not None:
            headers = {**headers, "X-Grafana-Org-Id": str(org_id)}
        response = self.client.request(
            method,
            f"{self.settings.url}{path}",
            auth=(self.settings.admin_user or "", self.settings.admin_password or ""),
            headers=headers,
            **kwargs,
        )
        response.raise_for_status()
        return response

    def ensure_org(self, username: str) -> int:
        """Return a per-user Grafana org id, creating it when missing."""
        org_name = f"bench-{username}"
        response = self.client.get(
            f"{self.settings.url}/api/orgs/name/{org_name}",
            auth=(self.settings.admin_user or "", self.settings.admin_password or ""),
            timeout=15.0,
        )
        if response.status_code == 200:
            return int(response.json()["id"])
        response = self._request("POST", "/api/orgs", json={"name": org_name})
        return int(response.json().get("orgId") or response.json()["id"])

    def ensure_user(self, username: str, password: str | None = None) -> int:
        """Return a Grafana user id, creating or updating it when missing."""
        response = self.client.get(
            f"{self.settings.url}/api/users/lookup?loginOrEmail={username}",
            auth=(self.settings.admin_user or "", self.settings.admin_password or ""),
            timeout=15.0,
        )
        if response.status_code == 200:
            user_id = int(response.json()["id"])
            if password:
                self._request("PUT", f"/api/admin/users/{user_id}/password", json={"password": password})
            return user_id
        response = self._request(
            "POST",
            "/api/admin/users",
            json={
                "name": username,
                "email": f"{username}@benchmarking-suite.local",
                "login": username,
                "password": password or secrets.token_urlsafe(32),
            },
        )
        return int(response.json()["id"])

    def ensure_user_membership(self, *, username: str, grafana_user_id: int, org_id: int) -> None:
        """Add a Grafana user to their own org as Editor.

        Editors can adjust panels and inspect SQL inside their private org, but
        the datasource still connects through a per-user read-only Postgres role
        protected by row-level security.
        """
        response = self.client.post(
            f"{self.settings.url}/api/orgs/{org_id}/users",
            auth=(self.settings.admin_user or "", self.settings.admin_password or ""),
            json={"loginOrEmail": username, "role": "Editor"},
            timeout=15.0,
        )
        if response.status_code == 409:
            update = self.client.patch(
                f"{self.settings.url}/api/orgs/{org_id}/users/{grafana_user_id}",
                auth=(self.settings.admin_user or "", self.settings.admin_password or ""),
                json={"role": "Editor"},
                timeout=15.0,
            )
            update.raise_for_status()
            return
        if response.status_code != 200:
            response.raise_for_status()

    def select_user_org(self, *, grafana_user_id: int, org_id: int) -> None:
        """Make the user's private organization their active Grafana org."""
        self._request("POST", f"/api/users/{grafana_user_id}/using/{org_id}")

    def remove_main_org_membership(self, *, grafana_user_id: int, org_id: int) -> None:
        """Remove the default Main Org membership after private org setup."""
        if org_id == 1:
            return
        response = self.client.delete(
            f"{self.settings.url}/api/orgs/1/users/{grafana_user_id}",
            auth=(self.settings.admin_user or "", self.settings.admin_password or ""),
            timeout=15.0,
        )
        if response.status_code not in {200, 404}:
            response.raise_for_status()

    def ensure_postgres_datasource(self, *, user_id: str, username: str, org_id: int) -> None:
        """Create or update the user's Grafana Postgres datasource."""
        workspace = get_or_create_user_workspace(user_id)
        datasource = {
            "name": "My Benchmark Data",
            "uid": self.datasource_uid(user_id),
            "type": "postgres",
            "access": "proxy",
            "url": f"{self.settings.postgres_host}:{self.settings.postgres_port}",
            "database": self.settings.postgres_db,
            "user": workspace["postgres_role"],
            "isDefault": True,
            "jsonData": {
                "database": self.settings.postgres_db,
                "sslmode": "disable",
                "postgresVersion": 1600,
                "timescaledb": False,
            },
            "secureJsonData": {"password": workspace["postgres_password"]},
        }
        response = self.client.get(
            f"{self.settings.url}/api/datasources/uid/{datasource['uid']}",
            auth=(self.settings.admin_user or "", self.settings.admin_password or ""),
            headers={"X-Grafana-Org-Id": str(org_id)},
            timeout=15.0,
        )
        if response.status_code == 200:
            self._request("PUT", f"/api/datasources/uid/{datasource['uid']}", org_id=org_id, json=datasource)
        else:
            self._request("POST", "/api/datasources", org_id=org_id, json=datasource)

    def ensure_dashboard(self, *, user_id: str, org_id: int) -> None:
        """Create or update a starter benchmark dashboard for the user org."""
        datasource_uid = self.datasource_uid(user_id)
        dashboard = {
            "uid": "my-benchmark-data",
            "title": "My Benchmark Data",
            "schemaVersion": 39,
            "version": 1,
            "time": {"from": "now-30d", "to": "now"},
            "templating": {
                "list": [
                    {
                        "name": "benchmark",
                        "type": "query",
                        "label": "Benchmark",
                        "datasource": {"uid": datasource_uid, "type": "postgres"},
                        "query": "select distinct coalesce(benchmark_name, 'unknown') as benchmark_name from benchmark_jobs order by 1",
                        "refresh": 2,
                        "sort": 1,
                        "includeAll": True,
                        "allValue": "%",
                        "multi": False,
                        "hide": 0,
                        "current": {"selected": True, "text": "All", "value": "%"},
                    },
                    {
                        "name": "job_trace",
                        "type": "query",
                        "label": "Job Trace",
                        "datasource": {"uid": datasource_uid, "type": "postgres"},
                        "query": (
                            "select distinct job_id::text as job_id from benchmark_jobs "
                            "where job_id is not null "
                            "and ('${benchmark:raw}' = '%' or coalesce(benchmark_name, 'unknown') = '${benchmark:raw}') "
                            "order by 1"
                        ),
                        "refresh": 2,
                        "sort": 1,
                        "includeAll": True,
                        "allValue": "%",
                        "multi": False,
                        "hide": 0,
                        "current": {"selected": True, "text": "All", "value": "%"},
                    },
                ]
            },
            "panels": [
                {
                    "id": 1,
                    "type": "stat",
                    "title": "Processed Jobs",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "datasource": {"uid": datasource_uid, "type": "postgres"},
                            "format": "table",
                            "rawSql": (
                                "with ranked as (select *, row_number() over ("
                                "partition by coalesce(job_id, object_key), coalesce(benchmark_name, 'unknown'), "
                                "coalesce(compute_node, 'unknown') order by "
                                "case when original_filename like '%_batch_%' then 0 else 1 end, "
                                "coalesce(max_elapsed_time_s, 0) desc, coalesce(sample_count, 0) desc, processed_at desc) as rn "
                                "from benchmark_jobs where $__timeFilter(coalesce(measured_at, processed_at))), "
                                "canonical as (select * from ranked where rn = 1) "
                                "select count(*)::bigint as processed_jobs from canonical "
                                "where ('${benchmark:raw}' = '%' or coalesce(benchmark_name, 'unknown') = '${benchmark:raw}')"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": {"defaults": {"unit": "short"}, "overrides": []},
                    "gridPos": {"h": 4, "w": 6, "x": 0, "y": 0},
                },
                {
                    "id": 2,
                    "type": "stat",
                    "title": "Total Energy",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "datasource": {"uid": datasource_uid, "type": "postgres"},
                            "format": "table",
                            "rawSql": (
                                "with ranked as (select *, row_number() over ("
                                "partition by coalesce(job_id, object_key), coalesce(benchmark_name, 'unknown'), "
                                "coalesce(compute_node, 'unknown') order by "
                                "case when original_filename like '%_batch_%' then 0 else 1 end, "
                                "coalesce(max_elapsed_time_s, 0) desc, coalesce(sample_count, 0) desc, processed_at desc) as rn "
                                "from benchmark_jobs where $__timeFilter(coalesce(measured_at, processed_at))) "
                                "select coalesce(sum(total_energy_j), 0)::double precision as total_energy_j "
                                "from ranked where rn = 1 "
                                "and ('${benchmark:raw}' = '%' or coalesce(benchmark_name, 'unknown') = '${benchmark:raw}')"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": {"defaults": {"unit": "joule"}, "overrides": []},
                    "gridPos": {"h": 4, "w": 6, "x": 6, "y": 0},
                },
                {
                    "id": 3,
                    "type": "stat",
                    "title": "Cluster Time",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "datasource": {"uid": datasource_uid, "type": "postgres"},
                            "format": "table",
                            "rawSql": (
                                "with ranked as (select *, row_number() over ("
                                "partition by coalesce(job_id, object_key), coalesce(benchmark_name, 'unknown'), "
                                "coalesce(compute_node, 'unknown') order by "
                                "case when original_filename like '%_batch_%' then 0 else 1 end, "
                                "coalesce(max_elapsed_time_s, 0) desc, coalesce(sample_count, 0) desc, processed_at desc) as rn "
                                "from benchmark_jobs where $__timeFilter(coalesce(measured_at, processed_at))) "
                                "select coalesce(sum(max_elapsed_time_s), 0)::double precision as cluster_time_s "
                                "from ranked where rn = 1 "
                                "and ('${benchmark:raw}' = '%' or coalesce(benchmark_name, 'unknown') = '${benchmark:raw}')"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": {"defaults": {"unit": "s"}, "overrides": []},
                    "gridPos": {"h": 4, "w": 6, "x": 12, "y": 0},
                },
                {
                    "id": 11,
                    "type": "stat",
                    "title": "Electricity Cost",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "datasource": {"uid": datasource_uid, "type": "postgres"},
                            "format": "table",
                            "rawSql": (
                                "with ranked as (select *, row_number() over ("
                                "partition by coalesce(job_id, object_key), coalesce(benchmark_name, 'unknown'), "
                                "coalesce(compute_node, 'unknown') order by "
                                "case when original_filename like '%_batch_%' then 0 else 1 end, "
                                "coalesce(max_elapsed_time_s, 0) desc, coalesce(sample_count, 0) desc, processed_at desc) as rn "
                                "from benchmark_jobs where $__timeFilter(coalesce(measured_at, processed_at))) "
                                "select coalesce(sum(total_cost_eur), 0)::double precision as total_cost_eur "
                                "from ranked where rn = 1 "
                                "and ('${benchmark:raw}' = '%' or coalesce(benchmark_name, 'unknown') = '${benchmark:raw}')"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": {"defaults": {"unit": "currencyEUR"}, "overrides": []},
                    "gridPos": {"h": 4, "w": 6, "x": 18, "y": 0},
                },
                {
                    "id": 4,
                    "type": "table",
                    "title": "Recent Jobs",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "datasource": {"uid": datasource_uid, "type": "postgres"},
                            "format": "table",
                            "rawSql": (
                                "with ranked as (select *, row_number() over ("
                                "partition by coalesce(job_id, object_key), coalesce(benchmark_name, 'unknown'), "
                                "coalesce(compute_node, 'unknown') order by "
                                "case when original_filename like '%_batch_%' then 0 else 1 end, "
                                "coalesce(max_elapsed_time_s, 0) desc, coalesce(sample_count, 0) desc, processed_at desc) as rn "
                                "from benchmark_jobs where $__timeFilter(coalesce(measured_at, processed_at))) "
                                "select coalesce(measured_at, processed_at) as time, job_id, coalesce(benchmark_name, 'unknown') as benchmark_name, compute_node, "
                                "original_filename, sample_count, max_power_w, mean_power_w, "
                                "total_energy_j, total_cost_eur, mean_price_eur_per_mwh, max_elapsed_time_s "
                                "from ranked where rn = 1 "
                                "and ('${benchmark:raw}' = '%' or coalesce(benchmark_name, 'unknown') = '${benchmark:raw}') "
                                "order by coalesce(measured_at, processed_at) desc nulls last"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": {
                        "defaults": {"custom": {"align": "auto", "cellOptions": {"type": "auto"}}},
                        "overrides": [],
                    },
                    "options": {"showHeader": True},
                    "gridPos": {"h": 8, "w": 24, "x": 0, "y": 4},
                },
                {
                    "id": 5,
                    "type": "barchart",
                    "title": "Mean Energy by Compute Node",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "datasource": {"uid": datasource_uid, "type": "postgres"},
                            "format": "table",
                            "rawSql": (
                                "with ranked as (select *, row_number() over ("
                                "partition by coalesce(job_id, object_key), coalesce(benchmark_name, 'unknown'), "
                                "coalesce(compute_node, 'unknown') order by "
                                "case when original_filename like '%_batch_%' then 0 else 1 end, "
                                "coalesce(max_elapsed_time_s, 0) desc, coalesce(sample_count, 0) desc, processed_at desc) as rn "
                                "from benchmark_jobs where $__timeFilter(coalesce(measured_at, processed_at))), "
                                "canonical as (select * from ranked where rn = 1) "
                                "select compute_node, avg(total_energy_j)::double precision as mean_energy_j from canonical "
                                "where compute_node is not null and total_energy_j is not null "
                                "and ('${benchmark:raw}' = '%' or coalesce(benchmark_name, 'unknown') = '${benchmark:raw}') "
                                "group by compute_node order by mean_energy_j desc"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": _bar_chart_field_config("joule"),
                    "options": _bar_chart_options(),
                    "gridPos": {"h": 8, "w": 8, "x": 0, "y": 12},
                },
                {
                    "id": 6,
                    "type": "barchart",
                    "title": "Mean Power by Compute Node",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "datasource": {"uid": datasource_uid, "type": "postgres"},
                            "format": "table",
                            "rawSql": (
                                "with ranked as (select *, row_number() over ("
                                "partition by coalesce(job_id, object_key), coalesce(benchmark_name, 'unknown'), "
                                "coalesce(compute_node, 'unknown') order by "
                                "case when original_filename like '%_batch_%' then 0 else 1 end, "
                                "coalesce(max_elapsed_time_s, 0) desc, coalesce(sample_count, 0) desc, processed_at desc) as rn "
                                "from benchmark_jobs where $__timeFilter(coalesce(measured_at, processed_at))), "
                                "canonical as (select * from ranked where rn = 1) "
                                "select compute_node, avg(mean_power_w)::double precision as mean_power_w from canonical "
                                "where compute_node is not null and mean_power_w is not null "
                                "and ('${benchmark:raw}' = '%' or coalesce(benchmark_name, 'unknown') = '${benchmark:raw}') "
                                "group by compute_node order by mean_power_w desc"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": _bar_chart_field_config("watt"),
                    "options": _bar_chart_options(),
                    "gridPos": {"h": 8, "w": 8, "x": 8, "y": 12},
                },
                {
                    "id": 7,
                    "type": "barchart",
                    "title": "Mean Elapsed Time by Compute Node",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "datasource": {"uid": datasource_uid, "type": "postgres"},
                            "format": "table",
                            "rawSql": (
                                "with ranked as (select *, row_number() over ("
                                "partition by coalesce(job_id, object_key), coalesce(benchmark_name, 'unknown'), "
                                "coalesce(compute_node, 'unknown') order by "
                                "case when original_filename like '%_batch_%' then 0 else 1 end, "
                                "coalesce(max_elapsed_time_s, 0) desc, coalesce(sample_count, 0) desc, processed_at desc) as rn "
                                "from benchmark_jobs where $__timeFilter(coalesce(measured_at, processed_at))), "
                                "canonical as (select * from ranked where rn = 1) "
                                "select compute_node, avg(max_elapsed_time_s)::double precision as mean_elapsed_time_s from canonical "
                                "where compute_node is not null and max_elapsed_time_s is not null "
                                "and ('${benchmark:raw}' = '%' or coalesce(benchmark_name, 'unknown') = '${benchmark:raw}') "
                                "group by compute_node order by mean_elapsed_time_s desc"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": _bar_chart_field_config("s"),
                    "options": _bar_chart_options(),
                    "gridPos": {"h": 8, "w": 8, "x": 16, "y": 12},
                },
                {
                    "id": 8,
                    "type": "table",
                    "title": "Compute Node Benchmark Summary",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "datasource": {"uid": datasource_uid, "type": "postgres"},
                            "format": "table",
                            "rawSql": (
                                "with ranked as (select *, row_number() over ("
                                "partition by coalesce(job_id, object_key), coalesce(benchmark_name, 'unknown'), "
                                "coalesce(compute_node, 'unknown') order by "
                                "case when original_filename like '%_batch_%' then 0 else 1 end, "
                                "coalesce(max_elapsed_time_s, 0) desc, coalesce(sample_count, 0) desc, processed_at desc) as rn "
                                "from benchmark_jobs where $__timeFilter(coalesce(measured_at, processed_at))), "
                                "canonical as (select * from ranked where rn = 1) "
                                "select coalesce(benchmark_name, 'unknown') as benchmark_name, compute_node, count(*)::bigint as runs, "
                                "avg(total_energy_j)::double precision as mean_energy_j, "
                                "avg(mean_power_w)::double precision as mean_power_w, "
                                "avg(max_elapsed_time_s)::double precision as mean_elapsed_time_s, "
                                "avg(total_cost_eur)::double precision as mean_cost_eur, "
                                "avg(mean_price_eur_per_mwh)::double precision as mean_price_eur_per_mwh, "
                                "max(coalesce(measured_at, processed_at)) as last_measured_at from canonical "
                                "where compute_node is not null "
                                "and ('${benchmark:raw}' = '%' or coalesce(benchmark_name, 'unknown') = '${benchmark:raw}') "
                                "group by coalesce(benchmark_name, 'unknown'), compute_node order by benchmark_name, mean_energy_j desc"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": {"defaults": {"custom": {"align": "auto", "cellOptions": {"type": "auto"}}}, "overrides": []},
                    "options": {"showHeader": True},
                    "gridPos": {"h": 8, "w": 24, "x": 0, "y": 20},
                },
                {
                    "id": 9,
                    "type": "timeseries",
                    "title": "Node Power",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "datasource": {"uid": datasource_uid, "type": "postgres"},
                            "format": "time_series",
                            "rawSql": (
                                "select to_timestamp(s.epoch_time) as time, j.original_filename as metric, "
                                "avg(s.node_power)::double precision as value "
                                "from benchmark_samples s join benchmark_jobs j on j.object_key = s.object_key "
                                "where s.epoch_time is not null and s.node_power is not null "
                                "and $__unixEpochFilter(s.epoch_time) "
                                "and ('${benchmark:raw}' = '%' or coalesce(j.benchmark_name, 'unknown') = '${benchmark:raw}') "
                                "and ('${job_trace:raw}' = '%' or j.job_id::text = '${job_trace:raw}') "
                                "group by 1, 2 order by 1"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": {"defaults": {"unit": "watt"}, "overrides": []},
                    "gridPos": {"h": 9, "w": 24, "x": 0, "y": 28},
                },
                {
                    "id": 10,
                    "type": "timeseries",
                    "title": "Cumulative Energy",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "datasource": {"uid": datasource_uid, "type": "postgres"},
                            "format": "time_series",
                            "rawSql": (
                                "select to_timestamp(s.epoch_time) as time, j.original_filename as metric, "
                                "max(s.energy_used_j)::double precision as value "
                                "from benchmark_samples s join benchmark_jobs j on j.object_key = s.object_key "
                                "where s.epoch_time is not null and s.energy_used_j is not null "
                                "and $__unixEpochFilter(s.epoch_time) "
                                "and ('${benchmark:raw}' = '%' or coalesce(j.benchmark_name, 'unknown') = '${benchmark:raw}') "
                                "and ('${job_trace:raw}' = '%' or j.job_id::text = '${job_trace:raw}') "
                                "group by 1, 2 order by 1"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": {"defaults": {"unit": "joule"}, "overrides": []},
                    "gridPos": {"h": 9, "w": 24, "x": 0, "y": 37},
                },
                {
                    "id": 12,
                    "type": "barchart",
                    "title": "LIKWID DP FLOP/s by Compute Node",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "datasource": {"uid": datasource_uid, "type": "postgres"},
                            "format": "table",
                            "rawSql": (
                                "select l.compute_node, avg(l.dp_mflops)::double precision as mean_dp_mflops from benchmark_likwid_samples l "
                                "join benchmark_jobs j on j.object_key = l.h5_object_key "
                                "where l.compute_node is not null and l.dp_mflops is not null "
                                "and $__timeFilter(coalesce(j.measured_at, j.processed_at)) "
                                "and ('${benchmark:raw}' = '%' or coalesce(l.benchmark_name, 'unknown') = '${benchmark:raw}') "
                                "group by l.compute_node order by mean_dp_mflops desc"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": _bar_chart_field_config("Mflops"),
                    "options": _bar_chart_options(),
                    "gridPos": {"h": 8, "w": 8, "x": 0, "y": 46},
                },
                {
                    "id": 13,
                    "type": "barchart",
                    "title": "LIKWID DP MFLOP/s per Watt",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "datasource": {"uid": datasource_uid, "type": "postgres"},
                            "format": "table",
                            "rawSql": (
                                "select l.compute_node, avg(l.dp_mflops / nullif(j.mean_power_w, 0))::double precision as mean_mflops_per_watt from benchmark_likwid_samples l "
                                "join benchmark_jobs j on j.object_key = l.h5_object_key "
                                "where l.compute_node is not null and l.dp_mflops is not null and j.mean_power_w is not null "
                                "and $__timeFilter(coalesce(j.measured_at, j.processed_at)) "
                                "and ('${benchmark:raw}' = '%' or coalesce(l.benchmark_name, 'unknown') = '${benchmark:raw}') "
                                "group by l.compute_node order by mean_mflops_per_watt desc"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": _bar_chart_field_config("none"),
                    "options": _bar_chart_options(),
                    "gridPos": {"h": 8, "w": 8, "x": 8, "y": 46},
                },
                {
                    "id": 14,
                    "type": "barchart",
                    "title": "LIKWID Vectorization by Compute Node",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "datasource": {"uid": datasource_uid, "type": "postgres"},
                            "format": "table",
                            "rawSql": (
                                "select l.compute_node, avg(l.vectorization_ratio_pct)::double precision as mean_vectorization_pct from benchmark_likwid_samples l "
                                "join benchmark_jobs j on j.object_key = l.h5_object_key "
                                "where l.compute_node is not null and l.vectorization_ratio_pct is not null "
                                "and $__timeFilter(coalesce(j.measured_at, j.processed_at)) "
                                "and ('${benchmark:raw}' = '%' or coalesce(l.benchmark_name, 'unknown') = '${benchmark:raw}') "
                                "group by l.compute_node order by mean_vectorization_pct desc"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": _bar_chart_field_config("percent"),
                    "options": _bar_chart_options(),
                    "gridPos": {"h": 8, "w": 8, "x": 16, "y": 46},
                },
                {
                    "id": 15,
                    "type": "barchart",
                    "title": "LIKWID CPI by Compute Node",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "datasource": {"uid": datasource_uid, "type": "postgres"},
                            "format": "table",
                            "rawSql": (
                                "select l.compute_node, avg(l.cpi)::double precision as mean_cpi from benchmark_likwid_samples l "
                                "join benchmark_jobs j on j.object_key = l.h5_object_key "
                                "where l.compute_node is not null and l.cpi is not null "
                                "and $__timeFilter(coalesce(j.measured_at, j.processed_at)) "
                                "and ('${benchmark:raw}' = '%' or coalesce(l.benchmark_name, 'unknown') = '${benchmark:raw}') "
                                "group by l.compute_node order by mean_cpi asc"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": _bar_chart_field_config("none"),
                    "options": _bar_chart_options(),
                    "gridPos": {"h": 8, "w": 8, "x": 0, "y": 54},
                },
                {
                    "id": 16,
                    "type": "barchart",
                    "title": "LIKWID Cost per GFLOP",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "datasource": {"uid": datasource_uid, "type": "postgres"},
                            "format": "table",
                            "rawSql": (
                                "select l.compute_node, avg(j.total_cost_eur / nullif((l.dp_mflops * l.elapsed_time_s / 1000.0), 0))::double precision as mean_eur_per_gflop from benchmark_likwid_samples l "
                                "join benchmark_jobs j on j.object_key = l.h5_object_key "
                                "where l.compute_node is not null and l.dp_mflops is not null and l.elapsed_time_s is not null and j.total_cost_eur is not null "
                                "and $__timeFilter(coalesce(j.measured_at, j.processed_at)) "
                                "and ('${benchmark:raw}' = '%' or coalesce(l.benchmark_name, 'unknown') = '${benchmark:raw}') "
                                "group by l.compute_node order by mean_eur_per_gflop asc"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": _bar_chart_field_config("currencyEUR"),
                    "options": _bar_chart_options(),
                    "gridPos": {"h": 8, "w": 8, "x": 8, "y": 54},
                },
                {
                    "id": 17,
                    "type": "table",
                    "title": "LIKWID Node Efficiency Summary",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "datasource": {"uid": datasource_uid, "type": "postgres"},
                            "format": "table",
                            "rawSql": (
                                "select coalesce(l.benchmark_name, 'unknown') as benchmark_name, l.compute_node, "
                                "count(distinct l.job_id)::bigint as jobs, "
                                "avg(l.dp_mflops)::double precision as mean_dp_mflops, "
                                "avg(l.dp_mflops / nullif(j.mean_power_w, 0))::double precision as mean_mflops_per_watt, "
                                "avg(l.vectorization_ratio_pct)::double precision as mean_vectorization_pct, "
                                "avg(l.cpi)::double precision as mean_cpi, "
                                "avg(l.clock_mhz)::double precision as mean_clock_mhz, "
                                "avg(j.total_cost_eur / nullif((l.dp_mflops * l.elapsed_time_s / 1000.0), 0))::double precision as mean_eur_per_gflop "
                                "from benchmark_likwid_samples l join benchmark_jobs j on j.object_key = l.h5_object_key "
                                "where l.compute_node is not null "
                                "and $__timeFilter(coalesce(j.measured_at, j.processed_at)) "
                                "and ('${benchmark:raw}' = '%' or coalesce(l.benchmark_name, 'unknown') = '${benchmark:raw}') "
                                "group by coalesce(l.benchmark_name, 'unknown'), l.compute_node "
                                "order by benchmark_name, mean_mflops_per_watt desc nulls last"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": {"defaults": {"custom": {"align": "auto", "cellOptions": {"type": "auto"}}}, "overrides": []},
                    "options": {"showHeader": True},
                    "gridPos": {"h": 8, "w": 8, "x": 16, "y": 54},
                },
            ],
        }
        self._request(
            "POST",
            "/api/dashboards/db",
            org_id=org_id,
            json={"dashboard": dashboard, "overwrite": True},
        )

    def provision_user(self, *, user_id: str, username: str, password: str | None = None) -> bool:
        """Provision Grafana resources for a backend user.

        Returns False when Grafana admin credentials are not configured.
        """
        if not self.enabled:
            return False
        workspace = get_or_create_user_workspace(user_id)
        self.ensure_postgres_role(user_id=user_id, workspace=workspace)
        org_id = self.ensure_org(username)
        set_user_workspace_grafana_org(user_id, org_id)
        grafana_user_id = self.ensure_user(username, password=password)
        self.ensure_user_membership(username=username, grafana_user_id=grafana_user_id, org_id=org_id)
        self.select_user_org(grafana_user_id=grafana_user_id, org_id=org_id)
        self.remove_main_org_membership(grafana_user_id=grafana_user_id, org_id=org_id)
        self.ensure_postgres_datasource(user_id=user_id, username=username, org_id=org_id)
        self.ensure_dashboard(user_id=user_id, org_id=org_id)
        return True

    def ensure_postgres_role(self, *, user_id: str, workspace: dict[str, Any]) -> None:
        """Create the per-user Postgres role used by Grafana RLS."""
        if not self.settings.postgres_admin_password:
            return
        role = workspace["postgres_role"]
        password = workspace["postgres_password"].replace("'", "''")
        safe_role = '"' + role.replace('"', '""') + '"'
        user_literal = user_id.replace("'", "''")
        with psycopg.connect(
            host=self.settings.postgres_host,
            port=self.settings.postgres_port,
            dbname=self.settings.postgres_db,
            user=self.settings.postgres_admin_user,
            password=self.settings.postgres_admin_password,
            autocommit=True,
        ) as connection:
            exists = connection.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)).fetchone()
            if exists:
                connection.execute(f"ALTER ROLE {safe_role} WITH PASSWORD '{password}'")
            else:
                connection.execute(f"CREATE ROLE {safe_role} LOGIN PASSWORD '{password}'")
            connection.execute(f"ALTER ROLE {safe_role} SET app.user_id = '{user_literal}'")
            connection.execute(f"GRANT USAGE ON SCHEMA public TO {safe_role}")
            for table_name in (
                "benchmark_jobs",
                "benchmark_samples",
                "benchmark_likwid_samples",
            ):
                if connection.execute("SELECT to_regclass(%s)", (f"public.{table_name}",)).fetchone()[0]:
                    connection.execute(f"GRANT SELECT ON public.{table_name} TO {safe_role}")
