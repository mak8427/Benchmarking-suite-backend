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
                            "rawSql": "select count(distinct job_id)::bigint as processed_jobs from benchmark_jobs",
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": {"defaults": {"unit": "short"}, "overrides": []},
                    "gridPos": {"h": 4, "w": 8, "x": 0, "y": 0},
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
                            "rawSql": "select coalesce(sum(total_energy_j), 0)::double precision as total_energy_j from (select job_id, max(total_energy_j) as total_energy_j from benchmark_jobs group by job_id) jobs",
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": {"defaults": {"unit": "joule"}, "overrides": []},
                    "gridPos": {"h": 4, "w": 8, "x": 8, "y": 0},
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
                            "rawSql": "select coalesce(sum(max_elapsed_time_s), 0)::double precision as cluster_time_s from (select job_id, max(max_elapsed_time_s) as max_elapsed_time_s from benchmark_jobs group by job_id) jobs",
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": {"defaults": {"unit": "s"}, "overrides": []},
                    "gridPos": {"h": 4, "w": 8, "x": 16, "y": 0},
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
                                "select max(processed_at) as time, job_id, "
                                "max(benchmark_name) as benchmark_name, max(compute_node) as compute_node, "
                                "string_agg(original_filename, ', ' order by original_filename) as files, "
                                "max(sample_count) as sample_count, max(max_power_w) as max_power_w, "
                                "(max(total_energy_j) / nullif(max(max_elapsed_time_s), 0))::double precision as mean_power_w, "
                                "max(total_energy_j) as total_energy_j, max(max_elapsed_time_s) as max_elapsed_time_s "
                                "from benchmark_jobs group by job_id order by max(processed_at) desc"
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
                                "group by 1, 2 order by 1"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": {"defaults": {"unit": "watt"}, "overrides": []},
                    "gridPos": {"h": 9, "w": 24, "x": 0, "y": 12},
                },
                {
                    "id": 6,
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
                                "group by 1, 2 order by 1"
                            ),
                            "rawQuery": True,
                        }
                    ],
                    "fieldConfig": {"defaults": {"unit": "joule"}, "overrides": []},
                    "gridPos": {"h": 9, "w": 24, "x": 0, "y": 21},
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
            for table_name in ("benchmark_jobs", "benchmark_samples"):
                if connection.execute("SELECT to_regclass(%s)", (f"public.{table_name}",)).fetchone()[0]:
                    connection.execute(f"GRANT SELECT ON public.{table_name} TO {safe_role}")
