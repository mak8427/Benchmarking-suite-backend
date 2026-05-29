"""Grafana provisioning helpers for backend users.

The backend remains the source of identity. Grafana is provisioned with a user,
an isolated organization, a Postgres datasource, and a starter dashboard. Users
should reach Grafana through auth-proxy headers rather than managing a second
Grafana password.
"""

from __future__ import annotations

import os
import secrets
from dataclasses import dataclass
from typing import Any

import duckdb
import httpx

from api_backend.db import get_or_create_user_workspace, set_user_workspace_grafana_org


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

    def ensure_user(self, username: str) -> int:
        """Return a Grafana user id, creating it when missing."""
        response = self.client.get(
            f"{self.settings.url}/api/users/lookup?loginOrEmail={username}",
            auth=(self.settings.admin_user or "", self.settings.admin_password or ""),
            timeout=15.0,
        )
        if response.status_code == 200:
            return int(response.json()["id"])
        response = self._request(
            "POST",
            "/api/admin/users",
            json={
                "name": username,
                "email": f"{username}@benchmarking-suite.local",
                "login": username,
                "password": secrets.token_urlsafe(32),
            },
        )
        return int(response.json()["id"])

    def ensure_user_membership(self, *, username: str, org_id: int) -> None:
        """Add a Grafana user to their own org as Viewer."""
        response = self.client.post(
            f"{self.settings.url}/api/orgs/{org_id}/users",
            auth=(self.settings.admin_user or "", self.settings.admin_password or ""),
            json={"loginOrEmail": username, "role": "Viewer"},
            timeout=15.0,
        )
        if response.status_code not in {200, 409}:
            response.raise_for_status()

    def ensure_postgres_datasource(self, *, user_id: str, username: str, org_id: int) -> None:
        """Create or update the user's Grafana Postgres datasource."""
        workspace = get_or_create_user_workspace(user_id)
        datasource = {
            "name": "My Benchmark Data",
            "uid": f"bench-postgres-{user_id}",
            "type": "postgres",
            "access": "proxy",
            "url": f"{self.settings.postgres_host}:{self.settings.postgres_port}",
            "database": self.settings.postgres_db,
            "user": workspace["postgres_role"],
            "isDefault": True,
            "jsonData": {"sslmode": "disable", "postgresVersion": 1600, "timescaledb": False},
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
        datasource_uid = f"bench-postgres-{user_id}"
        dashboard = {
            "uid": "my-benchmark-data",
            "title": "My Benchmark Data",
            "schemaVersion": 39,
            "version": 1,
            "panels": [
                {
                    "id": 1,
                    "type": "table",
                    "title": "Processed Jobs",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "format": "table",
                            "rawSql": (
                                "select original_filename, sample_count, max_power_w, total_energy_j, "
                                "max_elapsed_time_s, processed_at from benchmark_jobs order by processed_at desc"
                            ),
                        }
                    ],
                    "gridPos": {"h": 10, "w": 24, "x": 0, "y": 0},
                },
                {
                    "id": 2,
                    "type": "timeseries",
                    "title": "Node Power",
                    "datasource": {"uid": datasource_uid, "type": "postgres"},
                    "targets": [
                        {
                            "refId": "A",
                            "format": "time_series",
                            "rawSql": (
                                "select to_timestamp(epoch_time) as time, node_power as value "
                                "from benchmark_samples where epoch_time is not null order by epoch_time"
                            ),
                        }
                    ],
                    "gridPos": {"h": 10, "w": 24, "x": 0, "y": 10},
                },
            ],
        }
        self._request(
            "POST",
            "/api/dashboards/db",
            org_id=org_id,
            json={"dashboard": dashboard, "overwrite": True},
        )

    def provision_user(self, *, user_id: str, username: str) -> bool:
        """Provision Grafana resources for a backend user.

        Returns False when Grafana admin credentials are not configured.
        """
        if not self.enabled:
            return False
        workspace = get_or_create_user_workspace(user_id)
        self.ensure_postgres_role(user_id=user_id, workspace=workspace)
        org_id = self.ensure_org(username)
        set_user_workspace_grafana_org(user_id, org_id)
        self.ensure_user(username)
        self.ensure_user_membership(username=username, org_id=org_id)
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
        conn_str = (
            f"host={self.settings.postgres_host} port={self.settings.postgres_port} "
            f"dbname={self.settings.postgres_db} user={self.settings.postgres_admin_user} "
            f"password={self.settings.postgres_admin_password}"
        ).replace("'", "''")
        con = duckdb.connect()
        con.execute("INSTALL postgres;")
        con.execute("LOAD postgres;")
        con.execute(f"ATTACH '{conn_str}' AS pg (TYPE postgres);")
        con.execute(
            f"""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
                    CREATE ROLE {safe_role} LOGIN PASSWORD '{password}';
                ELSE
                    ALTER ROLE {safe_role} WITH PASSWORD '{password}';
                END IF;
                ALTER ROLE {safe_role} SET app.user_id = '{user_literal}';
                GRANT USAGE ON SCHEMA public TO {safe_role};
                IF to_regclass('public.benchmark_jobs') IS NOT NULL THEN
                    GRANT SELECT ON public.benchmark_jobs TO {safe_role};
                END IF;
                IF to_regclass('public.benchmark_samples') IS NOT NULL THEN
                    GRANT SELECT ON public.benchmark_samples TO {safe_role};
                END IF;
            END $$;
            """
        )
