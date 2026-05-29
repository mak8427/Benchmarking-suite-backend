"""Tests for backend-backed Grafana auth proxy sessions."""

from __future__ import annotations

import pytest

from api_backend import main

pytestmark = pytest.mark.anyio


async def _register(client, username="grafanauser", password="GrafanaPass123"):
    response = await client.post("/auth/register", json={"username": username, "password": password})
    assert response.status_code == 201
    return username, password


async def test_grafana_login_sets_session_cookie(client) -> None:
    """Backend credentials should produce a Grafana auth-proxy session."""
    username, password = await _register(client)

    response = await client.post(
        "/grafana-auth/login",
        content=f"username={username}&password={password}",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/grafana/"
    assert main.GRAFANA_SESSION_COOKIE in response.cookies


async def test_grafana_verify_emits_auth_proxy_headers(client) -> None:
    """A valid session cookie should become Grafana auth proxy headers."""
    username, password = await _register(client)
    login = await client.post(
        "/grafana-auth/login",
        content=f"username={username}&password={password}",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        follow_redirects=False,
    )

    response = await client.get(
        "/grafana-auth/verify",
        cookies={main.GRAFANA_SESSION_COOKIE: login.cookies[main.GRAFANA_SESSION_COOKIE]},
    )

    assert response.status_code == 204
    assert response.headers["x-webauth-user"] == username


async def test_grafana_verify_rejects_missing_cookie(client) -> None:
    """Anonymous Grafana requests should receive the backend login page."""
    response = await client.get("/grafana-auth/verify", follow_redirects=False)
    assert response.status_code == 401
    assert "Benchmarking Suite Login" in response.text
