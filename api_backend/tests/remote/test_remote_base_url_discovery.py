"""Tests for remote base URL discovery across env and config locations."""

from __future__ import annotations

import json
from pathlib import Path

from api_backend.tests.remote import conftest


def test_discover_base_url_returns_none_without_env_or_files(monkeypatch, tmp_path: Path) -> None:
    """Return ``None`` when no environment variable or config file is available.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        tmp_path (Path): Temporary working directory fixture.

    Examples:
        >>> isinstance("http://localhost", str)
        True
    """
    monkeypatch.delenv("REMOTE_BASE_URL", raising=False)
    monkeypatch.chdir(tmp_path)

    result = conftest._discover_base_url()

    assert result is None


def test_discover_base_url_reads_api_backend_private_env(monkeypatch, tmp_path: Path) -> None:
    """Read remote URL from ``api_backend/http-client.private.env.json``.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        tmp_path (Path): Temporary working directory fixture.

    Examples:
        >>> "baseUrl" in '{"baseUrl":"http://example.test"}'
        True
    """
    monkeypatch.delenv("REMOTE_BASE_URL", raising=False)
    monkeypatch.chdir(tmp_path)

    api_backend_dir = tmp_path / "api_backend"
    api_backend_dir.mkdir(parents=True, exist_ok=True)
    payload = {"remote": {"baseUrl": "http://example.test:7800"}}
    (api_backend_dir / "http-client.private.env.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )

    result = conftest._discover_base_url()

    assert result == "http://example.test:7800"


def test_discover_base_url_prefers_environment_variable(monkeypatch, tmp_path: Path) -> None:
    """Prefer ``REMOTE_BASE_URL`` over any file-based configuration.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        tmp_path (Path): Temporary working directory fixture.

    Examples:
        >>> "REMOTE_BASE_URL".startswith("REMOTE")
        True
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("REMOTE_BASE_URL", "http://env.example:9999")

    root_payload = {"remote": {"baseUrl": "http://file.example:7800"}}
    (tmp_path / "http-client.private.env.json").write_text(
        json.dumps(root_payload),
        encoding="utf-8",
    )

    result = conftest._discover_base_url()

    assert result == "http://env.example:9999"
