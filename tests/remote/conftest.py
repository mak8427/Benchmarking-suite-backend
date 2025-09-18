import json
import os
import secrets
from dataclasses import dataclass
from pathlib import Path

import pytest

httpx = pytest.importorskip("httpx")


def _discover_base_url() -> str | None:
    env_value = os.getenv("REMOTE_BASE_URL")
    if env_value:
        return env_value

    for candidate in (
        Path("http-client.private.env.json"),
        Path("http-client.env.json"),
    ):
        if not candidate.exists():
            continue
        try:
            with candidate.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
            raw = data.get("remote", {}).get("baseUrl") or data.get("remote", {}).get("base_url")
            base_url = raw.strip() if isinstance(raw, str) else None
            if base_url:
                return base_url
        except (json.JSONDecodeError, OSError):
            continue

    return None


_discovered_url = _discover_base_url()
DEFAULT_BASE_URL = "http://localhost:7800"
BASE_URL = _discovered_url or DEFAULT_BASE_URL
USING_DEFAULT_BASE = _discovered_url is None


@dataclass
class RemoteCredentials:
    username: str
    password: str


class RemoteAPI:
    def __init__(self, client: httpx.Client):
        self._client = client

    @property
    def client(self) -> httpx.Client:
        return self._client

    def root(self):
        return self._client.get("/")

    def register(self, creds: RemoteCredentials):
        payload = {"username": creds.username, "password": creds.password}
        return self._client.post("/auth/register", json=payload)

    def login(self, creds: RemoteCredentials):
        params = {"u": creds.username, "p": creds.password}
        return self._client.post("/auth/password", params=params)

    def refresh(self, refresh_token: str):
        return self._client.post("/auth/refresh", params={"rid": refresh_token})

    def presign_upload(self, token: str, object_name: str):
        headers = {"Authorization": f"Bearer {token}"}
        return self._client.post(
            "/storage/presign/upload", params={"object_name": object_name}, headers=headers
        )

    def presign_download(self, token: str, object_name: str):
        headers = {"Authorization": f"Bearer {token}"}
        return self._client.get(
            "/storage/presign/download", params={"object_name": object_name}, headers=headers
        )

    def list_objects(self, token: str):
        headers = {"Authorization": f"Bearer {token}"}
        return self._client.get("/storage/list", headers=headers)


@pytest.fixture(scope="session")
def remote_client():
    session = httpx.Client(base_url=BASE_URL, timeout=10.0, follow_redirects=True)
    try:
        response = session.get("/")
        response.raise_for_status()
    except Exception as exc:  # pragma: no cover - executed only when offline
        session.close()
        hint = "Set REMOTE_BASE_URL or update http-client.private.env.json" if USING_DEFAULT_BASE else "Remote server unreachable"
        pytest.skip(f"Remote server {BASE_URL} unavailable: {exc}. {hint}")
    yield session
    session.close()


@pytest.fixture
def remote_api(remote_client):
    return RemoteAPI(remote_client)


@pytest.fixture
def fresh_remote_user():
    suffix = secrets.token_hex(4)
    return RemoteCredentials(
        username=f"pytest_{suffix}",
        password=f"Pw{suffix}!A",
    )
