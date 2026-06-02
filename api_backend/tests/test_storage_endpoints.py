"""Storage endpoint integration tests."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import jwt
import pytest

from api_backend import main
from api_backend.db import get_storage_object
from api_backend.util.auth_utils import get_jwt_secret


class StubMinio:
    """Simple in-memory MinIO stub used for endpoint tests."""

    def __init__(self) -> None:
        """Initialize request history and failure toggles.

        The stub records PUT/GET/list calls and can raise injected exceptions.

        Examples:
            >>> stub = StubMinio()
            >>> stub.put_requests == []
            True
        """
        self.put_requests = []
        self.get_requests = []
        self.list_calls = []
        self.put_exception = None
        self.get_exception = None
        self.list_exception = None
        self.list_objects_response = []

    def presigned_put_object(self, bucket, key, expires, headers=None):
        """Return a stable upload URL and track invocation.

        Args:
            bucket: Bucket name.
            key: Object key.
            expires: Expiry timedelta.

        Examples:
            >>> stub = StubMinio()
            >>> stub.presigned_put_object("b", "k", 1)
            'https://example.com/upload'
        """
        if self.put_exception:
            raise self.put_exception
        self.put_requests.append((bucket, key, expires, headers or {}))
        return "https://example.com/upload"

    def presigned_get_object(self, bucket, key, expires):
        """Return a stable download URL and track invocation.

        Args:
            bucket: Bucket name.
            key: Object key.
            expires: Expiry timedelta.

        Examples:
            >>> stub = StubMinio()
            >>> stub.presigned_get_object("b", "k", 1)
            'https://example.com/download'
        """
        if self.get_exception:
            raise self.get_exception
        self.get_requests.append((bucket, key, expires))
        return "https://example.com/download"

    def list_objects(self, bucket, prefix, recursive):
        """Yield configured object listing data.

        Args:
            bucket: Bucket name.
            prefix: Prefix filter.
            recursive: Recursive listing flag.

        Examples:
            >>> stub = StubMinio()
            >>> list(stub.list_objects("b", "p/", True))
            []
        """
        if self.list_exception:
            raise self.list_exception
        self.list_calls.append((bucket, prefix, recursive))
        for entry in self.list_objects_response:
            yield entry


@pytest.fixture
def storage_clients(monkeypatch):
    """Override MinIO dependencies with in-memory stubs.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Examples:
        >>> isinstance(main.BUCKET, str)
        True
    """
    public = StubMinio()
    admin = StubMinio()
    monkeypatch.setattr(main, "PUBLIC_MINIO", public)
    monkeypatch.setattr(main, "ADMIN_MINIO", admin)
    yield public, admin


async def register_and_get_token(client):
    """Create a user and return access token and user id claim.

    Args:
        client: Async HTTP client fixture.

    Returns:
        tuple[str, str]: Access token and user id claim.

    Examples:
        >>> "HS256" in ["HS256"]
        True
    """
    response = await client.post(
        "/auth/register",
        json={"username": "storageuser", "password": "storagestr0ng"},
    )
    assert response.status_code == 201
    access = response.json()["access"]
    claims = jwt.decode(access, get_jwt_secret(), algorithms=["HS256"])
    return access, claims["sub"]


@pytest.mark.anyio
async def test_presign_upload_returns_url(client, storage_clients) -> None:
    """Verify upload presign returns URL and user-scoped key.

    Args:
        client: Async HTTP client fixture.
        storage_clients: Public and admin MinIO stubs.

    Examples:
        >>> "/files/presign/upload".startswith("/files")
        True
    """
    public_client, _ = storage_clients
    access, user_id = await register_and_get_token(client)

    response = await client.post(
        "/files/presign/upload",
        params={"filename": "report.csv", "benchmark_name": "stream_triad"},
        headers={"Authorization": f"Bearer {access}"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["url"] == "https://example.com/upload"
    assert body["headers"] == {"x-amz-acl": "private"}
    assert body["key"].startswith(f"{user_id}/")
    assert body["key"].endswith("_report.csv")
    assert public_client.put_requests[0][0] == main.BUCKET
    assert public_client.put_requests[0][3] == {"x-amz-acl": "private"}
    metadata = get_storage_object(body["key"])
    assert metadata is not None
    assert metadata["user_id"] == user_id
    assert metadata["original_filename"] == "report.csv"
    assert metadata["benchmark_name"] == "stream_triad"


@pytest.mark.anyio
async def test_presign_requires_authentication(client) -> None:
    """Verify upload presign requires a bearer token.

    Args:
        client: Async HTTP client fixture.

    Examples:
        >>> 401 == 401
        True
    """
    response = await client.post("/files/presign/upload", params={"filename": "report.csv"})
    assert response.status_code == 401


@pytest.mark.anyio
async def test_presign_rejects_unsafe_names(client, storage_clients) -> None:
    """Verify upload presign rejects unsafe filenames.

    Args:
        client: Async HTTP client fixture.
        storage_clients: Public and admin MinIO stubs.

    Examples:
        >>> "../x".startswith("..")
        True
    """
    access, _ = await register_and_get_token(client)
    response = await client.post(
        "/files/presign/upload",
        params={"filename": "../secrets.txt"},
        headers={"Authorization": f"Bearer {access}"},
    )
    assert response.status_code == 400


@pytest.mark.anyio
async def test_presign_propagates_storage_errors(client, storage_clients) -> None:
    """Verify upload presign maps MinIO failures to HTTP 400.

    Args:
        client: Async HTTP client fixture.
        storage_clients: Public and admin MinIO stubs.

    Examples:
        >>> "minio down".split()[0]
        'minio'
    """
    public_client, _ = storage_clients
    access, _ = await register_and_get_token(client)
    public_client.put_exception = RuntimeError("minio down")

    response = await client.post(
        "/files/presign/upload",
        params={"filename": "report.csv"},
        headers={"Authorization": f"Bearer {access}"},
    )

    assert response.status_code == 400


@pytest.mark.anyio
async def test_presign_reports_object_quota(client, storage_clients, monkeypatch) -> None:
    """Quota failures should expose enough detail for CLI diagnostics."""
    _, admin_client = storage_clients
    monkeypatch.setattr(main, "MAX_OBJECTS_PER_USER", 2)
    admin_client.list_objects_response = [SimpleNamespace(size=1), SimpleNamespace(size=1)]
    access, _ = await register_and_get_token(client)

    response = await client.post(
        "/files/presign/upload",
        params={"filename": "report.csv"},
        headers={"Authorization": f"Bearer {access}"},
    )

    assert response.status_code == 413
    assert response.json()["detail"] == {
        "error": "object_quota_exceeded",
        "message": "Object quota exceeded.",
        "used": 2,
        "limit": 2,
        "remaining": 0,
        "unit": "objects",
    }


@pytest.mark.anyio
async def test_presign_reports_storage_quota(client, storage_clients, monkeypatch) -> None:
    """Byte quota failures should include current usage and limit."""
    _, admin_client = storage_clients
    monkeypatch.setattr(main, "MAX_STORAGE_BYTES_PER_USER", 5)
    admin_client.list_objects_response = [SimpleNamespace(size=3), SimpleNamespace(size=2)]
    access, _ = await register_and_get_token(client)

    response = await client.post(
        "/files/presign/upload",
        params={"filename": "report.csv"},
        headers={"Authorization": f"Bearer {access}"},
    )

    assert response.status_code == 413
    assert response.json()["detail"] == {
        "error": "storage_quota_exceeded",
        "message": "Storage quota exceeded.",
        "used": 5,
        "limit": 5,
        "remaining": 0,
        "unit": "bytes",
    }


@pytest.mark.anyio
async def test_presign_download_returns_url(client, storage_clients) -> None:
    """Verify download presign returns URL for user-owned keys.

    Args:
        client: Async HTTP client fixture.
        storage_clients: Public and admin MinIO stubs.

    Examples:
        >>> "/files/presign/download".endswith("download")
        True
    """
    public_client, _ = storage_clients
    access, user_id = await register_and_get_token(client)

    response = await client.get(
        "/files/presign/download",
        params={"key": f"{user_id}/abc_report.csv"},
        headers={"Authorization": f"Bearer {access}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["url"] == "https://example.com/download"
    assert data["key"] == f"{user_id}/abc_report.csv"
    assert public_client.get_requests[0][0] == main.BUCKET


@pytest.mark.anyio
async def test_presign_download_forbidden_other_prefix(client, storage_clients) -> None:
    """Verify download presign rejects keys outside caller scope.

    Args:
        client: Async HTTP client fixture.
        storage_clients: Public and admin MinIO stubs.

    Examples:
        >>> "other-user/report.csv".startswith("other")
        True
    """
    access, _ = await register_and_get_token(client)
    response = await client.get(
        "/files/presign/download",
        params={"key": "other-user/report.csv"},
        headers={"Authorization": f"Bearer {access}"},
    )
    assert response.status_code == 403


@pytest.mark.anyio
async def test_presign_download_handles_errors(client, storage_clients) -> None:
    """Verify download presign maps MinIO failures to HTTP 400.

    Args:
        client: Async HTTP client fixture.
        storage_clients: Public and admin MinIO stubs.

    Examples:
        >>> "offline".upper()
        'OFFLINE'
    """
    public_client, _ = storage_clients
    access, user_id = await register_and_get_token(client)
    public_client.get_exception = RuntimeError("offline")

    response = await client.get(
        "/files/presign/download",
        params={"key": f"{user_id}/abc_report.csv"},
        headers={"Authorization": f"Bearer {access}"},
    )

    assert response.status_code == 400


@pytest.mark.anyio
async def test_list_objects_returns_user_items(client, storage_clients) -> None:
    """Verify object listing only returns user-scoped objects.

    Args:
        client: Async HTTP client fixture.
        storage_clients: Public and admin MinIO stubs.

    Examples:
        >>> datetime(2024, 1, 1, tzinfo=timezone.utc).year
        2024
    """
    _, admin_client = storage_clients
    access, user_id = await register_and_get_token(client)

    admin_client.list_objects_response = [
        SimpleNamespace(
            object_name=f"{user_id}/report.csv",
            size=1024,
            last_modified=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ),
        SimpleNamespace(
            object_name=f"{user_id}/reports/january.csv",
            size=512,
            last_modified=None,
        ),
        SimpleNamespace(
            object_name="otheruser/file.txt",
            size=1,
            last_modified=None,
        ),
    ]

    response = await client.get("/files/list", headers={"Authorization": f"Bearer {access}"})

    assert response.status_code == 200
    payload = response.json()
    returned_keys = {item["key"] for item in payload["objects"]}
    assert f"{user_id}/report.csv" in returned_keys
    assert f"{user_id}/reports/january.csv" in returned_keys
    assert all(not key.startswith("otheruser/") for key in returned_keys)


@pytest.mark.anyio
async def test_list_objects_handles_errors(client, storage_clients) -> None:
    """Verify object listing maps MinIO failures to HTTP 400.

    Args:
        client: Async HTTP client fixture.
        storage_clients: Public and admin MinIO stubs.

    Examples:
        >>> "fail" in "fail"
        True
    """
    _, admin_client = storage_clients
    access, _ = await register_and_get_token(client)
    admin_client.list_exception = RuntimeError("fail")

    response = await client.get("/files/list", headers={"Authorization": f"Bearer {access}"})
    assert response.status_code == 400


@pytest.mark.anyio
async def test_legacy_storage_routes_are_supported(client, storage_clients) -> None:
    """Verify legacy storage routes remain available during transition.

    Args:
        client: Async HTTP client fixture.
        storage_clients: Public and admin MinIO stubs.

    Examples:
        >>> "/storage/list".startswith("/storage")
        True
    """
    access, _ = await register_and_get_token(client)

    upload = await client.post(
        "/storage/presign/upload",
        params={"object_name": "report.csv"},
        headers={"Authorization": f"Bearer {access}"},
    )
    assert upload.status_code == 200
    assert upload.headers.get("Deprecation") == "true"

    listing = await client.get("/storage/list", headers={"Authorization": f"Bearer {access}"})
    assert listing.status_code == 200
    assert listing.headers.get("Deprecation") == "true"
