from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import main


class StubMinio:
    def __init__(self):
        self.put_requests = []
        self.get_requests = []
        self.list_calls = []
        self.put_exception = None
        self.get_exception = None
        self.list_exception = None
        self.list_objects_response = []

    def presigned_put_object(self, bucket, key, expires):
        if self.put_exception:
            raise self.put_exception
        self.put_requests.append((bucket, key, expires))
        return "https://example.com/upload"

    def presigned_get_object(self, bucket, key, expires):
        if self.get_exception:
            raise self.get_exception
        self.get_requests.append((bucket, key, expires))
        return "https://example.com/download"

    def list_objects(self, bucket, prefix, recursive):
        if self.list_exception:
            raise self.list_exception
        self.list_calls.append((bucket, prefix, recursive))
        for entry in self.list_objects_response:
            yield entry


@pytest.fixture
def storage_clients():
    public = StubMinio()
    admin = StubMinio()
    main.app.dependency_overrides[main.get_public_minio_client] = lambda: public
    main.app.dependency_overrides[main.get_admin_minio_client] = lambda: admin
    yield public, admin
    main.app.dependency_overrides.pop(main.get_public_minio_client, None)
    main.app.dependency_overrides.pop(main.get_admin_minio_client, None)


def register_and_get_token(client):
    response = client.post(
        "/auth/register",
        json={"username": "storageuser", "password": "storagestr0ng"},
    )
    assert response.status_code == 201
    payload = response.json()
    return payload["access"], payload["refresh"], "storageuser"


def test_presign_upload_returns_url(client, storage_clients):
    public_client, _ = storage_clients
    access, _, username = register_and_get_token(client)

    response = client.post(
        "/storage/presign/upload",
        params={"object_name": "report.csv"},
        headers={"Authorization": f"Bearer {access}"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["url"] == "https://example.com/upload"
    assert body["key"] == f"{username}/report.csv"
    assert public_client.put_requests[0][0] == main.BUCKET


def test_presign_requires_authentication(client):
    response = client.post(
        "/storage/presign/upload",
        params={"object_name": "report.csv"},
    )
    assert response.status_code == 401


def test_presign_rejects_unsafe_names(client, storage_clients):
    access, _, _ = register_and_get_token(client)
    response = client.post(
        "/storage/presign/upload",
        params={"object_name": "../secrets.txt"},
        headers={"Authorization": f"Bearer {access}"},
    )
    assert response.status_code == 400


def test_presign_propagates_storage_errors(client, storage_clients):
    public_client, _ = storage_clients
    access, _, _ = register_and_get_token(client)
    public_client.put_exception = RuntimeError("minio down")

    response = client.post(
        "/storage/presign/upload",
        params={"object_name": "report.csv"},
        headers={"Authorization": f"Bearer {access}"},
    )

    assert response.status_code == 400


def test_presign_download_returns_url(client, storage_clients):
    public_client, _ = storage_clients
    access, _, username = register_and_get_token(client)

    response = client.get(
        "/storage/presign/download",
        params={"object_name": "report.csv"},
        headers={"Authorization": f"Bearer {access}"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["url"] == "https://example.com/download"
    assert data["key"] == f"{username}/report.csv"
    assert public_client.get_requests[0][0] == main.BUCKET


def test_presign_download_handles_errors(client, storage_clients):
    public_client, _ = storage_clients
    access, _, _ = register_and_get_token(client)
    public_client.get_exception = RuntimeError("offline")

    response = client.get(
        "/storage/presign/download",
        params={"object_name": "report.csv"},
        headers={"Authorization": f"Bearer {access}"},
    )

    assert response.status_code == 400


def test_list_objects_returns_user_items(client, storage_clients):
    _, admin_client = storage_clients
    access, _, username = register_and_get_token(client)

    admin_client.list_objects_response = [
        SimpleNamespace(
            object_name=f"{username}/report.csv",
            size=1024,
            last_modified=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ),
        SimpleNamespace(
            object_name=f"{username}/reports/january.csv",
            size=512,
            last_modified=None,
        ),
        SimpleNamespace(
            object_name="otheruser/file.txt",
            size=1,
            last_modified=None,
        ),
    ]

    response = client.get(
        "/storage/list",
        headers={"Authorization": f"Bearer {access}"},
    )

    assert response.status_code == 200
    payload = response.json()
    returned_keys = {item["key"] for item in payload["objects"]}
    assert f"{username}/report.csv" in returned_keys
    assert f"{username}/reports/january.csv" in returned_keys
    assert all(not key.startswith("otheruser/") for key in returned_keys)


def test_list_objects_handles_errors(client, storage_clients):
    _, admin_client = storage_clients
    access, _, _ = register_and_get_token(client)
    admin_client.list_exception = RuntimeError("fail")

    response = client.get(
        "/storage/list",
        headers={"Authorization": f"Bearer {access}"},
    )

    assert response.status_code == 400
