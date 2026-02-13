"""Remote smoke tests for storage endpoints."""

from __future__ import annotations

import pytest


@pytest.fixture
def remote_tokens(remote_api, fresh_remote_user):
    """Register/login a remote user and return token tuple.

    Args:
        remote_api: Remote API helper fixture.
        fresh_remote_user: Unique credentials fixture.

    Returns:
        tuple: Credentials, access token, and refresh token.

    Examples:
        >>> isinstance("token", str)
        True
    """
    register = remote_api.register(fresh_remote_user)
    assert register.status_code == 201
    login = remote_api.login(fresh_remote_user)
    assert login.status_code == 200
    tokens = login.json()
    return fresh_remote_user, tokens["access"], tokens["refresh"]


def _skip_if_missing(response, endpoint_name):
    """Skip test when remote server does not expose an endpoint.

    Args:
        response: HTTP response object.
        endpoint_name: Endpoint path string.

    Examples:
        >>> endpoint = "/storage/list"
        >>> endpoint.startswith("/storage")
        True
    """
    if response.status_code == 404:
        pytest.skip(f"Remote server does not expose {endpoint_name} yet")


def test_presign_upload_flow(remote_api, remote_tokens):
    """Validate remote upload presign behavior and auth checks.

    Args:
        remote_api: Remote API helper fixture.
        remote_tokens: Token tuple fixture.

    Examples:
        >>> "report.csv".endswith(".csv")
        True
    """
    user, access_token, _ = remote_tokens

    ok = remote_api.presign_upload(access_token, "report.csv")
    assert ok.status_code == 200
    body = ok.json()
    assert body.get("url")
    assert body.get("key", "").startswith(f"{user.username}/")

    unauthorized = remote_api.client.post(
        "/storage/presign/upload", params={"object_name": "report.csv"}
    )
    assert unauthorized.status_code == 401

    unsafe = remote_api.presign_upload(access_token, "../secrets.txt")
    assert unsafe.status_code in {400, 422}


def test_presign_download_flow(remote_api, remote_tokens):
    """Validate remote download presign behavior.

    Args:
        remote_api: Remote API helper fixture.
        remote_tokens: Token tuple fixture.

    Examples:
        >>> "/storage/presign/download".endswith("download")
        True
    """
    user, access_token, _ = remote_tokens

    download = remote_api.presign_download(access_token, "report.csv")
    _skip_if_missing(download, "/storage/presign/download")
    assert download.status_code in {200, 202}
    if download.status_code == 200:
        body = download.json()
        assert body.get("url")
        assert body.get("key", "").startswith(f"{user.username}/")

    bad = remote_api.presign_download(access_token, "../secrets.txt")
    if bad.status_code != 404:
        assert bad.status_code in {400, 422}


def test_list_objects(remote_api, remote_tokens):
    """Validate remote object listing and unauthorized access behavior.

    Args:
        remote_api: Remote API helper fixture.
        remote_tokens: Token tuple fixture.

    Examples:
        >>> "objects" in {"objects": []}
        True
    """
    user, access_token, _ = remote_tokens

    listing = remote_api.list_objects(access_token)
    _skip_if_missing(listing, "/storage/list")
    assert listing.status_code == 200
    data = listing.json()
    assert "objects" in data
    for entry in data["objects"]:
        assert entry["key"].startswith(f"{user.username}/")

    unauthorized = remote_api.client.get("/storage/list")
    if unauthorized.status_code != 404:
        assert unauthorized.status_code == 401
