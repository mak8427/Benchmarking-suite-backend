"""Authentication endpoint integration tests."""

from __future__ import annotations

import time

import pytest

from api_backend.db import create_refresh_token, get_user_by_username


async def register(
    client,
    username: str = "user123",
    password: str = "strongPASS1",
):
    """Register a user through the API.

    Args:
        client: Async HTTP client fixture.
        username (str): Username to register.
        password (str): Password to register.

    Returns:
        Response: HTTP response object.

    Examples:
        >>> "/auth/register".startswith("/auth")
        True
    """
    return await client.post(
        "/auth/register",
        json={"username": username, "password": password},
    )


async def login(
    client,
    username: str = "user123",
    password: str = "strongPASS1",
):
    """Authenticate a user through the API.

    Args:
        client: Async HTTP client fixture.
        username (str): Username to authenticate.
        password (str): Password to authenticate.

    Returns:
        Response: HTTP response object.

    Examples:
        >>> "u" in {"u": "user", "p": "pass"}
        True
    """
    return await client.post(
        "/auth/login",
        params={"u": username, "p": password},
    )


@pytest.mark.anyio
async def test_register_creates_user_and_tokens(client) -> None:
    """Verify register returns a token pair.

    Args:
        client: Async HTTP client fixture.

    Examples:
        >>> all(name in "access,refresh" for name in ["access", "refresh"])
        True
    """
    response = await register(client)
    assert response.status_code == 201
    data = response.json()
    assert data["access"]
    assert data["refresh"]


@pytest.mark.anyio
async def test_register_rejects_duplicates(client) -> None:
    """Verify duplicate usernames are rejected.

    Args:
        client: Async HTTP client fixture.

    Examples:
        >>> 409 == 409
        True
    """
    first = await register(client)
    assert first.status_code == 201
    duplicate = await register(client)
    assert duplicate.status_code == 409


@pytest.mark.anyio
async def test_login_returns_tokens_for_valid_credentials(client) -> None:
    """Verify login returns a token pair for valid credentials.

    Args:
        client: Async HTTP client fixture.

    Examples:
        >>> "login" in "/auth/login"
        True
    """
    await register(client)
    response = await login(client)
    assert response.status_code == 200
    body = response.json()
    assert body["access"]
    assert body["refresh"]


@pytest.mark.anyio
async def test_login_rejects_unknown_user(client) -> None:
    """Verify login fails for unknown usernames.

    Args:
        client: Async HTTP client fixture.

    Examples:
        >>> 401 in {400, 401, 403}
        True
    """
    response = await login(client, username="nobody", password="nope1234")
    assert response.status_code == 401


@pytest.mark.anyio
async def test_login_rejects_bad_password(client) -> None:
    """Verify login fails when password is incorrect.

    Args:
        client: Async HTTP client fixture.

    Examples:
        >>> "wrong" != "correct"
        True
    """
    await register(client)
    response = await login(client, password="wrongpass1")
    assert response.status_code == 401


@pytest.mark.anyio
async def test_refresh_issues_new_tokens(client) -> None:
    """Verify refresh rotates refresh tokens.

    Args:
        client: Async HTTP client fixture.

    Examples:
        >>> "rid" in "rid"
        True
    """
    register_response = await register(client)
    refresh_token = register_response.json()["refresh"]
    response = await client.post("/auth/refresh", params={"rid": refresh_token})
    assert response.status_code == 200
    body = response.json()
    assert body["refresh"] != refresh_token
    assert body["access"]


@pytest.mark.anyio
async def test_refresh_rejects_unknown_token(client) -> None:
    """Verify refresh rejects unknown token identifiers.

    Args:
        client: Async HTTP client fixture.

    Examples:
        >>> "does-not-exist".count("-")
        2
    """
    response = await client.post("/auth/refresh", params={"rid": "does-not-exist"})
    assert response.status_code == 401


@pytest.mark.anyio
async def test_refresh_rejects_expired_token(client) -> None:
    """Verify refresh rejects expired token records.

    Args:
        client: Async HTTP client fixture.

    Examples:
        >>> int(time.time()) > 0
        True
    """
    await register(client, username="expired1")
    user = get_user_by_username("expired1")
    assert user is not None
    create_refresh_token("expired-token", user["id"], time.time() - 10)
    response = await client.post("/auth/refresh", params={"rid": "expired-token"})
    assert response.status_code == 401


@pytest.mark.anyio
async def test_legacy_password_route_maps_to_login(client) -> None:
    """Verify legacy auth route remains available during migration.

    Args:
        client: Async HTTP client fixture.

    Examples:
        >>> "/auth/password".endswith("password")
        True
    """
    await register(client, username="legacy1")
    response = await client.post("/auth/password", params={"u": "legacy1", "p": "strongPASS1"})
    assert response.status_code == 200
    assert response.headers.get("Deprecation") == "true"
