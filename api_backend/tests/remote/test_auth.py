"""Remote smoke tests for authentication endpoints."""

from __future__ import annotations


def test_root_endpoint_returns_health_message(remote_api):
    """Validate remote root endpoint returns the expected payload.

    Args:
        remote_api: Remote API helper fixture.

    Examples:
        >>> "Hello" in "Hello World"
        True
    """
    response = remote_api.root()
    assert response.status_code == 200
    assert response.json() == {"message": "Hello World"}


def test_register_and_duplicate_detection(remote_api, fresh_remote_user):
    """Validate duplicate registration is rejected remotely.

    Args:
        remote_api: Remote API helper fixture.
        fresh_remote_user: Unique credentials fixture.

    Examples:
        >>> 201 != 409
        True
    """
    first = remote_api.register(fresh_remote_user)
    assert first.status_code == 201
    payload = first.json()
    assert payload.get("access")
    assert payload.get("refresh")

    duplicate = remote_api.register(fresh_remote_user)
    assert duplicate.status_code == 409


def test_login_success_and_failure_modes(remote_api, fresh_remote_user):
    """Validate login success and bad-password failure modes.

    Args:
        remote_api: Remote API helper fixture.
        fresh_remote_user: Unique credentials fixture.

    Examples:
        >>> "password" in "bad-password"
        True
    """
    remote_api.register(fresh_remote_user)

    ok = remote_api.login(fresh_remote_user)
    assert ok.status_code == 200
    tokens = ok.json()
    assert tokens.get("access")
    assert tokens.get("refresh")

    bad_credentials = type(fresh_remote_user)(
        username=fresh_remote_user.username,
        password=fresh_remote_user.password + "!",
    )
    bad = remote_api.login(bad_credentials)
    assert bad.status_code == 401


def test_refresh_tokens_behaviour(remote_api, fresh_remote_user):
    """Validate refresh issues a new token and rejects invalid token IDs.

    Args:
        remote_api: Remote API helper fixture.
        fresh_remote_user: Unique credentials fixture.

    Examples:
        >>> "bad-token".startswith("bad")
        True
    """
    register_payload = remote_api.register(fresh_remote_user).json()
    refresh_token = register_payload["refresh"]

    refreshed = remote_api.refresh(refresh_token)
    assert refreshed.status_code == 200
    data = refreshed.json()
    assert data.get("access")
    assert data.get("refresh") != refresh_token

    invalid = remote_api.refresh("bad-token")
    assert invalid.status_code == 401
