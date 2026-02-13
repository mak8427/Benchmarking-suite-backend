"""Unit tests for auth utility helpers."""

from __future__ import annotations

import time

import jwt
import pytest
from fastapi import HTTPException

from api_backend.util.auth_utils import decode_user, get_jwt_secret, sanitize


def test_sanitize_allows_safe_names() -> None:
    """Ensure safe names remain unchanged after sanitization.

    The sanitizer should preserve already-valid filenames.

    Examples:
        >>> sanitize("report.csv")
        'report.csv'
    """
    assert sanitize("report.csv") == "report.csv"
    assert sanitize("folder-1.data") == "folder-1.data"


def test_sanitize_rejects_or_normalizes_bad_names() -> None:
    """Ensure unsafe names are rejected or normalized.

    Path traversal input must raise and invalid separators are normalized.

    Examples:
        >>> sanitize("semi;colon.txt")
        'semi_colon.txt'
    """
    with pytest.raises(HTTPException):
        sanitize("../escape.txt")
    with pytest.raises(HTTPException):
        sanitize("")
    assert sanitize("invalid name.txt") == "invalid_name.txt"
    assert sanitize("semi;colon.txt") == "semi_colon.txt"


def test_current_user_requires_header() -> None:
    """Ensure auth decoding fails when header is missing.

    Missing Authorization headers are always unauthorized.

    Examples:
        >>> decode_user.__name__
        'decode_user'
    """
    with pytest.raises(HTTPException):
        decode_user(None)


def test_current_user_rejects_bad_scheme() -> None:
    """Ensure non-bearer schemes are rejected.

    Tokens with scheme prefixes other than ``Bearer`` must fail.

    Examples:
        >>> jwt.decode(jwt.encode({"sub": "u", "username": "n", "exp": time.time() + 10}, get_jwt_secret(), algorithm="HS256"), get_jwt_secret(), algorithms=["HS256"])["sub"]
        'u'
    """
    token = jwt.encode(
        {"sub": "uid-1", "username": "bob", "exp": time.time() + 60},
        get_jwt_secret(),
        algorithm="HS256",
    )
    with pytest.raises(HTTPException):
        decode_user(f"Token {token}")


def test_current_user_rejects_invalid_token() -> None:
    """Ensure malformed JWT payloads are rejected.

    Random token text should not be accepted as a valid bearer token.

    Examples:
        >>> "invalidtoken".startswith("invalid")
        True
    """
    with pytest.raises(HTTPException):
        decode_user("Bearer invalidtoken")


def test_current_user_decodes_valid_token() -> None:
    """Ensure valid JWT payloads are decoded into user claims.

    The decoder must return both ``user_id`` and ``username`` fields.

    Examples:
        >>> token = jwt.encode({"sub": "uid-1", "username": "bob", "exp": time.time() + 60}, get_jwt_secret(), algorithm="HS256")
        >>> decode_user(f"Bearer {token}") == {"user_id": "uid-1", "username": "bob"}
        True
    """
    token = jwt.encode(
        {"sub": "uid-1", "username": "bob", "exp": time.time() + 60},
        get_jwt_secret(),
        algorithm="HS256",
    )
    user = decode_user(f"Bearer {token}")
    assert user == {"user_id": "uid-1", "username": "bob"}
