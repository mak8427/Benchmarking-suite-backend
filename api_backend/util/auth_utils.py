"""Authentication and object-key sanitisation helpers."""

from __future__ import annotations

import os
import re
from typing import Annotated

import jwt
from fastapi import HTTPException, Header, status
from jwt import PyJWTError

AuthPayload = dict[str, str]


def get_jwt_secret() -> bytes:
    """Load the JWT secret from environment.

    This helper centralizes secret loading and validation.

    Returns:
        bytes: UTF-8 encoded secret.

    Raises:
        RuntimeError: Raised when `JWT_SECRET` is missing.

    Examples:
        >>> os.environ["JWT_SECRET"] = "abc"
        >>> get_jwt_secret()
        b'abc'
    """
    secret = os.getenv("JWT_SECRET")
    if not secret:
        raise RuntimeError("JWT_SECRET environment variable must be set.")
    return secret.encode("utf-8")


def decode_user(
    authorization: str | None,
) -> AuthPayload:
    """Validate a bearer token and return identity claims.

    Args:
        authorization (str | None): HTTP Authorization header value.

    Returns:
        AuthPayload: Dictionary with `user_id` and `username`.

    Raises:
        HTTPException: Raised when auth header or token is invalid.

    Examples:
        >>> os.environ["JWT_SECRET"] = "jwt-secret"
        >>> token = jwt.encode({"sub": "u1", "username": "alice"}, get_jwt_secret(), algorithm="HS256")
        >>> decode_user(f"Bearer {token}")
        {'user_id': 'u1', 'username': 'alice'}
    """
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header.",
        )
    try:
        scheme, token = authorization.split(" ", 1)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Malformed Authorization header.",
        ) from exc

    if scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unsupported authorization scheme.",
        )

    try:
        payload = jwt.decode(token, get_jwt_secret(), algorithms=["HS256"])
    except (PyJWTError, RuntimeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token.",
        ) from exc

    user_id = payload.get("sub")
    username = payload.get("username")
    if not user_id or not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing required claims.",
        )
    return {"user_id": str(user_id), "username": str(username)}


async def current_user(
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> AuthPayload:
    """FastAPI dependency wrapper for bearer-token validation.

    Args:
        authorization (str | None): HTTP Authorization header value.

    Returns:
        AuthPayload: Dictionary with `user_id` and `username`.

    Examples:
        >>> current_user.__name__
        'current_user'
    """
    return decode_user(authorization)


def sanitize(name: str) -> str:
    """Sanitize user-supplied object names.

    Args:
        name (str): Raw object name supplied by clients.

    Returns:
        str: Sanitized object-safe filename.

    Raises:
        HTTPException: Raised when input is unsafe or empty.

    Examples:
        >>> sanitize("unsafe name.txt")
        'unsafe_name.txt'
    """
    safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", name.strip())
    if not safe_name or ".." in safe_name or safe_name.startswith("/"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid object name.",
        )
    return safe_name
