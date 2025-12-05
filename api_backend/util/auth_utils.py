"""Utilities for authentication and request sanitisation."""

from __future__ import annotations

import os
import re
from typing import Annotated, Dict, Optional

import jwt
from fastapi import HTTPException, Header, status
from jwt import PyJWTError

AuthPayload = Dict[str, str]

# NOTE: replace placeholder secret during deployment.
SECRET = os.getenv("JWT_SECRET", "...").encode("utf-8")


def current_user(
    authorization: Annotated[Optional[str], Header(alias="Authorization")] = None
) -> AuthPayload:
    """Validate the Authorization header and return the associated identity.

    Args:
        authorization: Authorization header value (e.g. ``Bearer <token>``).

    Returns:
        A dictionary containing the extracted username.

    Raises:
        HTTPException: Raised if the header is missing or the token is invalid.
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
        payload = jwt.decode(token, SECRET, algorithms=["HS256"])
    except PyJWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token.",
        ) from exc

    username = payload.get("sub")
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing subject.",
        )
    return {"username": username}


def sanitize(name: str) -> str:
    """Sanitise user-supplied object names for safe storage usage.

    Args:
        name: Raw object name supplied by the user.

    Returns:
        A safe variant of the supplied name.

    Raises:
        HTTPException: Raised if the resulting name is empty or unsafe.
    """
    safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    if not safe_name or ".." in safe_name or safe_name.startswith("/"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid object name.",
        )
    return safe_name
