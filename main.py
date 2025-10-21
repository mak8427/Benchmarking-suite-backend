"""FastAPI application exposing authentication and MinIO-backed storage APIs."""

from __future__ import annotations

import logging
import os
import secrets
import time
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List

import jwt
from fastapi import Depends, FastAPI, HTTPException, Query, status
from passlib.hash import argon2
from pydantic import BaseModel, Field

from storage.minio_client import ADMIN_MINIO, BUCKET, PUBLIC_MINIO
from util.auth_utils import current_user, sanitize

LOG_FILE = Path(os.getenv("LOG_FILE_PATH", "process.log"))
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
LOGGER = logging.getLogger(__name__)
logger = LOGGER

Tokens = Dict[str, Dict[str, float]]
Users = Dict[str, str]

# NOTE: replace placeholder secret during deployment.
SECRET = os.getenv("JWT_SECRET", "...").encode("utf-8")
TOKENS: Tokens = {}
USERS: Users = {"alice": argon2.hash("pass")}
USERS_FILE = Path(os.getenv("USERS_FILE_PATH", "users.txt"))

ACCESS_TOKEN_TTL_SECONDS = 600
REFRESH_TOKEN_TTL_SECONDS = 30 * 86400
PRESIGN_EXPIRATION_MINUTES = 30
PRESIGN_EXPIRATION_SECONDS = 600

app = FastAPI(
    title="File Storage API",
    description=(
        "A secure file storage API with user authentication and MinIO integration."
    ),
    version="1.0.0",
)


class UserCreate(BaseModel):
    """Request model for user registration payloads."""

    username: str = Field(
        ...,
        min_length=5,
        max_length=20,
        description="Unique username for the account.",
        examples=["johndoe123"],
    )
    password: str = Field(
        ...,
        min_length=8,
        max_length=128,
        description="Password associated with the account.",
        examples=["SecurePass123!"],
    )


class PresignResponse(BaseModel):
    """Response payload for presigned URL requests."""

    key: str
    url: str
    expires_in: str


def get_public_minio_client():
    """Return the public MinIO client instance used for file operations."""
    return PUBLIC_MINIO


def get_admin_minio_client():
    """Return the MinIO client with administrative privileges."""
    return ADMIN_MINIO


def _persist_user(username: str, password_hash: str) -> None:
    """Append a user credential entry to the users file.

    Args:
        username: Username to persist.
        password_hash: Hashed password to persist.
    """
    USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with USERS_FILE.open("a", encoding="utf-8") as handle:
        handle.write(f"{username}:{password_hash}\n")


def _load_users_from_disk() -> None:
    """Populate the in-memory user store with credentials from disk."""
    if not USERS_FILE.exists():
        return
    with USERS_FILE.open("r", encoding="utf-8") as handle:
        for line in handle:
            username, hash_value = line.strip().split(":", maxsplit=1)
            USERS[username] = hash_value


def make_access(subject: str) -> str:
    """Create a short-lived access token for the provided subject.

    Args:
        subject: Username embedded in the token.

    Returns:
        Encoded JWT access token.
    """
    LOGGER.debug("Creating access token for user: %s", subject)
    payload = {
        "sub": subject,
        "scope": "upload",
        "exp": time.time() + ACCESS_TOKEN_TTL_SECONDS,
    }
    return jwt.encode(payload, SECRET, algorithm="HS256")


@app.get(
    "/",
    summary="Health Check",
    description="Simple endpoint to verify the API is running.",
)
async def root() -> Dict[str, str]:
    """Return a basic response for service health checks."""
    LOGGER.info("Root endpoint accessed")
    return {"message": "Hello World"}


@app.post(
    "/auth/register",
    status_code=status.HTTP_201_CREATED,
    summary="Register New User",
    description=(
        "Create a new user account. Returns access and refresh tokens upon "
        "successful registration."
    ),
)
async def register(payload: UserCreate) -> Dict[str, str]:
    """Register a new user and return access credentials.

    Args:
        payload: User registration model containing credentials.

    Returns:
        Dictionary holding access and refresh tokens.

    Raises:
        HTTPException: Raised if the username is already registered.
    """
    LOGGER.info("Registration attempt for username: %s", payload.username)
    _load_users_from_disk()

    if payload.username in USERS:
        LOGGER.warning("Registration failed: username %s already exists", payload.username)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Username already registered.",
        )

    password_hash = argon2.hash(payload.password)
    USERS[payload.username] = password_hash
    _persist_user(payload.username, password_hash)

    refresh_id = secrets.token_urlsafe(32)
    TOKENS[refresh_id] = {
        "sub": payload.username,
        "exp": time.time() + REFRESH_TOKEN_TTL_SECONDS,
    }

    LOGGER.info("User %s registered successfully", payload.username)
    return {"access": make_access(payload.username), "refresh": refresh_id}


@app.post(
    "/auth/password",
    summary="User Login",
    description="Authenticate user with username and password.",
)
async def login(
    username: str = Query(
        ...,
        description="Username for authentication.",
        examples=["johndoe123"],
        alias="u",
    ),
    password: str = Query(
        ...,
        description="Password for authentication.",
        examples=["SecurePass123!"],
        alias="p",
    ),
) -> Dict[str, str]:
    """Authenticate a user and issue access and refresh tokens.

    Args:
        username: Username provided for authentication.
        password: Password provided for authentication.

    Returns:
        Dictionary containing access and refresh tokens.

    Raises:
        HTTPException: Raised if the credentials are invalid.
    """
    LOGGER.info("Login attempt for username: %s", username)
    _load_users_from_disk()

    if username not in USERS or not argon2.verify(password, USERS[username]):
        LOGGER.warning("Failed login attempt for username: %s", username)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials.")

    refresh_id = secrets.token_urlsafe(32)
    TOKENS[refresh_id] = {
        "sub": username,
        "exp": time.time() + REFRESH_TOKEN_TTL_SECONDS,
    }
    LOGGER.info("User %s logged in successfully", username)
    return {"access": make_access(username), "refresh": refresh_id}


@app.post(
    "/auth/refresh",
    summary="Refresh Access Token",
    description="Exchange a valid refresh token for a new token pair.",
)
async def refresh(
    refresh_id: str = Query(
        ...,
        description="Refresh token identifier returned during login.",
        examples=["abc123def456"],
        alias="rid",
    ),
) -> Dict[str, str]:
    """Exchange a refresh token for a new pair of authentication tokens.

    Args:
        refresh_id: Identifier of the previously issued refresh token.

    Returns:
        Dictionary containing a fresh access token and refresh token.

    Raises:
        HTTPException: Raised if the refresh token is invalid or expired.
    """
    LOGGER.info("Refresh attempt for token id: %s", refresh_id)
    token_data = TOKENS.get(refresh_id)
    if not token_data:
        LOGGER.warning("Refresh failed: unknown token id %s", refresh_id)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid refresh token.")

    if token_data["exp"] < time.time():
        LOGGER.warning("Refresh failed: token id %s expired", refresh_id)
        TOKENS.pop(refresh_id, None)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Refresh token expired.")

    TOKENS.pop(refresh_id, None)
    new_refresh_id = secrets.token_urlsafe(32)
    TOKENS[new_refresh_id] = {
        "sub": token_data["sub"],
        "exp": time.time() + REFRESH_TOKEN_TTL_SECONDS,
    }
    LOGGER.info("Token refreshed for user: %s", token_data["sub"])
    return {"access": make_access(token_data["sub"]), "refresh": new_refresh_id}


@app.post(
    "/storage/presign/upload",
    summary="Create Upload URL",
    description=(
        "Generate a presigned URL that allows the caller to upload a file "
        "into their storage namespace."
    )

)
async def create_upload_url(
    object_name: str = Query(
        ...,
        description="Name of the object to be uploaded.",
    ),
    user: Dict[str, str] = Depends(current_user),
    minio_client=Depends(get_public_minio_client),
) -> PresignResponse:
    """Create a presigned PUT URL for uploading a file.

    Args:
        object_name: Desired object name provided by the caller.
        user: Authenticated user injected via dependency.
        minio_client: MinIO client used to generate the URL.

    Returns:
        Dictionary containing the storage key, presigned URL, and expiration.

    Raises:
        HTTPException: Raised if the presign operation fails.
    """
    safe_name = sanitize(object_name)
    storage_key = f"{user['username']}/{safe_name}"
    try:
        expires = timedelta(minutes=PRESIGN_EXPIRATION_MINUTES)
        url = minio_client.presigned_put_object(BUCKET, storage_key, expires=expires)
        LOGGER.info("Presigned PUT generated for %s", storage_key)
        return PresignResponse(
            key=storage_key,
            url=url,
            expires_in=str(PRESIGN_EXPIRATION_SECONDS),
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Upload presign failed for %s: %s", storage_key, exc)
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="Failed to create upload URL.",
        ) from exc


@app.get(
    "/storage/presign/download",
    summary="Create Download URL",
    description="Generate a presigned URL to download a file from storage.",
)
async def create_download_url(
    object_name: str = Query(
        ...,
        description="Name of the object to download.",
    ),
    user: Dict[str, str] = Depends(current_user),
    minio_client=Depends(get_public_minio_client),
) -> PresignResponse:
    """Create a presigned GET URL for downloading a file.

    Args:
        object_name: Desired object name provided by the caller.
        user: Authenticated user injected via dependency.
        minio_client: MinIO client used to generate the URL.

    Returns:
        Dictionary containing the storage key, presigned URL, and expiration.

    Raises:
        HTTPException: Raised if the presign operation fails.
    """
    safe_name = sanitize(object_name)
    storage_key = f"{user['username']}/{safe_name}"
    try:
        expires = timedelta(minutes=PRESIGN_EXPIRATION_MINUTES)
        url = minio_client.presigned_get_object(BUCKET, storage_key, expires=expires)
        LOGGER.info("Presigned GET generated for %s", storage_key)
        return PresignResponse(
            key=storage_key,
            url=url,
            expires_in=str(PRESIGN_EXPIRATION_SECONDS),
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Download presign failed for %s: %s", storage_key, exc)
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="Failed to create download URL.",
        ) from exc


@app.get(
    "/storage/list",
    summary="List User Files",
    description="List objects stored under the authenticated user's namespace.",
)
async def list_objects(
    user: Dict[str, str] = Depends(current_user),
    minio_client=Depends(get_admin_minio_client),
) -> Dict[str, List[Dict[str, Any]]]:
    """List objects for the authenticated user.

    Args:
        user: Authenticated user dictionary injected via dependency.
        minio_client: Administrative MinIO client.

    Returns:
        Dictionary containing object metadata entries.

    Raises:
        HTTPException: Raised if listing objects fails.
    """
    prefix = f"{user['username']}/"
    objects: List[Dict[str, Any]] = []
    try:
        for obj in minio_client.list_objects(BUCKET, prefix=prefix, recursive=True):
            object_name = getattr(obj, "object_name", "")
            if not object_name.startswith(prefix):
                continue
            last_modified = getattr(obj, "last_modified", None)
            objects.append(
                {
                    "key": object_name,
                    "size": getattr(obj, "size", 0),
                    "last_modified": last_modified.isoformat() if last_modified else None,
                }
            )
        LOGGER.info(
            "Listed %d objects for user %s", len(objects), user["username"]
        )
        return {"objects": objects}
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Object list failed for %s: %s", user["username"], exc)
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="Failed to list objects.",
        ) from exc


if __name__ == "__main__":
    import uvicorn

    LOGGER.info("Starting application in direct execution mode")
    uvicorn.run(app, host="0.0.0.0", port=8000)
