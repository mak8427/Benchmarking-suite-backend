import os
import re
import time, jwt, secrets
from datetime import timedelta
from pathlib import Path

from fastapi import FastAPI, HTTPException, status, Header, Query, Depends
from passlib.hash import argon2
from pydantic import BaseModel, Field
from storage.minio_client import BUCKET, PUBLIC_MINIO, ADMIN_MINIO
import logging

from util.auth_utils import sanitize, current_user

# Configure logging at module level
LOG_FILE = Path(os.getenv("LOG_FILE_PATH", "process.log"))
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="File Storage API",
    description="A secure file storage API with user authentication and MinIO integration",
    version="1.0.0"
)
SECRET = b"..."
TOKENS = {}
USERS = {"alice": argon2.hash("pass")}
USERS_FILE = Path(os.getenv("USERS_FILE_PATH", "users.txt"))


def get_public_minio_client():
    """Get the public MinIO client for standard file operations"""
    return PUBLIC_MINIO


def get_admin_minio_client():
    """Get the admin MinIO client for administrative operations"""
    return ADMIN_MINIO


class UserCreate(BaseModel):
    """Model for user registration data"""
    username: str = Field(
        ...,
        min_length=5,
        max_length=20,
        description="Unique username for the account. Must be between 5-20 characters long.",
        example="johndoe123"
    )
    password: str = Field(
        ...,
        min_length=8,
        max_length=128,
        description="Password for the account. Must be at least 8 characters long for security.",
        example="SecurePass123!"
    )


@app.get("/", summary="Health Check", description="Simple endpoint to verify the API is running")
async def root():
    """
    Root endpoint that returns a welcome message.
    Used for health checks and API availability verification.
    """
    logger.info("Root endpoint accessed")
    return {"message": "Hello World"}


def make_access(sub):
    """
    Create a short-lived access token for the given subject.

    Args:
        sub (str): The subject (username) for whom to create the token

    Returns:
        str: JWT access token valid for 10 minutes
    """
    logger.debug(f"Creating access token for user: {sub}")
    return jwt.encode({"sub": sub, "scope": "upload", "exp": time.time() + 600}, SECRET, "HS256")


@app.post(
    "/auth/register",
    status_code=status.HTTP_201_CREATED,
    summary="Register New User",
    description="Create a new user account with username and password. Returns access and refresh tokens upon successful registration."
)
async def register(payload: UserCreate):
    """
    Register a new user account.

    Creates a new user with the provided credentials and returns authentication tokens.
    The user data is stored both in memory and persisted to a text file.

    Args:
        payload (UserCreate): User registration data containing username and password

    Returns:
        dict: Contains access token (10min validity) and refresh token (30 day validity)

    Raises:
        HTTPException: 409 if username already exists
    """
    logger.info(f"Registration attempt for username: {payload.username}")

    if payload.username in USERS:
        logger.warning(f"Registration failed: Username {payload.username} already exists")
        raise HTTPException(status_code=409, detail="Username already registered")

    USERS[payload.username] = argon2.hash(payload.password)
    rid = secrets.token_urlsafe(32)
    TOKENS[rid] = {"sub": payload.username, "exp": time.time() + 30 * 86400}

    # Save user to plain text, for now
    USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with USERS_FILE.open("a", encoding="utf-8") as f:
        f.write(f"{payload.username}:{USERS[payload.username]}\n")
        logger.debug(f"User {payload.username} saved to users.txt")

    logger.info(f"User {payload.username} registered successfully")
    return {
        "access": make_access(payload.username),
        "refresh": rid
    }


@app.post(
    "/auth/password",
    summary="User Login",
    description="Authenticate user with username and password. Returns access and refresh tokens for authenticated sessions."
)
def login(
        u: str = Query(..., description="Username for authentication", example="johndoe123"),
        p: str = Query(..., description="Password for authentication", example="SecurePass123!")
):
    """
    Authenticate user and return access and refresh tokens.

    Validates user credentials against stored user data and returns JWT tokens
    for authenticated API access. Access tokens are valid for 10 minutes,
    refresh tokens are valid for 30 days.

    Args:
        u (str): Username for authentication
        p (str): Password for authentication

    Returns:
        dict: Contains access token and refresh token

    Raises:
        HTTPException: 401 if credentials are invalid
    """
    logger.info(f"Login attempt for username: {u}")

    try:
        with USERS_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                user, hashv = line.split(":")
                USERS[user] = hashv.strip()
        logger.debug("User data loaded from users.txt")
    except FileNotFoundError:
        logger.warning("users.txt not found, using in-memory user data only")
        pass

    if u not in USERS or not argon2.verify(p, USERS[u]):
        logger.warning(f"Failed login attempt for username: {u}")
        raise HTTPException(401)

    rid = secrets.token_urlsafe(32)
    TOKENS[rid] = {"sub": u, "exp": time.time() + 30 * 86400}
    logger.info(f"User {u} logged in successfully")
    return {"access": make_access(u), "refresh": rid}


@app.post(
    "/auth/refresh",
    summary="Refresh Access Token",
    description="Exchange a valid refresh token for a new access token and refresh token pair."
)
def refresh(
        rid: str = Query(..., description="Refresh token ID obtained from login or previous refresh",
                         example="abc123def456...")
):
    """
    Refresh authentication tokens using a valid refresh token.

    Exchanges an existing refresh token for a new access token and refresh token.
    The old refresh token is invalidated and replaced with a new one.

    Args:
        rid (str): Refresh token ID to exchange for new tokens

    Returns:
        dict: New access token and refresh token

    Raises:
        HTTPException: 401 if refresh token is invalid or expired
    """
    logger.info("Token refresh attempt")
    t = TOKENS.get(rid)
    if not t or t["exp"] < time.time():
        logger.warning(f"Invalid or expired refresh token")
        raise HTTPException(401)

    new = secrets.token_urlsafe(32)
    TOKENS[new] = t
    TOKENS.pop(rid, None)
    logger.info(f"Successfully refreshed token for user: {t['sub']}")
    return {"access": make_access(t["sub"]), "refresh": new}


@app.post(
    "/storage/presign/upload",
    summary="Generate Upload URL",
    description="Create a presigned URL for uploading files to MinIO storage. Requires authentication."
)
def create_upload_url(
        object_name: str = Query(
            ...,
            description="Name of the file to upload. Will be sanitized and prefixed with username.",
            example="my-document.pdf"
        ),
        user=Depends(current_user),
        minio_client=Depends(get_public_minio_client),
):
    """
    Generate a presigned URL for file upload to MinIO storage.

    Creates a temporary upload URL that allows direct file upload to MinIO storage.
    The file will be stored under the authenticated user's namespace.
    URL expires after 30 minutes for security.

    Args:
        object_name (str): Name of the file to upload
        user: Authenticated user information (injected by dependency)
        minio_client: MinIO client instance (injected by dependency)

    Returns:
        dict: Contains the storage key, presigned URL, and expiration time

    Raises:
        HTTPException: 400 if URL generation fails
    """
    safe = sanitize(object_name)
    key = f"{user['username']}/{safe}"  # prefix by caller identity
    try:
        url = minio_client.presigned_put_object(BUCKET, key, expires=timedelta(minutes=30))
        logger.info(f"presigned PUT for {key}")
        return {"key": key, "url": url, "expires_in": 600}
    except Exception as e:
        logger.error(f"presign failed for {key}: {e}")
        raise HTTPException(400, "failed to create upload URL")


@app.get(
    "/storage/presign/download",
    summary="Generate Download URL",
    description="Create a presigned URL for downloading files from MinIO storage. Requires authentication."
)
def create_download_url(
        object_name: str = Query(
            ...,
            description="Name of the file to download from your storage namespace.",
            example="my-document.pdf"
        ),
        user=Depends(current_user),
        minio_client=Depends(get_public_minio_client),
):
    """
    Generate a presigned URL for file download from MinIO storage.

    Creates a temporary download URL for files stored under the authenticated user's namespace.
    URL expires after 30 minutes for security.

    Args:
        object_name (str): Name of the file to download
        user: Authenticated user information (injected by dependency)
        minio_client: MinIO client instance (injected by dependency)

    Returns:
        dict: Contains the storage key, presigned URL, and expiration time

    Raises:
        HTTPException: 400 if URL generation fails
    """
    safe = sanitize(object_name)
    key = f"{user['username']}/{safe}"
    try:
        url = minio_client.presigned_get_object(BUCKET, key, expires=timedelta(minutes=30))
        logger.info(f"presigned GET for {key}")
        return {"key": key, "url": url, "expires_in": 600}
    except Exception as e:
        logger.error(f"download presign failed for {key}: {e}")
        raise HTTPException(400, "failed to create download URL")


@app.get(
    "/storage/list",
    summary="List User Files",
    description="Retrieve a list of all files stored under the authenticated user's namespace."
)
def list_objects(
        user=Depends(current_user),
        minio_client=Depends(get_admin_minio_client),
):
    """
    List all files stored under the authenticated user's namespace.

    Retrieves metadata for all files belonging to the authenticated user,
    including file size and last modification date.

    Args:
        user: Authenticated user information (injected by dependency)
        minio_client: Admin MinIO client instance (injected by dependency)

    Returns:
        dict: List of objects with metadata (key, size, last_modified)

    Raises:
        HTTPException: 400 if listing fails
    """
    prefix = f"{user['username']}/"
    objects = []
    try:
        for obj in minio_client.list_objects(BUCKET, prefix=prefix, recursive=True):
            if not getattr(obj, "object_name", "").startswith(prefix):
                continue
            objects.append(
                {
                    "key": obj.object_name,
                    "size": getattr(obj, "size", 0),
                    "last_modified": getattr(obj, "last_modified", None).isoformat()
                    if getattr(obj, "last_modified", None)
                    else None,
                }
            )
        logger.info(f"listed objects for {user['username']}: {len(objects)} items")
        return {"objects": objects}
    except Exception as e:
        logger.error(f"list objects failed for {user['username']}: {e}")
        raise HTTPException(400, "failed to list objects")


# This block only runs when the script is executed directly
if __name__ == "__main__":
    logger.info("Starting application in direct execution mode")
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)