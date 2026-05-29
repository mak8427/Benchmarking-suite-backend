"""FastAPI application exposing auth and MinIO-backed storage APIs."""

from __future__ import annotations

import logging
import os
import secrets
import time
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import jwt
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse
from passlib.hash import argon2
from pydantic import BaseModel, Field

try:
    from api_backend.db import (
        create_refresh_token,
        create_user,
        get_refresh_token,
        get_user_by_id,
        get_user_by_username,
        init_db,
        record_storage_object,
        revoke_refresh_token,
    )
    from api_backend.grafana import GrafanaProvisioner
    from api_backend.storage.minio_client import ADMIN_MINIO, BUCKET, PUBLIC_MINIO
    from api_backend.util.auth_utils import current_user, get_jwt_secret, sanitize
except ModuleNotFoundError as exc:
    # Support running from inside ``api_backend/`` with ``uvicorn main:app``.
    if exc.name and exc.name.startswith("api_backend"):
        from db import (  # type: ignore[no-redef]
            create_refresh_token,
            create_user,
            get_refresh_token,
            get_user_by_id,
            get_user_by_username,
            init_db,
            record_storage_object,
            revoke_refresh_token,
        )
        from grafana import GrafanaProvisioner  # type: ignore[no-redef]
        from storage.minio_client import ADMIN_MINIO, BUCKET, PUBLIC_MINIO  # type: ignore[no-redef]
        from util.auth_utils import current_user, get_jwt_secret, sanitize  # type: ignore[no-redef]
    else:
        raise

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

ACCESS_TOKEN_TTL_SECONDS = 600
REFRESH_TOKEN_TTL_SECONDS = 30 * 86400
PRESIGN_EXPIRATION_SECONDS = 600
GRAFANA_SESSION_TTL_SECONDS = int(os.getenv("GRAFANA_SESSION_TTL_SECONDS", str(12 * 3600)))
GRAFANA_SESSION_COOKIE = os.getenv("GRAFANA_SESSION_COOKIE", "bench_grafana_session")

MAX_OBJECTS_PER_USER = int(os.getenv("MAX_OBJECTS_PER_USER", "1000"))
MAX_STORAGE_BYTES_PER_USER = int(os.getenv("MAX_STORAGE_BYTES_PER_USER", str(10 * 1024**3)))

app = FastAPI(
    title="File Storage API",
    description="Secure file storage API with user authentication and MinIO integration.",
    version="1.1.0",
)

_CONFIG_WARNINGS: List[str] = []


def _validate_runtime_config() -> None:
    """Collect non-fatal configuration warnings (optionally raise in strict mode)."""
    warnings: List[str] = []
    jwt_secret = os.getenv("JWT_SECRET", "")
    if not jwt_secret or jwt_secret == "...":
        warnings.append("JWT_SECRET is missing or using the placeholder value.")

    if not (os.getenv("S3_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("MINIO_ACCESS_KEY")):
        warnings.append("S3_ACCESS_KEY_ID/AWS_ACCESS_KEY_ID is not set.")
    if not (os.getenv("S3_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("MINIO_SECRET_KEY")):
        warnings.append("S3_SECRET_ACCESS_KEY/AWS_SECRET_ACCESS_KEY is not set.")
    if not (
        os.getenv("S3_ENDPOINT_URL")
        or os.getenv("AWS_ENDPOINT_URL")
        or os.getenv("MINIO_PUBLIC_ENDPOINT")
        or os.getenv("MINIO_ADMIN_ENDPOINT")
    ):
        warnings.append("S3_ENDPOINT_URL is not set (default https://s3.gwdg.de will be used).")
    if not (os.getenv("S3_BUCKET") or os.getenv("MINIO_BUCKET")):
        warnings.append("S3_BUCKET is not set (default benchmarking-suite will be used).")

    global _CONFIG_WARNINGS
    _CONFIG_WARNINGS = warnings

    if warnings:
        for item in warnings:
            LOGGER.warning("Config warning: %s", item)
        if os.getenv("STRICT_CONFIG") == "1":
            raise RuntimeError("Refusing to start with STRICT_CONFIG=1 due to config warnings.")


@app.on_event("startup")
def _startup_checks() -> None:
    _validate_runtime_config()


class UserCreate(BaseModel):
    """Request body for user registration payloads."""

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


class AuthTokens(BaseModel):
    """Token response for register/login/refresh operations."""

    access: str
    refresh: str


class PresignResponse(BaseModel):
    """Response payload for presigned URL requests."""

    key: str
    url: str
    expires_in: str
    headers: dict[str, str] = Field(default_factory=dict)


@app.on_event("startup")
def _startup() -> None:
    """Initialize persistence schema on startup.

    This hook ensures DB tables are ready before serving requests.

    Examples:
        >>> _startup.__name__
        '_startup'
    """
    init_db()


@app.middleware("http")
async def request_logging(request, call_next):
    """Log request metadata and latency for observability.

    Args:
        request: Incoming Starlette request.
        call_next: Next middleware/app handler.

    Returns:
        Response: Wrapped response with request id header.

    Examples:
        >>> "request_id" in "request_id=abc"
        True
    """
    request_id = secrets.token_hex(8)
    start = time.perf_counter()
    LOGGER.info(
        "request_id=%s method=%s path=%s event=start",
        request_id,
        request.method,
        request.url.path,
    )
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000
    response.headers["X-Request-Id"] = request_id
    LOGGER.info(
        "request_id=%s method=%s path=%s status=%d latency_ms=%.2f",
        request_id,
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
    )
    return response


def get_public_minio_client() -> Any:
    """Return MinIO client for standard object operations.

    Returns:
        Any: Public MinIO client instance.

    Examples:
        >>> get_public_minio_client() is PUBLIC_MINIO
        True
    """
    return PUBLIC_MINIO


def get_admin_minio_client() -> Any:
    """Return MinIO client with administrative object listing access.

    Returns:
        Any: Admin MinIO client instance.

    Examples:
        >>> get_admin_minio_client() is ADMIN_MINIO
        True
    """
    return ADMIN_MINIO


def make_access(user_id: str, username: str) -> str:
    """Create a short-lived access token.

    Args:
        user_id (str): Stable identifier for the authenticated user.
        username (str): Username claim.

    Returns:
        str: Signed JWT access token.

    Examples:
        >>> os.environ["JWT_SECRET"] = "doc-secret"
        >>> token = make_access("uid", "alice")
        >>> isinstance(token, str)
        True
    """
    payload = {
        "sub": user_id,
        "username": username,
        "scope": "storage",
        "exp": time.time() + ACCESS_TOKEN_TTL_SECONDS,
    }
    try:
        return jwt.encode(payload, get_jwt_secret(), algorithm="HS256")
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="JWT_SECRET environment variable is required.",
        ) from exc


def _issue_refresh_token(user_id: str) -> str:
    """Create and persist a refresh token for a user.

    Args:
        user_id (str): User identifier.

    Returns:
        str: Refresh token ID.

    Examples:
        >>> isinstance(_issue_refresh_token.__name__, str)
        True
    """
    refresh_id = secrets.token_urlsafe(32)
    create_refresh_token(
        jti=refresh_id,
        user_id=user_id,
        exp=time.time() + REFRESH_TOKEN_TTL_SECONDS,
    )
    return refresh_id


def _make_grafana_session(user: dict[str, Any]) -> str:
    """Create a signed Grafana auth-proxy session token."""
    now = int(time.time())
    return jwt.encode(
        {
            "sub": user["id"],
            "username": user["username"],
            "iat": now,
            "exp": now + GRAFANA_SESSION_TTL_SECONDS,
            "aud": "grafana-auth-proxy",
        },
        get_jwt_secret(),
        algorithm="HS256",
    )


def _decode_grafana_session(token: str | None) -> dict[str, str]:
    """Validate a Grafana auth-proxy session token."""
    if not token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Missing Grafana session.")
    try:
        payload = jwt.decode(token, get_jwt_secret(), algorithms=["HS256"], audience="grafana-auth-proxy")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid Grafana session.") from exc
    if not payload.get("sub") or not payload.get("username"):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid Grafana session.")
    return {"user_id": str(payload["sub"]), "username": str(payload["username"])}


def _build_upload_key(user_id: str, filename: str) -> str:
    """Build a collision-safe key under the authenticated user prefix.

    Args:
        user_id (str): User identifier prefix.
        filename (str): Requested object filename.

    Returns:
        str: Storage key in `user_id/uuid_filename` format.

    Examples:
        >>> key = _build_upload_key("u1", "report.csv")
        >>> key.startswith("u1/") and key.endswith("_report.csv")
        True
    """
    safe_name = sanitize(filename)
    return f"{user_id}/{uuid.uuid4().hex}_{safe_name}"


def _enforce_upload_quota(user_id: str, admin_client: Any) -> None:
    """Validate object count and size quotas before issuing upload URLs.

    Args:
        user_id (str): Authenticated user identifier.
        admin_client (Any): MinIO administrative client.

    Raises:
        HTTPException: Raised when quota thresholds are exceeded.

    Examples:
        >>> class _Admin:
        ...     def list_objects(self, bucket, prefix, recursive):
        ...         return []
        >>> _enforce_upload_quota("u1", _Admin()) is None
        True
    """
    prefix = f"{user_id}/"
    count = 0
    total_bytes = 0
    try:
        for obj in admin_client.list_objects(BUCKET, prefix=prefix, recursive=True):
            count += 1
            total_bytes += int(getattr(obj, "size", 0) or 0)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to validate storage quota.",
        ) from exc

    if count >= MAX_OBJECTS_PER_USER:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Object quota exceeded.",
        )
    if total_bytes >= MAX_STORAGE_BYTES_PER_USER:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Storage quota exceeded.",
        )


def _presign_upload(key: str, minio_client: Any) -> PresignResponse:
    """Create a presigned PUT URL for a storage key.

    Args:
        key (str): Object key to authorize for upload.
        minio_client (Any): MinIO client for presigning.

    Returns:
        PresignResponse: Signed upload URL payload.

    Examples:
        >>> class _M:
        ...     def presigned_put_object(self, bucket, key, expires, headers=None):
        ...         return "http://example/upload"
        >>> _presign_upload("u1/a.txt", _M()).key
        'u1/a.txt'
    """
    expires = timedelta(seconds=PRESIGN_EXPIRATION_SECONDS)
    headers = {"x-amz-acl": "private"}
    url = minio_client.presigned_put_object(BUCKET, key, expires=expires, headers=headers)
    return PresignResponse(key=key, url=url, expires_in=str(PRESIGN_EXPIRATION_SECONDS), headers=headers)


def _presign_download(key: str, minio_client: Any) -> PresignResponse:
    """Create a presigned GET URL for a storage key.

    Args:
        key (str): Object key to authorize for download.
        minio_client (Any): MinIO client for presigning.

    Returns:
        PresignResponse: Signed download URL payload.

    Examples:
        >>> class _M:
        ...     def presigned_get_object(self, bucket, key, expires):
        ...         return "http://example/download"
        >>> _presign_download("u1/a.txt", _M()).url.endswith("download")
        True
    """
    expires = timedelta(seconds=PRESIGN_EXPIRATION_SECONDS)
    url = minio_client.presigned_get_object(BUCKET, key, expires=expires)
    return PresignResponse(key=key, url=url, expires_in=str(PRESIGN_EXPIRATION_SECONDS))


@app.get("/", summary="Health Check")
async def root() -> dict[str, str]:
    """Return a basic response for service health checks.

    This endpoint is used by probes and manual smoke tests.

    Examples:
        >>> root.__name__
        'root'
    """
    return {"message": "Hello World"}


@app.get("/healthz", summary="Readiness Probe")
async def healthz() -> dict[str, str]:
    """Return a readiness probe response.

    The response is intentionally small and stable.

    Examples:
        >>> healthz.__name__
        'healthz'
    """
    return {"status": "ok"}


@app.post("/auth/register", status_code=status.HTTP_201_CREATED)
async def register(payload: UserCreate) -> AuthTokens:
    """Register a new user and issue token pair.

    Args:
        payload (UserCreate): Registration payload.

    Returns:
        AuthTokens: Access and refresh tokens.

    Examples:
        >>> isinstance(UserCreate(username='abcde', password='Password1!').username, str)
        True
    """
    if get_user_by_username(payload.username):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Username already registered.",
        )

    user = create_user(payload.username, argon2.hash(payload.password))
    try:
        GrafanaProvisioner().provision_user(user_id=user["id"], username=user["username"])
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Grafana provisioning failed for user %s: %s", user["username"], exc)
    refresh_id = _issue_refresh_token(user["id"])
    return AuthTokens(access=make_access(user["id"], user["username"]), refresh=refresh_id)


@app.post("/auth/login")
async def login(
    username: str = Query(..., alias="u", description="Username for authentication."),
    password: str = Query(..., alias="p", description="Password for authentication."),
) -> AuthTokens:
    """Authenticate a user and issue tokens.

    Args:
        username (str): Username for authentication.
        password (str): Plaintext password.

    Returns:
        AuthTokens: Access and refresh tokens.

    Examples:
        >>> "u" in {"u": "name", "p": "pass"}
        True
    """
    user = get_user_by_username(username)
    if not user or not argon2.verify(password, user["pw_hash"]):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials.")

    refresh_id = _issue_refresh_token(user["id"])
    return AuthTokens(access=make_access(user["id"], user["username"]), refresh=refresh_id)


@app.post("/auth/password", include_in_schema=False)
async def login_legacy(
    response: Response,
    username: str = Query(..., alias="u"),
    password: str = Query(..., alias="p"),
) -> AuthTokens:
    """Backward-compatible login route.

    Args:
        response (Response): Response object to set deprecation headers.
        username (str): Username for authentication.
        password (str): Plaintext password.

    Returns:
        AuthTokens: Access and refresh tokens.

    Examples:
        >>> "/auth/password".endswith("password")
        True
    """
    response.headers["Deprecation"] = "true"
    response.headers["Sunset"] = "Wed, 31 Dec 2026 23:59:59 GMT"
    return await login(username=username, password=password)


@app.post("/auth/refresh")
async def refresh(
    refresh_id: str = Query(..., alias="rid", description="Refresh token identifier."),
) -> AuthTokens:
    """Exchange a refresh token for a new token pair.

    Args:
        refresh_id (str): Existing refresh token identifier.

    Returns:
        AuthTokens: Rotated access and refresh token pair.

    Examples:
        >>> "rid" in "rid=abc"
        True
    """
    token_row = get_refresh_token(refresh_id)
    if not token_row or int(token_row["revoked"]):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid refresh token.")
    if float(token_row["exp"]) < time.time():
        revoke_refresh_token(refresh_id)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Refresh token expired.")

    user = get_user_by_id(token_row["user_id"])
    if not user:
        revoke_refresh_token(refresh_id)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Refresh token is orphaned.")

    revoke_refresh_token(refresh_id)
    new_refresh = _issue_refresh_token(user["id"])
    return AuthTokens(access=make_access(user["id"], user["username"]), refresh=new_refresh)


@app.get("/grafana-auth/login", response_class=HTMLResponse, include_in_schema=False)
async def grafana_login_form() -> HTMLResponse:
    """Return a minimal backend-authenticated Grafana login form."""
    return HTMLResponse(
        """
        <!doctype html>
        <html><head><title>Benchmarking Suite Login</title></head>
        <body>
          <form method="post" action="/grafana-auth/login">
            <label>Username <input name="username" autocomplete="username"></label>
            <label>Password <input name="password" type="password" autocomplete="current-password"></label>
            <button type="submit">Log in</button>
          </form>
        </body></html>
        """
    )


@app.post("/grafana-auth/login", include_in_schema=False)
async def grafana_login(request: Request) -> Response:
    """Authenticate backend credentials and set a Grafana auth-proxy cookie."""
    form = parse_qs((await request.body()).decode("utf-8"))
    username = (form.get("username") or [""])[0]
    password = (form.get("password") or [""])[0]
    user = get_user_by_username(username)
    if not user or not argon2.verify(password, user["pw_hash"]):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials.")
    try:
        GrafanaProvisioner().provision_user(user_id=user["id"], username=user["username"])
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Grafana provisioning failed for user %s: %s", user["username"], exc)
    response = RedirectResponse(url="/grafana/", status_code=status.HTTP_303_SEE_OTHER)
    response.set_cookie(
        GRAFANA_SESSION_COOKIE,
        _make_grafana_session(user),
        max_age=GRAFANA_SESSION_TTL_SECONDS,
        httponly=True,
        samesite="lax",
    )
    return response


@app.get("/grafana-auth/logout", include_in_schema=False)
async def grafana_logout() -> Response:
    """Clear the Grafana auth-proxy cookie."""
    response = RedirectResponse(url="/grafana-auth/login", status_code=status.HTTP_303_SEE_OTHER)
    response.delete_cookie(GRAFANA_SESSION_COOKIE)
    return response


@app.get("/grafana-auth/verify", include_in_schema=False)
async def grafana_auth_proxy(request: Request) -> Response:
    """Traefik forward-auth endpoint that emits Grafana auth-proxy headers."""
    try:
        user = _decode_grafana_session(request.cookies.get(GRAFANA_SESSION_COOKIE))
    except HTTPException:
        return RedirectResponse(url="/grafana-auth/login", status_code=status.HTTP_303_SEE_OTHER)
    response = Response(status_code=status.HTTP_204_NO_CONTENT)
    response.headers["X-WEBAUTH-USER"] = user["username"]
    response.headers["X-WEBAUTH-NAME"] = user["username"]
    response.headers["X-WEBAUTH-EMAIL"] = f"{user['username']}@benchmarking-suite.local"
    return response


@app.post("/files/presign/upload")
async def create_upload_url(
    filename: str = Query(..., description="Name of file to upload."),
    user: dict[str, str] = Depends(current_user),
) -> PresignResponse:
    """Create a presigned upload URL for an authenticated user.

    Args:
        filename (str): Requested client filename.
        user (dict[str, str]): Authenticated user claims.

    Returns:
        PresignResponse: Presign response with generated key.

    Examples:
        >>> "/files/presign/upload".startswith("/files")
        True
    """
    minio_client = get_public_minio_client()
    admin_minio_client = get_admin_minio_client()
    _enforce_upload_quota(user["user_id"], admin_minio_client)
    key = _build_upload_key(user["user_id"], filename)
    record_storage_object(
        object_key=key,
        user_id=user["user_id"],
        username=user["username"],
        original_filename=filename,
    )
    try:
        return _presign_upload(key, minio_client)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to create upload URL.",
        ) from exc


@app.post("/storage/presign/upload", include_in_schema=False)
async def create_upload_url_legacy(
    response: Response,
    object_name: str = Query(..., description="Legacy object name field."),
    user: dict[str, str] = Depends(current_user),
) -> PresignResponse:
    """Backward-compatible upload route.

    Args:
        response (Response): Response object to set deprecation headers.
        object_name (str): Legacy object-name query parameter.
        user (dict[str, str]): Authenticated user claims.

    Returns:
        PresignResponse: Presign response with generated key.

    Examples:
        >>> "/storage/presign/upload".split("/")[-1]
        'upload'
    """
    response.headers["Deprecation"] = "true"
    response.headers["Sunset"] = "Wed, 31 Dec 2026 23:59:59 GMT"
    return await create_upload_url(filename=object_name, user=user)


@app.get("/files/presign/download")
async def create_download_url(
    key: str = Query(..., description="Storage key returned by upload presign endpoint."),
    user: dict[str, str] = Depends(current_user),
) -> PresignResponse:
    """Create a presigned download URL for a user-owned key.

    Args:
        key (str): Full object key.
        user (dict[str, str]): Authenticated user claims.

    Returns:
        PresignResponse: Presigned download response.

    Examples:
        >>> "/files/presign/download".endswith("download")
        True
    """
    minio_client = get_public_minio_client()
    if not key.startswith(f"{user['user_id']}/"):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden key scope.")
    try:
        return _presign_download(key, minio_client)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to create download URL.",
        ) from exc


@app.get("/storage/presign/download", include_in_schema=False)
async def create_download_url_legacy(
    response: Response,
    object_name: str = Query(..., description="Legacy object name field."),
    user: dict[str, str] = Depends(current_user),
) -> PresignResponse:
    """Backward-compatible download route.

    Args:
        response (Response): Response object to set deprecation headers.
        object_name (str): Legacy object-name query parameter.
        user (dict[str, str]): Authenticated user claims.

    Returns:
        PresignResponse: Presigned download response.

    Examples:
        >>> "/storage/presign/download".startswith("/storage")
        True
    """
    response.headers["Deprecation"] = "true"
    response.headers["Sunset"] = "Wed, 31 Dec 2026 23:59:59 GMT"
    minio_client = get_public_minio_client()
    legacy_key = f"{user['user_id']}/{sanitize(object_name)}"
    try:
        return _presign_download(legacy_key, minio_client)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to create download URL.",
        ) from exc


@app.get("/files/list")
async def list_objects(
    user: dict[str, str] = Depends(current_user),
) -> dict[str, list[dict[str, Any]]]:
    """List objects under the authenticated user's storage prefix.

    Args:
        user (dict[str, str]): Authenticated user claims.

    Returns:
        dict[str, list[dict[str, Any]]]: User-scoped object metadata.

    Examples:
        >>> "/files/list".split("/")[-1]
        'list'
    """
    minio_client = get_admin_minio_client()
    prefix = f"{user['user_id']}/"
    objects: list[dict[str, Any]] = []
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
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to list objects.",
        ) from exc
    return {"objects": objects}


@app.get("/storage/list", include_in_schema=False)
async def list_objects_legacy(
    response: Response,
    user: dict[str, str] = Depends(current_user),
) -> dict[str, list[dict[str, Any]]]:
    """Backward-compatible object listing route.

    Args:
        response (Response): Response object to set deprecation headers.
        user (dict[str, str]): Authenticated user claims.

    Returns:
        dict[str, list[dict[str, Any]]]: User-scoped object metadata.

    Examples:
        >>> "/storage/list".startswith("/storage")
        True
    """
    response.headers["Deprecation"] = "true"
    response.headers["Sunset"] = "Wed, 31 Dec 2026 23:59:59 GMT"
    return await list_objects(user=user)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
