import os
import re
import datetime as dt
from typing import Tuple

from minio import Minio
from minio.error import S3Error
import jwt
from jwt import InvalidTokenError
MINIO = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
)

BUCKET_PREFIX = os.getenv("MINIO_BUCKET_PREFIX", "user-")
ISSUER = os.getenv("BUCKET_TOKEN_ISS", "auth-service")
BUCKET_TOKEN_SECRET = os.getenv("BUCKET_TOKEN_SECRET", os.getenv("JWT_SECRET", "CHANGE_ME"))
BUCKET_TOKEN_TTL_MIN = int(os.getenv("BUCKET_TOKEN_TTL_MIN", "60"))  # 60 minutes default

_slug_re = re.compile(r"[^a-z0-9-]+")

def _sanitize_bucket_name(username: str) -> str:
    """
    Convert a username to a safe S3 bucket name. Removes unsafe characters and enforces length limits.
    3-63 chars, lowercase letters, numbers, dashes, no leading/trailing dash
    """
    slug = _slug_re.sub("-", username.strip().lower()).strip("-")
    if len(slug) < 3:
        slug = (slug + "-xxx")[:3]
    bucket = f"{BUCKET_PREFIX}{slug}"
    return bucket[:63]

def ensure_user_bucket(username: str) -> str:
    """
    Ensure a bucket exists for the given username, creating it if necessary.
    :param username:
    :returns: The name of the user's bucket.
    """
    bucket = _sanitize_bucket_name(username)
    if not MINIO.bucket_exists(bucket):
        MINIO.make_bucket(bucket)
    return bucket

def make_bucket_token(username: str) -> Tuple[str, str]:
    bucket = ensure_user_bucket(username)
    now = dt.datetime.now(dt.timezone.utc)
    payload = {
        "sub": username,
        "bucket": bucket,
        "scope": "bucket:write",
        "iat": int(now.timestamp()),
        "exp": int((now + dt.timedelta(minutes=BUCKET_TOKEN_TTL_MIN)).timestamp()),
        "iss": ISSUER,
    }
    token = jwt.encode(payload, BUCKET_TOKEN_SECRET, algorithm="HS256")
    return token, bucket

def verify_bucket_token(token: str) -> dict:
    return jwt.decode(token, BUCKET_TOKEN_SECRET, algorithms=["HS256"], options={"require": ["exp", "sub", "bucket"]})

def _is_safe_object_name(name: str) -> bool:
    if not name or len(name) > 1024 or name.startswith("/") or ".." in name:
        return False
    return re.fullmatch(r"[A-Za-z0-9/_\-.]+", name) is not None

def presigned_put_url(bucket_token: str, object_name: str, expires_seconds: int = 600) -> str:
    claims = verify_bucket_token(bucket_token)
    bucket = claims["bucket"]
    if claims.get("scope") != "bucket:write":
        raise PermissionError("invalid scope")
    if not _is_safe_object_name(object_name):
        raise ValueError("invalid object name")
    return MINIO.presigned_put_object(bucket, object_name, expires=dt.timedelta(seconds=expires_seconds))