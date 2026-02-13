"""MinIO client configuration and shared constants."""

from __future__ import annotations

import os
import re

from minio import Minio


DEFAULT_MINIO_ENDPOINT = os.getenv("MINIO_DEFAULT_ENDPOINT", "localhost:9000")


def _truthy(value: str | None) -> bool:
    """Return whether an environment variable value is logically true.

    Args:
        value (str | None): Environment variable value.

    Returns:
        bool: Parsed truthiness flag.

    Examples:
        >>> _truthy("yes")
        True
        >>> _truthy("0")
        False
    """
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


ADMIN_MINIO = Minio(
    os.getenv("MINIO_ADMIN_ENDPOINT", DEFAULT_MINIO_ENDPOINT),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=_truthy(os.getenv("MINIO_SECURE")),
)

PUBLIC_MINIO = Minio(
    os.getenv("MINIO_PUBLIC_ENDPOINT", DEFAULT_MINIO_ENDPOINT),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=_truthy(os.getenv("MINIO_SECURE")),
)

BUCKET_PREFIX = os.getenv("MINIO_BUCKET_PREFIX", "user-")
ISSUER = os.getenv("BUCKET_TOKEN_ISS", "auth-service")
BUCKET_TOKEN_SECRET = os.getenv("BUCKET_TOKEN_SECRET", "")
BUCKET_TOKEN_TTL_MIN = int(os.getenv("BUCKET_TOKEN_TTL_MIN", "60"))
BUCKET = os.getenv("MINIO_BUCKET") or "mybucket"

_slug_re = re.compile(r"[^a-z0-9-]+")
