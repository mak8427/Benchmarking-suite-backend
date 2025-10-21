"""MinIO client configuration and shared constants."""

from __future__ import annotations

import os
import re

from minio import Minio

DEFAULT_MINIO_ENDPOINT = os.getenv("MINIO_DEFAULT_ENDPOINT", "localhost:9000")

ADMIN_MINIO = Minio(
    os.getenv("MINIO_ADMIN_ENDPOINT", DEFAULT_MINIO_ENDPOINT),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)

PUBLIC_MINIO = Minio(
    os.getenv("MINIO_PUBLIC_ENDPOINT", DEFAULT_MINIO_ENDPOINT),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,  # set True if you expose HTTPS
)
BUCKET_PREFIX = os.getenv("MINIO_BUCKET_PREFIX", "user-")
ISSUER = os.getenv("BUCKET_TOKEN_ISS", "auth-service")
BUCKET_TOKEN_SECRET = os.getenv("BUCKET_TOKEN_SECRET", os.getenv("JWT_SECRET", "CHANGE_ME"))
BUCKET_TOKEN_TTL_MIN = int(os.getenv("BUCKET_TOKEN_TTL_MIN", "60"))  # 60 minutes default
BUCKET = os.getenv("MINIO_BUCKET") or "mybucket"


_slug_re = re.compile(r"[^a-z0-9-]+")

