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
BUCKET = os.getenv("MINIO_BUCKET") or "mybucket"


_slug_re = re.compile(r"[^a-z0-9-]+")



