"""S3-compatible storage client configuration.

The module name is kept for backward compatibility with existing imports, but
the implementation targets generic S3-compatible storage such as GWDG S3.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import timedelta
from types import SimpleNamespace
from typing import Iterator
from urllib.parse import urlparse

import boto3
from botocore.config import Config


DEFAULT_S3_ENDPOINT_URL = os.getenv("S3_DEFAULT_ENDPOINT_URL", "https://s3.gwdg.de")


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


def _setting(*names: str, default: str | None = None) -> str | None:
    """Return the first non-empty environment setting from ``names``."""
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def _normalize_endpoint_url(value: str | None) -> str:
    """Normalize endpoint settings to a boto3 endpoint URL.

    ``MINIO_*`` deployments often use ``host:port`` plus ``MINIO_SECURE`` while
    GWDG S3 uses a full HTTPS URL. This helper accepts both forms.
    """
    endpoint = value or DEFAULT_S3_ENDPOINT_URL
    if endpoint.startswith(("http://", "https://")):
        return endpoint.rstrip("/")
    scheme = "https" if _truthy(os.getenv("MINIO_SECURE")) else "http"
    return f"{scheme}://{endpoint.rstrip('/')}"


def _boto_config() -> Config:
    """Build botocore config compatible with GWDG S3/Ceph.

    Recent botocore versions may use checksum/chunked upload behavior that some
    S3-compatible services reject with ``MissingContentLength``. The checksum
    options keep checksums to the cases where the service explicitly requires
    them, while still supporting older botocore versions.
    """
    kwargs = {
        "signature_version": "s3v4",
        "s3": {
            "addressing_style": _setting("S3_ADDRESSING_STYLE", default="path"),
            "payload_signing_enabled": _truthy(os.getenv("S3_PAYLOAD_SIGNING")),
        },
    }
    try:
        return Config(
            **kwargs,
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        )
    except TypeError:
        return Config(**kwargs)


@dataclass(frozen=True)
class StorageSettings:
    """Resolved S3-compatible storage configuration."""

    endpoint_url: str
    access_key: str | None
    secret_key: str | None
    region: str
    bucket: str


def resolve_storage_settings() -> StorageSettings:
    """Resolve S3 settings with legacy MinIO environment aliases."""
    endpoint = _setting(
        "S3_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
        "AWS_ENDPOINT_URL",
        "MINIO_ADMIN_ENDPOINT",
        "MINIO_PUBLIC_ENDPOINT",
        "MINIO_ENDPOINT",
        default=DEFAULT_S3_ENDPOINT_URL,
    )
    return StorageSettings(
        endpoint_url=_normalize_endpoint_url(endpoint),
        access_key=_setting("S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID", "MINIO_ACCESS_KEY"),
        secret_key=_setting("S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY", "MINIO_SECRET_KEY"),
        region=_setting("S3_REGION", "AWS_DEFAULT_REGION", default="us-east-1") or "us-east-1",
        bucket=_setting("S3_BUCKET", "MINIO_BUCKET", default="benchmarking-suite") or "benchmarking-suite",
    )


class S3StorageClient:
    """Small compatibility wrapper around boto3 S3 client.

    The public methods mirror the MinIO methods used by the application so the
    API layer and tests can migrate gradually.
    """

    def __init__(self, settings: StorageSettings | None = None) -> None:
        self.settings = settings or resolve_storage_settings()
        self._client = boto3.client(
            "s3",
            endpoint_url=self.settings.endpoint_url,
            aws_access_key_id=self.settings.access_key or "",
            aws_secret_access_key=self.settings.secret_key or "",
            region_name=self.settings.region,
            config=_boto_config(),
        )

    def presigned_put_object(self, bucket: str, key: str, expires: timedelta) -> str:
        """Return a presigned PUT URL for ``bucket/key``."""
        return self._client.generate_presigned_url(
            "put_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=int(expires.total_seconds()),
            HttpMethod="PUT",
        )

    def presigned_get_object(self, bucket: str, key: str, expires: timedelta) -> str:
        """Return a presigned GET URL for ``bucket/key``."""
        return self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=int(expires.total_seconds()),
            HttpMethod="GET",
        )

    def list_objects(self, bucket: str, prefix: str, recursive: bool) -> Iterator[SimpleNamespace]:
        """Yield object metadata using MinIO-compatible attribute names."""
        delimiter = None if recursive else "/"
        paginator = self._client.get_paginator("list_objects_v2")
        page_args = {"Bucket": bucket, "Prefix": prefix}
        if delimiter:
            page_args["Delimiter"] = delimiter
        for page in paginator.paginate(**page_args):
            for item in page.get("Contents", []):
                yield SimpleNamespace(
                    object_name=item.get("Key", ""),
                    size=item.get("Size", 0),
                    last_modified=item.get("LastModified"),
                )

    def list_buckets(self) -> list[SimpleNamespace]:
        """Return buckets using MinIO-compatible ``.name`` attributes."""
        response = self._client.list_buckets()
        return [SimpleNamespace(name=item["Name"]) for item in response.get("Buckets", [])]

    def fget_object(self, bucket: str, object_name: str, file_path: str) -> None:
        """Download ``bucket/object_name`` to ``file_path``."""
        self._client.download_file(bucket, object_name, file_path)


SETTINGS = resolve_storage_settings()
ADMIN_MINIO = S3StorageClient(SETTINGS)
PUBLIC_MINIO = S3StorageClient(SETTINGS)

BUCKET_PREFIX = _setting("S3_BUCKET_PREFIX", "MINIO_BUCKET_PREFIX", default="user-") or "user-"
ISSUER = os.getenv("BUCKET_TOKEN_ISS", "auth-service")
BUCKET_TOKEN_SECRET = os.getenv("BUCKET_TOKEN_SECRET", "")
BUCKET_TOKEN_TTL_MIN = int(os.getenv("BUCKET_TOKEN_TTL_MIN", "60"))
BUCKET = SETTINGS.bucket

_slug_re = re.compile(r"[^a-z0-9-]+")
