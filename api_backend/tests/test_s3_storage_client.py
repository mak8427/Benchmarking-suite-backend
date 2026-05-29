"""Tests for S3-compatible storage configuration."""

from __future__ import annotations

from datetime import timedelta

from api_backend.storage import minio_client


def test_resolve_storage_settings_prefers_s3_names(monkeypatch) -> None:
    """S3_* variables should be first-class configuration names."""
    monkeypatch.setenv("S3_ENDPOINT_URL", "https://s3.gwdg.de")
    monkeypatch.setenv("S3_ACCESS_KEY_ID", "access")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("S3_BUCKET", "benchmarking-suite")
    monkeypatch.setenv("S3_REGION", "eu-test-1")

    settings = minio_client.resolve_storage_settings()

    assert settings.endpoint_url == "https://s3.gwdg.de"
    assert settings.access_key == "access"
    assert settings.secret_key == "secret"
    assert settings.bucket == "benchmarking-suite"
    assert settings.region == "eu-test-1"


def test_resolve_storage_settings_keeps_minio_aliases(monkeypatch) -> None:
    """Legacy MINIO_* names should keep old deployments working."""
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("S3_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("S3_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("S3_BUCKET", raising=False)
    monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "minio-access")
    monkeypatch.setenv("MINIO_SECRET_KEY", "minio-secret")
    monkeypatch.setenv("MINIO_BUCKET", "benchwrap")

    settings = minio_client.resolve_storage_settings()

    assert settings.endpoint_url == "http://localhost:9000"
    assert settings.access_key == "minio-access"
    assert settings.secret_key == "minio-secret"
    assert settings.bucket == "benchwrap"


def test_s3_storage_client_presigns_with_expected_methods(monkeypatch) -> None:
    """The wrapper should expose the MinIO-style presign methods used by the API."""
    calls = []

    class FakeBotoClient:
        def generate_presigned_url(self, method, Params, ExpiresIn, HttpMethod):
            calls.append((method, Params, ExpiresIn, HttpMethod))
            return f"https://example.test/{method}"

    monkeypatch.setattr(minio_client.boto3, "client", lambda *args, **kwargs: FakeBotoClient())

    client = minio_client.S3StorageClient(
        minio_client.StorageSettings(
            endpoint_url="https://s3.gwdg.de",
            access_key="access",
            secret_key="secret",
            region="us-east-1",
            bucket="benchmarking-suite",
        )
    )

    assert client.presigned_put_object("bucket", "key.txt", timedelta(seconds=60)).endswith("put_object")
    assert client.presigned_get_object("bucket", "key.txt", timedelta(seconds=30)).endswith("get_object")
    assert calls == [
        ("put_object", {"Bucket": "bucket", "Key": "key.txt"}, 60, "PUT"),
        ("get_object", {"Bucket": "bucket", "Key": "key.txt"}, 30, "GET"),
    ]


def test_s3_storage_client_lists_objects_with_minio_attribute_names(monkeypatch) -> None:
    """Object listings should keep the attributes consumed by API code."""

    class FakePaginator:
        def paginate(self, **kwargs):
            assert kwargs == {"Bucket": "bucket", "Prefix": "u1/"}
            yield {"Contents": [{"Key": "u1/a.txt", "Size": 7, "LastModified": None}]}

    class FakeBotoClient:
        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return FakePaginator()

    monkeypatch.setattr(minio_client.boto3, "client", lambda *args, **kwargs: FakeBotoClient())
    client = minio_client.S3StorageClient(
        minio_client.StorageSettings("https://s3.gwdg.de", "access", "secret", "us-east-1", "bucket")
    )

    objects = list(client.list_objects("bucket", "u1/", recursive=True))

    assert objects[0].object_name == "u1/a.txt"
    assert objects[0].size == 7
