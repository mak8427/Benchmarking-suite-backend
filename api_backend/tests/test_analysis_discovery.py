"""Tests for analysis pipeline S3 discovery glue."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from analysis_module.connectors import discovery
from analysis_module.pipeline_core.config import PriceSettings, PipelineConfig, validate_source


class Logger:
    """Minimal logger used by discovery tests."""

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass


def _config(base: Path) -> PipelineConfig:
    return PipelineConfig(
        base_dir=base,
        source_dir=base / "missing-source",
        output_dir=base / "out",
        stats_dir=base / "stats",
        summary_dir=base / "summary",
        price_dir=base / "price",
        log_file=base / "analysis.log",
        allow_missing_source=False,
        price=PriceSettings(4169, "DE-LU", "quarterhour"),
        fetch_price=False,
    )


def test_validate_source_allows_s3_sync(monkeypatch, tmp_path) -> None:
    """Remote S3 runs should not require the local source directory to exist."""
    monkeypatch.setenv("S3_SYNC", "1")
    validate_source(_config(tmp_path))


def test_discover_h5_files_uses_collect_signature(monkeypatch, tmp_path) -> None:
    """Discovery passes the required keep_batch_files argument."""
    local = tmp_path / "job_1.h5"
    local.write_bytes(b"placeholder")
    calls = []

    def fake_collect(config, keep_batch_files):
        calls.append(keep_batch_files)
        return [local]

    monkeypatch.setattr(discovery, "collect_h5_files", fake_collect)
    monkeypatch.delenv("S3_SYNC", raising=False)
    monkeypatch.delenv("MINIO_SYNC", raising=False)

    files = discovery.discover_h5_files(
        _config(tmp_path),
        minio_client=object(),
        minio_settings={"bucket": "b", "prefix": ""},
        logger=Logger(),
    )

    assert calls == [False]
    assert files == [("job_1", local)]
