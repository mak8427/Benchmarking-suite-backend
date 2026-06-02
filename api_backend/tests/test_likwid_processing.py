"""Tests for LIKWID parsing and energy alignment."""

from __future__ import annotations

import polars as pl

from analysis_module.processing.likwid import (
    match_elapsed_times,
    parse_likwid_csv,
    parse_likwid_stderr,
)
from analysis_module.processing import likwid_ingestion

HISTORICAL_STDERR = """
--------------------------------------------------------------------------------
CPU name:\tIntel(R) Xeon(R) Platinum 9242 CPU @ 2.30GHz
# GID|MetricsCount|CpuCount|Total runtime [s]|Runtime (RDTSC) [s]|Runtime unhalted [s]|Clock [MHz]|CPI|DP [MFLOP/s]|AVX DP [MFLOP/s]|AVX512 DP [MFLOP/s]|Packed [MUOPS/s]|Scalar [MUOPS/s]|Vectorization ratio [%]
1 10 1 1.000073 1.000035 0.865393 2668.010656 0.440459 0.010408 0.000248 0.000248 0.000035 0.010152 0.343575
coremark_mini digest=621589429 rounds=167 score=53236915.59 ops/s
--------------------------------------------------------------------------------
"""


def test_parse_historical_likwid_stderr_table() -> None:
    """Historical stderr should yield normalized LIKWID sample rows."""
    result = parse_likwid_stderr(HISTORICAL_STDERR)

    assert result.source_kind == "stderr"
    assert len(result.rows) == 1
    row = result.rows[0]
    assert row["likwid_group_id"] == 1
    assert row["metrics_count"] == 10
    assert row["cpu_count"] == 1
    assert row["elapsed_time_s"] == 1.000073
    assert row["clock_mhz"] == 2668.010656
    assert row["cpi"] == 0.440459
    assert row["dp_mflops"] == 0.010408
    assert row["vectorization_ratio_pct"] == 0.343575


def test_parse_future_likwid_csv() -> None:
    """Future CSV exports should use the same normalized column contract."""
    result = parse_likwid_csv(
        "GID,MetricsCount,CpuCount,Total runtime [s],Clock [MHz],CPI,DP [MFLOP/s]\n"
        "1,10,1,2.0,2880.5,0.42,123.4\n"
    )

    assert result.source_kind == "csv"
    assert result.rows == [
        {
            "likwid_group_id": 1,
            "metrics_count": 10,
            "cpu_count": 1,
            "elapsed_time_s": 2.0,
            "clock_mhz": 2880.5,
            "cpi": 0.42,
            "dp_mflops": 123.4,
        }
    ]


def test_match_likwid_elapsed_to_nearest_energy_sample() -> None:
    """LIKWID rows should align to nearby Slurm/HDF5 elapsed samples."""
    rows = [{"elapsed_time_s": 30.001, "cpi": 0.4}, {"elapsed_time_s": 200.0}]

    matched = match_elapsed_times(rows, [0.0, 30.0, 32.0], tolerance_s=5.0)

    assert matched[0]["matched_energy_elapsed_time_s"] == 30.0
    assert round(matched[0]["matched_energy_delta_s"], 3) == -0.001
    assert matched[1]["matched_energy_elapsed_time_s"] is None


def test_ingest_related_likwid_objects_matches_hdf5_elapsed(
    monkeypatch, tmp_path
) -> None:
    """HDF5 processing should ingest related LIKWID stderr siblings."""
    source = tmp_path / "slurm-14038010.err"
    source.write_text(HISTORICAL_STDERR, encoding="utf-8")
    written = []

    class Obj:
        object_name = "user-1/slurm-14038010.err"

    class Client:
        def list_objects(self, _bucket, prefix, recursive):
            assert prefix == "user-1/"
            assert recursive is True
            return [Obj()]

        def fget_object(self, _bucket, object_name, target):
            assert object_name == "user-1/slurm-14038010.err"
            from shutil import copyfile

            copyfile(source, target)

    def fake_metadata(_label):
        return {
            "object_key": "user-1/14038010_0_amp044.h5",
            "owner_user_id": "user-1",
            "owner_username": "alice",
            "original_filename": "14038010_0_amp044.h5",
            "benchmark_name": "coremark_mini",
        }

    def fake_write(_con, rows, **kwargs):
        written.append((rows, kwargs))

    monkeypatch.setattr(likwid_ingestion, "infer_owner_metadata", fake_metadata)
    monkeypatch.setattr(likwid_ingestion, "write_likwid_samples", fake_write)

    likwid_ingestion.ingest_related_likwid_objects(
        object(),
        pl.DataFrame({"ElapsedTime": [0.0, 1.0, 2.0]}),
        h5_file_label="user-1/14038010_0_amp044.h5",
        minio_client=Client(),
        minio_settings={"bucket": "benchmarking-suite"},
        logger=type(
            "L",
            (),
            {
                "debug": lambda *a, **k: None,
                "info": lambda *a, **k: None,
                "warning": lambda *a, **k: None,
            },
        )(),
    )

    assert len(written) == 1
    rows, kwargs = written[0]
    assert kwargs["source_object_key"] == "user-1/slurm-14038010.err"
    assert kwargs["source_kind"] == "stderr"
    assert rows[0]["matched_energy_elapsed_time_s"] == 1.0
