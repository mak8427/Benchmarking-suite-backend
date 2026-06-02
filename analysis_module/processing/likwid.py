"""LIKWID output parsing and elapsed-time alignment helpers."""

from __future__ import annotations

import csv
import math
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Iterable

COLUMN_ALIASES = {
    "GID": "likwid_group_id",
    "MetricsCount": "metrics_count",
    "CpuCount": "cpu_count",
    "Total runtime [s]": "elapsed_time_s",
    "Runtime (RDTSC) [s]": "runtime_rdtsc_s",
    "Runtime unhalted [s]": "runtime_unhalted_s",
    "Clock [MHz]": "clock_mhz",
    "CPI": "cpi",
    "DP [MFLOP/s]": "dp_mflops",
    "AVX DP [MFLOP/s]": "avx_dp_mflops",
    "AVX512 DP [MFLOP/s]": "avx512_dp_mflops",
    "Packed [MUOPS/s]": "packed_muops_s",
    "Scalar [MUOPS/s]": "scalar_muops_s",
    "Vectorization ratio [%]": "vectorization_ratio_pct",
}

NUMERIC_COLUMNS = set(COLUMN_ALIASES.values())


@dataclass(frozen=True)
class LikwidParseResult:
    """Parsed LIKWID rows and lightweight diagnostics."""

    rows: list[dict[str, float | int | None]]
    source_kind: str
    ignored_rows: int = 0


def _parse_float(value: str) -> float | None:
    value = value.strip()
    if not value or value == "-":
        return None
    try:
        number = float(value)
    except ValueError:
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _normalise_row(
    headers: list[str], values: list[str]
) -> dict[str, float | int | None] | None:
    if len(headers) != len(values):
        return None
    row: dict[str, float | int | None] = {}
    for header, raw_value in zip(headers, values):
        name = COLUMN_ALIASES.get(header.strip())
        if not name:
            continue
        value = _parse_float(raw_value)
        if name in {"likwid_group_id", "metrics_count", "cpu_count"}:
            row[name] = int(value) if value is not None else None
        else:
            row[name] = value
    elapsed = row.get("elapsed_time_s")
    if elapsed is None or elapsed < 0 or elapsed > 3600:
        return None
    return row


def parse_likwid_stderr(text: str) -> LikwidParseResult:
    """Parse historical LIKWID table blocks embedded in Slurm stderr."""
    headers: list[str] | None = None
    rows: list[dict[str, float | int | None]] = []
    ignored = 0
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("# GID|"):
            headers = [part.strip() for part in stripped[2:].split("|")]
            continue
        if not headers or not stripped or stripped.startswith("-"):
            continue
        parsed = _normalise_row(headers, stripped.split())
        if parsed is None:
            ignored += 1
            continue
        rows.append(parsed)
    return LikwidParseResult(rows=rows, source_kind="stderr", ignored_rows=ignored)


def parse_likwid_csv(text: str) -> LikwidParseResult:
    """Parse future LIKWID CSV exports with stable header names."""
    stream = StringIO(text)
    sample = text[:2048]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;|")
    except csv.Error:
        dialect = csv.excel
    reader = csv.DictReader(stream, dialect=dialect)
    if not reader.fieldnames:
        return LikwidParseResult(rows=[], source_kind="csv", ignored_rows=0)
    headers = [header.strip() for header in reader.fieldnames]
    rows: list[dict[str, float | int | None]] = []
    ignored = 0
    for raw_row in reader:
        values = [raw_row.get(header, "") or "" for header in reader.fieldnames]
        parsed = _normalise_row(headers, values)
        if parsed is None:
            ignored += 1
            continue
        rows.append(parsed)
    return LikwidParseResult(rows=rows, source_kind="csv", ignored_rows=ignored)


def parse_likwid_file(path: Path) -> LikwidParseResult:
    """Parse a LIKWID CSV or historical Slurm stderr file."""
    text = path.read_text(errors="replace")
    if path.suffix.lower() == ".csv" and "# GID|" not in text:
        return parse_likwid_csv(text)
    return parse_likwid_stderr(text)


def match_elapsed_times(
    likwid_rows: Iterable[dict[str, float | int | None]],
    energy_elapsed_times: Iterable[float],
    *,
    tolerance_s: float = 5.0,
) -> list[dict[str, float | int | None]]:
    """Attach nearest HDF5 energy elapsed time to each LIKWID row."""
    energy_values = sorted(
        float(value) for value in energy_elapsed_times if 0 <= float(value) <= 3600
    )
    matched: list[dict[str, float | int | None]] = []
    for row in likwid_rows:
        elapsed = row.get("elapsed_time_s")
        output = dict(row)
        output["matched_energy_elapsed_time_s"] = None
        output["matched_energy_delta_s"] = None
        if elapsed is not None and energy_values:
            nearest = min(energy_values, key=lambda value: abs(value - float(elapsed)))
            delta = nearest - float(elapsed)
            if abs(delta) <= tolerance_s:
                output["matched_energy_elapsed_time_s"] = nearest
                output["matched_energy_delta_s"] = delta
        matched.append(output)
    return matched
