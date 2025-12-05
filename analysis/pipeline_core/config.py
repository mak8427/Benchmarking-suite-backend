"""
Configuration parsing and logging initialisation.
"""

from __future__ import annotations

import argparse
import dataclasses
import os
from pathlib import Path
from typing import Optional


def build_parser() -> argparse.ArgumentParser:
    """Construct and return the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Parse GROM SLURM energy HDF5 files and export CSV data and summary stats.",
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=Path("u18101"),
        help="Directory containing input .h5 files (default: u18101 relative to this script).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Directory for exported CSV data (default: output relative to this script).",
    )
    parser.add_argument(
        "--stats-dir",
        type=Path,
        default=Path("stats"),
        help="Directory for CSV stats summaries (default: stats relative to this script).",
    )
    parser.add_argument(
        "--summary-dir",
        type=Path,
        default=Path("summaries"),
        help="Directory for per-job metric summaries (default: summaries relative to this script).",
    )
    parser.add_argument(
        "--price-dir",
        type=Path,
        default=Path("prices"),
        help="Directory for fetched SMARD price series (default: prices relative to this script).",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=Path("analysis.log"),
        help="File path for detailed processing logs (default: analysis.log relative to this script).",
    )
    parser.add_argument(
        "--fetch-price",
        action="store_true",
        help="When set, fetch SMARD market data to estimate cumulative job cost.",
    )
    parser.add_argument(
        "--price-filter-id",
        type=int,
        default=4169,
        help="SMARD filter identifier (default: 4169 – Day-ahead auction).",
    )
    parser.add_argument(
        "--price-region",
        type=str,
        default="DE-LU",
        help="SMARD bidding zone/region (default: DE-LU).",
    )
    parser.add_argument(
        "--price-resolution",
        type=str,
        default="quarterhour",
        help="SMARD resolution string (default: quarterhour).",
    )
    parser.add_argument(
        "--allow-missing-source",
        action="store_true",
        help="Skip local source directory validation (useful for MinIO/S3 inputs).",
    )
    return parser


@dataclasses.dataclass(slots=True)
class PriceSettings:
    """SMARD pricing configuration."""

    filter_id: int
    region: str
    resolution: str


@dataclasses.dataclass(slots=True)
class PipelineConfig:
    """Aggregated runtime configuration for the analysis pipeline."""

    base_dir: Path
    source_dir: Path
    output_dir: Path
    stats_dir: Path
    summary_dir: Path
    price_dir: Path
    log_file: Path
    fetch_price: bool
    price: PriceSettings
    allow_missing_source: bool

    @classmethod
    def from_args(cls, args: argparse.Namespace, base_dir: Path) -> "PipelineConfig":
        """Create a pipeline config from parsed CLI arguments."""

        def resolve(path: Path) -> Path:
            return path if path.is_absolute() else base_dir / path

        return cls(
            base_dir=base_dir,
            source_dir=resolve(args.source),
            output_dir=resolve(args.output_dir),
            stats_dir=resolve(args.stats_dir),
            summary_dir=resolve(args.summary_dir),
            price_dir=resolve(args.price_dir),
            log_file=resolve(args.log_file),
            fetch_price=args.fetch_price,
            allow_missing_source=args.allow_missing_source,
            price=PriceSettings(
                filter_id=args.price_filter_id,
                region=args.price_region,
                resolution=args.price_resolution,
            ),
        )


def ensure_directories(config: PipelineConfig) -> None:
    """Ensure all output directories requested in *config* exist."""

    config.output_dir.mkdir(parents=True, exist_ok=True)
    config.stats_dir.mkdir(parents=True, exist_ok=True)
    config.summary_dir.mkdir(parents=True, exist_ok=True)
    if config.fetch_price:
        config.price_dir.mkdir(parents=True, exist_ok=True)


def validate_source(config: PipelineConfig) -> None:
    """Raise if the configured source directory does not exist."""
    if config.allow_missing_source or os.getenv("MINIO_SYNC"):
        return
    if not config.source_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {config.source_dir}")
