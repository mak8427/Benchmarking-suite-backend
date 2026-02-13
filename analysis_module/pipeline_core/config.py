"""Configuration parsing and runtime validation for the analysis pipeline."""

from __future__ import annotations

import argparse
import dataclasses
import os
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG_PATH = Path("pipeline_config.yml")


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML config file from disk.

    Args:
        path (Path): Configuration file path.

    Returns:
        dict[str, Any]: Parsed config mapping.

    Examples:
        >>> from tempfile import TemporaryDirectory
        >>> with TemporaryDirectory() as tmp:
        ...     cfg = Path(tmp) / "pipeline.yml"
        ...     _ = cfg.write_text("fetch_price: true\\n", encoding="utf-8")
        ...     _load_yaml(cfg)["fetch_price"]
        True
    """
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Config file must contain a mapping: {path}")
    return payload


def build_parser() -> argparse.ArgumentParser:
    """Construct and return the CLI argument parser.

    Returns:
        argparse.ArgumentParser: Parser for runtime configuration.

    Examples:
        >>> parser = build_parser()
        >>> parser.parse_args(["--source", "inputs"]).source
        PosixPath('inputs')
    """
    parser = argparse.ArgumentParser(
        description="Parse GROM SLURM energy HDF5 files and export analysis tables.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="YAML config path (default: pipeline_config.yml).",
    )
    parser.add_argument("--source", type=Path, default=None, help="Override source .h5 directory.")
    parser.add_argument("--output-dir", type=Path, default=None, help="Override output CSV directory.")
    parser.add_argument("--stats-dir", type=Path, default=None, help="Override stats directory.")
    parser.add_argument("--summary-dir", type=Path, default=None, help="Override summary directory.")
    parser.add_argument("--price-dir", type=Path, default=None, help="Override price data directory.")
    parser.add_argument("--log-file", type=Path, default=None, help="Override log output file.")
    parser.add_argument(
        "--fetch-price",
        action="store_true",
        default=None,
        help="Enable market price integration.",
    )
    parser.add_argument("--price-filter-id", type=int, default=None, help="SMARD filter id.")
    parser.add_argument("--price-region", type=str, default=None, help="SMARD region.")
    parser.add_argument("--price-resolution", type=str, default=None, help="SMARD resolution.")
    parser.add_argument(
        "--allow-missing-source",
        action="store_true",
        default=None,
        help="Skip local source-dir existence check.",
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
        """Create a pipeline config from parsed CLI arguments and optional YAML.

        Args:
            args (argparse.Namespace): Parsed command-line arguments.
            base_dir (Path): Base directory for relative path resolution.

        Returns:
            PipelineConfig: Materialized runtime config.

        Examples:
            >>> parser = build_parser()
            >>> namespace = parser.parse_args(["--source", "raw", "--fetch-price"])
            >>> cfg = PipelineConfig.from_args(namespace, Path("."))
            >>> cfg.source_dir.name
            'raw'
        """

        config_path = args.config if args.config.is_absolute() else base_dir / args.config
        file_values = _load_yaml(config_path)

        def _pick(name: str, default: Any) -> Any:
            """Choose argument value, then YAML value, then fallback default.

            Args:
                name (str): Field name to resolve.
                default (Any): Final fallback value.

            Returns:
                Any: Selected value for the field.
            """
            value = getattr(args, name)
            if value is not None:
                return value
            if name in file_values:
                return file_values[name]
            return default

        def _resolve(path_like: Any, fallback: str) -> Path:
            """Resolve a potentially relative path against ``base_dir``.

            Args:
                path_like (Any): Candidate path-like value.
                fallback (str): Default relative path when missing.

            Returns:
                Path: Absolute or base-dir-resolved path.
            """
            selected = Path(path_like or fallback)
            return selected if selected.is_absolute() else base_dir / selected

        return cls(
            base_dir=base_dir,
            source_dir=_resolve(_pick("source", "u18101"), "u18101"),
            output_dir=_resolve(_pick("output_dir", "output"), "output"),
            stats_dir=_resolve(_pick("stats_dir", "stats"), "stats"),
            summary_dir=_resolve(_pick("summary_dir", "summaries"), "summaries"),
            price_dir=_resolve(_pick("price_dir", "prices"), "prices"),
            log_file=_resolve(_pick("log_file", "analysis.log"), "analysis.log"),
            fetch_price=bool(_pick("fetch_price", False)),
            allow_missing_source=bool(_pick("allow_missing_source", False)),
            price=PriceSettings(
                filter_id=int(_pick("price_filter_id", 4169)),
                region=str(_pick("price_region", "DE-LU")),
                resolution=str(_pick("price_resolution", "quarterhour")),
            ),
        )


def ensure_directories(config: PipelineConfig) -> None:
    """Ensure all output directories requested in `config` exist.

    Args:
        config (PipelineConfig): Pipeline runtime configuration.

    Examples:
        >>> from tempfile import TemporaryDirectory
        >>> with TemporaryDirectory() as tmp:
        ...     base = Path(tmp)
        ...     cfg = PipelineConfig(base, base, base / "out", base / "stats", base / "summary", base / "price", base / "run.log", True, PriceSettings(1, "DE-LU", "quarterhour"), True)
        ...     ensure_directories(cfg)
        ...     cfg.output_dir.exists() and cfg.price_dir.exists()
        True
    """
    config.output_dir.mkdir(parents=True, exist_ok=True)
    config.stats_dir.mkdir(parents=True, exist_ok=True)
    config.summary_dir.mkdir(parents=True, exist_ok=True)
    if config.fetch_price:
        config.price_dir.mkdir(parents=True, exist_ok=True)


def validate_source(config: PipelineConfig) -> None:
    """Raise when a required source directory is missing.

    Args:
        config (PipelineConfig): Pipeline runtime configuration.

    Raises:
        FileNotFoundError: Raised when source directory does not exist.

    Examples:
        >>> from tempfile import TemporaryDirectory
        >>> with TemporaryDirectory() as tmp:
        ...     base = Path(tmp)
        ...     cfg = PipelineConfig(base, base / "missing", base / "out", base / "stats", base / "summary", base / "price", base / "run.log", False, PriceSettings(1, "DE-LU", "quarterhour"), True)
        ...     validate_source(cfg)
    """
    if config.allow_missing_source or os.getenv("MINIO_SYNC"):
        return
    if not config.source_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {config.source_dir}")
