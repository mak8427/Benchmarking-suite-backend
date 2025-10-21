"""
Script entry point for the analysis pipeline.
"""

from __future__ import annotations

from pathlib import Path

from analysis_pipeline import (
    PipelineConfig,
    build_parser,
    collect_h5_files,
    configure_logging,
    ensure_directories,
    process_h5_file,
    validate_source,
)


def main() -> None:
    """CLI entry point orchestrating the analysis workflow."""
    parser = build_parser()
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent
    config = PipelineConfig.from_args(args, base_dir=base_dir)

    logger = configure_logging(config.log_file)
    logger.info("Step 1/4: validating configuration and preparing directories.")
    validate_source(config)
    ensure_directories(config)

    logger.info("Step 2/4: discovering input HDF5 files under %s.", config.source_dir)
    h5_files = collect_h5_files(config)
    if not h5_files:
        logger.warning("No .h5 files found in %s. Nothing to process.", config.source_dir)
        return
    logger.info("Detected %d file(s) for analysis.", len(h5_files))

    logger.info("Step 3/4: analysing discovered jobs.")
    for index, file_path in enumerate(h5_files, start=1):
        logger.info("  Step 3.%d: analysing %s", index, file_path.name)
        process_h5_file(file_path, config, logger=logger)

    logger.info("Step 4/4: pipeline complete. Artifacts available under %s.", config.base_dir)


if __name__ == "__main__":
    main()
