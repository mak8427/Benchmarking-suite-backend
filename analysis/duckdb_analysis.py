
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




if __name__ ==  "__main__":
    #1) Load the data
    base_dir = Path(__file__).resolve().parent
    print(f"Base directory: {base_dir}")
    config = PipelineConfig.from_args(build_parser().parse_args([]), base_dir=base_dir)
    print(f"Config: {config}")