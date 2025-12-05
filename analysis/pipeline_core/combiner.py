"""Frame combination helpers."""

from __future__ import annotations

from typing import List, Tuple

import polars as pl


def combine_frames(frames: List[Tuple[str, pl.DataFrame]]) -> pl.DataFrame:
    """Merge task/energy frames on elapsed time and interpolate numeric fields."""

    timeline = (
        pl.concat([frame.select("ElapsedTime") for _, frame in frames], how="vertical")
        .unique()
        .sort("ElapsedTime")
        .with_columns(pl.col("ElapsedTime").cast(pl.UInt64))
    )

    combined = timeline
    for prefix, frame in frames:
        rename_map = {column: f"{prefix}__{column}" for column in frame.columns if column != "ElapsedTime"}
        joined = frame.rename(rename_map)
        combined = combined.join(joined, on="ElapsedTime", how="left")

    combined = combined.sort("ElapsedTime")

    interpolation_columns = [
        column
        for column, dtype in combined.schema.items()
        if column != "ElapsedTime" and getattr(dtype, "is_numeric", lambda: False)()
    ]

    if interpolation_columns:
        combined = combined.with_columns(
            [
                pl.col(column)
                .cast(pl.Float64)
                .interpolate()
                .alias(column)
                for column in interpolation_columns
            ]
        )
    return combined
