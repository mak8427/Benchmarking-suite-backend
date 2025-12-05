from __future__ import annotations

import polars as pl

from analysis.utils.common import timing_decorator


@timing_decorator
def cast_all_columns(df: pl.DataFrame, *, logger=None) -> pl.DataFrame:
    """Cast columns to optimized types based on naming conventions."""

    cast_exprs = []

    for column in df.columns:
        if column == "ElapsedTime":
            MAX_ELAPSED = 2**63 - 1
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, MAX_ELAPSED))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.UInt64)
                .alias(column)
            )
        elif column == "ElapsedTime_Diff":
            MAX_ELAPSED_DIFF = 365 * 24 * 3600 * 1000  # 1 year in ms
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, MAX_ELAPSED_DIFF))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.UInt64)
                .alias(column)
            )
        elif column == "EpochTime" or column.endswith("__EpochTime"):
            MIN_EPOCH, MAX_EPOCH = 0, 4102444800
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(MIN_EPOCH, MAX_EPOCH))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Int64)
                .alias(column)
            )
        elif column.endswith("__CPUFrequency"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, 10000))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.UInt16)
                .alias(column)
            )
        elif column.endswith("__RSS") or column.endswith("__VMSize"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, 2**63 - 1))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.UInt64)
                .alias(column)
            )
        elif column.endswith("__GPUMemMB") or column.endswith("__RSS_MB"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, 1e6))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Float32)
                .alias(column)
            )
        elif column.endswith("__Pages"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, 2**32 - 1))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.UInt32)
                .alias(column)
            )
        elif column == "NodePower" or column.endswith("__NodePower"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, 10000))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Float32)
                .alias(column)
            )
        elif column == "CurrPower" or column.endswith("__CurrPower"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, 10000))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Float32)
                .alias(column)
            )
        elif column in ("Energy_Increment_J", "Energy_used_J"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & (pl.col(column) >= 0))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Float32)
                .alias(column)
            )
        elif column == "Energy" or column.endswith("__Energy"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & (pl.col(column) >= 0))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Float32)
                .alias(column)
            )
        elif column.endswith("__CPUUtilization") or column.endswith("__GPUUtilization"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, 100))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Float32)
                .alias(column)
            )
        elif column.endswith("__CPUUtilization_normalized"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & pl.col(column).is_between(0, 1))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Float32)
                .alias(column)
            )
        elif column.endswith("__CPUTime"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & (pl.col(column) >= 0))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Float32)
                .alias(column)
            )
        elif column.endswith("__ReadMB") or column.endswith("__WriteMB"):
            cast_exprs.append(
                pl.when(pl.col(column).is_finite() & (pl.col(column) >= 0))
                .then(pl.col(column))
                .otherwise(None)
                .cast(pl.Float32)
                .alias(column)
            )

    if cast_exprs:
        df = df.with_columns(cast_exprs)

    return df
