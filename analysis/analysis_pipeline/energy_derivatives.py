"""Derived column helpers for task metrics."""

from __future__ import annotations

import polars as pl


def add_task_derivatives(df: pl.DataFrame) -> pl.DataFrame:
    """Append derived task columns such as RSS in megabytes and normalized CPU util."""

    derived_columns = []
    for column in df.columns:
        if column.endswith("__RSS"):
            derived_columns.append(
                (pl.col(column) / 1024.0).alias(column.replace("__RSS", "__RSS_MB"))
            )
        if column.endswith("__CPUUtilization"):
            derived_columns.append(
                (pl.col(column) / 32.0).alias(
                    column.replace("__CPUUtilization", "__CPUUtilization_normalized")
                )
            )
    if derived_columns:
        df = df.with_columns(derived_columns)
    return df
