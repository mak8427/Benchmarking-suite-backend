"""Derived column helpers for task metrics."""

from __future__ import annotations

import polars as pl


def add_task_derivatives(df: pl.DataFrame) -> pl.DataFrame:
    """Append derived task columns such as RSS MB and normalized CPU usage.

    Args:
        df (pl.DataFrame): Combined dataframe with raw task telemetry columns.

    Returns:
        pl.DataFrame: Dataframe with additional derived columns when available.

    Examples:
        >>> frame = pl.DataFrame({"Task__RSS": [1024.0], "Task__CPUUtilization": [16.0]})
        >>> out = add_task_derivatives(frame)
        >>> sorted(out.columns)
        ['Task__CPUUtilization', 'Task__CPUUtilization_normalized', 'Task__RSS', 'Task__RSS_MB']
    """

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
