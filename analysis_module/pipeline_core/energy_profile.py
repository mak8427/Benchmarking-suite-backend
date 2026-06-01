"""Energy profile computation for combined frames."""

from __future__ import annotations

import math
from typing import Dict, Optional, Tuple

import polars as pl

from .energy_summary import describe_energy_use


def compute_energy_profile(
    df: pl.DataFrame,
    job_id: str,
    group_name: str,
    *,
    logger,
) -> Tuple[pl.DataFrame, Optional[Dict[str, float]]]:
    """Compute cumulative energy and power metrics for a combined dataframe.

    Args:
        df (pl.DataFrame): Combined dataframe with ``ElapsedTime`` and power/energy fields.
        job_id (str): Job identifier for logs and metrics.
        group_name (str): Group identifier for logs and metrics.
        logger: Logger-like object exposing ``info`` and ``warning`` methods.

    Returns:
        Tuple[pl.DataFrame, Optional[Dict[str, float]]]: Updated dataframe and
        summary metrics, or ``None`` metrics when computation is impossible.

    Examples:
        >>> class L:
        ...     def info(self, *args, **kwargs): pass
        ...     def warning(self, *args, **kwargs): pass
        >>> frame = pl.DataFrame({"ElapsedTime": [0.0, 1.0, 2.0], "NodePower": [10.0, 10.0, 10.0]})
        >>> output, metrics = compute_energy_profile(frame, "job-1", "g1", logger=L())
        >>> round(metrics["energy_to_solution_j"], 1)
        20.0
    """

    if df.is_empty():
        logger.warning(
            "Combined dataframe empty for job=%s group=%s; skipping energy metrics.",
            job_id,
            group_name,
        )
        return df, None

    power_cols = [
        col
        for col in df.columns
        if col == "NodePower" or col.endswith("__NodePower") or col.endswith("__CurrPower") or col == "CurrPower"
    ]
    energy_cols = [col for col in df.columns if col == "Energy" or col.endswith("__Energy")]

    power_column = power_cols[0] if power_cols else None
    energy_column = energy_cols[0] if energy_cols else None

    if not power_column and not energy_column:
        logger.warning(
            "No power or energy column found for job=%s group=%s; skipping energy metrics.",
            job_id,
            group_name,
        )
        return df, None

    df = df.with_columns(pl.col("ElapsedTime").cast(pl.Float64))
    before_rows = df.height
    df = df.filter((pl.col("ElapsedTime") >= 0) & (pl.col("ElapsedTime") < 10_000_000))
    dropped_rows = before_rows - df.height
    if dropped_rows:
        logger.warning(
            "Dropped %d invalid elapsed-time row(s) for job=%s group=%s before energy integration.",
            dropped_rows,
            job_id,
            group_name,
        )
    if df.is_empty():
        logger.warning(
            "No valid elapsed-time rows for job=%s group=%s; skipping energy metrics.",
            job_id,
            group_name,
        )
        return df, None

    if power_column:
        logger.info(
            "Energy profile for job=%s group=%s using power column %s",
            job_id,
            group_name,
            power_column,
        )
        df = df.with_columns(pl.col(power_column).cast(pl.Float64).alias("NodePower"))
        df = df.with_columns(pl.col("ElapsedTime").diff().fill_null(0.0).alias("ElapsedTime_Diff"))
        df = df.with_columns((pl.col("ElapsedTime_Diff") * pl.col("NodePower")).alias("Energy_Increment_J"))
        df = df.with_columns(pl.col("Energy_Increment_J").cum_sum().alias("Energy_used_J"))
    else:
        logger.info(
            "Energy profile for job=%s group=%s using cumulative energy column %s (power missing)",
            job_id,
            group_name,
            energy_column,
        )
        df = df.with_columns(pl.col(energy_column).cast(pl.Float64).alias("Energy_used_J"))
        df = df.with_columns(pl.col("Energy_used_J").diff().fill_null(0.0).alias("Energy_Increment_J"))
        df = df.with_columns(
            pl.when(pl.col("ElapsedTime").diff() > 0)
            .then(pl.col("Energy_Increment_J") / pl.col("ElapsedTime").diff())
            .otherwise(None)
            .alias("NodePower")
        )

    ets = df.select(pl.col("Energy_used_J").max()).item()
    tts = df.select(pl.col("ElapsedTime").max()).item()
    peak_row = df.select(["NodePower", "ElapsedTime"]).drop_nulls("NodePower").sort("NodePower", descending=True).head(1)

    peak_power = peak_row["NodePower"][0] if peak_row.height else float("nan")
    peak_time = peak_row["ElapsedTime"][0] if peak_row.height else float("nan")
    peak_power = float(peak_power) if peak_power is not None and not math.isnan(peak_power) else float("nan")
    peak_time = float(peak_time) if peak_time is not None and not math.isnan(peak_time) else float("nan")

    has_energy = ets is not None and not math.isnan(ets)
    avg_power = ets / tts if has_energy and tts and tts != 0 else float("nan")
    edp = ets * tts if has_energy and tts else float("nan")

    appliance_name, appliance_amount, appliance_unit = describe_energy_use(ets, return_tuple=True)

    metrics = {
        "energy_to_solution_j": ets,
        "time_to_solution_s": tts,
        "average_power_w": avg_power,
        "peak_power_w": peak_power,
        "peak_power_time_s": peak_time,
        "energy_delay_product": edp,
        "appliance_name": appliance_name,
        "appliance_amount": appliance_amount,
        "appliance_unit": appliance_unit,
        "appliance_description": describe_energy_use(ets),
    }
    return df, metrics
