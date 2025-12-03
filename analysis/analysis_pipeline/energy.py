"""
Energy metric derivations and summary builders.
"""

from __future__ import annotations

import math
from typing import Dict, List, Optional, Tuple

import polars as pl


APPLIANCE_CATALOG: List[Tuple[str, int]] = [
    ("a hair dryer", 1800),
    ("a microwave oven", 1200),
    ("an electric kettle", 1500),
    ("a vacuum cleaner", 800),
    ("a refrigerator", 150),
    ("a light bulb", 60),
    ("a washing machine", 500),
    ("a gaming PC", 400),
    ("a laptop", 60),
    ("a dishwasher", 1000),
]


def describe_energy_use(joules: float, return_tuple: bool = False):
    """Translate energy usage into an everyday comparison.

    Args:
        joules: Energy expenditure expressed in joules.
        return_tuple: Whether to return a tuple `(appliance, amount, unit)`
            instead of a formatted sentence.

    Returns:
        Either a descriptive string or a tuple describing the equivalent
        appliance usage.
    """
    if joules is None or joules <= 0 or math.isnan(joules):
        message = "Energy use too small to compare with everyday appliances."
        return ("negligible usage", 0.0, "s") if return_tuple else message

    best_name = "a light bulb"
    best_seconds = joules / 60  # default to light bulb
    target_minutes = 10.0
    best_delta = float("inf")

    for name, watts in APPLIANCE_CATALOG:
        seconds = joules / watts
        if seconds < 1:
            continue
        minutes = seconds / 60.0
        delta = abs(minutes - target_minutes)
        if 1 <= minutes <= 120 and delta < best_delta:
            best_name = name
            best_seconds = seconds
            best_delta = delta

    if math.isinf(best_delta):
        name, watts = min(
            APPLIANCE_CATALOG,
            key=lambda item: abs((joules / item[1]) / 60.0 - target_minutes),
        )
        best_name = name
        best_seconds = joules / watts

    if best_seconds < 60:
        amount = round(best_seconds, 1)
        unit = "s"
        human_time = f"{amount} seconds"
    elif best_seconds < 3600:
        amount = round(best_seconds / 60.0, 1)
        unit = "m"
        human_time = f"{amount} minutes"
    else:
        amount = round(best_seconds / 3600.0, 2)
        unit = "h"
        human_time = f"{amount} hours"

    if return_tuple:
        return best_name, amount, unit

    return f"That's about the same energy as using {best_name} for {human_time}."


def add_task_derivatives(df: pl.DataFrame) -> pl.DataFrame:
    """Append derived columns such as RSS in megabytes.

    Args:
        df: Combined Polars DataFrame containing raw metrics.

    Returns:
        The input DataFrame augmented with derived columns.
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


def compute_energy_profile(
    df: pl.DataFrame,
    job_id: str,
    group_name: str,
    *,
    logger,
) -> Tuple[pl.DataFrame, Optional[Dict[str, float]]]:
    """Compute cumulative energy metrics for a combined dataframe.

    Args:
        df: Input DataFrame containing merged job metrics.
        job_id: Identifier of the job being processed.
        group_name: HDF5 group identifier within the file.
        logger: Logger used to emit diagnostic messages.

    Returns:
        A tuple containing the mutated DataFrame and a dictionary of energy
        metrics suitable for summary reporting. The dictionary is ``None`` if
        the required data is missing.
    """
    if df.is_empty():
        logger.warning(
            "Combined dataframe empty for job=%s group=%s; skipping energy metrics.",
            job_id,
            group_name,
        )
        return df, None

    node_power_columns = [
        col
        for col in df.columns
        if col == "NodePower" or col.endswith("__NodePower") or col.endswith("__CurrPower") or col == "CurrPower"
    ]
    energy_columns = [
        col for col in df.columns if col == "Energy" or col.endswith("__Energy")
    ]

    power_column = node_power_columns[0] if node_power_columns else None
    energy_column = energy_columns[0] if energy_columns else None

    if not power_column and not energy_column:
        logger.warning(
            "No power or energy column found for job=%s group=%s; skipping energy metrics.",
            job_id,
            group_name,
        )
        return df, None

    df = df.with_columns(pl.col("ElapsedTime").cast(pl.Float64))

    if power_column:
        logger.info(
            "Energy profile for job=%s group=%s using power column %s",
            job_id,
            group_name,
            power_column,
        )
        df = df.with_columns(pl.col(power_column).cast(pl.Float64).alias("NodePower"))
        df = df.with_columns(
            pl.col("ElapsedTime").diff().fill_null(0.0).alias("ElapsedTime_Diff")
        )
        df = df.with_columns(
            (pl.col("ElapsedTime_Diff") * pl.col("NodePower")).alias("Energy_Increment_J")
        )
        df = df.with_columns(
            pl.col("Energy_Increment_J").cum_sum().alias("Energy_used_J")
        )
    else:
        # Use cumulative energy directly when power is missing
        logger.info(
            "Energy profile for job=%s group=%s using cumulative energy column %s (power missing)",
            job_id,
            group_name,
            energy_column,
        )
        df = df.with_columns(
            pl.col(energy_column).cast(pl.Float64).alias("Energy_used_J")
        )
        df = df.with_columns(
            pl.col("Energy_used_J").diff().fill_null(0.0).alias("Energy_Increment_J")
        )
        df = df.with_columns(
            pl.when(pl.col("ElapsedTime").diff() > 0)
            .then(pl.col("Energy_Increment_J") / pl.col("ElapsedTime").diff())
            .otherwise(None)
            .alias("NodePower")
        )

    ets = df.select(pl.col("Energy_used_J").max()).item()
    tts = df.select(pl.col("ElapsedTime").max()).item()
    peak_row = (
        df.select(["NodePower", "ElapsedTime"])
        .drop_nulls("NodePower")
        .sort("NodePower", descending=True)
        .head(1)
    )

    peak_power = peak_row["NodePower"][0] if peak_row.height else float("nan")
    peak_time = peak_row["ElapsedTime"][0] if peak_row.height else float("nan")

    peak_power = (
        float(peak_power) if peak_power is not None and not math.isnan(peak_power) else float("nan")
    )
    peak_time = (
        float(peak_time) if peak_time is not None and not math.isnan(peak_time) else float("nan")
    )

    avg_power = ets / tts if tts and tts != 0 else float("nan")
    edp = ets * tts if tts and not math.isnan(ets) else float("nan")

    appliance_name, appliance_amount, appliance_unit = describe_energy_use(
        ets, return_tuple=True
    )

    logger.info(
        "Energy summary for job=%s group=%s: energy_to_solution_j=%.2f, time_to_solution_s=%.2f, "
        "avg_power_w=%.2f, peak_power_w=%.2f at t=%.2f",
        job_id,
        group_name,
        ets,
        tts,
        avg_power,
        peak_power,
        peak_time,
    )

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


def build_summary_dataframe(
    job_id: str,
    group_name: str,
    metrics: Dict[str, float],
) -> pl.DataFrame:
    """Create a tabular summary from computed metrics.

    Args:
        job_id: Identifier of the processed job.
        group_name: HDF5 group identifier covered by the summary.
        metrics: Calculated energy metrics dictionary.

    Returns:
        A Polars DataFrame with human-readable metric descriptions.
    """
    ets = metrics["energy_to_solution_j"]
    tts = metrics["time_to_solution_s"]
    avg_power = metrics["average_power_w"]
    peak_power = metrics["peak_power_w"]
    peak_time = metrics["peak_power_time_s"]
    edp = metrics["energy_delay_product"]
    appliance_name = metrics["appliance_name"]
    appliance_amount = metrics["appliance_amount"]
    appliance_unit = metrics["appliance_unit"]

    units_map = {"s": "seconds", "m": "minutes", "h": "hours"}
    appliance_unit_human = units_map.get(appliance_unit, appliance_unit)

    rows = [
        (
            "Energy-to-solution",
            f"{ets:.2f}",
            "J",
            "Total energy consumed by the job from start to finish.",
        ),
        (
            "Time-to-solution",
            f"{tts:.2f}",
            "s",
            "Total runtime of the job (wall-clock time from start to end).",
        ),
        (
            "Average power",
            f"{avg_power:.2f}",
            "W",
            "Mean power draw during the job.",
        ),
        (
            "Peak power",
            f"{peak_power:.2f} at {peak_time:.2f}s",
            "W",
            "Maximum instantaneous power draw observed during execution.",
        ),
        (
            "Energy-Delay Product (EDP)",
            f"{edp:.2f}",
            "J·s",
            "Energy-to-solution × Time-to-solution (lower is better).",
        ),
        (
            f"Equivalent to running {appliance_name}",
            f"{appliance_amount:.2f}",
            appliance_unit_human,
            "Everyday appliance analogy for the job's total energy.",
        ),
    ]

    summary_df = pl.DataFrame(
        rows,
        schema=["Metric", "Value", "Unit", "Definition"],
        orient="row",
    ).with_columns(
        pl.col("Metric").cast(pl.Utf8),
        pl.col("Value").cast(pl.Utf8),
        pl.col("Unit").cast(pl.Utf8),
        pl.col("Definition").cast(pl.Utf8),
    )

    return summary_df.with_columns(
        pl.lit(job_id).alias("JobID"),
        pl.lit(group_name).alias("Group"),
    )
