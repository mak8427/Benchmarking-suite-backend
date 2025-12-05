"""Summary helpers for energy metrics."""

from __future__ import annotations

from typing import Dict, List, Tuple

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
    """Translate energy usage into an everyday comparison."""
    import math

    if joules is None or joules <= 0 or math.isnan(joules):
        message = "Energy use too small to compare with everyday appliances."
        return ("negligible usage", 0.0, "s") if return_tuple else message

    best_name = "a light bulb"
    best_seconds = joules / 60
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
    elif best_seconds < 3600:
        amount = round(best_seconds / 60.0, 1)
        unit = "m"
    else:
        amount = round(best_seconds / 3600.0, 2)
        unit = "h"

    if return_tuple:
        return best_name, amount, unit

    human_time = f"{amount} {'seconds' if unit=='s' else 'minutes' if unit=='m' else 'hours'}"
    return f"That's about the same energy as using {best_name} for {human_time}."


def build_summary_dataframe(
    job_id: str,
    group_name: str,
    metrics: Dict[str, float],
) -> pl.DataFrame:
    """Create a tabular summary from computed metrics."""

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
        ("Time-to-solution", f"{tts:.2f}", "s", "Elapsed time for the job."),
        ("Average power", f"{avg_power:.2f}", "W", "Mean power draw."),
        ("Peak power", f"{peak_power:.2f}", "W", f"Peak power observed at t={peak_time:.2f}s."),
        ("Energy delay product", f"{edp:.2f}", "J*s", "Energy-delay product."),
        (
            "Appliance comparison",
            f"{appliance_amount:.2f}",
            appliance_unit_human,
            f"Equivalent to running {appliance_name}.",
        ),
    ]

    return pl.DataFrame(
        rows,
        schema=["Metric", "Value", "Unit", "Description"],
    )
