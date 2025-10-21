"""
Utilities for retrieving SMARD price series and merging them into the dataset.
"""

from __future__ import annotations

import bisect
import os
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Tuple

import polars as pl

try:
    import requests
except ImportError:  # pragma: no cover - optional dependency
    requests = None


def determine_series_blocks(
    min_timestamp: int, max_timestamp: int, available: List[int]
) -> List[int]:
    """Select SMARD block start times that cover the required interval.

    Args:
        min_timestamp: Earliest epoch (seconds) required.
        max_timestamp: Latest epoch (seconds) required.
        available: Sorted list of available block start times.

    Returns:
        Sorted list of block start times to request.
    """
    if not available:
        return []

    available = sorted(available)
    idx = bisect.bisect_left(available, min_timestamp)
    blocks = set()

    if idx < len(available):
        blocks.add(available[idx])
    if idx > 0:
        blocks.add(available[idx - 1])

    for ts in available[idx:]:
        if ts <= max_timestamp:
            blocks.add(ts)
        else:
            break

    return sorted(blocks)


def fetch_smard_prices(
    epoch_times: pl.Series,
    *,
    filter_id: int,
    region: str,
    resolution: str,
    logger,
) -> Optional[pl.DataFrame]:
    """Fetch SMARD price data covering ``epoch_times`` if possible.

    Args:
        epoch_times: Series of epoch timestamps to cover.
        filter_id: Identifier of the SMARD dataset to query.
        region: Bidding zone requested.
        resolution: Resolution string (e.g. ``quarterhour``).
        logger: Logger instance for status messages.

    Returns:
        Price DataFrame aligned on seconds, or ``None`` if fetching fails.
    """
    if requests is None:
        logger.warning("requests package not available; skipping SMARD price fetch.")
        return None

    if epoch_times.is_null().all():
        logger.warning("Epoch time series is empty; skipping SMARD price fetch.")
        return None

    min_ts = int(epoch_times.min())
    max_ts = int(epoch_times.max())
    index_url = f"https://www.smard.de/app/chart_data/{filter_id}/{region}/index_{resolution}.json"

    try:
        index_response = requests.get(index_url, timeout=10)
        index_response.raise_for_status()
        timestamps = sorted(
            (int(ts) // 1000) for ts in index_response.json().get("timestamps", [])
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to fetch SMARD index data: %s", exc)
        return None

    block_list = determine_series_blocks(min_ts, max_ts, timestamps)
    if not block_list:
        logger.warning(
            "No SMARD data blocks covering %s - %s for region=%s.", min_ts, max_ts, region
        )
        return None

    cpu_count = os.cpu_count() or 1
    max_workers = max(
        1, min(int(os.getenv("SLURM_CPUS_ON_NODE", cpu_count)), len(block_list))
    )

    def fetch_block(ts: int) -> List[List[float]]:
        url = (
            f"https://www.smard.de/app/chart_data/{filter_id}/{region}/"
            f"{filter_id}_{region}_{resolution}_{ts * 1000}.json"
        )
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json().get("series", [])

    rows: List[List[float]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_block, ts): ts for ts in block_list}
        for future in futures:
            ts = futures[future]
            try:
                block_rows = future.result()
                rows.extend(block_rows)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to fetch SMARD block %s: %s", ts, exc)

    rows = [row for row in rows if row and row[1] is not None]
    if not rows:
        logger.warning("No SMARD price rows retrieved for requested interval.")
        return None

    price_df = pl.DataFrame(
        rows,
        schema=["EpochTime_ms", "Price_EUR_per_MWh"],
    )
    price_df = price_df.with_columns(
        (pl.col("EpochTime_ms") // 1000).cast(pl.Int64).alias("EpochTime"),
        pl.col("Price_EUR_per_MWh").cast(pl.Float64),
    ).drop("EpochTime_ms")

    price_df = price_df.unique(subset=["EpochTime"]).sort("EpochTime")
    logger.info(
        "Fetched %s SMARD price points for %s-%s (%s).",
        price_df.height,
        min_ts,
        max_ts,
        region,
    )
    return price_df


def integrate_price_data(
    df: pl.DataFrame,
    epoch_column: Optional[str],
    *,
    filter_id: int,
    region: str,
    resolution: str,
    logger,
) -> Tuple[pl.DataFrame, Optional[pl.DataFrame]]:
    """Join SMARD price data onto ``df`` using a nearest-time asof join.

    Args:
        df: Combined metrics DataFrame to augment with pricing.
        epoch_column: Name of the column containing epoch timestamps.
        filter_id: SMARD dataset identifier.
        region: Requested bidding zone.
        resolution: SMARD resolution string.
        logger: Logger instance for diagnostic output.

    Returns:
        Tuple of the augmented DataFrame and the retrieved price table
        (``None`` if pricing data is unavailable).
    """
    if epoch_column is None:
        logger.warning("No epoch time column detected; skipping price integration.")
        return df, None

    price_df = fetch_smard_prices(
        df[epoch_column],
        filter_id=filter_id,
        region=region,
        resolution=resolution,
        logger=logger,
    )
    if price_df is None:
        return df, None

    df = df.with_columns(pl.col(epoch_column).cast(pl.Int64)).sort(epoch_column)
    df = df.join_asof(
        price_df,
        left_on=epoch_column,
        right_on="EpochTime",
        strategy="nearest",
    )

    df = df.with_columns(
        pl.col("Price_EUR_per_MWh").interpolate().alias("Price_EUR_per_MWh")
    )

    if "Energy_used_J" in df.columns:
        df = df.with_columns(
            (
                (pl.col("Energy_used_J") / 3_600_000.0)
                * pl.col("Price_EUR_per_MWh")
            ).alias("Cumulative_cost_EUR")
        )

    return df, price_df
