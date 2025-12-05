"""Pricing integration using SMARD data."""

from __future__ import annotations

from typing import Optional, Tuple

import polars as pl

from .pricing_fetch import fetch_smard_prices

__all__ = ["integrate_price_data", "fetch_smard_prices"]


def integrate_price_data(
    df: pl.DataFrame,
    epoch_column: Optional[str],
    *,
    filter_id: int,
    region: str,
    resolution: str,
    logger,
) -> Tuple[pl.DataFrame, Optional[pl.DataFrame]]:
    """Join SMARD price data onto ``df`` using a nearest-time asof join."""

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
    df = df.join_asof(price_df, left_on=epoch_column, right_on="EpochTime", strategy="nearest")

    df = df.with_columns(pl.col("Price_EUR_per_MWh").interpolate().alias("Price_EUR_per_MWh"))

    if "Energy_used_J" in df.columns:
        df = df.with_columns(
            ((pl.col("Energy_used_J") / 3_600_000.0) * pl.col("Price_EUR_per_MWh")).alias("Cumulative_cost_EUR")
        )

    return df, price_df
