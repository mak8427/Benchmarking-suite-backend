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
    """Attach SMARD prices to the telemetry dataframe via asof join.

    Args:
        df (pl.DataFrame): Telemetry dataframe.
        epoch_column (Optional[str]): Epoch-time column used as join key.
        filter_id (int): SMARD filter identifier.
        region (str): SMARD region code.
        resolution (str): SMARD resolution token.
        logger: Logger used for diagnostics.

    Returns:
        Tuple[pl.DataFrame, Optional[pl.DataFrame]]: Updated dataframe and raw
        price dataframe when available.

    Examples:
        >>> class L:
        ...     def warning(self, *args, **kwargs): pass
        >>> sample = pl.DataFrame({"EpochTime": [1, 2], "Energy_used_J": [0.0, 10.0]})
        >>> enriched, prices = integrate_price_data(
        ...     sample,
        ...     None,
        ...     filter_id=4169,
        ...     region="DE-LU",
        ...     resolution="quarterhour",
        ...     logger=L(),
        ... )
        >>> prices is None and enriched.shape == (2, 2)
        True
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
    df = df.join_asof(price_df, left_on=epoch_column, right_on="EpochTime", strategy="nearest")

    df = df.with_columns(pl.col("Price_EUR_per_MWh").interpolate().alias("Price_EUR_per_MWh"))

    if "Energy_used_J" in df.columns:
        df = df.with_columns(
            ((pl.col("Energy_used_J") / 3_600_000.0) * pl.col("Price_EUR_per_MWh")).alias("Cumulative_cost_EUR")
        )

    return df, price_df
