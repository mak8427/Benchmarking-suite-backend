"""Tests for normalized cost backfill helpers."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import polars as pl


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "backfill-costs-from-smard.py"


def _load_backfill_module():
    spec = importlib.util.spec_from_file_location("backfill_costs_from_smard", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_priced_samples_computes_cumulative_cost() -> None:
    """Backfill should attach nearest price and cumulative EUR cost."""
    module = _load_backfill_module()
    samples = pl.DataFrame(
        {
            "object_key": ["job-1", "job-1"],
            "elapsed_time": [0.0, 1.0],
            "epoch_time": [100, 160],
            "energy_used_j": [0.0, 3_600_000.0],
        }
    )
    prices = pl.DataFrame(
        {"EpochTime": [90, 150], "Price_EUR_per_MWh": [50.0, 100.0]}
    )

    priced = module._priced_samples(samples, prices)

    assert priced.to_dicts()[0]["price_eur_per_mwh"] == 50.0
    assert priced.to_dicts()[1]["price_eur_per_mwh"] == 100.0
    assert priced.to_dicts()[1]["cumulative_cost_eur"] == 0.1
