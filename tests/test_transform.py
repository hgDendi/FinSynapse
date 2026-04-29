from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from finsynapse.transform.divergence import PAIRS, compute_divergence
from finsynapse.transform.health_check import check
from finsynapse.transform.normalize import collect_bronze
from finsynapse.transform.percentile import compute_percentiles
from finsynapse.transform.temperature import WeightsConfig, compute_temperature


def _build_macro(indicators: dict[str, list[float]], start="2010-01-04") -> pd.DataFrame:
    """Build a long-format macro frame from indicator -> daily values mapping."""
    start_d = pd.Timestamp(start)
    frames = []
    for indicator, values in indicators.items():
        dates = pd.bdate_range(start_d, periods=len(values))
        frames.append(
            pd.DataFrame({"date": [d.date() for d in dates], "indicator": indicator, "value": values, "source": "test"})
        )
    return pd.concat(frames, ignore_index=True)


def test_collect_bronze_empty_returns_canonical_schema(tmp_data_dir):
    df = collect_bronze()
    assert list(df.columns) == ["date", "indicator", "value", "source"]
    assert len(df) == 0


def test_collect_bronze_dedups_overlapping_dates(tmp_data_dir):
    """If two bronze fetches contain the same (date, indicator), keep one."""
    bronze_dir = tmp_data_dir / "bronze" / "macro" / "yfinance_macro"
    bronze_dir.mkdir(parents=True)
    d = date(2026, 4, 1)
    df1 = pd.DataFrame({"date": [d], "indicator": ["vix"], "value": [15.0], "source_symbol": ["^VIX"]})
    df2 = pd.DataFrame({"date": [d], "indicator": ["vix"], "value": [15.5], "source_symbol": ["^VIX"]})
    df1.to_parquet(bronze_dir / "2026-04-01.parquet", index=False)
    df2.to_parquet(bronze_dir / "2026-04-02.parquet", index=False)

    out = collect_bronze()
    assert len(out) == 1
    assert out["value"].iloc[0] in (15.0, 15.5)


def test_health_check_flags_out_of_bounds_and_zero():
    macro = _build_macro({"vix": [20.0, 25.0, 0.0, 500.0, 22.0]})
    clean, issues = check(macro)
    rules = {i.rule for i in issues}
    assert "zero" in rules
    assert "out_of_bounds" in rules
    # Both bad rows dropped
    assert len(clean) == 3


def test_health_check_passes_clean_data():
    macro = _build_macro({"vix": [20.0, 21.0, 19.0, 22.0]})
    clean, issues = check(macro)
    assert all(i.severity != "fail" for i in issues)
    assert len(clean) == 4


def test_percentile_endpoints_are_extreme():
    """The smallest value in a series should be at low percentile;
    the largest at high percentile (within the trailing window)."""
    n = 300
    values = list(np.linspace(10, 100, n))
    macro = _build_macro({"vix": values})
    pct = compute_percentiles(macro)

    last = pct[pct["indicator"] == "vix"].sort_values("date").iloc[-1]
    assert last["pct_1y"] >= 95.0  # last value is the max within 1Y window


def test_temperature_handles_missing_indicators_gracefully(tmp_path):
    """When a market has no indicators configured, calculator must skip it
    without affecting other markets. Uses an inline config (not the live yaml)
    so the test stays valid even as Phase 1b/2 add real CN/HK indicators."""
    # Inline config: only US has indicators; CN/HK explicitly empty.
    cfg = WeightsConfig(
        sub_weights={
            "cn": {"valuation": 0.5, "sentiment": 0.3, "liquidity": 0.2},
            "hk": {"valuation": 0.6, "sentiment": 0.25, "liquidity": 0.15},
            "us": {"valuation": 0.4, "sentiment": 0.35, "liquidity": 0.25},
        },
        indicator_weights={
            "us_valuation": {
                "us_pe_ttm": {"weight": 0.5, "direction": "+"},
                "us_cape": {"weight": 0.5, "direction": "+"},
            },
            "us_sentiment": {"vix": {"weight": 1.0, "direction": "-"}},
            "us_liquidity": {"dxy": {"weight": 1.0, "direction": "-"}},
            "cn_valuation": {}, "cn_sentiment": {}, "cn_liquidity": {},
            "hk_valuation": {}, "hk_sentiment": {}, "hk_liquidity": {},
        },
        percentile_window="pct_10y",
    )
    n = 50
    pct = pd.DataFrame(
        {
            "date": pd.bdate_range("2026-01-01", periods=n).date.tolist() * 4,
            "indicator": ["us_pe_ttm"] * n + ["us_cape"] * n + ["vix"] * n + ["dxy"] * n,
            "value": list(np.linspace(15, 30, n)) * 4,
            "pct_1y": [50.0] * (n * 4),
            "pct_5y": [60.0] * (n * 4),
            "pct_10y": [70.0] * (n * 4),
        }
    )
    temp = compute_temperature(pct, cfg)
    markets = set(temp["market"].unique())
    assert "us" in markets
    assert "cn" not in markets
    assert "hk" not in markets
    us_last = temp[temp["market"] == "us"].iloc[-1]
    assert us_last["data_quality"] == "ok"


def test_temperature_renormalizes_when_one_sub_unavailable(tmp_path):
    """If liquidity inputs are missing, valuation+sentiment must still produce
    a sensible overall (renormalized weights), with data_quality flagging."""
    cfg = WeightsConfig(
        sub_weights={"us": {"valuation": 0.4, "sentiment": 0.35, "liquidity": 0.25}},
        indicator_weights={
            "us_valuation": {
                "us_pe_ttm": {"weight": 0.5, "direction": "+"},
                "us_cape": {"weight": 0.5, "direction": "+"},
            },
            "us_sentiment": {"vix": {"weight": 1.0, "direction": "-"}},
            "us_liquidity": {"dxy": {"weight": 1.0, "direction": "-"}},
        },
        percentile_window="pct_10y",
    )
    n = 30
    pct = pd.DataFrame(
        {
            "date": pd.bdate_range("2026-01-01", periods=n).date.tolist() * 3,
            "indicator": ["us_pe_ttm"] * n + ["us_cape"] * n + ["vix"] * n,
            "value": [20.0] * (n * 3),
            "pct_1y": [50.0] * (n * 3),
            "pct_5y": [60.0] * (n * 3),
            "pct_10y": [80.0] * (n * 3),  # high valuation + low sentiment temp due to direction-
        }
    )
    temp = compute_temperature(pct, cfg)
    us = temp[temp["market"] == "us"].iloc[-1]
    assert us["data_quality"] == "liquidity_unavailable"
    assert not pd.isna(us["overall"])
    assert pd.isna(us["liquidity"])


def test_divergence_detects_signal_pair_disagreement():
    macro = _build_macro(
        {
            # SP500 up 1% each day, VIX up 1% each day → divergence (expected: opposite)
            "sp500": [100.0, 101.0, 102.0, 103.0],
            "vix": [20.0, 20.2, 20.4, 20.6],
        }
    )
    div = compute_divergence(macro)
    sp500_vix = div[div["pair_name"] == "sp500_vix"]
    # All non-first days are divergent (both rising)
    assert sp500_vix["is_divergent"].all()


def test_divergence_skips_pairs_missing_indicators():
    macro = _build_macro({"sp500": [100.0, 101.0, 102.0]})  # no vix
    div = compute_divergence(macro)
    assert "sp500_vix" not in div["pair_name"].unique()
