from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from finsynapse.transform.divergence import compute_divergence
from finsynapse.transform.health_check import check
from finsynapse.transform.normalize import collect_bronze, derive_indicators
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
            "cn_valuation": {},
            "cn_sentiment": {},
            "cn_liquidity": {},
            "hk_valuation": {},
            "hk_sentiment": {},
            "hk_liquidity": {},
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


def test_derive_indicators_computes_us_erp_with_monthly_pe_ffill():
    """ERP = 100/PE − real_yield. PE published only first-of-month (mimicking
    multpl's actual monthly cadence); real_yield daily. Verifies the ffill
    path actually works — not the previous test's all-aligned-daily fixture
    which would pass even if ffill was broken."""
    # Real yield: daily, 60 business days
    daily_dates = pd.bdate_range("2026-01-01", periods=60)
    # PE: only the 3 month-start rows (Jan/Feb/Mar 2026) — must ffill to fill the gaps
    pe_dates = [pd.Timestamp("2026-01-02"), pd.Timestamp("2026-02-02"), pd.Timestamp("2026-03-02")]
    macro = pd.concat(
        [
            pd.DataFrame(
                {
                    "date": [d.date() for d in pe_dates],
                    "indicator": "us_pe_ttm",
                    "value": [20.0, 22.0, 25.0],  # changes month-over-month so ffill mistakes are detectable
                    "source": "multpl",
                }
            ),
            pd.DataFrame(
                {
                    "date": [d.date() for d in daily_dates],
                    "indicator": "us10y_real_yield",
                    "value": [1.5] * len(daily_dates),
                    "source": "fred",
                }
            ),
        ],
        ignore_index=True,
    )
    out = derive_indicators(macro)
    erp = out[out["indicator"] == "us_erp"].copy()
    erp["date"] = pd.to_datetime(erp["date"])
    erp = erp.set_index("date").sort_index()

    assert not erp.empty
    # Mid-January (PE=20, EY=5%) → ERP = 5 − 1.5 = 3.5
    jan_15 = erp.loc["2026-01-15"]
    assert 3.4 < jan_15["value"] < 3.6, f"Jan ffill broken: got {jan_15['value']}"
    # Mid-February (PE=22, EY=4.545%) → ERP = 4.545 − 1.5 = 3.045
    feb_15 = erp.loc["2026-02-13"]  # last bday before Feb 15 weekend
    assert 3.0 < feb_15["value"] < 3.1, f"Feb ffill picked wrong PE: got {feb_15['value']}"
    # Mid-March (PE=25, EY=4.0%) → ERP = 4.0 − 1.5 = 2.5
    mar_15 = erp.loc["2026-03-13"]
    assert 2.45 < mar_15["value"] < 2.55, f"Mar ffill picked wrong PE: got {mar_15['value']}"
    assert (out[out["indicator"] == "us_erp"]["source"] == "derived").all()


def test_derive_indicators_guards_against_non_positive_pe():
    """If multpl returns PE=0 (parse error) or PE<0 (historical 2009Q1
    negative-EPS scenario), ERP must NOT produce inf or sign-flipped values
    that would later get inverted by direction:'-' in weights.yaml into
    bogus 'extreme hot' US valuation readings."""
    dates = pd.bdate_range("2026-01-01", periods=10)
    macro = pd.DataFrame(
        {
            "date": [d.date() for d in dates] * 2,
            "indicator": ["us_pe_ttm"] * 10 + ["us10y_real_yield"] * 10,
            # Mix of poison values: 0, negative, and one valid 20.0 at the end
            "value": [0.0, 0.0, -5.0, -2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 20.0] + [1.5] * 10,
            "source": ["multpl"] * 10 + ["fred"] * 10,
        }
    )
    out = derive_indicators(macro)
    erp = out[out["indicator"] == "us_erp"]
    # Only the last row (PE=20) should produce a valid ERP. All others guarded.
    assert len(erp) == 1, f"expected 1 valid ERP, got {len(erp)}: {erp['value'].tolist()}"
    assert 3.4 < erp["value"].iloc[0] < 3.6
    # Critically: no inf or negative ERP smuggled through
    import numpy as np

    assert not np.isinf(erp["value"]).any()


def test_weights_config_rejects_unbalanced_block(tmp_path):
    """Sub-block weights MUST sum to 1.0; loading an unbalanced config
    must raise immediately, not silently produce miscalibrated temperatures."""
    bad_yaml = tmp_path / "bad_weights.yaml"
    bad_yaml.write_text(
        """sub_weights:
  us: { valuation: 1.0, sentiment: 0.0, liquidity: 0.0 }
indicator_weights:
  us_valuation:
    us_pe_ttm: { weight: 0.5, direction: "+" }
    us_cape:   { weight: 0.7, direction: "+" }
percentile_window: pct_10y
"""
    )
    with pytest.raises(ValueError, match="sums to"):
        WeightsConfig.load(bad_yaml)


def test_weights_config_rejects_inconsistent_window_override(tmp_path):
    """Same indicator across blocks must use the same window override —
    otherwise window_for() returns whichever block iterates first."""
    bad_yaml = tmp_path / "bad_window.yaml"
    bad_yaml.write_text(
        """sub_weights:
  us: { valuation: 0.5, sentiment: 0.5, liquidity: 0.0 }
  hk: { valuation: 0.0, sentiment: 0.0, liquidity: 1.0 }
indicator_weights:
  us_valuation:
    dxy: { weight: 1.0, direction: "-", window: pct_5y }
  us_sentiment: {}
  hk_liquidity:
    dxy: { weight: 1.0, direction: "-", window: pct_10y }
percentile_window: pct_10y
"""
    )
    with pytest.raises(ValueError, match="inconsistent"):
        WeightsConfig.load(bad_yaml)


def test_derive_indicators_skips_when_inputs_missing():
    """If only us_pe_ttm exists (no real yield), us_erp should NOT be emitted
    rather than producing NaN/garbage rows."""
    n = 10
    dates = pd.bdate_range("2026-01-01", periods=n)
    macro = pd.DataFrame(
        {
            "date": [d.date() for d in dates],
            "indicator": "us_pe_ttm",
            "value": [22.0] * n,
            "source": "multpl",
        }
    )
    out = derive_indicators(macro)
    assert "us_erp" not in out["indicator"].unique()


def test_temperature_per_indicator_window_override():
    """An indicator with window: pct_5y must read pct_5y; without override
    must read the global percentile_window. Verifies refactor of pct_wide
    construction picks up per-indicator columns."""
    cfg = WeightsConfig(
        sub_weights={"us": {"valuation": 1.0, "sentiment": 0.0, "liquidity": 0.0}},
        indicator_weights={
            "us_valuation": {
                # us_pe_ttm uses default (pct_10y); us_cape overrides to pct_5y.
                "us_pe_ttm": {"weight": 0.5, "direction": "+"},
                "us_cape": {"weight": 0.5, "direction": "+", "window": "pct_5y"},
            },
            "us_sentiment": {},
            "us_liquidity": {},
        },
        percentile_window="pct_10y",
    )
    n = 30
    pct = pd.DataFrame(
        {
            "date": pd.bdate_range("2026-01-01", periods=n).date.tolist() * 2,
            "indicator": ["us_pe_ttm"] * n + ["us_cape"] * n,
            "value": [20.0] * (n * 2),
            "pct_1y": [10.0] * (n * 2),
            # pe_ttm reads pct_10y=80; cape reads pct_5y=40 (override).
            # Equal weight → val ~= (80 + 40) / 2 = 60.
            "pct_5y": [99.0] * n + [40.0] * n,  # pe_ttm 99 must NOT be picked
            "pct_10y": [80.0] * n + [99.0] * n,  # cape 99 must NOT be picked
        }
    )
    temp = compute_temperature(pct, cfg)
    last = temp[temp["market"] == "us"].iloc[-1]
    assert 55.0 < last["valuation"] < 65.0, f"expected ~60, got {last['valuation']}"


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
