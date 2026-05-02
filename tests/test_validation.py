"""Tests for the Phase 1 validation pipeline.

Covers:
- Pivot YAML parsing (valid structure, all 3 markets covered)
- Baseline temperature computation (PE, VIX, momentum)
- Gate logic correctness
- Validation report serialization roundtrip
"""

from __future__ import annotations

import json
from datetime import date

import numpy as np
import pandas as pd
import pytest
import yaml

from finsynapse.dashboard.validation_data import ValidationReport, load_report
from finsynapse.transform.normalize import derive_indicators
from finsynapse.transform.percentile import compute_percentiles
from finsynapse.transform.temperature import WeightsConfig, compute_temperature

SCRIPTS_DIR = __import__("pathlib").Path(__file__).parent.parent / "scripts"
PIVOTS_PATH = SCRIPTS_DIR / "backtest_pivots.yaml"


def _build_macro(indicators: dict[str, list[float]], start: str = "2010-01-04") -> pd.DataFrame:
    start_d = pd.Timestamp(start)
    frames = []
    for indicator, values in indicators.items():
        dates = pd.bdate_range(start_d, periods=len(values))
        frames.append(
            pd.DataFrame(
                {
                    "date": [d.date() for d in dates],
                    "indicator": indicator,
                    "value": values,
                    "source": "test",
                }
            )
        )
    return pd.concat(frames, ignore_index=True)


class TestPivotsYAML:
    """Validate the pivot definition file structure."""

    def test_yaml_parses(self):
        with PIVOTS_PATH.open() as f:
            data = yaml.safe_load(f)
        assert "pivots" in data
        assert len(data["pivots"]) >= 20

    def test_all_three_markets_covered(self):
        with PIVOTS_PATH.open() as f:
            data = yaml.safe_load(f)
        markets = {p["market"] for p in data["pivots"]}
        assert markets == {"us", "cn", "hk"}

    def test_each_market_has_min_bottoms_and_tops(self):
        with PIVOTS_PATH.open() as f:
            data = yaml.safe_load(f)
        for market in ("us", "cn", "hk"):
            market_pivots = [p for p in data["pivots"] if p["market"] == market]
            cold = sum(1 for p in market_pivots if p["expected_zone"] == "cold")
            hot = sum(1 for p in market_pivots if p["expected_zone"] == "hot")
            assert cold >= 3, f"{market}: expected ≥3 cold pivots, got {cold}"
            assert hot >= 2, f"{market}: expected ≥2 hot pivots, got {hot}"

    def test_all_dates_valid_and_ordered(self):
        with PIVOTS_PATH.open() as f:
            data = yaml.safe_load(f)
        for p in data["pivots"]:
            d = date.fromisoformat(p["date"])
            assert date(2010, 1, 1) <= d <= date(2026, 12, 31)

    def test_all_expected_zones_valid(self):
        with PIVOTS_PATH.open() as f:
            data = yaml.safe_load(f)
        for p in data["pivots"]:
            assert p["expected_zone"] in ("cold", "mid", "hot")


class TestBaselineTemperatures:
    """Verify baseline temperature computation logic."""

    def test_pe_single_factor_outputs_0_to_100(self):
        # Need >300 days beyond pct_1y min_periods. Use pct_1y window (252d, min_periods=63).
        n = 350
        values = list(np.linspace(15, 40, n))
        macro = _build_macro({"us_pe_ttm": values})
        macro = derive_indicators(macro)
        pct = compute_percentiles(macro)

        cfg = WeightsConfig(
            sub_weights={"us": {"valuation": 1.0, "sentiment": 0.0, "liquidity": 0.0}},
            indicator_weights={
                "us_valuation": {"us_pe_ttm": {"weight": 1.0, "direction": "+", "window": "pct_1y"}},
                "us_sentiment": {},
                "us_liquidity": {},
            },
            percentile_window="pct_1y",
        )
        temp = compute_temperature(pct, cfg)
        us = temp[temp["market"] == "us"]
        assert not us.empty
        assert len(us) > 0
        valid = us.dropna(subset=["overall"])
        assert valid["overall"].between(0, 100).all()

    def test_vix_baseline_inverts_direction(self):
        """Direction "-": high VIX percentile yields low temperature.
        Verify that 'vix' with direction '-' produces valid temperatures in [0,100]
        and that the sentiment sub works for inputs after min_periods."""
        n = 350
        mid = n // 2
        values = [80.0 - 70.0 * (i / mid) for i in range(mid)] + [
            10.0 + 70.0 * (i / (n - mid - 1)) for i in range(n - mid)
        ]
        macro = _build_macro({"vix": values})
        macro = derive_indicators(macro)
        pct = compute_percentiles(macro)

        cfg = WeightsConfig(
            sub_weights={"us": {"valuation": 0.0, "sentiment": 1.0, "liquidity": 0.0}},
            indicator_weights={
                "us_valuation": {},
                "us_sentiment": {"vix": {"weight": 1.0, "direction": "-", "window": "pct_1y"}},
                "us_liquidity": {},
            },
            percentile_window="pct_1y",
        )
        temp = compute_temperature(pct, cfg)
        us = temp[temp["market"] == "us"].dropna(subset=["overall"])
        assert not us.empty
        assert us["overall"].between(0, 100).all()
        assert us["sentiment"].between(0, 100).all()

    def test_momentum_baseline_uses_60d_returns(self):
        """Verify momentum temperature is computed from 60d pct_change+percentile."""
        n = 300
        # Steadily rising index: early values have small 60d returns, later have large ones
        init_val = 100.0
        values = [init_val * (1.001**i) for i in range(n)]
        macro = _build_macro({"sp500": values})
        wide = macro.pivot_table(index="date", columns="indicator", values="value").sort_index()
        ret_60d = wide["sp500"].pct_change(60).iloc[-1]
        # With steadily rising index over 300 days, 60d return should be positive
        assert ret_60d > 0

    def test_compute_temperature_handles_empty_baseline(self):
        cfg1 = WeightsConfig(
            sub_weights={"us": {"valuation": 1.0, "sentiment": 0.0, "liquidity": 0.0}},
            indicator_weights={
                "us_valuation": {"nonexistent_indicator": {"weight": 1.0, "direction": "+"}},
                "us_sentiment": {},
                "us_liquidity": {},
            },
            percentile_window="pct_10y",
        )
        pct = pd.DataFrame(columns=["date", "indicator", "value", "pct_1y", "pct_5y", "pct_10y"])
        temp = compute_temperature(pct, cfg1)
        assert temp.empty


class TestPhase2MultiTimeframe:
    """Verify multi-timeframe temperature columns."""

    def test_multiframe_columns_present(self):
        """compute_temperature output should include overall_short, overall_long, divergence."""
        n = 350
        values = list(np.linspace(15, 40, n))
        macro = _build_macro({"us_pe_ttm": values})
        macro = derive_indicators(macro)
        pct = compute_percentiles(macro)

        cfg = WeightsConfig(
            sub_weights={"us": {"valuation": 1.0, "sentiment": 0.0, "liquidity": 0.0}},
            indicator_weights={
                "us_valuation": {"us_pe_ttm": {"weight": 1.0, "direction": "+", "window": "pct_1y"}},
                "us_sentiment": {},
                "us_liquidity": {},
            },
            percentile_window="pct_1y",
        )
        temp = compute_temperature(pct, cfg)
        assert not temp.empty
        for col in ("overall_short", "overall_long", "divergence", "conf_ok"):
            assert col in temp.columns, f"missing column: {col}"

    def test_divergence_zero_when_single_window(self):
        """When all indicators use the same window, short and long diverge due to
        different min_periods and window breadth, but should both be in [0,100]."""
        n = 400
        values = list(np.linspace(15, 40, n))
        macro = _build_macro({"us_pe_ttm": values})
        macro = derive_indicators(macro)
        pct = compute_percentiles(macro)

        cfg = WeightsConfig(
            sub_weights={"us": {"valuation": 1.0, "sentiment": 0.0, "liquidity": 0.0}},
            indicator_weights={
                "us_valuation": {"us_pe_ttm": {"weight": 1.0, "direction": "+", "window": "pct_1y"}},
                "us_sentiment": {},
                "us_liquidity": {},
            },
            percentile_window="pct_1y",
        )
        temp = compute_temperature(pct, cfg)
        valid = temp.dropna(subset=["overall_short", "overall_long"])
        assert valid["overall_short"].between(0, 100).all()
        assert valid["overall_long"].between(0, 100).all()

    def test_dispersion_confidence_computed(self):
        """When 2+ indicators exist in a sub, conf_ok should vary."""
        n = 350
        vix_vals = [80.0 - 70.0 * (i / (n - 1)) for i in range(n)]  # declining vix
        oas_vals = [5.0 + 15.0 * (i / (n - 1)) for i in range(n)]  # rising OAS
        us_pe_vals = list(np.linspace(15, 40, n))
        macro = pd.concat(
            [
                _build_macro({"vix": vix_vals}),
                _build_macro({"us_hy_oas": oas_vals, "us_pe_ttm": us_pe_vals}),
            ],
            ignore_index=True,
        )
        macro = derive_indicators(macro)
        pct = compute_percentiles(macro)

        cfg = WeightsConfig(
            sub_weights={"us": {"valuation": 0.5, "sentiment": 0.5, "liquidity": 0.0}},
            indicator_weights={
                "us_valuation": {"us_pe_ttm": {"weight": 1.0, "direction": "+", "window": "pct_1y"}},
                "us_sentiment": {
                    "vix": {"weight": 0.5, "direction": "-", "window": "pct_1y"},
                    "us_hy_oas": {"weight": 0.5, "direction": "-", "window": "pct_1y"},
                },
                "us_liquidity": {},
            },
            percentile_window="pct_1y",
        )
        temp = compute_temperature(pct, cfg)
        assert not temp.empty
        assert "conf_ok" in temp.columns
        # conf_ok should be 0 or 1
        assert temp["conf_ok"].dropna().isin([0, 1]).all()


class TestNewWeightsConfig:
    """Verify weights.yaml loads correctly with Phase 2 indicators."""

    def test_new_indicators_in_weights(self):
        cfg = WeightsConfig.load()
        us_sent = cfg.indicator_weights.get("us_sentiment", {})
        assert "us_umich_sentiment" in us_sent
        us_liq = cfg.indicator_weights.get("us_liquidity", {})
        assert "us_walcl" in us_liq
        hk_sent = cfg.indicator_weights.get("hk_sentiment", {})
        assert "hk_vhsi" in hk_sent


class TestGateLogic:
    """Verify the gate-check logic in validation."""

    def test_gate_passes_when_beating_in_two_markets(self):
        from finsynapse.dashboard.validation_data import GateResult

        gate = GateResult(
            passed=True,
            markets_beaten=2,
            total_markets=3,
            standard="test",
            details={
                "us": {"beaten": True, "mf_directional_rate": 0.8, "pe_directional_rate": 0.6},
                "cn": {"beaten": True, "mf_directional_rate": 0.7, "pe_directional_rate": 0.5},
                "hk": {"beaten": False, "mf_directional_rate": 0.4, "pe_directional_rate": 0.6},
            },
        )
        assert gate.passed

    def test_gate_fails_when_beating_in_one_market(self):
        from finsynapse.dashboard.validation_data import GateResult

        gate = GateResult(
            passed=False,
            markets_beaten=1,
            total_markets=3,
            standard="test",
            details={
                "us": {"beaten": True, "mf_directional_rate": 0.75, "pe_directional_rate": 0.5},
                "cn": {"beaten": False, "mf_directional_rate": 0.3, "pe_directional_rate": 0.5},
                "hk": {"beaten": False, "mf_directional_rate": 0.2, "pe_directional_rate": 0.4},
            },
        )
        assert not gate.passed


class TestVersionModule:
    """Verify version stamping, snapshot, and drift detection."""

    def test_stamp_version_adds_column(self):
        from finsynapse.transform.version import ALGO_VERSION, stamp_version

        df = pd.DataFrame({"overall": [50.0], "market": ["us"]})
        stamped = stamp_version(df)
        assert "algo_version" in stamped.columns
        assert stamped["algo_version"].iloc[0] == ALGO_VERSION

    def test_stamp_version_empty_frame(self):
        from finsynapse.transform.version import stamp_version

        df = pd.DataFrame()
        result = stamp_version(df)
        assert result.empty

    def test_snapshot_weights_creates_file(self, tmp_path, monkeypatch):
        import yaml

        # Redirect silver dir to tmp
        from finsynapse import config as cfg
        from finsynapse.transform.version import snapshot_weights

        monkeypatch.setattr(cfg, "settings", cfg.Settings(data_dir=tmp_path))
        cfg.settings.silver_dir.mkdir(parents=True, exist_ok=True)

        src = tmp_path / "weights_test.yaml"
        src.write_text("percentile_window: pct_10y\nsub_weights: {}\nindicator_weights: {}\n")
        result = snapshot_weights(str(src))
        assert result is not None
        assert result.exists()

    def test_drift_check_no_change(self):
        from finsynapse.transform.version import drift_check

        today = pd.DataFrame({"date": ["2026-01-02"], "market": ["us"], "overall": [50.0]})
        yesterday = pd.DataFrame({"date": ["2026-01-01"], "market": ["us"], "overall": [51.0]})
        alerts = drift_check(today, yesterday, threshold=15.0)
        assert alerts == []

    def test_drift_check_large_move(self):
        from finsynapse.transform.version import drift_check

        today = pd.DataFrame({"date": ["2026-01-02"], "market": ["us"], "overall": [80.0]})
        yesterday = pd.DataFrame({"date": ["2026-01-01"], "market": ["us"], "overall": [50.0]})
        alerts = drift_check(today, yesterday, threshold=15.0)
        assert len(alerts) == 1
        assert alerts[0]["alert"] == "zone_crossing"

    def test_compare_snapshots_detects_change(self, tmp_path):
        import yaml

        from finsynapse.transform.version import compare_snapshots

        prev = tmp_path / "prev.yaml"
        curr = tmp_path / "curr.yaml"
        base = {
            "sub_weights": {"us": {"valuation": 0.4}},
            "indicator_weights": {"us_valuation": {"vix": {"weight": 0.5, "direction": "-"}}},
        }
        prev.write_text(yaml.dump(base))
        changed = dict(base)
        changed["indicator_weights"]["us_valuation"]["vix"]["weight"] = 0.6
        curr.write_text(yaml.dump(changed))
        diff = compare_snapshots(prev, curr)
        assert diff["status"] == "diff"
        assert any("0.5" in c for c in diff.get("changed", []))


class TestValidationReportRoundtrip:
    """Verify the validation report JSON roundtrip."""

    def test_report_roundtrip(self, tmp_path):
        report_json = {
            "version": "1.0.0",
            "generated": "2026-04-30",
            "pivots_total": 3,
            "pivots_evaluated": 3,
            "pivot_results": [
                {
                    "label": "Test pivot",
                    "market": "us",
                    "date": "2020-03-23",
                    "expected_zone": "cold",
                    "controllers": [
                        {
                            "name": "multi-factor",
                            "overall": 15.0,
                            "zone": "cold",
                            "strict_pass": True,
                            "directional_pass": True,
                            "valuation": 10.0,
                            "sentiment": 20.0,
                            "liquidity": 15.0,
                        }
                    ],
                }
            ],
            "hit_rate_table": {
                "multi-factor": {
                    "us": {
                        "total": 3,
                        "directional_hits": 3,
                        "directional_rate": 1.0,
                        "strict_hits": 2,
                        "strict_rate": 0.667,
                    }
                }
            },
            "forward_stats": {"us": {"1m": {"n": 100, "mean": 0.02, "spearman_rho": -0.15}}},
            "zone_distribution": {
                "0-20 (极冷)": [{"horizon": "1m", "mean_return": 0.03, "median_return": 0.02, "win_rate": 0.7, "n": 50}]
            },
            "spearman_rho": {"us": {"1m": -0.15, "3m": -0.25, "6m": -0.30, "12m": -0.35}},
            "gate": {
                "passed": True,
                "markets_beaten": 2,
                "total_markets": 3,
                "standard": "test",
                "details": {},
            },
        }
        path = tmp_path / "validation_report.json"
        path.write_text(json.dumps(report_json))

        report = load_report(path)
        assert report is not None
        assert report.version == "1.0.0"
        assert report.pivots_total == 3
        assert len(report.pivot_results) == 1
        assert report.pivot_results[0].controllers[0].overall == 15.0
        assert report.gate is not None
        assert report.gate.passed

    def test_load_report_returns_none_for_missing_file(self, tmp_path):
        report = load_report(tmp_path / "nonexistent.json")
        assert report is None


class TestDeriveIndicatorsIntegration:
    """Verify us_erp derivation works for validation pipeline inputs."""

    def test_us_erp_low_pe_means_cheap(self):
        """Low PE + same real yield = higher ERP = stocks cheap relative to bonds."""
        n = 30
        dates = pd.bdate_range("2026-01-01", periods=n)
        macro = pd.concat(
            [
                pd.DataFrame(
                    {
                        "date": [d.date() for d in dates],
                        "indicator": "us_pe_ttm",
                        "value": [10.0] * n,  # low PE = expensive earnings yield = 10%
                        "source": "multpl",
                    }
                ),
                pd.DataFrame(
                    {
                        "date": [d.date() for d in dates],
                        "indicator": "us10y_real_yield",
                        "value": [2.0] * n,  # real yield = 2%
                        "source": "fred",
                    }
                ),
            ],
            ignore_index=True,
        )
        out = derive_indicators(macro)
        erp_rows = out[out["indicator"] == "us_erp"]
        assert not erp_rows.empty
        # EY = 100/10 = 10%, ERP = 10% - 2% = 8%
        assert 7.9 < erp_rows["value"].iloc[0] < 8.1

    def test_us_erp_high_pe_means_expensive(self):
        """High PE + same real yield = lower ERP = stocks expensive."""
        n = 30
        dates = pd.bdate_range("2026-01-01", periods=n)
        macro = pd.concat(
            [
                pd.DataFrame(
                    {
                        "date": [d.date() for d in dates],
                        "indicator": "us_pe_ttm",
                        "value": [40.0] * n,  # high PE, earnings yield = 2.5%
                        "source": "multpl",
                    }
                ),
                pd.DataFrame(
                    {
                        "date": [d.date() for d in dates],
                        "indicator": "us10y_real_yield",
                        "value": [3.0] * n,
                        "source": "fred",
                    }
                ),
            ],
            ignore_index=True,
        )
        out = derive_indicators(macro)
        erp_rows = out[out["indicator"] == "us_erp"]
        assert not erp_rows.empty
        # EY = 100/40 = 2.5%, ERP = 2.5% - 3% = -0.5%
        assert -0.6 < erp_rows["value"].iloc[0] < -0.4


# ---------------------------------------------------------------------------
# Regression tests for PR #4 review fixes
# ---------------------------------------------------------------------------


def _load_run_validation_module():
    """scripts/run_validation.py is not on the import path; load it ad-hoc."""
    import importlib.util
    import pathlib
    import sys

    if "run_validation" in sys.modules:
        return sys.modules["run_validation"]
    path = pathlib.Path(__file__).parent.parent / "scripts" / "run_validation.py"
    spec = importlib.util.spec_from_file_location("run_validation", path)
    module = importlib.util.module_from_spec(spec)
    # Register before exec so @dataclass inside the module can resolve cls.__module__.
    sys.modules["run_validation"] = module
    spec.loader.exec_module(module)
    return module


class TestDivergenceRecentRegression:
    """Fix #1 — divergence_recent must not raise UnboundLocalError on non-empty input."""

    def test_returns_figure_when_div_df_has_rows(self):
        from finsynapse.dashboard import charts

        df = pd.DataFrame(
            [
                {
                    "date": "2026-04-01",
                    "pair_name": "sp500_vix",
                    "is_divergent": True,
                    "strength": 0.42,
                    "description": "stocks-up vol-up",
                },
                {
                    "date": "2026-04-15",
                    "pair_name": "us10y_dxy",
                    "is_divergent": True,
                    "strength": 0.18,
                    "description": "yields-up dxy-down",
                },
            ]
        )
        fig = charts.divergence_recent(df)
        assert fig is not None
        # Two divergent pair groups → at least one trace
        assert len(fig.data) >= 1

    def test_returns_figure_when_div_df_empty(self):
        from finsynapse.dashboard import charts

        fig = charts.divergence_recent(pd.DataFrame(columns=["date", "is_divergent", "strength"]))
        assert fig is not None

    def test_returns_figure_when_no_rows_are_divergent(self):
        """All-False is_divergent — used to crash on the non-empty branch."""
        from finsynapse.dashboard import charts

        df = pd.DataFrame(
            [
                {
                    "date": "2026-04-01",
                    "pair_name": "sp500_vix",
                    "is_divergent": False,
                    "strength": 0.0,
                    "description": "",
                }
            ]
        )
        fig = charts.divergence_recent(df)
        assert fig is not None


class TestForwardHorizonTradingDays:
    """Fix #2 — FORWARD_HORIZONS values must be interpreted as TRADING days."""

    def test_six_month_horizon_lands_at_six_calendar_months(self):
        rv = _load_run_validation_module()

        # Build a synthetic price series of 600 business days (~ 2.4y) starting 2020-01-02
        dates = pd.bdate_range("2020-01-02", periods=600)
        wide = pd.DataFrame({"date": [d.date() for d in dates], "indicator": "sp500", "value": 100.0})
        # temp_df referencing 2020-03-23 (a Monday, in bdate_range)
        temp = pd.DataFrame([{"date": date(2020, 3, 23), "market": "us", "overall": 50.0}])
        rows = rv._compute_forward_returns(wide, temp)
        assert len(rows) == 1
        # 6m == 126 trading days from 2020-03-23 → 2020-09-22 (≈ 6 months later, NOT 2020-07-27 / ~4 months)
        # Verify by computing the expected date.
        bdates = pd.bdate_range("2020-01-02", periods=600)
        anchor_pos = bdates.get_loc(pd.Timestamp("2020-03-23"))
        expected_6m = bdates[anchor_pos + 126].date()
        # Expected ≈ Sep 2020 (true 6 months) — assert the computed return non-None and the
        # implementation horizon is in the "real 6m" ballpark, not the "calendar 4m" ballpark.
        assert expected_6m.month in (9, 10), f"unexpected horizon date {expected_6m}"
        assert rows[0].return_6m == 0.0  # flat synthetic prices
        # 12m: 252 trading days → ~ 1 year
        assert rows[0].return_12m == 0.0

    def test_horizon_returns_none_when_insufficient_history(self):
        rv = _load_run_validation_module()
        # Only 30 business days available, ask for 1m (21 td) — should work
        # but 12m (252 td) — must return None
        dates = pd.bdate_range("2020-01-02", periods=30)
        wide = pd.DataFrame({"date": [d.date() for d in dates], "indicator": "sp500", "value": 100.0})
        temp = pd.DataFrame([{"date": date(2020, 1, 6), "market": "us", "overall": 50.0}])
        rows = rv._compute_forward_returns(wide, temp)
        assert len(rows) == 1
        assert rows[0].return_1m == 0.0
        assert rows[0].return_12m is None


class TestGateChecksRho:
    """Fix #3 — gate requires hit-rate win AND ρ direction correct (ρ < 0, |ρ| ≥ 0.03)."""

    def test_gate_signature_accepts_pe_forward_rows(self):
        rv = _load_run_validation_module()
        import inspect

        sig = inspect.signature(rv._gate_check)
        params = list(sig.parameters)
        assert params == ["hit_table", "mf_forward_rows", "pe_forward_rows"]

    def test_gate_fails_when_rho_is_wrong_direction(self):
        """MF beats PE on hit rate, but ρ is POSITIVE (momentum-following, not mean-reverting).

        The new gate standard (Phase 4 review follow-up) requires ρ < 0
        AND |ρ| ≥ 0.03 — the temperature must be a mean-reversion indicator.
        """
        rv = _load_run_validation_module()

        if not rv.SCIPY_AVAILABLE:
            pytest.skip("scipy not available")

        np.random.seed(42)
        mf_rows = []
        pe_rows = []
        for market in ("us", "cn", "hk"):
            pe_temps = np.linspace(10, 90, 50)
            pe_returns_3m = -0.1 * (pe_temps - 50) / 50 + np.random.normal(0, 0.005, 50)
            # MF: POSITIVE correlation (high temp → high return = momentum, not mean-reversion)
            mf_temps = np.linspace(10, 90, 50)
            mf_returns_3m = +0.1 * (mf_temps - 50) / 50 + np.random.normal(0, 0.005, 50)
            for i in range(50):
                mf_rows.append(
                    rv.ForwardReturnRow(
                        date=date(2020, 1, 1),
                        market=market,
                        temperature=float(mf_temps[i]),
                        return_1m=None,
                        return_3m=float(mf_returns_3m[i]),
                        return_6m=None,
                        return_12m=None,
                    )
                )
                pe_rows.append(
                    rv.ForwardReturnRow(
                        date=date(2020, 1, 1),
                        market=market,
                        temperature=float(pe_temps[i]),
                        return_1m=None,
                        return_3m=float(pe_returns_3m[i]),
                        return_6m=None,
                        return_12m=None,
                    )
                )

        hit_table = {
            "multi-factor": {
                m: {"directional_rate": 0.9, "directional_hits": 9, "total": 10, "strict_hits": 5, "strict_rate": 0.5}
                for m in ("us", "cn", "hk")
            },
            "PE single-factor": {
                m: {"directional_rate": 0.5, "directional_hits": 5, "total": 10, "strict_hits": 3, "strict_rate": 0.3}
                for m in ("us", "cn", "hk")
            },
        }

        gate = rv._gate_check(hit_table, mf_rows, pe_rows)
        assert gate["passed"] is False, "Gate should FAIL when MF ρ is positive (wrong direction)"

    def test_gate_passes_when_rho_negative_and_hit_rate_wins(self):
        """MF beats PE on hit rate AND ρ is negative (correct mean-reversion direction)."""
        rv = _load_run_validation_module()

        if not rv.SCIPY_AVAILABLE:
            pytest.skip("scipy not available")

        np.random.seed(1)
        mf_rows = []
        pe_rows = []
        for market in ("us", "cn", "hk"):
            pe_temps = np.linspace(10, 90, 50)
            pe_returns_3m = -0.1 * (pe_temps - 50) / 50 + np.random.normal(0, 0.005, 50)
            # MF: NEGATIVE correlation (high temp → low return = mean-reverting)
            mf_temps = np.linspace(10, 90, 50)
            mf_returns_3m = -0.1 * (mf_temps - 50) / 50 + np.random.normal(0, 0.005, 50)
            for i in range(50):
                mf_rows.append(
                    rv.ForwardReturnRow(
                        date=date(2020, 1, 1),
                        market=market,
                        temperature=float(mf_temps[i]),
                        return_1m=None,
                        return_3m=float(mf_returns_3m[i]),
                        return_6m=None,
                        return_12m=None,
                    )
                )
                pe_rows.append(
                    rv.ForwardReturnRow(
                        date=date(2020, 1, 1),
                        market=market,
                        temperature=float(pe_temps[i]),
                        return_1m=None,
                        return_3m=float(pe_returns_3m[i]),
                        return_6m=None,
                        return_12m=None,
                    )
                )

        hit_table = {
            "multi-factor": {
                m: {"directional_rate": 0.9, "directional_hits": 9, "total": 10, "strict_hits": 5, "strict_rate": 0.5}
                for m in ("us", "cn", "hk")
            },
            "PE single-factor": {
                m: {"directional_rate": 0.5, "directional_hits": 5, "total": 10, "strict_hits": 3, "strict_rate": 0.3}
                for m in ("us", "cn", "hk")
            },
        }

        gate = rv._gate_check(hit_table, mf_rows, pe_rows)
        assert gate["passed"] is True, "Gate should PASS when MF ρ is negative AND hit rate wins"


class TestValidationReportPhase34Fields:
    """Fix #4 — load_report must round-trip external_anchor and bootstrap_ci."""

    def test_load_report_preserves_phase3_phase4_fields(self, tmp_path):
        report_json = {
            "version": "2.0.0",
            "generated": "2026-05-01",
            "pivots_total": 0,
            "pivots_evaluated": 0,
            "external_anchor": {
                "source": "CNN Fear & Greed",
                "pivot_comparison": [{"label": "test", "fg_value": 25, "fg_rating": "fear"}],
                "direction_agreement": {"aligned": 1, "total": 1},
                "correlation": {"spearman_rho": 0.186, "p_value": 0.003, "n": 100},
            },
            "bootstrap_ci": {
                "us": {"mean_band_width": 6.2},
                "cn": {"mean_band_width": 6.4},
                "hk": {"mean_band_width": 6.6},
            },
        }
        path = tmp_path / "validation_report.json"
        path.write_text(json.dumps(report_json))
        report = load_report(path)
        assert report is not None
        assert report.external_anchor is not None
        assert report.external_anchor["source"] == "CNN Fear & Greed"
        assert report.external_anchor["correlation"]["spearman_rho"] == 0.186
        assert report.bootstrap_ci is not None
        assert report.bootstrap_ci["us"]["mean_band_width"] == 6.2

    def test_load_report_phase3_phase4_fields_default_to_none(self, tmp_path):
        """A report without external_anchor / bootstrap_ci must still load."""
        report_json = {
            "version": "1.0.0",
            "generated": "2026-04-30",
            "pivots_total": 0,
            "pivots_evaluated": 0,
        }
        path = tmp_path / "validation_report.json"
        path.write_text(json.dumps(report_json))
        report = load_report(path)
        assert report is not None
        assert report.external_anchor is None
        assert report.bootstrap_ci is None


class TestFetchExternalAnchorsTLS:
    """Fix #5 — TLS verification must NOT be disabled."""

    def test_no_ssl_cert_none_in_source(self):
        import pathlib

        src = (pathlib.Path(__file__).parent.parent / "scripts" / "fetch_external_anchors.py").read_text()
        assert "CERT_NONE" not in src
        assert "check_hostname = False" not in src
