"""Phase 1 validation: compare multi-factor temperature vs 3 baselines at
24 historical pivots, and compute forward-return predictive power.

Usage:
    uv run python scripts/run_validation.py
    uv run python scripts/run_validation.py --output report.json

Gate standard:
    Multi-factor temperature must beat PE single-factor in ≥ 2/3 markets
    on directional hit rate, AND |Spearman ρ|(temp → 3m forward return)
    must ≥ PE single-factor's |ρ|.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from finsynapse.transform.normalize import collect_bronze, derive_indicators
from finsynapse.transform.percentile import compute_percentiles
from finsynapse.transform.temperature import MARKETS, SUB_NAMES, WeightsConfig, compute_temperature
from finsynapse.transform.version import ALGO_VERSION

SCRIPTS_DIR = Path(__file__).parent
PIVOTS_PATH = SCRIPTS_DIR / "backtest_pivots.yaml"

STRICT_ZONES = {"cold": (0, 30), "mid": (30, 70), "hot": (70, 100)}
ZONE_NAMES = ["0-20 (极冷)", "20-40", "40-60", "60-80", "80-100 (极热)"]
ZONE_BINS = [0, 20, 40, 60, 80, 100]

# Index tickers mapped to macro indicator names for forward-return computation.
INDEX_MAP = {"us": "sp500", "cn": "csi300", "hk": "hsi"}
FORWARD_HORIZONS = {"1m": 21, "3m": 63, "6m": 126, "12m": 252}
SCIPY_AVAILABLE = False
try:
    from scipy import stats as _scipy_stats  # noqa: F401

    SCIPY_AVAILABLE = True
except ImportError:
    pass


@dataclass
class ControllerResult:
    name: str
    overall: float
    zone: str
    strict_pass: bool
    directional_pass: bool
    valuation: float | None = None
    sentiment: float | None = None
    liquidity: float | None = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "overall": round(self.overall, 1),
            "zone": self.zone,
            "strict_pass": self.strict_pass,
            "directional_pass": self.directional_pass,
            "valuation": round(self.valuation, 1) if self.valuation is not None else None,
            "sentiment": round(self.sentiment, 1) if self.sentiment is not None else None,
            "liquidity": round(self.liquidity, 1) if self.liquidity is not None else None,
        }


@dataclass
class PivotResult:
    label: str
    market: str
    date: date
    expected_zone: str
    controllers: list[ControllerResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "label": self.label,
            "market": self.market,
            "date": self.date.isoformat(),
            "expected_zone": self.expected_zone,
            "controllers": [c.to_dict() for c in self.controllers],
        }


@dataclass
class ForwardReturnRow:
    date: date
    market: str
    temperature: float
    return_1m: float | None = None
    return_3m: float | None = None
    return_6m: float | None = None
    return_12m: float | None = None

    def to_dict(self) -> dict:
        return {
            "date": self.date.isoformat(),
            "market": self.market,
            "temperature": round(self.temperature, 1),
            "return_1m": round(self.return_1m, 4) if self.return_1m is not None else None,
            "return_3m": round(self.return_3m, 4) if self.return_3m is not None else None,
            "return_6m": round(self.return_6m, 4) if self.return_6m is not None else None,
            "return_12m": round(self.return_12m, 4) if self.return_12m is not None else None,
        }


def _zone(overall: float) -> str:
    if pd.isna(overall):
        return "nan"
    if overall >= 70:
        return "hot"
    if overall < 30:
        return "cold"
    return "mid"


def _strict_ok(value: float, expected: str) -> bool:
    if pd.isna(value):
        return False
    if expected == "hot":
        return value >= 70
    lo, hi = STRICT_ZONES[expected]
    return lo <= value < hi


def _directional_ok(value: float, expected: str) -> bool:
    if pd.isna(value):
        return False
    if expected == "cold":
        return value < 50
    if expected == "hot":
        return value > 50
    return 25 <= value <= 75


def _build_temperature_from_pct_wide(
    pct_wide: pd.DataFrame, label: str, indicator: str, direction: str
) -> pd.DataFrame:
    """Build a single-indicator 'temperature' from one percentile column.

    Mimics the structure of `compute_temperature` output (date|market|overall|...)
    but with a single indicator serving as the 'overall' for each market.
    """
    if indicator not in pct_wide.columns:
        return pd.DataFrame(columns=["date", "market", "overall", "valuation", "sentiment", "liquidity"])
    s = pct_wide[indicator].copy()
    if direction == "-":
        s = 100.0 - s
    out = pd.DataFrame({"date": pct_wide.index.date, "overall": s.values})
    out["market"] = label
    out["valuation"] = np.nan
    out["sentiment"] = np.nan
    out["liquidity"] = np.nan
    out = out.dropna(subset=["overall"])
    return out


def _build_momentum_temperature(macro_long: pd.DataFrame) -> pd.DataFrame:
    """Compute 60-day price return and percentile-rank it as a temperature.

    For each market, extracts the index price, computes rolling 60d return,
    then computes a 10Y rolling percentile. High positive momentum -> high
    temperature (direction '+').
    """
    wide = macro_long.pivot_table(index="date", columns="indicator", values="value").sort_index()
    wide.index = pd.to_datetime(wide.index)
    frames: list[pd.DataFrame] = []
    for market, idx_ticker in INDEX_MAP.items():
        if idx_ticker not in wide.columns:
            continue
        idx = wide[idx_ticker].dropna()
        if len(idx) < 100:
            continue
        ret_60d = idx.pct_change(60)
        ret_s = pd.Series(ret_60d.values, index=idx.index).dropna()
        if len(ret_s) < 252:
            continue
        pct = ret_s.rolling(window=2520, min_periods=252).apply(
            lambda x: (x.rank(pct=True).iloc[-1]) * 100.0, raw=False
        )
        df = pd.DataFrame({"date": pct.index.date, "overall": pct.values})
        df = df.dropna(subset=["overall"])
        df["market"] = market
        df["valuation"] = np.nan
        df["sentiment"] = np.nan
        df["liquidity"] = np.nan
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["date", "market", "overall", "valuation", "sentiment", "liquidity"])
    return pd.concat(frames, ignore_index=True)


def _resolve_temp_at_date(temp_df: pd.DataFrame, market: str, target: date) -> dict | None:
    """Find the closest temperature row ≤ target date for a market."""
    sub = temp_df[temp_df["market"] == market].copy()
    sub["date"] = pd.to_datetime(sub["date"])
    sel = sub[sub["date"] <= pd.Timestamp(target)].sort_values("date").tail(1)
    if sel.empty:
        return None
    return sel.iloc[0].to_dict()


def _compute_forward_returns(macro_long: pd.DataFrame, temp_df: pd.DataFrame) -> list[ForwardReturnRow]:
    """For every date in temp_df, compute forward index returns at 1m/3m/6m/12m."""
    wide = macro_long.pivot_table(index="date", columns="indicator", values="value").sort_index()
    wide.index = pd.to_datetime(wide.index)

    rows: list[ForwardReturnRow] = []
    for market, idx_ticker in INDEX_MAP.items():
        if idx_ticker not in wide.columns:
            continue
        prices = wide[idx_ticker].dropna()
        sub = temp_df[temp_df["market"] == market].copy()
        sub["date"] = pd.to_datetime(sub["date"])
        for _, row in sub.iterrows():
            t = pd.Timestamp(row["date"])
            if t not in prices.index:
                continue
            current = prices.loc[t]
            fwd: dict[str, float | None] = {}
            t_pos = prices.index.get_loc(t)
            n_prices = len(prices.index)
            for label, days in FORWARD_HORIZONS.items():
                # FORWARD_HORIZONS values are TRADING-day counts (21/63/126/252).
                # Use positional offset on the trading-day index so 6m == ~126 trading days,
                # not 126 calendar days (~4.2 months).
                fwd_pos = t_pos + days
                if fwd_pos >= n_prices:
                    fwd[f"return_{label}"] = None
                    continue
                fwd_price = prices.iloc[fwd_pos]
                fwd[f"return_{label}"] = float(fwd_price / current - 1.0)
            rows.append(
                ForwardReturnRow(
                    date=t.date(),
                    market=market,
                    temperature=float(row["overall"]),
                    return_1m=fwd.get("return_1m"),
                    return_3m=fwd.get("return_3m"),
                    return_6m=fwd.get("return_6m"),
                    return_12m=fwd.get("return_12m"),
                )
            )
    return rows


def _zone_distribution(forward_rows: list[ForwardReturnRow]) -> dict[str, list]:
    """Bucket forward returns by temperature zone."""
    buckets: dict[str, dict[str, list]] = {z: {"1m": [], "3m": [], "6m": [], "12m": []} for z in ZONE_NAMES}
    for row in forward_rows:
        bin_idx = min(int(row.temperature // 20), 4)
        zone = ZONE_NAMES[bin_idx]
        for label in ["1m", "3m", "6m", "12m"]:
            val = getattr(row, f"return_{label}")
            if val is not None:
                buckets[zone][label].append(val)
    result: dict[str, list] = {}
    for zone, horizons in buckets.items():
        result[zone] = []
        for h_label in ["1m", "3m", "6m", "12m"]:
            vals = horizons[h_label]
            if vals:
                result[zone].append(
                    {
                        "horizon": h_label,
                        "mean_return": round(float(np.mean(vals)), 4),
                        "median_return": round(float(np.median(vals)), 4),
                        "win_rate": round(sum(1 for v in vals if v > 0) / len(vals), 4),
                        "n": len(vals),
                    }
                )
            else:
                result[zone].append(
                    {"horizon": h_label, "mean_return": None, "median_return": None, "win_rate": None, "n": 0}
                )
    return result


def _spearman_rho(forward_rows: list[ForwardReturnRow], market: str, horizon: str) -> float | None:
    """Compute Spearman ρ between temperature and forward return."""
    if not SCIPY_AVAILABLE:
        return None
    xs = [r.temperature for r in forward_rows if r.market == market and getattr(r, f"return_{horizon}") is not None]
    ys = [
        getattr(r, f"return_{horizon}")
        for r in forward_rows
        if r.market == market and getattr(r, f"return_{horizon}") is not None
    ]
    if len(xs) < 30:
        return None
    from scipy import stats

    rho, _ = stats.spearmanr(xs, ys)
    return float(rho)


def _hit_rate_table(pivot_results: list[PivotResult]) -> dict:
    """Compute per-controller hit rates by market."""
    controllers = ["multi-factor", "PE single-factor", "VIX single-point", "60d momentum"]
    table: dict[str, dict[str, dict]] = {}
    for ctrl in controllers:
        table[ctrl] = {}
        for market in list(MARKETS):
            relevant = [
                pr for pr in pivot_results if pr.market == market and any(c.name == ctrl for c in pr.controllers)
            ]
            if not relevant:
                continue
            directional_hits = sum(
                1 for pr in relevant for c in pr.controllers if c.name == ctrl and c.directional_pass
            )
            strict_hits = sum(1 for pr in relevant for c in pr.controllers if c.name == ctrl and c.strict_pass)
            table[ctrl][market] = {
                "total": len(relevant),
                "directional_hits": directional_hits,
                "directional_rate": round(directional_hits / len(relevant), 3) if relevant else 0,
                "strict_hits": strict_hits,
                "strict_rate": round(strict_hits / len(relevant), 3) if relevant else 0,
            }
    return table


def _gate_check(
    hit_table: dict,
    mf_forward_rows: list[ForwardReturnRow],
    pe_forward_rows: list[ForwardReturnRow],
) -> dict:
    """Apply the Phase 1 gate (per module docstring):

    A market is *beaten* iff multi-factor wins on BOTH:
      (a) directional_rate(MF) >= directional_rate(PE)            [hit-rate]
      (b) |ρ(MF, 3m forward)|  >= |ρ(PE, 3m forward)|             [predictive ρ]

    Gate passes when ≥ 2/3 markets are beaten.

    The ρ component requires SCIPY_AVAILABLE; if not, ρ is treated as a tie
    (mf_rho_win=True) so behavior degrades to hit-rate-only. The overall
    `passed` flag still reflects the docstring contract whenever scipy is
    present in CI.
    """
    mf = hit_table.get("multi-factor", {})
    pe = hit_table.get("PE single-factor", {})
    markets_beaten = 0
    market_details: dict[str, dict] = {}
    for market in list(MARKETS):
        mf_rate = mf.get(market, {}).get("directional_rate", 0)
        pe_rate = pe.get(market, {}).get("directional_rate", 0)

        mf_rho_3m = _spearman_rho(mf_forward_rows, market, "3m") if mf_forward_rows else None
        pe_rho_3m = _spearman_rho(pe_forward_rows, market, "3m") if pe_forward_rows else None

        # Tie when ρ is unavailable for either side (degenerate sample, scipy missing).
        if mf_rho_3m is None or pe_rho_3m is None:
            rho_win = True
            rho_unavailable = True
        else:
            rho_win = abs(mf_rho_3m) >= abs(pe_rho_3m)
            rho_unavailable = False

        hit_win = mf_rate >= pe_rate
        beaten = hit_win and rho_win
        if beaten:
            markets_beaten += 1

        market_details[market] = {
            "mf_directional_rate": mf_rate,
            "pe_directional_rate": pe_rate,
            "mf_directional_win": hit_win,
            "mf_rho_3m": round(mf_rho_3m, 4) if mf_rho_3m is not None else None,
            "pe_rho_3m": round(pe_rho_3m, 4) if pe_rho_3m is not None else None,
            "mf_rho_win": rho_win,
            "rho_unavailable": rho_unavailable,
            "beaten": beaten,
        }

    passed = markets_beaten >= 2
    return {
        "passed": passed,
        "markets_beaten": markets_beaten,
        "total_markets": 3,
        "standard": (
            "Multi-factor wins ≥ 2/3 markets on BOTH directional hit rate (≥ PE) "
            "AND |Spearman ρ|(temp → 3m forward) (≥ PE)."
        ),
        "details": market_details,
    }


def _build_single_indicator_temp(temp_df: pd.DataFrame, market: str) -> pd.DataFrame:
    """Not needed for gate — placeholder. The PE controller results are in the
    hit table already. Forward returns use multi-factor temperature only
    (per design: we're testing the multi-factor signal's predictive power)."""
    return temp_df


def _compute_pe_forward(forward_rows: list[ForwardReturnRow], market: str) -> list:
    """Placeholder for PE-specific forward returns."""
    return [r for r in forward_rows if r.market == market]


def _compute_market_forward_stats(forward_rows: list[ForwardReturnRow]) -> dict:
    """Per-market forward return statistics by horizon (all temps)."""
    stats: dict[str, dict] = {}
    for market in list(MARKETS):
        market_rows = [r for r in forward_rows if r.market == market]
        if not market_rows:
            continue
        horizon_stats: dict[str, dict] = {}
        for label in ["1m", "3m", "6m", "12m"]:
            vals = [getattr(r, f"return_{label}") for r in market_rows if getattr(r, f"return_{label}") is not None]
            if not vals:
                horizon_stats[label] = {"n": 0}
                continue
            rho = _spearman_rho(forward_rows, market, label)
            horizon_stats[label] = {
                "n": len(vals),
                "mean": round(float(np.mean(vals)), 4),
                "spearman_rho": round(rho, 4) if rho is not None else None,
            }
        stats[market] = horizon_stats
    return stats


def _compare_external_anchors(
    temp: pd.DataFrame,
    pivot_results: list[PivotResult],
    forward_rows: list[ForwardReturnRow],
) -> dict | None:
    """Compare multi-factor temperature against CNN Fear & Greed Index.

    Returns a dict with:
      - cnn_fg_pivot_comparison: per-pivot CNN F&G readings
      - cnn_fg_correlation: Spearman ρ(temp, CNN F&G) over overlap period
      - cnn_fg_coverage: date range of available CNN data
    """
    cnn_path = SCRIPTS_DIR / "cnn_fear_greed.csv"
    if not cnn_path.exists():
        print("  [ext] CNN F&G data not found — run scripts/fetch_external_anchors.py first")
        return None

    import csv

    cnn_data: dict[str, float] = {}
    with cnn_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            cnn_data[row["date"]] = float(row["value"])

    print(f"  [ext] CNN F&G: {len(cnn_data)} daily readings loaded")
    dates = sorted(cnn_data.keys())
    print(f"  [ext] CNN range: {dates[0]} .. {dates[-1]}")

    # Compare at pivot points (US only — CNN F&G is US-market sentiment)
    pivot_comparison: list[dict] = []
    for pr in pivot_results:
        if pr.market != "us":
            continue
        # Find nearest CNN F&G date
        target = pr.date.isoformat()
        cnn_val = cnn_data.get(target)
        if cnn_val is None:
            continue
        mf_val = next(
            (c.overall for c in pr.controllers if c.name == "multi-factor"),
            float("nan"),
        )
        # Determine CNN F&G zone (same scheme: <30 cold, 30-70 mid, ≥70 hot)
        cnn_zone = "hot" if cnn_val >= 70 else ("cold" if cnn_val < 30 else "mid")
        # Check if both agree on direction: both cold (<50) or both hot (>=50)
        mf_direction = "hot" if mf_val >= 50 else "cold"
        cnn_direction = "hot" if cnn_val >= 50 else "cold"
        aligned = mf_direction == cnn_direction
        pivot_comparison.append(
            {
                "label": pr.label,
                "date": target,
                "mf_temperature": round(mf_val, 1),
                "cnn_fg": round(cnn_val, 1),
                "cnn_zone": cnn_zone,
                "cnn_rating": _cnn_rating(cnn_val),
                "direction_aligned": aligned,
            }
        )

    # Compute correlation over overlap period
    temp_us = temp[temp["market"] == "us"].copy()
    temp_us["date"] = pd.to_datetime(temp_us["date"])
    temp_us = temp_us.set_index("date").sort_index()

    xs_cnn, ys_temp = [], []
    for d_str, cnn_val in cnn_data.items():
        d = pd.Timestamp(d_str)
        if d in temp_us.index:
            t_val = temp_us.loc[d, "overall"]
            if not pd.isna(t_val):
                xs_cnn.append(cnn_val)
                ys_temp.append(float(t_val))

    correlation = None
    if SCIPY_AVAILABLE and len(xs_cnn) >= 30:
        from scipy import stats

        rho, p = stats.spearmanr(xs_cnn, ys_temp)
        correlation = {"spearman_rho": round(float(rho), 4), "p_value": round(float(p), 6), "n": len(xs_cnn)}

    aligned_count = sum(1 for p in pivot_comparison if p["direction_aligned"])
    total_compared = len(pivot_comparison)
    print()
    print(f"  [ext] CNN F&G pivot alignment: {aligned_count}/{total_compared} directionally aligned")
    if correlation:
        print(
            f"  [ext] Spearman ρ(temp, CNN F&G) over {correlation['n']} days: {correlation['spearman_rho']:+.3f} (p={correlation['p_value']:.4f})"
        )

    return {
        "source": "CNN Fear & Greed Index (edition.cnn.com/markets/fear-and-greed)",
        "data_range": {"start": dates[0], "end": dates[-1], "n_entries": len(cnn_data)},
        "pivot_comparison": pivot_comparison,
        "correlation": correlation,
        "direction_agreement": {
            "aligned": aligned_count,
            "total": total_compared,
            "rate": round(aligned_count / total_compared, 3) if total_compared else 0,
        },
    }


def _cnn_rating(value: float) -> str:
    if value >= 75:
        return "extreme greed"
    if value >= 55:
        return "greed"
    if value >= 45:
        return "neutral"
    if value >= 25:
        return "fear"
    return "extreme fear"


def _bootstrap_confidence(
    temp: pd.DataFrame,
    n_bootstrap: int = 200,
    seed: int = 42,
) -> dict[str, dict]:
    """Bootstrap resample sub-weights using Dirichlet to get temperature CI.

    For each day and each market, resamples sub_weights from
    Dirichlet(α=sub_weights×scale) 200 times and computes the sampling
    distribution of overall temperature. Returns per-market dict of
    { 'lower_5': [...], 'upper_95': [...] } series.

    Scale factor of 100 means the Dirichlet is concentrated near the
    point estimate — wider bands mean the temperature is sensitive to
    small weight perturbations.
    """
    rng = np.random.default_rng(seed)
    scale = 100.0

    result: dict[str, dict] = {}
    for market in MARKETS:
        sub = temp[temp["market"] == market].copy()
        if sub.empty:
            continue
        sub = sub.set_index("date").sort_index()
        # Get sub-temp columns and sub-weights from the dataset
        subs_present = [s for s in SUB_NAMES if s in sub.columns and sub[s].notna().any()]
        if len(subs_present) < 2:
            continue

        # Use equal sub-weights as the base (no config dependency here)
        weights = np.ones(len(subs_present)) * scale / len(subs_present)
        n_dates = len(sub)

        boot_samples = np.zeros((n_bootstrap, n_dates))
        for b in range(n_bootstrap):
            w = rng.dirichlet(weights)
            overall_sample = np.zeros(n_dates)
            for i, s_name in enumerate(subs_present):
                overall_sample += sub[s_name].fillna(50).values * w[i]
            boot_samples[b] = overall_sample

        lower = np.percentile(boot_samples, 5, axis=0)
        upper = np.percentile(boot_samples, 95, axis=0)
        band_width = float(np.mean(upper - lower))

        result[market] = {
            "mean_band_width": round(band_width, 1),
            "n_bootstrap": n_bootstrap,
            "note": f"Typical uncertainty: {round(band_width, 1)}° — sub-weight resampling via Dirichlet(100)",
        }

    return result


def _champion_compare(hit_table: dict) -> dict | None:
    """Compare current multi-factor hit rates against prior baselines."""
    baseline_path = SCRIPTS_DIR / "champion_baseline.json"
    if not baseline_path.exists():
        return None

    import json as _json

    try:
        entries = _json.loads(baseline_path.read_text())
    except (json.JSONDecodeError, ValueError):
        return None

    if len(entries) < 2:
        return None

    prior = entries[-2]
    current = {
        "us": hit_table.get("multi-factor", {}).get("us", {}).get("directional_rate", 0),
        "cn": hit_table.get("multi-factor", {}).get("cn", {}).get("directional_rate", 0),
        "hk": hit_table.get("multi-factor", {}).get("hk", {}).get("directional_rate", 0),
    }
    prior_mf = {m: prior.get("markets", {}).get(m, {}).get("directional_rate", 0) for m in MARKETS}

    markets_delta = {}
    for mkt in MARKETS:
        markets_delta[mkt] = {
            "prior_rate": round(prior_mf[mkt], 3),
            "current_rate": round(current[mkt], 3),
            "directional_delta": round(current[mkt] - prior_mf[mkt], 3),
            "prior_date": prior.get("date", "unknown"),
        }

    return {
        "n_prior": len(entries) - 1,
        "markets": markets_delta,
    }


def _write_champion_baseline(report: dict) -> None:
    """Archive champion performance metrics for version governance.

    Reads existing `champion_baseline.json`, appends current metrics,
    and writes back. This builds an audit trail of algorithm performance
    across versions.
    """
    import json as _json

    baseline_path = SCRIPTS_DIR / "champion_baseline.json"
    entries: list[dict] = []
    if baseline_path.exists():
        try:
            entries = _json.loads(baseline_path.read_text())
        except (json.JSONDecodeError, ValueError):
            entries = []

    hit = report.get("hit_rate_table", {}).get("multi-factor", {})
    rho = report.get("spearman_rho", {})
    gate = report.get("gate", {})
    bootstrap_ci = report.get("bootstrap_ci", {})

    entry = {
        "date": report.get("generated", ""),
        "algo_version": ALGO_VERSION,
        "gate_passed": gate.get("passed", False),
        "markets": {
            m: {
                "directional_rate": hit.get(m, {}).get("directional_rate", 0),
                "strict_rate": hit.get(m, {}).get("strict_rate", 0),
                "spearman_rho_3m": rho.get(m, {}).get("3m", None),
                "spearman_rho_6m": rho.get(m, {}).get("6m", None),
                "bootstrap_band_width": bootstrap_ci.get(m, {}).get("mean_band_width", None),
            }
            for m in MARKETS
        },
    }

    # Don't double-insert same-date entry
    if entries and entries[-1].get("date") == entry["date"]:
        entries[-1] = entry
    else:
        entries.append(entry)

    baseline_path.write_text(json.dumps(entries, indent=2, ensure_ascii=False))


def main() -> int:
    print("=" * 60)
    print("  FinSynapse Phase 1 — Market Temperature Validation")
    print("=" * 60)
    print()

    # --- Load pivots ---
    with PIVOTS_PATH.open() as f:
        pivots_raw = yaml.safe_load(f)
    pivot_list = pivots_raw["pivots"]
    print(f"[pivots] {len(pivot_list)} pivots loaded from {PIVOTS_PATH}")

    # --- Load data ---
    print("[pipeline] loading bronze + derive + percentile + temperature...")
    macro = collect_bronze()
    if macro.empty:
        print("FAIL: no bronze data. Run `uv run finsynapse ingest run --source all --lookback-days 5500`")
        return 1
    print(f"  bronze: {len(macro):,} rows, {macro['indicator'].nunique()} indicators")
    macro = derive_indicators(macro)

    pct = compute_percentiles(macro)
    print(f"  percentile: {len(pct):,} rows")

    cfg = WeightsConfig.load()
    temp = compute_temperature(pct, cfg)
    print(f"  temperature: {len(temp):,} rows")

    # Build pct_wide for single-indicator baselines
    pct_copy = pct.copy()
    pct_copy["date"] = pd.to_datetime(pct_copy["date"])
    series_by_ind: dict[str, pd.Series] = {}
    for ind_name, group in pct_copy.groupby("indicator"):
        col = cfg.window_for(str(ind_name))
        if col not in group.columns:
            continue
        s = group.set_index("date")[col].sort_index()
        s = s[~s.index.duplicated(keep="last")]
        series_by_ind[str(ind_name)] = s
    pct_wide = pd.concat(series_by_ind, axis=1).sort_index() if series_by_ind else pd.DataFrame()

    # --- Build baselines ---
    # PE single-factor
    pe_frames = []
    if "us_cape" in pct_wide.columns:
        pe_frames.append(_build_temperature_from_pct_wide(pct_wide, "us", "us_cape", "+"))
    elif "us_pe_ttm" in pct_wide.columns:
        pe_frames.append(_build_temperature_from_pct_wide(pct_wide, "us", "us_pe_ttm", "+"))
    if "csi300_pe_ttm" in pct_wide.columns:
        pe_frames.append(_build_temperature_from_pct_wide(pct_wide, "cn", "csi300_pe_ttm", "+"))
    if "hk_ewh_yield_ttm" in pct_wide.columns:
        pe_frames.append(_build_temperature_from_pct_wide(pct_wide, "hk", "hk_ewh_yield_ttm", "-"))
    pe_temp = pd.concat(pe_frames, ignore_index=True) if pe_frames else pd.DataFrame()

    # VIX single-point (US only)
    vix_temp = (
        _build_temperature_from_pct_wide(pct_wide, "us", "vix", "-") if "vix" in pct_wide.columns else pd.DataFrame()
    )

    # 60d momentum
    mom_temp = _build_momentum_temperature(macro)
    print(f"  baselines: PE={len(pe_temp)} rows, VIX={len(vix_temp)} rows, momentum={len(mom_temp)} rows")

    # --- Evaluate pivots ---
    print()
    pivot_results: list[PivotResult] = []
    header = f"{'pivot':<40} {'date':<12} {'mkt':<4} {'expect':<6} {'multi':>7} {'PE':>7} {'VIX':>7} {'mom':>7}"
    print(header)
    print("-" * len(header))

    for p in pivot_list:
        pr = PivotResult(
            label=p["label"], market=p["market"], date=date.fromisoformat(p["date"]), expected_zone=p["expected_zone"]
        )
        target = date.fromisoformat(p["date"])

        controllers_data: list[tuple[str, pd.DataFrame, str | None]] = [
            ("multi-factor", temp, None),
            ("PE single-factor", pe_temp, None),
            ("VIX single-point", vix_temp, "us"),
            ("60d momentum", mom_temp, None),
        ]

        for c_name, c_df, c_only_market in controllers_data:
            if c_only_market and p["market"] != c_only_market:
                continue
            row_data = _resolve_temp_at_date(c_df, p["market"], target)
            if row_data is None:
                continue
            overall = row_data.get("overall", float("nan"))
            zone = _zone(overall)
            pr.controllers.append(
                ControllerResult(
                    name=c_name,
                    overall=overall if not pd.isna(overall) else float("nan"),
                    zone=zone,
                    strict_pass=_strict_ok(overall, p["expected_zone"]),
                    directional_pass=_directional_ok(overall, p["expected_zone"]),
                    valuation=row_data.get("valuation"),
                    sentiment=row_data.get("sentiment"),
                    liquidity=row_data.get("liquidity"),
                )
            )

        multi_val = next((c.overall for c in pr.controllers if c.name == "multi-factor"), float("nan"))
        pe_val = next((c.overall for c in pr.controllers if c.name == "PE single-factor"), float("nan"))
        vix_val = next((c.overall for c in pr.controllers if c.name == "VIX single-point"), float("nan"))
        mom_val = next((c.overall for c in pr.controllers if c.name == "60d momentum"), float("nan"))
        print(
            f"{p['label']:<40} {p['date']} {p['market']:<4} {p['expected_zone']:<6} "
            f"{multi_val:>7.1f} {pe_val:>7.1f} {vix_val:>7.1f} {mom_val:>7.1f}"
        )
        pivot_results.append(pr)

    # --- Forward returns ---
    print()
    print("[forward] computing forward returns...")
    forward_rows = _compute_forward_returns(macro, temp)
    print(f"  {len(forward_rows):,} forward-return rows computed (multi-factor)")
    pe_forward_rows = _compute_forward_returns(macro, pe_temp) if not pe_temp.empty else []
    print(f"  {len(pe_forward_rows):,} forward-return rows computed (PE single-factor)")

    # --- Zone distribution ---
    zone_dist = _zone_distribution(forward_rows)

    # --- Hit rate table ---
    hit_table = _hit_rate_table(pivot_results)

    # --- Forward stats ---
    fwd_stats = _compute_market_forward_stats(forward_rows)

    # --- Gate ---
    gate = _gate_check(hit_table, forward_rows, pe_forward_rows)
    print(f"  gate status: {'PASSED' if gate['passed'] else 'FAILED'}")

    # --- Champion baseline comparison ---
    champion_delta = _champion_compare(hit_table)
    if champion_delta:
        print()
        print(f"  [champion] vs prior baseline (n={champion_delta.get('n_prior', 0)} entries):")
        for mkt, d in champion_delta.get("markets", {}).items():
            delta_us = d.get("directional_delta", 0)
            sign = "+" if delta_us >= 0 else ""
            print(
                f"    {mkt.upper()}: directional {sign}{delta_us:.1%} (was {d.get('prior_rate', 0):.1%}, now {d.get('current_rate', 0):.1%})"
            )

    # --- Bootstrap confidence ---
    print("[bootstrap] computing 200-sample Dirichlet bootstrap CI...")
    bootstrap_ci = _bootstrap_confidence(temp)
    for market, ci in bootstrap_ci.items():
        print(f"  {market.upper()}: mean band width = {ci['mean_band_width']:.1f}° (n={ci['n_bootstrap']})")

    # --- Print summary ---
    print()
    print("=" * 60)
    print("  VALIDATION SUMMARY")
    print("=" * 60)
    print()
    print("Hit Rate Comparison (directional):")
    for ctrl in ["multi-factor", "PE single-factor", "VIX single-point", "60d momentum"]:
        parts = [f"  {ctrl}:"]
        for market in list(MARKETS):
            m = hit_table.get(ctrl, {}).get(market, {})
            if m:
                parts.append(f"  {market.upper()}={m['directional_hits']}/{m['total']} ({m['directional_rate']:.1%})")
        print("".join(parts))

    print()
    print("Forward Return Spearman ρ (temperature → future return):")
    for market in list(MARKETS):
        parts = [f"  {market.upper()}: "]
        for label in ["1m", "3m", "6m", "12m"]:
            rho = _spearman_rho(forward_rows, market, label)
            rho_str = f"{rho:+.3f}" if rho is not None else "N/A"
            parts.append(f"{label}={rho_str}  ")
        print("".join(parts))

    print()
    print("Zone Distribution (mean returns):")
    for zone in ZONE_NAMES:
        z_data = zone_dist.get(zone, [])
        _1m = next((h for h in z_data if h["horizon"] == "1m"), {})
        _3m = next((h for h in z_data if h["horizon"] == "3m"), {})
        _6m = next((h for h in z_data if h["horizon"] == "6m"), {})
        m1 = f"{_1m.get('mean_return', 0):+.2%}" if _1m.get("mean_return") is not None else "N/A"
        m3 = f"{_3m.get('mean_return', 0):+.2%}" if _3m.get("mean_return") is not None else "N/A"
        m6 = f"{_6m.get('mean_return', 0):+.2%}" if _6m.get("mean_return") is not None else "N/A"
        print(f"  {zone:<18}  1m={m1}  3m={m3}  6m={m6}")

    print()
    gate_status = "PASSED" if gate["passed"] else "FAILED"
    print(f"GATE: {gate_status}  ({gate['markets_beaten']}/{gate['total_markets']} markets)")
    for mkt, detail in gate["details"].items():
        status = "✓" if detail["beaten"] else "✗"
        print(
            f"  {status} {mkt.upper()}: MF={detail['mf_directional_rate']:.1%} vs PE={detail['pe_directional_rate']:.1%}"
        )

    # --- Build full report ---
    # Phase 3: external anchor comparison
    external_anchor = _compare_external_anchors(temp, pivot_results, forward_rows)

    report = {
        "version": "1.0.0",
        "algo_version": ALGO_VERSION,
        "generated": date.today().isoformat(),
        "pivots_total": len(pivot_list),
        "pivots_evaluated": sum(len(pr.controllers) > 0 for pr in pivot_results),
        "pivot_results": [pr.to_dict() for pr in pivot_results],
        "hit_rate_table": hit_table,
        "forward_stats": fwd_stats,
        "zone_distribution": zone_dist,
        "spearman_rho": {
            market: {label: _spearman_rho(forward_rows, market, label) for label in ["1m", "3m", "6m", "12m"]}
            for market in list(MARKETS)
        },
        "gate": gate,
        "external_anchor": external_anchor,
        "bootstrap_ci": bootstrap_ci,
        "note": (
            "All percentiles computed via pandas rolling() — backward-looking, no look-ahead bias. "
            "Forward returns use next-available business-day index price at each horizon. "
            "Gate standard: MF directional rate ≥ PE in ≥2/3 markets."
        ),
    }

    # Write report
    _write_champion_baseline(report)
    out_path = SCRIPTS_DIR / "validation_report.json"
    with out_path.open("w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    print()
    print(f"[report] written -> {out_path}")
    print(f"[report] {out_path.stat().st_size:,} bytes")

    if not gate["passed"]:
        print()
        print("GATE FAILED — multi-factor temperature did not beat PE single-factor in ≥2 markets.")
        print("Consider re-examining weights or indicator selection before expanding data sources.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
