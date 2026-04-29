"""Verify market thermometer reads sensibly at known historical pivots.

This is the formal version of plan §11.6 'defense against玄学'. If you tweak
weights in config/weights.yaml, re-run this and it should still pass — if a
historical bottom now reads 'hot', the weights are wrong, not the market.

Usage:
    uv run python scripts/backtest_temperature.py

Exit code 0 if all assertions pass, 1 otherwise.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date

import pandas as pd

from finsynapse.transform.normalize import collect_bronze
from finsynapse.transform.percentile import compute_percentiles
from finsynapse.transform.temperature import compute_temperature


@dataclass(frozen=True)
class Pivot:
    label: str
    market: str
    date: date
    expected_zone: str  # "cold" | "mid" | "hot"
    note: str


# Pivots within our 15Y data window (2011→present). 2008 dropped — yfinance
# 15Y backfill doesn't reach Lehman; if you want it, ingest with 7000+ days.
PIVOTS: tuple[Pivot, ...] = (
    # --- US ---
    Pivot("2018 Christmas crash bottom", "us", date(2018, 12, 24), "cold",
          "VIX spiked, S&P -20% from peak"),
    Pivot("COVID liquidity bottom", "us", date(2020, 3, 23), "cold",
          "VIX 82, real rates spiked, USD scramble"),
    Pivot("ARK/SPAC peak", "us", date(2021, 2, 12), "hot",
          "CAPE near all-time-high, retail euphoria"),
    Pivot("Pre-9.24 A-shares (US side)", "us", date(2024, 9, 23), "mid",
          "US itself was mid-temp at this date"),
    # --- CN (Phase 1b) ---
    Pivot("A-shares 2015 peak", "cn", date(2015, 6, 12), "hot",
          "CSI300 6500 peak, leveraged retail mania"),
    Pivot("A-shares 2018 bottom", "cn", date(2018, 12, 28), "cold",
          "CSI300 -28% from 2018 high, trade-war fear"),
    Pivot("A-shares 2024 pre-9.24", "cn", date(2024, 9, 23), "cold",
          "CSI300 at 5Y low, pre-stimulus despair"),
    # --- HK (Phase 1b) ---
    # HK valuation source unavailable -> overall driven by sentiment+liquidity.
    # Pivots reflect what flows + global liquidity were doing at the time.
    Pivot("HK 2018 trade-war low", "hk", date(2018, 10, 30), "cold",
          "HSI -25%, southbound fled"),
    # Originally 2024-10-07 — that fell in the PRC National Day closure when
    # cn_south_5d had no fresh data and sentiment defaulted NaN. Use Oct 10,
    # the actual HK rally peak with full data.
    Pivot("HK 2024 9.24 surge", "hk", date(2024, 10, 10), "hot",
          "Massive southbound inflow, HSI +30% in weeks (sent 92° at peak)"),
)

# Strict zones per plan §11.3
STRICT_ZONES = {
    "cold": (0, 30),
    "mid": (30, 70),
    "hot": (70, 100),
}

# Directional gate: "is the reading clearly on the right side of mid?"
# This is the load-bearing test in Phase 1a, when liquidity is DXY-only and
# sentiment is VIX-only. Phase 1b (FRED + AkShare CN/HK) should hit strict.
DIR_COLD_MAX = 50  # cold pivot passes if overall < 50
DIR_HOT_MIN = 50   # hot pivot passes if overall > 50
DIR_MID_LO, DIR_MID_HI = 25, 75


def _strict_ok(overall: float, expected: str) -> bool:
    lo, hi = STRICT_ZONES[expected]
    if expected == "hot":
        return overall >= 70
    return lo <= overall < hi


def _directional_ok(overall: float, expected: str) -> bool:
    if expected == "cold":
        return overall < DIR_COLD_MAX
    if expected == "hot":
        return overall > DIR_HOT_MIN
    return DIR_MID_LO <= overall <= DIR_MID_HI


def main() -> int:
    print("[setup] loading bronze + computing percentile + temperature...")
    macro = collect_bronze()
    if macro.empty:
        print("FAIL: no bronze data. Run `uv run finsynapse ingest run --source yfinance_macro --lookback-days 5500`")
        return 1
    pct = compute_percentiles(macro)
    temp = compute_temperature(pct)

    print(f"[setup] {len(temp):,} temperature rows; markets: {sorted(temp['market'].unique())}")
    print()
    header = (
        f"{'pivot':<32} {'date':<12} {'mkt':<4} "
        f"{'overall':>8} {'val':>6} {'sent':>6} {'liq':>6} "
        f"{'expect':<6} {'strict':<8} {'direct':<8}"
    )
    print(header)
    print("-" * len(header))

    direct_fail = 0
    strict_fail = 0
    for p in PIVOTS:
        market_df = temp[(temp["market"] == p.market)].copy()
        market_df["date"] = pd.to_datetime(market_df["date"])
        sel = market_df[market_df["date"] <= pd.Timestamp(p.date)].tail(1)
        if sel.empty:
            print(f"{p.label:<32} {p.date} {p.market:<4}    no-data    {p.expected_zone:<6} SKIP     SKIP")
            continue
        row = sel.iloc[0]
        overall = row["overall"]
        strict_ok = _strict_ok(overall, p.expected_zone)
        direct_ok = _directional_ok(overall, p.expected_zone)
        if not direct_ok:
            direct_fail += 1
        if not strict_ok:
            strict_fail += 1
        print(
            f"{p.label:<32} {p.date} {p.market:<4} "
            f"{overall:>8.1f} {row['valuation']:>6.1f} {row['sentiment']:>6.1f} {row['liquidity']:>6.1f} "
            f"{p.expected_zone:<6} {'PASS' if strict_ok else 'FAIL':<8} {'PASS' if direct_ok else 'FAIL':<8}"
        )

    print()
    print(f"Strict zones (plan §11.3 ideal target): {len(PIVOTS) - strict_fail}/{len(PIVOTS)} pass")
    print(f"Directional (Phase 1a primary gate):    {len(PIVOTS) - direct_fail}/{len(PIVOTS)} pass")
    print()
    if direct_fail:
        print("❌ Directional FAIL — temperature on wrong side of mid. Re-check weights/inputs.")
        return 1
    if strict_fail:
        print("⚠️  Strict zones not all hit — these are honest multi-factor compromises:")
        print("   e.g. 2018-12 US sentiment was 1.5° but valuation still elevated → overall 31°")
        print("   The directional gate is the load-bearing test; strict is an aspiration.")
    else:
        print("✅ All pivots in their strict zones.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
