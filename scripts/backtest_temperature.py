"""Verify market thermometer reads sensibly at known historical pivots.

Lightweight wrapper that shares validation logic and pivot definitions
with run_validation.py.  Pivots are loaded from backtest_pivots.yaml.

Usage:
    uv run python scripts/backtest_temperature.py

Exit code 0 if all directional assertions pass, 1 otherwise.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import yaml

from finsynapse.transform.normalize import collect_bronze, derive_indicators
from finsynapse.transform.percentile import compute_percentiles
from finsynapse.transform.temperature import compute_temperature

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from run_validation import _directional_ok, _resolve_temp_at_date, _strict_ok  # noqa: E402

PIVOTS_PATH = SCRIPTS_DIR / "backtest_pivots.yaml"


def main() -> int:
    print("[setup] loading pivots + bronze + derive + percentile + temperature...")

    with PIVOTS_PATH.open() as f:
        pivots_raw = yaml.safe_load(f)
    pivot_list = pivots_raw["pivots"]
    print(f"[pivots] {len(pivot_list)} pivots loaded from {PIVOTS_PATH}")

    macro = collect_bronze()
    if macro.empty:
        print("FAIL: no bronze data. Run `uv run finsynapse ingest run --source all --lookback-days 5500`")
        return 1
    macro = derive_indicators(macro)
    pct = compute_percentiles(macro)
    temp = compute_temperature(pct)

    print(f"[setup] {len(temp):,} temperature rows; markets: {sorted(temp['market'].unique())}")
    print()
    header = (
        f"{'pivot':<44} {'date':<12} {'mkt':<4} "
        f"{'overall':>8} {'val':>6} {'sent':>6} {'liq':>6} "
        f"{'expect':<6} {'strict':<8} {'direct':<8}"
    )
    print(header)
    print("-" * len(header))

    direct_fail = 0
    strict_fail = 0
    for p in pivot_list:
        target = pd.Timestamp(p["date"]).date()
        row_data = _resolve_temp_at_date(temp, p["market"], target)
        if row_data is None:
            print(f"{p['label']:<44} {p['date']} {p['market']:<4}    no-data    {p['expected_zone']:<6} SKIP     SKIP")
            continue
        overall = row_data["overall"]
        strict_ok = _strict_ok(overall, p["expected_zone"])
        direct_ok = _directional_ok(overall, p["expected_zone"])
        if not direct_ok:
            direct_fail += 1
        if not strict_ok:
            strict_fail += 1
        print(
            f"{p['label']:<44} {p['date']} {p['market']:<4} "
            f"{overall:>8.1f} {row_data.get('valuation', float('nan')):>6.1f} "
            f"{row_data.get('sentiment', float('nan')):>6.1f} "
            f"{row_data.get('liquidity', float('nan')):>6.1f} "
            f"{p['expected_zone']:<6} {'PASS' if strict_ok else 'FAIL':<8} {'PASS' if direct_ok else 'FAIL':<8}"
        )

    print()
    total = len(pivot_list)
    print(f"Strict zones:  {total - strict_fail}/{total} pass")
    print(f"Directional:   {total - direct_fail}/{total} pass")
    print()
    if direct_fail:
        print("❌ Directional FAIL — temperature on wrong side of mid. Re-check weights/inputs.")
        return 1
    if strict_fail:
        print("⚠️  Strict zones not all hit — these may be honest multi-factor compromises:")
        print("   The directional gate is the load-bearing test; strict is an aspiration.")
    else:
        print("✅ All pivots in their strict zones.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
