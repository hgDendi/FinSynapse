"""Rolling out-of-sample IC for the multi-factor temperature.

Cuts the full forward-return series into rolling windows, computes
Spearman ρ (= IC) for each window, and reports:

  - IC time series per (market, horizon)
  - IC IR = mean(IC) / std(IC)  (Information Ratio for the IC stream)
  - Hit rate of IC < 0 (we want negative ρ — high temperature = low future return)
  - Decay across horizons (1m / 3m / 6m / 12m)

Why this matters: the existing `run_validation.py` measures ρ over the
full sample. That tells you "the model worked on this data" but not
"the model is stable across regimes". Rolling IC catches regime-specific
breakdowns (e.g. signal works pre-2020 but inverts post-2022).

Output:
  scripts/oos_results.json      versioned audit trail
  stdout                         human summary

Usage:
  uv run python scripts/oos_backtest.py
  uv run python scripts/oos_backtest.py --window-months 24 --step-months 1
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from finsynapse.transform.normalize import collect_bronze, derive_indicators
from finsynapse.transform.percentile import compute_percentiles
from finsynapse.transform.temperature import MARKETS, WeightsConfig, compute_temperature
from finsynapse.transform.version import ALGO_VERSION
from scripts.validation_lib import (
    FORWARD_HORIZONS,
    SCIPY_AVAILABLE,
    ForwardReturnRow,
    compute_forward_returns,
)

SCRIPTS_DIR = Path(__file__).parent
DEFAULT_WINDOW_MONTHS = 36
DEFAULT_STEP_MONTHS = 3
MIN_OBS_PER_WINDOW = 30


@dataclass
class WindowIC:
    window_start: str
    window_end: str
    n_obs: int
    ic: float


def _slice_window(rows: list[ForwardReturnRow], market: str, start: date, end: date) -> list[ForwardReturnRow]:
    return [r for r in rows if r.market == market and start <= r.date <= end]


def rolling_ic(
    forward_rows: list[ForwardReturnRow],
    market: str,
    horizon: str,
    window_months: int = DEFAULT_WINDOW_MONTHS,
    step_months: int = DEFAULT_STEP_MONTHS,
) -> list[WindowIC]:
    """Compute IC over rolling windows. Returns one WindowIC per window
    that has at least MIN_OBS_PER_WINDOW non-null observations."""
    if not SCIPY_AVAILABLE:
        return []
    market_rows = sorted([r for r in forward_rows if r.market == market], key=lambda r: r.date)
    if not market_rows:
        return []
    start_d = market_rows[0].date
    end_d = market_rows[-1].date
    out: list[WindowIC] = []
    cursor = start_d
    while True:
        window_end = cursor + pd.DateOffset(months=window_months)
        window_end_date = window_end.date()
        if window_end_date > end_d:
            break
        slice_ = _slice_window(forward_rows, market, cursor, window_end_date)
        xs = [r.temperature for r in slice_ if getattr(r, f"return_{horizon}") is not None]
        ys = [getattr(r, f"return_{horizon}") for r in slice_ if getattr(r, f"return_{horizon}") is not None]
        if len(xs) >= MIN_OBS_PER_WINDOW:
            from scipy import stats

            rho, _ = stats.spearmanr(xs, ys)
            if not np.isnan(rho):
                out.append(
                    WindowIC(
                        window_start=cursor.isoformat(),
                        window_end=window_end_date.isoformat(),
                        n_obs=len(xs),
                        ic=round(float(rho), 4),
                    )
                )
        cursor = (cursor + pd.DateOffset(months=step_months)).date()
    return out


def _summarize(window_ics: list[WindowIC]) -> dict:
    if not window_ics:
        return {"n_windows": 0, "ic_mean": None, "ic_std": None, "ic_ir": None, "ic_neg_rate": None}
    ics = np.array([w.ic for w in window_ics])
    mean = float(np.mean(ics))
    std = float(np.std(ics, ddof=1)) if len(ics) > 1 else 0.0
    ir = float(mean / std) if std > 0 else None
    neg_rate = float((ics < 0).mean())
    return {
        "n_windows": len(window_ics),
        "ic_mean": round(mean, 4),
        "ic_std": round(std, 4),
        "ic_ir": round(ir, 4) if ir is not None else None,
        "ic_neg_rate": round(neg_rate, 4),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Rolling OOS IC for FinSynapse temperature")
    parser.add_argument("--window-months", type=int, default=DEFAULT_WINDOW_MONTHS)
    parser.add_argument("--step-months", type=int, default=DEFAULT_STEP_MONTHS)
    parser.add_argument("--output", type=Path, default=SCRIPTS_DIR / "oos_results.json")
    args = parser.parse_args()

    print("=" * 60)
    print("  FinSynapse — Rolling OOS Backtest")
    print("=" * 60)
    print(f"  window={args.window_months}m  step={args.step_months}m  algo={ALGO_VERSION}")
    print()

    print("[pipeline] loading bronze + derive + percentile + temperature...")
    macro = collect_bronze()
    if macro.empty:
        print("FAIL: no bronze data. Run `uv run finsynapse ingest run --source all --lookback-days 5500`")
        return 1
    macro = derive_indicators(macro)
    pct = compute_percentiles(macro)
    cfg = WeightsConfig.load()
    temp = compute_temperature(pct, cfg)
    forward_rows = compute_forward_returns(macro, temp)
    print(f"  forward rows: {len(forward_rows):,}")

    results: dict = {"algo_version": ALGO_VERSION, "generated": date.today().isoformat(), "config": {}, "by_market": {}}
    args_dict = {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()}
    results["config"] = args_dict

    for market in MARKETS:
        market_block: dict = {}
        for horizon in FORWARD_HORIZONS:
            windows = rolling_ic(forward_rows, market, horizon, args.window_months, args.step_months)
            summary = _summarize(windows)
            market_block[horizon] = {"summary": summary, "windows": [asdict(w) for w in windows]}
        results["by_market"][market] = market_block

    print()
    print(f"{'market':<6} {'horizon':<8} {'n_win':>6} {'ic_mean':>10} {'ic_std':>8} {'ic_ir':>8} {'neg_rate':>10}")
    print("-" * 60)
    for market in MARKETS:
        for horizon in FORWARD_HORIZONS:
            s = results["by_market"][market][horizon]["summary"]
            n = s["n_windows"]
            mean = f"{s['ic_mean']:+.4f}" if s["ic_mean"] is not None else "  N/A "
            std = f"{s['ic_std']:.4f}" if s["ic_std"] is not None else " N/A"
            ir = f"{s['ic_ir']:+.3f}" if s["ic_ir"] is not None else "  N/A"
            neg = f"{s['ic_neg_rate']:.2%}" if s["ic_neg_rate"] is not None else "  N/A"
            print(f"{market:<6} {horizon:<8} {n:>6} {mean:>10} {std:>8} {ir:>8} {neg:>10}")

    args.output.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    print()
    print(f"[report] written -> {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
