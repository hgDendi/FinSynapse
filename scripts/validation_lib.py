"""Shared validation primitives.

Extracted from `run_validation.py` so `oos_backtest.py` and
`grid_search_weights.py` can reuse the exact same logic. Keeping these
functions in one place is the only way to prevent the same kind of
silent drift that the legacy `backtest_temperature.py` had.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

SCRIPTS_DIR = Path(__file__).parent
PIVOTS_PATH = SCRIPTS_DIR / "backtest_pivots.yaml"

STRICT_ZONES = {"cold": (0, 30), "mid": (30, 70), "hot": (70, 100)}
INDEX_MAP = {"us": "sp500", "cn": "csi300", "hk": "hsi"}
FORWARD_HORIZONS = {"1m": 21, "3m": 63, "6m": 126, "12m": 252}

SCIPY_AVAILABLE = False
try:
    from scipy import stats as _scipy_stats  # noqa: F401

    SCIPY_AVAILABLE = True
except ImportError:
    pass


@dataclass
class ForwardReturnRow:
    date: date
    market: str
    temperature: float
    return_1m: float | None = None
    return_3m: float | None = None
    return_6m: float | None = None
    return_12m: float | None = None


def zone_of(overall: float) -> str:
    if pd.isna(overall):
        return "nan"
    if overall >= 70:
        return "hot"
    if overall < 30:
        return "cold"
    return "mid"


def strict_ok(value: float, expected: str) -> bool:
    if pd.isna(value):
        return False
    if expected == "hot":
        return value >= 70
    lo, hi = STRICT_ZONES[expected]
    return lo <= value < hi


def directional_ok(value: float, expected: str) -> bool:
    if pd.isna(value):
        return False
    if expected == "cold":
        return value < 50
    if expected == "hot":
        return value > 50
    return 25 <= value <= 75


def compute_forward_returns(macro_long: pd.DataFrame, temp_df: pd.DataFrame) -> list[ForwardReturnRow]:
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
            t_pos = prices.index.get_loc(t)
            n_prices = len(prices.index)
            fwd: dict[str, float | None] = {}
            for label, days in FORWARD_HORIZONS.items():
                fwd_pos = t_pos + days
                if fwd_pos >= n_prices:
                    fwd[f"return_{label}"] = None
                    continue
                fwd[f"return_{label}"] = float(prices.iloc[fwd_pos] / current - 1.0)
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


def spearman_rho(rows: list[ForwardReturnRow], market: str, horizon: str) -> float | None:
    if not SCIPY_AVAILABLE:
        return None
    xs = [r.temperature for r in rows if r.market == market and getattr(r, f"return_{horizon}") is not None]
    ys = [
        getattr(r, f"return_{horizon}")
        for r in rows
        if r.market == market and getattr(r, f"return_{horizon}") is not None
    ]
    if len(xs) < 30:
        return None
    from scipy import stats

    rho, _ = stats.spearmanr(xs, ys)
    return float(rho)
