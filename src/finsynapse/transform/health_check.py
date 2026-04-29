from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from finsynapse import config as _cfg

# Plausibility bounds. Values outside these are categorically wrong (unit drift,
# bad parse, decimal-point error). NOT for "extreme but possible" values — the
# percentile machinery handles those. These are sanity floors/ceilings only.
PLAUSIBLE_BOUNDS: dict[str, tuple[float, float]] = {
    "vix": (5.0, 200.0),
    "us10y_yield": (0.1, 25.0),
    "us10y_real_yield": (-5.0, 15.0),
    "dxy": (50.0, 200.0),
    "usdcny": (4.0, 10.0),
    "hkdcny": (0.5, 1.5),
    "sp500": (100.0, 20000.0),
    "hsi": (5000.0, 80000.0),
    "csi300": (1000.0, 20000.0),
    "gold_futures": (300.0, 10000.0),
    "us_pe_ttm": (3.0, 200.0),
    "us_cape": (5.0, 100.0),
    # --- Phase 1b additions ---
    "csi300_pe_ttm": (3.0, 100.0),
    "csi300_pb": (0.5, 10.0),
    "cn_m2_yoy": (-5.0, 50.0),                # historical 6-30%; allow some headroom
    "cn_social_financing_12m": (5e4, 1e7),    # 12m rolling sum, 亿元 scale
    "cn_north_5d": (-2000.0, 2000.0),         # 5-day net flow, 亿元
    "cn_south_5d": (-2000.0, 3000.0),
    # --- Phase 1c additions ---
    "cn_a_turnover_5d": (1e8, 1e12),          # 5d-mean of (SSE+SZSE) volume in shares
    "hk_ewh_yield_ttm": (0.1, 12.0),          # TTM dividend yield % — 0.7-6.9 historically
}

# How many trailing-window stdevs constitutes a "jump". 5σ is intentionally
# loose — we only want to catch unit drifts (e.g. a price suddenly 100x), not
# legitimate market moves.
JUMP_SIGMA_THRESHOLD = 5.0
JUMP_LOOKBACK = 60  # trading days


@dataclass(frozen=True)
class HealthIssue:
    date: pd.Timestamp
    indicator: str
    rule: str  # "out_of_bounds" | "nan" | "zero" | "jump_5sigma" | "stale"
    detail: str
    severity: str  # "fail" | "warn"


def check(macro_long: pd.DataFrame) -> tuple[pd.DataFrame, list[HealthIssue]]:
    """Run all health checks. Return (clean_frame, issues).

    `clean_frame` strips any row marked severity=fail. `issues` records every
    finding (warn + fail) for the audit log.
    """
    if macro_long.empty:
        return macro_long, []

    issues: list[HealthIssue] = []
    fail_keys: set[tuple] = set()

    df = macro_long.copy()
    df["_dt"] = pd.to_datetime(df["date"])

    for indicator, group in df.groupby("indicator"):
        g = group.sort_values("_dt").reset_index(drop=True)

        # Rule 1: NaN
        nan_mask = g["value"].isna()
        for _, row in g[nan_mask].iterrows():
            issues.append(HealthIssue(row["_dt"], indicator, "nan", "value is NaN", "fail"))
            fail_keys.add((row["_dt"], indicator))

        # Rule 2: Zero (suspect for prices/rates that should never be 0)
        # Real rate, north/south flows can legitimately be 0 or negative.
        if indicator not in {"us10y_real_yield", "cn_north_5d", "cn_south_5d"}:
            zero_mask = g["value"] == 0
            for _, row in g[zero_mask].iterrows():
                issues.append(
                    HealthIssue(row["_dt"], indicator, "zero", "value is exactly 0", "fail")
                )
                fail_keys.add((row["_dt"], indicator))

        # Rule 3: Out-of-bounds
        if indicator in PLAUSIBLE_BOUNDS:
            lo, hi = PLAUSIBLE_BOUNDS[indicator]
            oob_mask = (g["value"] < lo) | (g["value"] > hi)
            for _, row in g[oob_mask].iterrows():
                issues.append(
                    HealthIssue(
                        row["_dt"],
                        indicator,
                        "out_of_bounds",
                        f"value={row['value']} outside [{lo},{hi}]",
                        "fail",
                    )
                )
                fail_keys.add((row["_dt"], indicator))

        # Rule 4: 5-sigma jump vs trailing window
        if len(g) >= JUMP_LOOKBACK + 1:
            ret = g["value"].pct_change()
            roll_std = ret.rolling(JUMP_LOOKBACK).std()
            jump_mask = ret.abs() > (JUMP_SIGMA_THRESHOLD * roll_std)
            jump_mask = jump_mask.fillna(False)
            for i in g.index[jump_mask]:
                row = g.loc[i]
                # Jumps are warn-only by default — extreme but legitimate moves
                # do happen (Aug 2024 JPY carry unwind, COVID 2020). Operator
                # decides whether to act.
                issues.append(
                    HealthIssue(
                        row["_dt"],
                        indicator,
                        "jump_5sigma",
                        f"pct_change={ret.loc[i]:.4f} vs roll_std={roll_std.loc[i]:.4f}",
                        "warn",
                    )
                )

    # Build clean frame: drop fail rows
    if fail_keys:
        mask = ~df.apply(lambda r: (r["_dt"], r["indicator"]) in fail_keys, axis=1)
        clean = df[mask].drop(columns=["_dt"]).reset_index(drop=True)
    else:
        clean = df.drop(columns=["_dt"])

    return clean, issues


def write_health_log(issues: list[HealthIssue]) -> Path:
    silver = _cfg.settings.silver_dir
    silver.mkdir(parents=True, exist_ok=True)
    path = silver / "health_log.parquet"
    if not issues:
        empty = pd.DataFrame(columns=["date", "indicator", "rule", "detail", "severity"])
        empty.to_parquet(path, index=False)
        return path
    df = pd.DataFrame(
        [
            {
                "date": i.date.date() if hasattr(i.date, "date") else i.date,
                "indicator": i.indicator,
                "rule": i.rule,
                "detail": i.detail,
                "severity": i.severity,
            }
            for i in issues
        ]
    )
    df.to_parquet(path, index=False)
    return path
