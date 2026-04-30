from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from finsynapse import config as _cfg

_DIVERGENCE_COLUMNS = ["date", "pair_name", "a_change", "b_change", "is_divergent", "strength", "description"]


@dataclass(frozen=True)
class SignalPair:
    """A pair of indicators whose 1-day direction agreement carries
    a known financial meaning. `expected` is the 'normal' co-movement
    pattern; deviations are flagged as divergences and surfaced to the
    LLM prompt high-priority section (per plan §13)."""

    name: str
    a: str
    b: str
    expected: str  # "same" | "opposite"
    description_normal: str
    description_divergent: str


# Five hardcoded pairs with explicit financial meaning. Picking a small
# fixed set instead of statistical anomaly detection — over-engineering
# would drown signal in noise (plan §13 design principle).
PAIRS: tuple[SignalPair, ...] = (
    SignalPair(
        name="sp500_vix",
        a="sp500",
        b="vix",
        expected="opposite",
        description_normal="SP500 ↑ + VIX ↓: risk-on as expected",
        description_divergent="SP500 ↑ + VIX ↑: rising on rising fear — beware",
    ),
    SignalPair(
        name="us10y_dxy",
        a="us10y_yield",
        b="dxy",
        expected="same",
        description_normal="10Y ↑ + DXY ↑: tightening in sync",
        description_divergent="10Y ↑ + DXY ↓: yields up, dollar down — credit/inflation regime shift?",
    ),
    SignalPair(
        name="gold_real_rate",
        a="gold_futures",
        b="us10y_real_yield",
        expected="opposite",
        description_normal="Gold ↑ + real yield ↓: classic safe-haven",
        description_divergent="Gold ↑ + real yield ↑: de-dollarization / sovereign hedge bid?",
    ),
    SignalPair(
        name="sp500_us10y",
        a="sp500",
        b="us10y_yield",
        expected="same",
        description_normal="SP500 and 10Y move together: growth narrative dominant",
        description_divergent="SP500 ↑ + 10Y ↓: liquidity-driven rally without growth confirmation",
    ),
    SignalPair(
        name="hsi_dxy",
        a="hsi",
        b="dxy",
        expected="opposite",
        description_normal="HSI ↑ + DXY ↓: weak USD supports HK / EM",
        description_divergent="HSI ↑ + DXY ↑: HK rallies despite USD strength — domestic catalyst?",
    ),
    # CN price vs participation: a-share rally that *isn't* confirmed by
    # 5-day rolling turnover is distribution, not accumulation. Same logic
    # in reverse for selloffs on falling volume (no panic = floor).
    SignalPair(
        name="csi300_volume",
        a="csi300",
        b="cn_a_turnover_5d",
        expected="same",
        description_normal="CSI300 and A-share turnover move together: trend confirmed by participation",
        description_divergent="CSI300 ↑ + turnover ↓: rally without participation — distribution risk",
    ),
    # HSI vs southbound (Stock Connect): mainland money is the largest
    # marginal buyer of HK. Price moving against southbound flow signals
    # that the local/foreign bid (or lack thereof) is driving — note the
    # opposite-direction read.
    SignalPair(
        name="hsi_southbound",
        a="hsi",
        b="cn_south_5d",
        expected="same",
        description_normal="HSI and southbound 5d in sync: mainland flow drives HK as expected",
        description_divergent="HSI ↑ + southbound ↓: HK rises without mainland support — foreign-led rally?",
    ),
)


def compute_divergence(macro_long: pd.DataFrame) -> pd.DataFrame:
    """For each signal pair, compare daily directions; flag mismatches.

    Output schema:
        date | pair_name | a_change | b_change | is_divergent | strength | description
    `strength` = product of |pct_change| of both indicators — used to rank
    "interesting" divergences (a 0.1% mismatch is noise, a 2% mismatch matters).
    """
    if macro_long.empty:
        return pd.DataFrame(columns=_DIVERGENCE_COLUMNS)

    wide = macro_long.pivot_table(index="date", columns="indicator", values="value").sort_index()
    wide.index = pd.to_datetime(wide.index)
    pct_change = wide.pct_change()

    rows = []
    for pair in PAIRS:
        if pair.a not in pct_change.columns or pair.b not in pct_change.columns:
            continue
        a = pct_change[pair.a]
        b = pct_change[pair.b]
        # Skip days where either is NaN (one side missing).
        valid = a.notna() & b.notna() & (a != 0) & (b != 0)

        same_sign = (a > 0) == (b > 0)
        divergent = ~same_sign if pair.expected == "same" else same_sign

        strength = a.abs() * b.abs() * 100  # scale for readability

        for dt in wide.index[valid]:
            is_div = bool(divergent.loc[dt])
            rows.append(
                {
                    "date": dt.date(),
                    "pair_name": pair.name,
                    "a_change": float(a.loc[dt]),
                    "b_change": float(b.loc[dt]),
                    "is_divergent": is_div,
                    "strength": float(strength.loc[dt]),
                    "description": pair.description_divergent if is_div else pair.description_normal,
                }
            )

    if not rows:
        return pd.DataFrame(columns=_DIVERGENCE_COLUMNS)
    return pd.DataFrame(rows)


def write_silver_divergence(df: pd.DataFrame) -> Path:
    silver = _cfg.settings.silver_dir
    silver.mkdir(parents=True, exist_ok=True)
    path = silver / "divergence_daily.parquet"
    df.to_parquet(path, index=False)
    return path
