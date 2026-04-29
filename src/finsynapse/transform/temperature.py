from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import yaml

from finsynapse import config as _cfg

CONFIG_PATH = Path("config/weights.yaml")
MARKETS = ("cn", "hk", "us")
SUB_NAMES = ("valuation", "sentiment", "liquidity")


@dataclass
class WeightsConfig:
    sub_weights: dict
    indicator_weights: dict
    percentile_window: str

    @classmethod
    def load(cls, path: Path | None = None) -> "WeightsConfig":
        p = path or CONFIG_PATH
        with p.open() as f:
            raw = yaml.safe_load(f)
        return cls(**raw)


def _sub_temperature(
    pct_wide: pd.DataFrame,
    market: str,
    sub: str,
    cfg: WeightsConfig,
) -> pd.Series:
    """Compute one sub-temperature time series for one market.

    Returns NaN where ALL input indicators are missing on that date.
    Renormalizes weights across only-available indicators (so a missing
    PCR doesn't tank the HK sentiment signal — handled per plan §11.6)."""
    block_key = f"{market}_{sub}"
    block = cfg.indicator_weights.get(block_key, {})
    if not block:
        return pd.Series(index=pct_wide.index, dtype=float)

    contributions = {}
    available_weights = {}
    for indicator, spec in block.items():
        if indicator not in pct_wide.columns:
            continue
        col = pct_wide[indicator]
        if spec["direction"] == "-":
            col = 100.0 - col
        contributions[indicator] = col
        available_weights[indicator] = spec["weight"]

    if not contributions:
        return pd.Series(index=pct_wide.index, dtype=float)

    # Normalize available weights so they sum to 1.0 (handles missing indicators).
    total_w = sum(available_weights.values())
    sub_temp = pd.Series(0.0, index=pct_wide.index)
    weight_sum = pd.Series(0.0, index=pct_wide.index)
    for ind, contrib in contributions.items():
        w = available_weights[ind] / total_w
        valid = contrib.notna()
        sub_temp = sub_temp.add(contrib.fillna(0) * w, fill_value=0)
        weight_sum = weight_sum.add(valid.astype(float) * w, fill_value=0)
    # Where no indicator was valid -> NaN
    sub_temp = sub_temp.where(weight_sum > 0)
    # Re-normalize where weights summed to less than 1 due to per-day NaNs
    sub_temp = sub_temp / weight_sum.where(weight_sum > 0)
    return sub_temp


def _attribution_1w(daily: pd.DataFrame) -> pd.DataFrame:
    """For each market, decompose 1-week change in `overall` into contributions
    from valuation/sentiment/liquidity sub-temperatures."""
    out = []
    for market in MARKETS:
        sub = daily[daily["market"] == market].set_index("date").sort_index()
        if sub.empty:
            continue
        d_overall = sub["overall"].diff(5)
        for s in SUB_NAMES:
            d_sub = sub[s].diff(5)
            sub[f"{s}_contribution_1w"] = d_sub
        sub["overall_change_1w"] = d_overall
        sub["market"] = market
        out.append(sub.reset_index())
    if not out:
        return pd.DataFrame()
    return pd.concat(out, ignore_index=True)


def compute_temperature(percentile_long: pd.DataFrame, cfg: WeightsConfig | None = None) -> pd.DataFrame:
    """Build the temperature_daily table from the long percentile frame.

    Output schema:
        date | market | overall | valuation | sentiment | liquidity
            | overall_change_1w
            | valuation_contribution_1w | sentiment_contribution_1w | liquidity_contribution_1w
            | data_quality
    """
    cfg = cfg or WeightsConfig.load()
    if percentile_long.empty:
        return pd.DataFrame()

    pct_col = cfg.percentile_window  # e.g. 'pct_10y'
    pct_wide = (
        percentile_long.pivot_table(index="date", columns="indicator", values=pct_col)
        .sort_index()
    )
    pct_wide.index = pd.to_datetime(pct_wide.index)

    rows = []
    for market in MARKETS:
        sub_w = cfg.sub_weights.get(market, {})
        if not sub_w:
            continue
        sub_temps = {sub: _sub_temperature(pct_wide, market, sub, cfg) for sub in SUB_NAMES}

        # Renormalize sub_w over available sub-temperatures.
        avail_w = {}
        for sub in SUB_NAMES:
            if sub_temps[sub].notna().any():
                avail_w[sub] = sub_w[sub]
        if not avail_w:
            continue
        total = sum(avail_w.values())
        avail_w = {k: v / total for k, v in avail_w.items()}

        overall = pd.Series(0.0, index=pct_wide.index)
        weight_sum = pd.Series(0.0, index=pct_wide.index)
        for sub, w in avail_w.items():
            t = sub_temps[sub]
            valid = t.notna()
            overall = overall.add(t.fillna(0) * w, fill_value=0)
            weight_sum = weight_sum.add(valid.astype(float) * w, fill_value=0)
        overall = overall.where(weight_sum > 0) / weight_sum.where(weight_sum > 0)

        df = pd.DataFrame(
            {
                "date": pct_wide.index.date,
                "market": market,
                "overall": overall.values,
                "valuation": sub_temps["valuation"].values,
                "sentiment": sub_temps["sentiment"].values,
                "liquidity": sub_temps["liquidity"].values,
            }
        )
        # Per-row data_quality reflects ACTUAL nan presence (not just config),
        # so a row where M2 lags or north flow stopped publishing is flagged
        # accurately. This drives the dashboard's "data_quality: foo_unavailable"
        # warning per plan §11.6.
        def _row_quality(row: pd.Series) -> str:
            missing = [s for s in SUB_NAMES if pd.isna(row[s])]
            return ",".join(f"{s}_unavailable" for s in missing) if missing else "ok"

        df["data_quality"] = df.apply(_row_quality, axis=1)
        rows.append(df)

    if not rows:
        return pd.DataFrame()

    daily = pd.concat(rows, ignore_index=True).dropna(subset=["overall"]).reset_index(drop=True)
    return _attribution_1w(daily)


def write_silver_temperature(df: pd.DataFrame) -> Path:
    silver = _cfg.settings.silver_dir
    silver.mkdir(parents=True, exist_ok=True)
    path = silver / "temperature_daily.parquet"
    df.to_parquet(path, index=False)
    return path
