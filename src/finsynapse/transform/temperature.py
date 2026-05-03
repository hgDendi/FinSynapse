from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import yaml

from finsynapse import config as _cfg
from finsynapse.transform.version import snapshot_weights, stamp_version

CONFIG_PATH = Path("config/weights.yaml")
MARKETS = ("cn", "hk", "us")
SUB_NAMES = ("valuation", "sentiment", "liquidity")


@dataclass
class WeightsConfig:
    sub_weights: dict
    indicator_weights: dict
    percentile_window: str  # default; per-indicator `window:` field overrides

    def __post_init__(self) -> None:
        for block_name, block in self.indicator_weights.items():
            if not block:
                continue
            total = sum(spec["weight"] for spec in block.values())
            if abs(total - 1.0) > 1e-6:
                raise ValueError(
                    f"weights.yaml block '{block_name}' sums to {total} (expected 1.0). "
                    f"Each indicator_weights sub-block must be normalized."
                )
        seen_windows: dict[str, str] = {}
        for block in self.indicator_weights.values():
            for indicator, spec in block.items():
                w = spec.get("window")
                if not w:
                    continue
                prev = seen_windows.get(indicator)
                if prev and prev != w:
                    raise ValueError(
                        f"weights.yaml indicator '{indicator}' has inconsistent "
                        f"`window:` overrides ({prev} vs {w}). Make them match or "
                        f"remove one."
                    )
                seen_windows[indicator] = w

    @classmethod
    def load(cls, path: Path | None = None) -> WeightsConfig:
        p = path or CONFIG_PATH
        with p.open() as f:
            raw = yaml.safe_load(f)
        return cls(**raw)

    def window_for(self, indicator: str) -> str:
        for block in self.indicator_weights.values():
            spec = block.get(indicator)
            if spec and spec.get("window"):
                return spec["window"]
        return self.percentile_window


def _sub_temperature(
    pct_wide: pd.DataFrame,
    market: str,
    sub: str,
    cfg: WeightsConfig,
    with_confidence: bool = False,
) -> pd.Series | tuple[pd.Series, pd.Series]:
    """Compute one sub-temperature time series for one market.

    Returns NaN where ALL input indicators are missing on that date.
    When `with_confidence=True`, returns (sub_temp, confidence) where
    confidence ∈ [0, 1] captures within-block indicator dispersion."""
    block_key = f"{market}_{sub}"
    block = cfg.indicator_weights.get(block_key, {})
    if not block:
        if with_confidence:
            return pd.Series(index=pct_wide.index, dtype=float), pd.Series(index=pct_wide.index, dtype=float)
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
        if with_confidence:
            return pd.Series(index=pct_wide.index, dtype=float), pd.Series(index=pct_wide.index, dtype=float)
        return pd.Series(index=pct_wide.index, dtype=float)

    total_w = sum(available_weights.values())
    sub_temp = pd.Series(0.0, index=pct_wide.index)
    weight_sum = pd.Series(0.0, index=pct_wide.index)
    for ind, contrib in contributions.items():
        w = available_weights[ind] / total_w
        valid = contrib.notna()
        sub_temp = sub_temp.add(contrib.fillna(0) * w, fill_value=0)
        weight_sum = weight_sum.add(valid.astype(float) * w, fill_value=0)
    sub_temp = sub_temp.where(weight_sum > 0)
    sub_temp = sub_temp / weight_sum.where(weight_sum > 0)

    if not with_confidence:
        return sub_temp

    if len(contributions) < 2:
        confidence = pd.Series(0.8, index=pct_wide.index)
        confidence = confidence.where(sub_temp.notna())
        return sub_temp, confidence

    contrib_list = list(contributions.values())
    contrib_df = pd.concat(contrib_list, axis=1)
    # Dispersion requires at least 2 non-NaN indicators on a given date.
    # Filling missing-indicator dispersion to 0 would falsely report "perfect
    # agreement" when really we just lacked data — biasing confidence high.
    valid_count = contrib_df.notna().sum(axis=1)
    max_contrib = contrib_df.max(axis=1)
    min_contrib = contrib_df.min(axis=1)
    dispersion = (max_contrib - min_contrib).where(valid_count >= 2)
    confidence = 1.0 - (dispersion / 50.0).clip(0, 1)
    # When dispersion is undefined (only one indicator that day), fall back
    # to the same single-indicator default we use in the early-return branch.
    confidence = confidence.where(dispersion.notna(), 0.8)
    confidence = confidence.where(sub_temp.notna())
    return sub_temp, confidence


def _build_pct_wide(percentile_long: pd.DataFrame, cfg: WeightsConfig, force_window: str | None = None) -> pd.DataFrame:
    """Build wide percentile frame. If force_window is set, all indicators use that window column."""
    pl = percentile_long.copy()
    pl["date"] = pd.to_datetime(pl["date"])
    series_by_ind: dict[str, pd.Series] = {}
    for indicator, group in pl.groupby("indicator"):
        col = force_window or cfg.window_for(str(indicator))
        if col not in group.columns:
            continue
        s = group.set_index("date")[col].sort_index()
        s = s[~s.index.duplicated(keep="last")]
        series_by_ind[str(indicator)] = s
    if not series_by_ind:
        return pd.DataFrame()
    return pd.concat(series_by_ind, axis=1).sort_index()


def _compute_market_rows(
    pct_wide: pd.DataFrame,
    cfg: WeightsConfig,
    with_dispersion: bool = False,
) -> list[pd.DataFrame]:
    """Compute per-market temperature rows from a wide percentile frame."""
    rows = []
    for market in MARKETS:
        sub_w = cfg.sub_weights.get(market, {})
        if not sub_w:
            continue

        if with_dispersion:
            sub_temps: dict[str, pd.Series] = {}
            sub_confs: dict[str, pd.Series] = {}
            for sub in SUB_NAMES:
                st, sc = _sub_temperature(pct_wide, market, sub, cfg, with_confidence=True)
                sub_temps[sub] = st
                sub_confs[sub] = sc
        else:
            sub_temps = {sub: _sub_temperature(pct_wide, market, sub, cfg) for sub in SUB_NAMES}

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
            conf = sub_confs[sub] if with_dispersion else pd.Series(1.0, index=t.index)
            eff_w = pd.Series(w, index=t.index) * conf.fillna(0)
            valid = t.notna() & (eff_w > 0)
            overall = overall.add(t.fillna(0) * eff_w, fill_value=0)
            weight_sum = weight_sum.add(valid.astype(float) * eff_w, fill_value=0)
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

        def _row_quality(row: pd.Series) -> str:
            missing = [s for s in SUB_NAMES if pd.isna(row[s])]
            return ",".join(f"{s}_unavailable" for s in missing) if missing else "ok"

        df["data_quality"] = df.apply(_row_quality, axis=1)
        df["subtemp_completeness"] = df[["valuation", "sentiment", "liquidity"]].notna().sum(axis=1)
        df["is_complete"] = df["subtemp_completeness"] == 3
        rows.append(df)
    return rows


def _attribution_1w(daily: pd.DataFrame) -> pd.DataFrame:
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

    Phase 2 additions:
      overall_short  — all indicators forced to pct_1y (short-term temperature)
      overall_long   — all indicators forced to pct_10y (long-term temperature)
      divergence     — overall_short - overall_long (positive = short-term hot/overbought vs history)
      dispersion_ok  — 1 where all active sub-temps have confidence ≥0.5, 0 otherwise

    Regular overall uses dispersion-weighted sub-temps: when indicators within
    a sub disagree (dispersion > 50pp), that sub's contribution to overall is
    attenuated.
    """
    cfg = cfg or WeightsConfig.load()
    if percentile_long.empty:
        return pd.DataFrame()

    # Normal temperature (per-indicator window config, no dispersion)
    pct_wide = _build_pct_wide(percentile_long, cfg)
    if pct_wide.empty:
        return pd.DataFrame()
    normal_rows = _compute_market_rows(pct_wide, cfg, with_dispersion=False)
    if not normal_rows:
        return pd.DataFrame()

    # Short-term temperature (all pct_1y)
    pct_wide_1y = _build_pct_wide(percentile_long, cfg, force_window="pct_1y")
    if pct_wide_1y.empty:
        short_rows: list[pd.DataFrame] = []
    else:
        short_rows = _compute_market_rows(pct_wide_1y, cfg, with_dispersion=False)

    # Long-term temperature (all pct_10y)
    pct_wide_10y = _build_pct_wide(percentile_long, cfg, force_window="pct_10y")
    if pct_wide_10y.empty:
        long_rows: list[pd.DataFrame] = []
    else:
        long_rows = _compute_market_rows(pct_wide_10y, cfg, with_dispersion=False)

    # Dispersion-weighted temperature
    disp_rows = _compute_market_rows(pct_wide, cfg, with_dispersion=True)

    normal_daily = pd.concat(normal_rows, ignore_index=True)
    disp_daily = pd.concat(disp_rows, ignore_index=True)
    short_daily = pd.concat(short_rows, ignore_index=True) if short_rows else pd.DataFrame()
    long_daily = pd.concat(long_rows, ignore_index=True) if long_rows else pd.DataFrame()

    daily = normal_daily.dropna(subset=["overall"]).reset_index(drop=True)

    # Merge dispersion-weighted overall as the primary "overall" column.
    disp_map = disp_daily.set_index(["date", "market"])["overall"]
    daily = daily.set_index(["date", "market"])
    daily["overall_dispersion"] = disp_map
    # Use dispersion-weighted overall when available, fall back to standard
    daily["overall"] = daily["overall_dispersion"].fillna(daily["overall"])
    daily = daily.drop(columns=["overall_dispersion"]).reset_index()

    # Merge short/long temperatures
    def _merge_aux(aux_df: pd.DataFrame, col_name: str) -> pd.DataFrame:
        if aux_df.empty:
            daily[col_name] = float("nan")
            return daily
        aux = aux_df[["date", "market", "overall"]].copy()
        aux = aux.rename(columns={"overall": col_name})
        return daily.merge(aux, on=["date", "market"], how="left")

    daily = _merge_aux(short_daily, "overall_short")
    daily = _merge_aux(long_daily, "overall_long")
    daily["divergence"] = daily["overall_short"] - daily["overall_long"]

    # Dispersion quality flag: 1 where all active sub-temps have confidence ≥ 0.5
    # Compute per-sub confidence from the dispersion-weighted pass
    pct_wide_disp = _build_pct_wide(percentile_long, cfg)
    conf_series: dict[str, pd.Series] = {}
    for market in MARKETS:
        sub_w = cfg.sub_weights.get(market, {})
        if not sub_w:
            continue
        for sub in SUB_NAMES:
            key = f"{market}_{sub}"
            if sub not in sub_w or sub_w[sub] == 0:
                continue
            if key in conf_series:
                continue
            _, conf = _sub_temperature(pct_wide_disp, market, sub, cfg, with_confidence=True)
            if conf.notna().any():
                conf_df = pd.DataFrame({"date": pct_wide_disp.index.date, "market": market, "confidence": conf.values})
                conf_series[key] = conf_df

    if conf_series:
        all_conf = pd.concat(conf_series.values(), ignore_index=True)
        all_conf["date"] = pd.to_datetime(all_conf["date"])
        all_conf["conf_ok"] = (all_conf["confidence"] >= 0.5).astype(int)
        daily_sub = all_conf.groupby(["date", "market"])["conf_ok"].min()
        daily["date"] = pd.to_datetime(daily["date"])
        daily = daily.merge(daily_sub.reset_index(), on=["date", "market"], how="left")
        daily["conf_ok"] = daily["conf_ok"].fillna(1).astype(int)
    else:
        daily["conf_ok"] = 1

    return _attribution_1w(daily)


def write_silver_temperature(df: pd.DataFrame) -> Path:
    silver = _cfg.settings.silver_dir
    silver.mkdir(parents=True, exist_ok=True)
    df = stamp_version(df)
    path = silver / "temperature_daily.parquet"
    df.to_parquet(path, index=False)
    snapshot_weights()
    return path
