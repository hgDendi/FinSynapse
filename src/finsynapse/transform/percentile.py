from __future__ import annotations

from pathlib import Path

import pandas as pd

from finsynapse import config as _cfg

# Default rolling windows (trading days). Monthly indicators (CAPE, PE) get
# upsampled to daily via forward-fill before the rolling window is applied.
WINDOWS = {
    "1y": 252,
    "5y": 252 * 5,
    "10y": 252 * 10,
}

# Indicators with mixed monthly/daily frequency must be ffill'd onto a daily
# index before percentile, otherwise the rolling window sees only ~12 points/y.
MONTHLY_INDICATORS = {"us_cape", "us_pe_ttm", "cn_m2_yoy", "cn_social_financing_12m"}


def _to_daily(series: pd.Series, end: pd.Timestamp | None = None) -> pd.Series:
    """Reindex a non-daily series onto a business-day grid via forward-fill.
    Series must be indexed by date (ascending). When `end` is provided (use the
    global silver max date), ffill carries the last known value forward — this
    matters for monthly indicators (M2, CAPE) that publish with lag."""
    if series.empty:
        return series
    end_dt = end if end is not None else series.index.max()
    idx = pd.date_range(series.index.min(), end_dt, freq="B")
    return series.reindex(idx, method="ffill")


def compute_percentiles(macro_long: pd.DataFrame) -> pd.DataFrame:
    """For each indicator, compute 1Y/5Y/10Y rolling percentile rank of the
    current value within its trailing window.

    Returns a long-format frame: date | indicator | value | pct_1y | pct_5y | pct_10y.
    """
    if macro_long.empty:
        return pd.DataFrame(columns=["date", "indicator", "value", "pct_1y", "pct_5y", "pct_10y"])

    # Global max date across all indicators — monthly series ffill up to here
    # so percentile/temperature have current values even when M2 lags by 2 months.
    global_max = pd.to_datetime(macro_long["date"]).max()

    out_frames: list[pd.DataFrame] = []
    for indicator, group in macro_long.groupby("indicator"):
        s = group.set_index(pd.to_datetime(group["date"]))["value"].sort_index()
        # Drop duplicate timestamps if any (e.g. same date from multiple sources after dedup)
        s = s[~s.index.duplicated(keep="last")]
        if indicator in MONTHLY_INDICATORS:
            s = _to_daily(s, end=global_max)

        result = pd.DataFrame({"value": s})
        for label, window in WINDOWS.items():
            # rank='average' / pct=True gives the percentile of the LAST value
            # within the trailing window. min_periods avoids garbage early values.
            min_periods = max(60, window // 4)
            result[f"pct_{label}"] = s.rolling(window=window, min_periods=min_periods).apply(
                lambda x: (x.rank(pct=True).iloc[-1]) * 100.0, raw=False
            )

        result["indicator"] = indicator
        result["date"] = result.index.date
        out_frames.append(result.reset_index(drop=True))

    out = pd.concat(out_frames, ignore_index=True)
    return out[["date", "indicator", "value", "pct_1y", "pct_5y", "pct_10y"]]


def write_silver_percentile(df: pd.DataFrame) -> Path:
    silver = _cfg.settings.silver_dir
    silver.mkdir(parents=True, exist_ok=True)
    path = silver / "percentile_daily.parquet"
    df.to_parquet(path, index=False)
    return path
