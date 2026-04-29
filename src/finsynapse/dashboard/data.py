from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from finsynapse import config as _cfg

MARKETS = ("cn", "hk", "us")


@dataclass
class DashboardData:
    temperature: pd.DataFrame
    macro: pd.DataFrame
    percentile: pd.DataFrame
    divergence: pd.DataFrame
    health: pd.DataFrame
    silver_dir: Path

    def latest_per_market(self) -> dict[str, pd.Series]:
        """Pick the most informative recent row per market.

        Naive `tail(1)` would pick rows where monthly indicators were
        ffill'd past the last available daily date, leaving daily sub-temps
        NaN (e.g. M2 ffilled to today but VIX hasn't updated yet). Instead,
        within the last 10 trading days, pick the row with maximum
        sub-temperature completeness; tiebreak by latest date.
        """
        out = {}
        if self.temperature.empty:
            return out
        for m in MARKETS:
            sub = self.temperature[self.temperature["market"] == m].copy()
            if sub.empty:
                continue
            sub["_completeness"] = sub[["valuation", "sentiment", "liquidity"]].notna().sum(axis=1)
            recent = sub.sort_values("date").tail(10)
            best = recent.sort_values(["_completeness", "date"], ascending=[False, False]).iloc[0]
            out[m] = best
        return out

    def asof(self) -> pd.Timestamp | None:
        if self.temperature.empty:
            return None
        return pd.to_datetime(self.temperature["date"].max())


def load(silver_dir: Path | None = None) -> DashboardData:
    silver = Path(silver_dir or _cfg.settings.silver_dir)

    def _read(name: str) -> pd.DataFrame:
        p = silver / name
        if not p.exists():
            return pd.DataFrame()
        return pd.read_parquet(p)

    return DashboardData(
        temperature=_read("temperature_daily.parquet"),
        macro=_read("macro_daily.parquet"),
        percentile=_read("percentile_daily.parquet"),
        divergence=_read("divergence_daily.parquet"),
        health=_read("health_log.parquet"),
        silver_dir=silver,
    )
