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
        temp = self.temperature.copy()
        for m in MARKETS:
            sub = temp[temp["market"] == m].copy()
            if sub.empty:
                continue
            if "subtemp_completeness" in sub.columns:
                sub["_completeness"] = sub["subtemp_completeness"]
            else:
                sub["_completeness"] = sub[["valuation", "sentiment", "liquidity"]].notna().sum(axis=1)
            recent = sub.sort_values("date").tail(10)
            best = recent.sort_values(["_completeness", "date"], ascending=[False, False]).iloc[0]
            out[m] = best
        return out

    def latest_complete_date(self) -> dict[str, str | None]:
        """Latest date per market where all 3 sub-temperatures are available.

        Returns date strings (YYYY-MM-DD) or None for markets with no
        data. This is the truthful "latest good date" — distinct from
        `asof()` which returns the raw max date from temperature.parquet
        and may include rows that are incomplete or misleading.
        """
        out: dict[str, str | None] = {}
        if self.temperature.empty:
            return {m: None for m in MARKETS}
        temp = self.temperature.copy()
        temp["date"] = pd.to_datetime(temp["date"])
        for m in MARKETS:
            sub = temp[temp["market"] == m]
            if sub.empty:
                out[m] = None
                continue
            complete = sub[sub["is_complete"] == True] if "is_complete" in sub.columns else sub  # noqa: E712
            if complete.empty:
                out[m] = None
            else:
                latest = complete["date"].max()
                out[m] = latest.strftime("%Y-%m-%d") if pd.notna(latest) else None
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
