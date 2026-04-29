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
        out = {}
        if self.temperature.empty:
            return out
        for m in MARKETS:
            sub = self.temperature[self.temperature["market"] == m]
            if sub.empty:
                continue
            out[m] = sub.sort_values("date").iloc[-1]
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
