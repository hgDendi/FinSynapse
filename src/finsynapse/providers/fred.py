from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from finsynapse import config as _cfg
from finsynapse.providers.base import FetchRange, Provider
from finsynapse.providers.retry import requests_session


@dataclass(frozen=True)
class FredSeries:
    series_id: str
    indicator: str  # canonical name


# US10Y real rate: headline liquidity input.
# HY OAS (BAMLH0A0HYM2): high-yield credit spread — most reliable risk-off
#   leading indicator; sentiment block.
#   ⚠️ KNOWN LIMITATION (verified 2026-04-30): per FRED series metadata,
#   "Starting in April 2026, this series will only include 3 years of
#    observations. For more data, go to the source." This is an ICE BofA
#   licensing restriction. Forward-looking signal still works (us_hy_oas
#   gets fresh data daily, percentile baseline grows over time), but
#   historical backtest pivots before 2023-04 will see this indicator as
#   missing and the sentiment block renormalizes onto VIX alone — same
#   degraded behavior as a missing indicator on any other day. Future
#   alternative: HYG/IEF ETF yield-spread via yfinance (full history,
#   ~0.9 correlated with OAS). Backlog'd, not blocking.
# NFCI: Chicago Fed National Financial Conditions Index — single composite
#   bundling credit, vol, funding stress; weekly (Wed publish), ffill'd to
#   daily by transform/percentile.LOWFREQ_INDICATORS. Liquidity block.
#   Full 1971→present history available (Chicago Fed is the original source,
#   no third-party license issue).
SERIES: tuple[FredSeries, ...] = (
    FredSeries(series_id="DFII10", indicator="us10y_real_yield"),
    FredSeries(series_id="BAMLH0A0HYM2", indicator="us_hy_oas"),
    FredSeries(series_id="NFCI", indicator="us_nfci"),
)

API_BASE = "https://api.stlouisfed.org/fred/series/observations"


class FredProvider(Provider):
    name = "fred"
    layer = "macro"

    def fetch(self, fetch_range: FetchRange) -> pd.DataFrame:
        if not _cfg.settings.fred_api_key:
            raise RuntimeError(
                "FRED_API_KEY not configured. Get a free key at "
                "https://fred.stlouisfed.org/docs/api/api_key.html and put it in .env"
            )

        frames: list[pd.DataFrame] = []
        for series in SERIES:
            df = self._fetch_one(series, fetch_range)
            frames.append(df)
        out = pd.concat(frames, ignore_index=True)
        if out.empty:
            raise RuntimeError(f"FRED returned 0 rows for {fetch_range.start}..{fetch_range.end}")
        return out.sort_values(["indicator", "date"]).reset_index(drop=True)

    def _fetch_one(self, series: FredSeries, fetch_range: FetchRange) -> pd.DataFrame:
        params = {
            "series_id": series.series_id,
            "api_key": _cfg.settings.fred_api_key,
            "file_type": "json",
            "observation_start": fetch_range.start.isoformat(),
            "observation_end": fetch_range.end.isoformat(),
        }
        r = requests_session().get(API_BASE, params=params, timeout=(5, 30))
        r.raise_for_status()
        payload = r.json()
        observations = payload.get("observations", [])

        rows = []
        for obs in observations:
            value_str = obs.get("value", ".")
            if value_str in (".", "", None):  # FRED uses '.' for missing
                continue
            try:
                value = float(value_str)
            except ValueError:
                continue
            rows.append(
                {
                    "date": pd.to_datetime(obs["date"]).date(),
                    "indicator": series.indicator,
                    "value": value,
                    "source_symbol": series.series_id,
                }
            )

        return pd.DataFrame(rows)


def run(fetch_range: FetchRange, fetch_date: date | None = None) -> tuple[pd.DataFrame, str]:
    provider = FredProvider()
    df = provider.fetch(fetch_range)
    path = provider.write_bronze(df, fetch_date or date.today())
    return df, str(path)
