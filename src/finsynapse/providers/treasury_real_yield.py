"""US Treasury daily real yield curve — keyless fallback for FRED `DFII10`.

The official endpoint at home.treasury.gov serves CSVs per-year and needs no
API key. We fetch one CSV per calendar year inside the requested range and
keep the 10Y column as `us10y_real_yield`. Same canonical name as
`providers.fred` so silver normalization treats them as one series; when both
sources are present, `collect_bronze` deduplicates by (date, indicator) and
either source's value is acceptable (they agree to two decimals).
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from datetime import date

import pandas as pd

from finsynapse.providers.base import FetchRange, Provider
from finsynapse.providers.retry import requests_session

UA = {"User-Agent": "Mozilla/5.0 (FinSynapse data fetch)"}
BASE = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/{year}/all"
PARAMS = {"type": "daily_treasury_real_yield_curve", "_format": "csv"}


@dataclass(frozen=True)
class _Series:
    csv_column: str
    indicator: str


SERIES: tuple[_Series, ...] = (_Series(csv_column="10 YR", indicator="us10y_real_yield"),)


class TreasuryRealYieldProvider(Provider):
    name = "treasury_real_yield"
    layer = "macro"

    def fetch(self, fetch_range: FetchRange) -> pd.DataFrame:
        years = range(fetch_range.start.year, fetch_range.end.year + 1)
        frames: list[pd.DataFrame] = []
        for y in years:
            try:
                frames.append(self._fetch_year(y))
            except Exception as exc:
                # Skip empty/missing years (early years before TIPS started in 2003
                # have no data). Re-raise only on systemic failure (handled below).
                if y >= 2003:
                    raise RuntimeError(f"treasury real yield {y}: {exc}") from exc

        if not frames:
            raise RuntimeError(f"treasury real yield: 0 rows for {years.start}..{years.stop - 1}")
        df = pd.concat(frames, ignore_index=True)
        df = df[(df["date"] >= fetch_range.start) & (df["date"] <= fetch_range.end)]
        if df.empty:
            raise RuntimeError(f"treasury real yield: 0 rows in window {fetch_range.start}..{fetch_range.end}")
        return df.sort_values(["indicator", "date"]).reset_index(drop=True)

    def _fetch_year(self, year: int) -> pd.DataFrame:
        url = BASE.format(year=year)
        params = {**PARAMS, "field_tdr_date_value": str(year)}
        r = requests_session().get(url, params=params, headers=UA, timeout=(10, 30))
        r.raise_for_status()
        raw = pd.read_csv(io.StringIO(r.text))
        if raw.empty or "Date" not in raw.columns:
            return pd.DataFrame(columns=["date", "indicator", "value", "source_symbol"])

        raw["date"] = pd.to_datetime(raw["Date"], format="%m/%d/%Y", errors="coerce").dt.date
        rows: list[pd.DataFrame] = []
        for series in SERIES:
            if series.csv_column not in raw.columns:
                continue
            sub = raw[["date", series.csv_column]].rename(columns={series.csv_column: "value"})
            sub = sub.dropna(subset=["date", "value"])
            sub["value"] = pd.to_numeric(sub["value"], errors="coerce")
            sub = sub.dropna(subset=["value"])
            sub["indicator"] = series.indicator
            sub["source_symbol"] = f"USTREAS:{series.csv_column.replace(' ', '')}"
            rows.append(sub)
        if not rows:
            return pd.DataFrame(columns=["date", "indicator", "value", "source_symbol"])
        return pd.concat(rows, ignore_index=True)


def run(fetch_range: FetchRange, fetch_date: date | None = None) -> tuple[pd.DataFrame, str]:
    provider = TreasuryRealYieldProvider()
    df = provider.fetch(fetch_range)
    path = provider.write_bronze(df, fetch_date or date.today())
    return df, str(path)
