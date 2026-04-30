from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

import pandas as pd
from bs4 import BeautifulSoup

from finsynapse.providers.base import FetchRange, Provider
from finsynapse.providers.retry import requests_session


@dataclass(frozen=True)
class MultplTable:
    slug: str
    indicator: str  # canonical name written to bronze


# Tables proven viable in Phase 0 probe. SP500 P/B (s-p-500-price-to-book) is
# excluded: only annual data since 1999 (~28 points), too sparse for 10Y rolling
# percentile that depends on a long stable distribution.
TABLES: tuple[MultplTable, ...] = (
    MultplTable(slug="shiller-pe", indicator="us_cape"),
    MultplTable(slug="s-p-500-pe-ratio", indicator="us_pe_ttm"),
)

UA = {"User-Agent": "Mozilla/5.0 (FinSynapse data fetch)"}
BASE = "https://www.multpl.com"


class MultplProvider(Provider):
    name = "multpl"
    layer = "valuation"

    def fetch(self, fetch_range: FetchRange) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for table in TABLES:
            df = self._fetch_one(table)
            df = df[(df["date"] >= pd.Timestamp(fetch_range.start)) & (df["date"] <= pd.Timestamp(fetch_range.end))]
            frames.append(df)
        out = pd.concat(frames, ignore_index=True)
        if out.empty:
            raise RuntimeError(f"multpl returned 0 rows in range {fetch_range.start}..{fetch_range.end}")
        out["date"] = out["date"].dt.date
        return out.sort_values(["indicator", "date"]).reset_index(drop=True)

    def _fetch_one(self, table: MultplTable) -> pd.DataFrame:
        url = f"{BASE}/{table.slug}/table/by-month"
        r = requests_session().get(url, headers=UA, timeout=(10, 20))
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        html_table = soup.find("table", {"id": "datatable"}) or soup.find("table")
        if html_table is None:
            raise RuntimeError(f"multpl {table.slug}: no <table> in response")

        rows: list[tuple[pd.Timestamp, float]] = []
        for tr in html_table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cells) < 2:
                continue
            ts = pd.to_datetime(cells[0], errors="coerce")
            # multpl prefixes estimated values with †; strip it.
            raw_value = re.sub(r"[^\d.\-]", "", cells[1])
            if not raw_value or pd.isna(ts):
                continue
            try:
                rows.append((ts, float(raw_value)))
            except ValueError:
                continue

        if not rows:
            raise RuntimeError(f"multpl {table.slug}: parsed 0 rows")

        df = pd.DataFrame(rows, columns=["date", "value"])
        df["indicator"] = table.indicator
        df["source_symbol"] = table.slug
        return df


def run(fetch_range: FetchRange, fetch_date: date | None = None) -> tuple[pd.DataFrame, str]:
    provider = MultplProvider()
    df = provider.fetch(fetch_range)
    path = provider.write_bronze(df, fetch_date or date.today())
    return df, str(path)
