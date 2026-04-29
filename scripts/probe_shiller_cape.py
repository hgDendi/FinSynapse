"""Probe data sources for Shiller CAPE (Cyclically Adjusted PE).

Tries, in order:
  1. multpl.com  /shiller-pe/table/by-month   (HTML table, monthly)
  2. Yale Shiller Excel feed (authoritative, monthly)

Run:
  uv run python scripts/probe_shiller_cape.py

Exit code 0 if at least one source works; prints recommendation.
"""
from __future__ import annotations

import io
import sys
import traceback

import pandas as pd
import requests
from bs4 import BeautifulSoup

UA = {"User-Agent": "Mozilla/5.0 (FinSynapse data probe; contact: hg.dendi@gmail.com)"}


def try_multpl() -> pd.DataFrame | None:
    url = "https://www.multpl.com/shiller-pe/table/by-month"
    print(f"\n[1] Trying multpl.com -> {url}")
    try:
        r = requests.get(url, headers=UA, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        table = soup.find("table", {"id": "datatable"}) or soup.find("table")
        if table is None:
            print("    no <table> found")
            return None
        rows = []
        for tr in table.find_all("tr")[1:]:  # skip header
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cells) >= 2:
                rows.append(cells[:2])
        df = pd.DataFrame(rows, columns=["date_str", "cape"])
        df["date"] = pd.to_datetime(df["date_str"], errors="coerce")
        df["cape"] = pd.to_numeric(df["cape"].str.replace(",", "").str.strip(), errors="coerce")
        df = df.dropna().sort_values("date").reset_index(drop=True)
        print(f"    OK rows={len(df)}, latest={df.iloc[-1].to_dict()}")
        return df[["date", "cape"]]
    except Exception:
        traceback.print_exc()
        return None


def try_yale_excel() -> pd.DataFrame | None:
    """Robert Shiller publishes ie_data.xls on his Yale page."""
    url = "https://shillerdata.com/?utm_source=finsynapse"
    landing = "https://shillerdata.com/"
    print(f"\n[2] Trying Yale Shiller Excel via landing {landing}")
    try:
        # The actual Excel URL changes (e.g. shillerdata.com files); try direct AWS-hosted file:
        candidate_urls = [
            "https://img1.wsimg.com/blobby/go/e5e77e0b-59d1-44d9-ab02-de5dd06c1a4d/downloads/ie_data.xls",
            "http://www.econ.yale.edu/~shiller/data/ie_data.xls",
        ]
        for u in candidate_urls:
            try:
                r = requests.get(u, headers=UA, timeout=20)
                r.raise_for_status()
                # The sheet "Data" has CAPE in column M (header on row 8)
                xl = pd.read_excel(io.BytesIO(r.content), sheet_name="Data", header=7)
                # Heuristic: find a column whose name contains 'CAPE'
                cape_cols = [c for c in xl.columns if "CAPE" in str(c).upper()]
                if not cape_cols:
                    print(f"    {u}: loaded but no CAPE column. cols={list(xl.columns)[:10]}")
                    continue
                col = cape_cols[0]
                date_col = xl.columns[0]
                df = xl[[date_col, col]].dropna()
                df.columns = ["date_raw", "cape"]
                df["cape"] = pd.to_numeric(df["cape"], errors="coerce")
                df = df.dropna().reset_index(drop=True)
                print(f"    OK via {u}: rows={len(df)}, last 3 cape values={df['cape'].tail(3).tolist()}")
                return df
            except Exception as e:
                print(f"    {u}: {type(e).__name__}: {e}")
        return None
    except Exception:
        traceback.print_exc()
        return None


def main() -> int:
    multpl_df = try_multpl()
    yale_df = try_yale_excel()

    print("\n=== Recommendation ===")
    if multpl_df is not None and len(multpl_df) > 100:
        print("PRIMARY:  multpl.com (clean HTML, monthly history sufficient)")
        if yale_df is not None:
            print("FALLBACK: Yale Shiller Excel")
        return 0
    if yale_df is not None and len(yale_df) > 100:
        print("PRIMARY:  Yale Shiller Excel (multpl scrape failed)")
        return 0
    print("FAILED: neither source returned usable data — escalate")
    return 1


if __name__ == "__main__":
    sys.exit(main())
