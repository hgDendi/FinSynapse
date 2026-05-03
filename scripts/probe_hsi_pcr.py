"""Probe data sources for HSI options Put/Call Ratio.

Tries:
  1. HKEX Daily Market Report (HSI Options) — text/HTML
  2. HKEX Consolidated CSV/PRN reports
  3. Cboe-style aggregator if available

Run:
  uv run python scripts/probe_hsi_pcr.py
"""

from __future__ import annotations

import re
import sys
from datetime import date, timedelta

import requests

UA = {"User-Agent": "Mozilla/5.0 (FinSynapse data probe; contact: hg.dendi@gmail.com)"}


def try_hkex_dqe(report_date: date) -> str | None:
    """Daily Market Report (Derivatives) — historic legacy URL pattern.
    File code 'dqe' = derivatives daily report."""
    yymmdd = report_date.strftime("%y%m%d")
    candidates = [
        f"https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/dqe{yymmdd}.htm",
        f"https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/dqe{yymmdd}.pdf",
    ]
    for url in candidates:
        print(f"[HKEX dqe] {url}")
        try:
            r = requests.get(url, headers=UA, timeout=15, allow_redirects=True)
            print(f"    status={r.status_code} len={len(r.content)} ctype={r.headers.get('content-type', '')[:50]}")
            if r.status_code == 200 and len(r.content) > 500:
                # Check for PCR mention
                text = (
                    r.text
                    if "html" in r.headers.get("content-type", "").lower()
                    or "text" in r.headers.get("content-type", "").lower()
                    else ""
                )
                if text and ("put/call" in text.lower() or "p/c" in text.lower() or "putcall" in text.lower()):
                    snippet = text[max(0, text.lower().find("put/call") - 50) : text.lower().find("put/call") + 200]
                    print(f"    HIT — snippet: {snippet!r}")
                    return url
        except Exception as e:
            print(f"    ERR {type(e).__name__}: {e}")
    return None


def try_hkex_hsi_options_listing() -> str | None:
    """HKEX market statistics landing pages."""
    urls = [
        "https://www.hkex.com.hk/Market-Data/Statistics/Consolidated-Reports/Derivative-Market-Statistics?sc_lang=en",
        "https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Statistics/Hong-Kong-and-Mainland-Market-Highlights?sc_lang=en",
    ]
    for url in urls:
        print(f"[HKEX listing] {url}")
        try:
            r = requests.get(url, headers=UA, timeout=15)
            print(f"    status={r.status_code} len={len(r.content)}")
            if r.status_code == 200:
                # Look for any link mentioning PCR / put-call
                hits = re.findall(r'href="([^"]*(?:put.?call|pcr)[^"]*)"', r.text, re.IGNORECASE)
                print(f"    PCR-related links: {hits[:5]}")
        except Exception as e:
            print(f"    ERR {type(e).__name__}: {e}")
    return None


def try_optioncharts_or_alt() -> str | None:
    """Some retail aggregators expose HSI option PCR for free."""
    urls = [
        "https://www.aastocks.com/en/futures/option-statistics.aspx?indexsymbol=HSI",
    ]
    for url in urls:
        print(f"[aggregator] {url}")
        try:
            r = requests.get(url, headers=UA, timeout=15)
            print(f"    status={r.status_code} len={len(r.content)}")
            if r.status_code == 200:
                txt = r.text.lower()
                for kw in ["put/call", "p/c ratio", "pcr"]:
                    if kw in txt:
                        idx = txt.find(kw)
                        print(f"    HIT '{kw}': {r.text[max(0, idx - 80) : idx + 200]!r}")
                        return url
        except Exception as e:
            print(f"    ERR {type(e).__name__}: {e}")
    return None


def main() -> int:
    # Use most recent weekday as HKEX has no weekend reports
    d = date.today()
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    # Also try a few days back in case today's report not yet published
    print(f"=== Probing HSI options PCR sources (target dates around {d}) ===\n")

    found = []
    for delta in (1, 2, 3, 4, 7):
        target = d - timedelta(days=delta)
        if target.weekday() >= 5:
            continue
        result = try_hkex_dqe(target)
        if result:
            found.append(("HKEX dqe", result))
            break

    print()
    try_hkex_hsi_options_listing()
    print()
    agg = try_optioncharts_or_alt()
    if agg:
        found.append(("aggregator", agg))

    print("\n=== Recommendation ===")
    if found:
        for name, url in found:
            print(f"  WORKS: {name} — {url}")
        return 0
    print("  NO FREE SOURCE WORKED.")
    print("  Notes:")
    print("    - HKEX dqe report only contains STOCK options (per-equity), not INDEX options (HSI).")
    print("    - HKEX listing pages are 403/404 (anti-scrape).")
    print("    - aastocks/optioncharts render via JS — would need headless browser, fragile.")
    print()
    print("  ACTION (per plan §11.6 fallback):")
    print("    - In transform/temperature.py, set HK sentiment weights to {south_5d: 0.5, ah_premium: 0.5}.")
    print("    - Write data_quality='pcr_unavailable' to silver temperature row.")
    print("    - Revisit if a paid feed becomes acceptable, or scrape via Playwright in a separate workflow.")
    return 2


if __name__ == "__main__":
    sys.exit(main())
