from __future__ import annotations

from pathlib import Path

import pandas as pd

from finsynapse import config as _cfg

CANONICAL_COLUMNS = ["date", "indicator", "value", "source"]


def collect_bronze(bronze_dir: Path | None = None) -> pd.DataFrame:
    """Walk every bronze parquet, concat into one long-format frame.

    Bronze files written by providers all share the schema produced by
    `Provider.fetch`: date, indicator, value, source_symbol. We re-tag the
    source column with the provider name (parent folder of the parquet)
    so silver consumers can attribute origin without parsing source_symbol.
    """
    bronze = Path(bronze_dir or _cfg.settings.bronze_dir)
    frames: list[pd.DataFrame] = []
    for parquet in sorted(bronze.rglob("*.parquet")):
        provider_name = parquet.parent.name
        df = pd.read_parquet(parquet)
        df["source"] = provider_name
        frames.append(df[["date", "indicator", "value", "source"]])

    if not frames:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    combined = pd.concat(frames, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.date
    # Same (date, indicator) may appear from multiple bronze fetches; keep
    # the latest non-null. For ties we keep the row from the lexically last
    # source — deterministic, easy to override later if a priority is needed.
    combined = (
        combined.sort_values(["date", "indicator", "source"])
        .drop_duplicates(subset=["date", "indicator"], keep="last")
        .reset_index(drop=True)
    )
    return combined[CANONICAL_COLUMNS]


# Maximum business days to forward-fill an upstream indicator before treating
# it as stale. ~90 BDays ≈ 4.3 months — generous enough to cover a missed
# monthly PE publication, tight enough that a 6-month multpl outage stops
# producing fake-fresh derived rows.
DERIVE_FFILL_LIMIT_BDAYS = 90


def derive_indicators(macro_long: pd.DataFrame) -> pd.DataFrame:
    """Append derived indicators (ones computed from other indicators) to the
    long-format macro frame. Run after collect_bronze, before health_check.

    Currently:
        us_erp = 100 / us_pe_ttm − us10y_real_yield   (real equity risk premium, %)
            Why this matters: percentile-of-PE alone has US locked at 90°+ for a
            decade because rates were near zero. ERP normalizes equity yield
            against the actual bond alternative.
            us_pe_ttm is monthly → ffill onto business-day grid (with
            DERIVE_FFILL_LIMIT_BDAYS cap so an upstream outage cannot produce
            fake-fresh ERP forever). PE non-positive guard prevents inf/sign-
            flip when multpl returns 0 or a historical negative-EPS reading
            (rare but real — 2009Q1 trailing S&P EPS briefly went negative).
            Health-check still runs after this, but health-check on the
            DERIVED row can't recover the truth of the bad input.
    """
    if macro_long.empty:
        return macro_long

    wide = macro_long.pivot_table(index="date", columns="indicator", values="value")
    wide.index = pd.to_datetime(wide.index)
    wide = wide.sort_index()
    if wide.empty:
        return macro_long
    bday_idx = pd.date_range(wide.index.min(), wide.index.max(), freq="B")
    wide_ffill = wide.reindex(bday_idx).ffill(limit=DERIVE_FFILL_LIMIT_BDAYS)

    derived: list[pd.DataFrame] = []

    if {"us_pe_ttm", "us10y_real_yield"}.issubset(wide_ffill.columns):
        pe_safe = wide_ffill["us_pe_ttm"].where(wide_ffill["us_pe_ttm"] > 0)
        ey = 100.0 / pe_safe
        erp = (ey - wide_ffill["us10y_real_yield"]).dropna()
        if not erp.empty:
            derived.append(
                pd.DataFrame(
                    {
                        "date": erp.index.date,
                        "indicator": "us_erp",
                        "value": erp.values,
                        "source": "derived",
                    }
                )
            )

    if "cn_social_financing_12m" in wide_ffill.columns:
        sf = wide_ffill["cn_social_financing_12m"].dropna()
        if len(sf) >= 13:
            credit_impulse = sf.pct_change(252).dropna()
            if not credit_impulse.empty:
                derived.append(
                    pd.DataFrame(
                        {
                            "date": credit_impulse.index.date,
                            "indicator": "cn_credit_impulse",
                            "value": credit_impulse.values,
                            "source": "derived",
                        }
                    )
                )

    if "usdcny" in wide_ffill.columns:
        usdcny = wide_ffill["usdcny"].dropna()
        if not usdcny.empty:
            derived.append(
                pd.DataFrame(
                    {
                        "date": usdcny.index.date,
                        "indicator": "cn_usdcny_pressure",
                        "value": usdcny.values,
                        "source": "derived",
                    }
                )
            )

    if not derived:
        return macro_long
    return pd.concat([macro_long, *derived], ignore_index=True)


def write_silver_macro(df: pd.DataFrame) -> Path:
    silver = _cfg.settings.silver_dir
    silver.mkdir(parents=True, exist_ok=True)
    path = silver / "macro_daily.parquet"
    df.to_parquet(path, index=False)
    return path
