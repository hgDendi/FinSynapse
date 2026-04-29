from __future__ import annotations

from datetime import date, timedelta

import typer

from finsynapse.providers.base import FetchRange
from finsynapse.providers.yfinance_macro import run as run_yfinance_macro

app = typer.Typer(add_completion=False, no_args_is_help=True, help="FinSynapse CLI")
ingest_app = typer.Typer(no_args_is_help=True, help="Ingest raw data into bronze layer")
app.add_typer(ingest_app, name="ingest")


SOURCES = {
    "yfinance_macro": run_yfinance_macro,
}


@ingest_app.command("run")
def ingest_run(
    source: str = typer.Option(..., "--source", "-s", help=f"One of: {list(SOURCES)}"),
    lookback_days: int = typer.Option(30, "--lookback-days", help="Days of history to fetch"),
    end: str | None = typer.Option(None, "--end", help="End date YYYY-MM-DD (default: today)"),
) -> None:
    if source not in SOURCES:
        raise typer.BadParameter(f"unknown source '{source}'. valid: {list(SOURCES)}")

    end_date = date.fromisoformat(end) if end else date.today()
    start_date = end_date - timedelta(days=lookback_days)
    fr = FetchRange(start=start_date, end=end_date)

    typer.echo(f"[ingest] source={source} range={start_date}..{end_date}")
    df, path = SOURCES[source](fr)
    typer.echo(f"[ingest] wrote {len(df):,} rows -> {path}")
    typer.echo(df.groupby("indicator")["value"].agg(["count", "min", "max"]).to_string())


if __name__ == "__main__":
    app()
