from __future__ import annotations

from datetime import date, timedelta

import typer

from finsynapse.providers.akshare_cn import run as run_akshare_cn
from finsynapse.providers.akshare_flow import run as run_akshare_flow
from finsynapse.providers.base import FetchRange
from finsynapse.providers.fred import run as run_fred
from finsynapse.providers.multpl import run as run_multpl
from finsynapse.providers.yfinance_hk import run as run_yfinance_hk
from finsynapse.providers.yfinance_macro import run as run_yfinance_macro

app = typer.Typer(add_completion=False, no_args_is_help=True, help="FinSynapse CLI")
ingest_app = typer.Typer(no_args_is_help=True, help="Ingest raw data into bronze layer")
transform_app = typer.Typer(no_args_is_help=True, help="Transform bronze -> silver layer")
dashboard_app = typer.Typer(no_args_is_help=True, help="Local Streamlit app + static HTML for GH Pages")
notify_app = typer.Typer(no_args_is_help=True, help="State-change push notifications (Bark / Telegram)")
app.add_typer(ingest_app, name="ingest")
app.add_typer(transform_app, name="transform")
app.add_typer(dashboard_app, name="dashboard")
app.add_typer(notify_app, name="notify")


SOURCES = {
    "yfinance_macro": run_yfinance_macro,
    "yfinance_hk": run_yfinance_hk,
    "multpl": run_multpl,
    "fred": run_fred,
    "akshare_cn": run_akshare_cn,
    "akshare_flow": run_akshare_flow,
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


@ingest_app.command("all")
def ingest_all(
    lookback_days: int = typer.Option(5500, "--lookback-days", help="Days of history (default 15Y)"),
) -> None:
    """Run every source we have configured. FRED is skipped if no API key set."""
    from finsynapse.config import settings

    end_date = date.today()
    start_date = end_date - timedelta(days=lookback_days)
    fr = FetchRange(start=start_date, end=end_date)
    typer.echo(f"[ingest all] range={start_date}..{end_date}")

    succeeded = 0
    skipped = 0
    failed = 0
    for name, fn in SOURCES.items():
        if name == "fred" and not settings.fred_api_key:
            typer.secho(f"  - {name}: SKIPPED (FRED_API_KEY not set)", fg=typer.colors.YELLOW)
            skipped += 1
            continue
        try:
            df, path = fn(fr)
            typer.secho(f"  ✓ {name}: {len(df):,} rows -> {path}", fg=typer.colors.GREEN)
            succeeded += 1
        except Exception as exc:
            typer.secho(f"  ✗ {name}: {type(exc).__name__}: {exc}", fg=typer.colors.RED)
            failed += 1

    typer.echo(f"\n[ingest all] {succeeded} ok, {skipped} skipped, {failed} failed")
    # Exit non-zero only when more than half of attempted (non-skipped) sources
    # failed. One flaky upstream shouldn't break the whole workflow, but a
    # systemic outage should — so the workflow's failure-issue step fires.
    attempted = succeeded + failed
    if attempted > 0 and failed * 2 > attempted:
        typer.secho(f"FATAL: majority of sources failed ({failed}/{attempted})", fg=typer.colors.RED)
        raise typer.Exit(code=1)


@transform_app.command("run")
def transform_run(
    layer: str = typer.Option(
        "all",
        "--layer",
        "-l",
        help="silver | percentile | health | temperature | divergence | all",
    ),
) -> None:
    """Run silver-layer transforms in dependency order."""
    from finsynapse.transform.divergence import compute_divergence, write_silver_divergence
    from finsynapse.transform.health_check import check, write_health_log
    from finsynapse.transform.normalize import collect_bronze, write_silver_macro
    from finsynapse.transform.percentile import compute_percentiles, write_silver_percentile
    from finsynapse.transform.temperature import compute_temperature, write_silver_temperature

    macro = collect_bronze()
    typer.echo(f"[bronze] collected {len(macro):,} rows across {macro['indicator'].nunique()} indicators")

    if layer in ("health", "all"):
        clean, issues = check(macro)
        path = write_health_log(issues)
        n_fail = sum(1 for i in issues if i.severity == "fail")
        n_warn = sum(1 for i in issues if i.severity == "warn")
        typer.echo(f"[health] kept {len(clean):,} rows; {n_fail} fail, {n_warn} warn -> {path}")
        macro = clean

    if layer in ("silver", "all"):
        path = write_silver_macro(macro)
        typer.echo(f"[silver] wrote -> {path}")

    if layer in ("percentile", "temperature", "all"):
        pct = compute_percentiles(macro)
        path = write_silver_percentile(pct)
        typer.echo(f"[percentile] {len(pct):,} rows -> {path}")

    if layer in ("temperature", "all"):
        temp = compute_temperature(pct)  # noqa: F821 — pct is defined above when layer hits this
        path = write_silver_temperature(temp)
        typer.echo(f"[temperature] {len(temp):,} rows -> {path}")
        if not temp.empty:
            latest = temp.sort_values("date").groupby("market").tail(1)
            typer.echo(latest[["date", "market", "overall", "valuation", "sentiment", "liquidity", "data_quality"]].to_string(index=False))

    if layer in ("divergence", "all"):
        div = compute_divergence(macro)
        path = write_silver_divergence(div)
        typer.echo(f"[divergence] {len(div):,} rows -> {path}")
        if not div.empty:
            today_div = div[div["is_divergent"]].sort_values("date").tail(5)
            if not today_div.empty:
                typer.echo("recent divergences:")
                typer.echo(today_div[["date", "pair_name", "strength", "description"]].to_string(index=False))


@dashboard_app.command("serve")
def dashboard_serve(
    port: int = typer.Option(8501, "--port", "-p"),
) -> None:
    """Run the Streamlit app locally for interactive exploration."""
    import subprocess
    import sys
    from pathlib import Path

    app_path = Path(__file__).parent / "dashboard" / "app.py"
    cmd = [sys.executable, "-m", "streamlit", "run", str(app_path), "--server.port", str(port)]
    typer.echo(f"[dashboard] launching streamlit on http://localhost:{port}")
    subprocess.run(cmd, check=False)


@dashboard_app.command("render")
def dashboard_render(
    out_dir: str = typer.Option("dist", "--out-dir", "-o", help="Output directory (default zh -> index.html, en -> en.html)"),
) -> None:
    """Render bilingual static HTML dashboard for GitHub Pages."""
    from pathlib import Path
    from finsynapse.dashboard.render_static import render

    paths = render(Path(out_dir))
    for p in paths:
        typer.secho(f"[dashboard] rendered -> {p} ({p.stat().st_size:,} bytes)", fg=typer.colors.GREEN)


@notify_app.command("check")
def notify_check() -> None:
    """Detect zone crossings + sub-temp extremes vs yesterday; push if any.

    Designed to run as the last step of the daily CI workflow. Silent when
    no changes (no false-alarm fatigue). Always exits 0 so a missing notify
    channel never fails the workflow.
    """
    from finsynapse.notify.dispatch import dispatch
    from finsynapse.notify.state import detect_changes

    events = detect_changes()
    typer.echo(f"[notify] {len(events)} event(s) detected")
    for e in events:
        typer.echo(f"  - {e.summary}")

    result = dispatch(events)
    typer.echo(
        f"[notify] bark: {result.bark_status or '—'} ({result.bark_skipped_reason or 'sent'}) | "
        f"telegram: {result.telegram_status or '—'} ({result.telegram_skipped_reason or 'sent'})"
    )


@notify_app.command("test")
def notify_test() -> None:
    """Send a test message on every configured channel — for first-time setup."""
    from finsynapse.notify.dispatch import send_bark, send_telegram

    title = "🌡️ FinSynapse test"
    body = "通道连通性测试 / connectivity check."
    bark_status, bark_err = send_bark(title, body)
    tg_status, tg_err = send_telegram(f"*{title}*\n{body}")
    typer.echo(f"bark:     {bark_status or '—'}  {bark_err or 'ok'}")
    typer.echo(f"telegram: {tg_status or '—'}  {tg_err or 'ok'}")


if __name__ == "__main__":
    app()
