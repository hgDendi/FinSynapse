from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape
from plotly.utils import PlotlyJSONEncoder

from finsynapse.dashboard import charts
from finsynapse.dashboard.data import MARKETS, DashboardData, load


def _fig_to_json(fig) -> str:
    return json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)


def render(out_path: Path, data: DashboardData | None = None) -> Path:
    """Render a self-contained HTML dashboard suitable for GitHub Pages.
    All charts embedded as plotly JSON; only external dep is plotly CDN."""
    data = data or load()
    if data.temperature.empty:
        raise RuntimeError("No silver data. Run `finsynapse transform run --layer all` first.")

    env = Environment(
        loader=FileSystemLoader(Path(__file__).parent / "templates"),
        autoescape=select_autoescape(["html"]),
    )
    template = env.get_template("static.html.j2")

    latest = data.latest_per_market()
    history_market = next(iter(latest), MARKETS[0])

    gauges = {}
    radars = {}
    attribs = {}
    data_quality = {}
    for market, row in latest.items():
        gauges[market] = _fig_to_json(charts.gauge(market, row["overall"], row.get("overall_change_1w")))
        radars[market] = _fig_to_json(charts.radar(market, {
            "valuation": row.get("valuation"),
            "sentiment": row.get("sentiment"),
            "liquidity": row.get("liquidity"),
        }))
        attribs[market] = _fig_to_json(charts.attribution_bars(row))
        data_quality[market] = row.get("data_quality", "ok")

    time_series_json = _fig_to_json(charts.time_series(data.temperature, history_market))
    divergence_json = _fig_to_json(charts.divergence_recent(data.divergence))

    div_table = []
    if not data.divergence.empty:
        df = data.divergence[data.divergence["is_divergent"]].copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date", ascending=False).head(10)
        div_table = df.to_dict(orient="records")

    health_summary = None
    health_table = []
    if not data.health.empty:
        h = data.health.copy()
        health_summary = {
            "total": len(h),
            "fail": int((h["severity"] == "fail").sum()),
            "warn": int((h["severity"] == "warn").sum()),
        }
        h = h.sort_values("date", ascending=False).head(50)
        health_table = h.to_dict(orient="records")

    html = template.render(
        asof=data.asof().date().isoformat(),
        markets=MARKETS,
        gauges=gauges,
        radars=radars,
        attribs=attribs,
        data_quality=data_quality,
        history_market=history_market,
        time_series_json=time_series_json,
        divergence_json=divergence_json,
        divergence_table=div_table,
        health_summary=health_summary,
        health_table=health_table,
    )

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    return out_path
