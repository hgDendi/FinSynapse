from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape
from plotly.utils import PlotlyJSONEncoder

from finsynapse.dashboard import charts
from finsynapse.dashboard.data import MARKETS, DashboardData, load
from finsynapse.dashboard.i18n import DEFAULT_LANG, SUPPORTED, TRANSLATIONS, t, translate_div


def _fig_to_json(fig) -> str:
    return json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)


def _i18n_namespace(lang: str) -> SimpleNamespace:
    """Pre-resolve every translation key for the template (`tx.foo` access)."""
    return SimpleNamespace(**{k: t(k, lang) for k in TRANSLATIONS})


def _render_one(env: Environment, data: DashboardData, lang: str, alt_href: str) -> str:
    latest = data.latest_per_market()
    history_market = next(iter(latest), MARKETS[0])

    gauges, radars, attribs, data_quality = {}, {}, {}, {}
    for market, row in latest.items():
        gauges[market] = _fig_to_json(charts.gauge(market, row["overall"], row.get("overall_change_1w"), lang))
        radars[market] = _fig_to_json(charts.radar(market, {
            "valuation": row.get("valuation"),
            "sentiment": row.get("sentiment"),
            "liquidity": row.get("liquidity"),
        }, lang))
        attribs[market] = _fig_to_json(charts.attribution_bars(row, lang))
        data_quality[market] = row.get("data_quality", "ok")

    time_series_json = _fig_to_json(charts.time_series(data.temperature, history_market, lang))
    divergence_json = _fig_to_json(charts.divergence_recent(data.divergence, lang=lang))

    div_table = []
    if not data.divergence.empty:
        df = data.divergence[data.divergence["is_divergent"]].copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date", ascending=False).head(10)
        df["description"] = df["description"].map(lambda d: translate_div(d, lang))
        div_table = df.to_dict(orient="records")

    health_summary, health_table = None, []
    if not data.health.empty:
        h = data.health.copy()
        health_summary = {
            "total": len(h),
            "fail": int((h["severity"] == "fail").sum()),
            "warn": int((h["severity"] == "warn").sum()),
        }
        health_table = h.sort_values("date", ascending=False).head(50).to_dict(orient="records")

    template = env.get_template("static.html.j2")
    return template.render(
        lang=lang,
        tx=_i18n_namespace(lang),
        alt_lang_href=alt_href,
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


# Filename convention: zh -> index.html (default landing), en -> en.html
LANG_FILENAME = {"zh": "index.html", "en": "en.html"}


def render(out_dir: Path | str = "dist", data: DashboardData | None = None) -> list[Path]:
    """Render one HTML file per supported language. Returns list of written paths.
    The default language file is always `index.html` so GitHub Pages serves it first."""
    data = data or load()
    if data.temperature.empty:
        raise RuntimeError("No silver data. Run `finsynapse transform run --layer all` first.")

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    env = Environment(
        loader=FileSystemLoader(Path(__file__).parent / "templates"),
        autoescape=select_autoescape(["html"]),
    )

    written: list[Path] = []
    for lang in SUPPORTED:
        # Other lang's file is the alternate href for the toggle in this lang's page.
        alt_lang = next(l for l in SUPPORTED if l != lang)
        alt_href = LANG_FILENAME[alt_lang]
        # Default lang lives at index.html; non-default at <lang>.html.
        if lang == DEFAULT_LANG:
            target = out_dir / "index.html"
            # Toggle link from default page must point to the alt-lang file
            alt_href = LANG_FILENAME[alt_lang]
        else:
            target = out_dir / LANG_FILENAME[lang]
            # Toggle link back to default goes to index.html (root), not zh.html
            alt_href = "index.html" if alt_lang == DEFAULT_LANG else LANG_FILENAME[alt_lang]

        html = _render_one(env, data, lang, alt_href)
        target.write_text(html, encoding="utf-8")
        written.append(target)

    return written
