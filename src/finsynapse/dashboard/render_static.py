from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from types import SimpleNamespace

import markdown as _md
import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup
from plotly.utils import PlotlyJSONEncoder

from finsynapse.dashboard import charts
from finsynapse.dashboard.api import write_all as _write_api_endpoints
from finsynapse.dashboard.data import MARKETS, DashboardData, load
from finsynapse.dashboard.historical_events import event_label
from finsynapse.dashboard.i18n import (
    DEFAULT_LANG,
    SUPPORTED,
    TRANSLATIONS,
    divergence_plain,
    indicator_plain_name,
    pair_plain_name,
    t,
    translate_div,
)
from finsynapse.report.brief import BriefMeta, list_briefs, load_latest_narrative
from finsynapse.transform.temperature import WeightsConfig

# Public source repo — surfaced in the footer and on the glossary page.
REPO_URL = "https://github.com/hgDendi/FinSynapse"


def _clarity_id() -> str | None:
    """Microsoft Clarity project ID, injected via the CLARITY_PROJECT_ID env var
    (set in the daily.yml workflow from a repo Variable). Absent in local dev so
    previews don't pollute production analytics — the partial renders to nothing
    when this is None."""
    val = os.environ.get("CLARITY_PROJECT_ID", "").strip()
    return val or None


# Map of market code -> display metadata used by the redesigned card UI.
# Colours mirror the chart palette (charts.py) so a card and its embedded
# Plotly figures share one visual identity.
MARKET_META = {
    "cn": {"label": "🇨🇳 CN", "name_zh": "中国 A 股", "name_en": "China A-share", "accent": "navy"},
    "hk": {"label": "🇭🇰 HK", "name_zh": "香港", "name_en": "Hong Kong", "accent": "gold"},
    "us": {"label": "🇺🇸 US", "name_zh": "美国", "name_en": "United States", "accent": "coral"},
}


# Strength → (risk bucket key, star count, accent token). Buckets calibrated
# against the empirical spread of strengths we see in silver — most days
# everything is < 0.01; > 0.5 is genuinely rare.
def _risk_bucket(strength: float) -> tuple[str, int, str]:
    if strength >= 0.5:
        return "risk_high", 4, "coral"
    if strength >= 0.1:
        return "risk_med", 3, "gold"
    if strength >= 0.01:
        return "risk_low", 2, "navy"
    return "risk_weak", 1, "navy"


def _zone_token(value: float | None) -> tuple[str, str]:
    """(zone_key, accent) for a 0–100 temperature."""
    if value is None or pd.isna(value):
        return "zone_mid", "gold"
    if value >= 70:
        return "zone_hot", "coral"
    if value < 30:
        return "zone_cold", "navy"
    return "zone_mid", "gold"


def _market_history_stats(temperature: pd.DataFrame) -> dict[str, dict]:
    """For each market, compute all-time hot / cold dates and where today's
    overall sits as a percentile within its own full history. Returned shape
    keyed by market: { 'today_pct': 0–100, 'hot_date', 'hot_temp',
    'cold_date', 'cold_temp', 'today_date', 'today_temp' }."""
    if temperature.empty:
        return {}
    df = temperature.copy()
    df["date"] = pd.to_datetime(df["date"])
    out: dict[str, dict] = {}
    for market in MARKETS:
        sub = df[df["market"] == market].dropna(subset=["overall"]).sort_values("date")
        if sub.empty:
            continue
        today_row = sub.iloc[-1]
        today_temp = float(today_row["overall"])
        # Inclusive rank (≤) so today's value itself counts.
        today_pct = float((sub["overall"] <= today_temp).mean() * 100.0)
        idx_hot = sub["overall"].idxmax()
        idx_cold = sub["overall"].idxmin()
        out[market] = {
            "today_pct": today_pct,
            "today_date": today_row["date"].date(),
            "today_temp": today_temp,
            "hot_date": sub.loc[idx_hot, "date"].date(),
            "hot_temp": float(sub.loc[idx_hot, "overall"]),
            "cold_date": sub.loc[idx_cold, "date"].date(),
            "cold_temp": float(sub.loc[idx_cold, "overall"]),
        }
    return out


def _build_market_cards(
    latest: dict,
    data_quality: dict,
    lang: str,
    history_stats: dict[str, dict] | None = None,
    complete_dates: dict[str, str | None] | None = None,
) -> list[dict]:
    """Compose the per-market hero cards.

    Returns one dict per market with everything the template needs. Pre-
    computing in Python (rather than doing the math in Jinja) keeps the
    template close to pure presentation.
    """
    history_stats = history_stats or {}
    complete_dates = complete_dates or {}
    cards = []
    for market in MARKETS:
        meta = MARKET_META[market]
        if market not in latest:
            cards.append({"market": market, "meta": meta, "missing": True})
            continue
        row = latest[market]
        overall = row.get("overall")
        zone_key, accent = _zone_token(overall)
        change_1w = row.get("overall_change_1w")
        sub_temps = []
        for sub_key in ("valuation", "sentiment", "liquidity"):
            v = row.get(sub_key)
            sub_temps.append(
                {
                    "key": sub_key,
                    "label": t(sub_key, lang),
                    "plain": t(f"{sub_key}_plain", lang),
                    "value": None if (v is None or pd.isna(v)) else float(v),
                    "contribution": (
                        None
                        if pd.isna(row.get(f"{sub_key}_contribution_1w"))
                        else float(row.get(f"{sub_key}_contribution_1w"))
                    ),
                }
            )
        hist = history_stats.get(market)
        history_widget = None
        if hist is not None:
            pct_int = round(hist["today_pct"])
            history_widget = {
                "pct": pct_int,
                "hover": t("card_history_pct_hover", lang).format(pct=pct_int),
                "extremes_hint": t("card_history_extremes_hint", lang).format(
                    hot_temp=hist["hot_temp"], cold_temp=hist["cold_temp"]
                ),
            }
        completeness = row.get("subtemp_completeness")
        cards.append(
            {
                "market": market,
                "meta": meta,
                "missing": False,
                "overall": float(overall) if overall is not None and not pd.isna(overall) else None,
                "overall_int": round(overall) if overall is not None and not pd.isna(overall) else None,
                "change_1w": None if (change_1w is None or pd.isna(change_1w)) else float(change_1w),
                "zone_label": t(f"{zone_key}_label", lang),
                "zone_key": zone_key,
                "accent": accent,
                "sub_temps": sub_temps,
                "data_quality": data_quality.get(market, "ok"),
                "history": history_widget,
                "latest_complete_date": complete_dates.get(market),
                "subtemp_completeness": int(completeness)
                if completeness is not None and not pd.isna(completeness)
                else None,
            }
        )
    return cards


def _build_divergence_cards(div_df: pd.DataFrame, lang: str, limit: int = 6) -> list[dict]:
    """Replace the old cryptic table with a card list — suggestion A.

    Each card carries: pair code (kept verbatim), plain pair name, headline
    (the existing terse "恒指↑+南向↓: ..." translation) and a longer plain
    explanation pulled from i18n. Strength becomes a 1-4 star bucket so
    users grasp severity without reading the float.
    """
    if div_df.empty:
        return []
    df = div_df[div_df["is_divergent"]].copy()
    if df.empty:
        return []
    df["date"] = pd.to_datetime(df["date"])
    cutoff = df["date"].max() - pd.Timedelta(days=90)
    df = df[df["date"] >= cutoff].sort_values("strength", ascending=False)
    # De-duplicate by pair: keep the strongest occurrence of each pair so the
    # user sees breadth (multiple distinct signals) instead of one pair filling
    # the page just because it spiked repeatedly.
    df = df.drop_duplicates(subset="pair_name", keep="first").head(limit)

    cards = []
    for _, d in df.iterrows():
        pair = d["pair_name"]
        strength = float(d["strength"])
        bucket_key, stars, accent = _risk_bucket(strength)
        cards.append(
            {
                "date": d["date"].date().isoformat(),
                "pair_code": pair,
                "pair_plain": pair_plain_name(pair, lang),
                "headline": translate_div(d["description"], lang),
                "plain_explanation": divergence_plain(d["description"], lang),
                "strength": strength,
                "stars": stars,
                "stars_empty": 4 - stars,
                "risk_label": t(bucket_key, lang),
                "accent": accent,
                "a_change_pct": float(d["a_change"]) * 100,
                "b_change_pct": float(d["b_change"]) * 100,
            }
        )
    return cards


def _build_key_takeaways(data: DashboardData, latest: dict, div_cards: list[dict], lang: str) -> list[dict]:
    """Produce up to 3 structured takeaways — suggestion C.

    Deterministic (no LLM call): we already pay an LLM for the long
    narrative below the takeaways, no need to spend tokens here. Picks:
      1. The market in the most extreme zone (hottest or coldest) + its
         weekly direction.
      2. The strongest recent divergence (re-uses div_cards we just built).
      3. The single most extreme percentile-10y reading.
    """
    out: list[dict] = []

    # 1. Most extreme market.
    rated = [
        (m, row.get("overall"), row.get("overall_change_1w"))
        for m, row in latest.items()
        if row.get("overall") is not None and not pd.isna(row.get("overall"))
    ]
    if rated:
        # Pick whichever is further from neutral (50).
        m, v, chg = max(rated, key=lambda x: abs(x[1] - 50))
        zone_key, accent = _zone_token(v)
        meta = MARKET_META[m]
        market_name = meta["name_zh"] if lang == "zh" else meta["name_en"]
        if lang == "zh":
            chg_phrase = ""
            if chg is not None and not pd.isna(chg) and abs(chg) >= 0.5:
                direction = "升" if chg > 0 else "降"
                chg_phrase = f"，本周{direction} {abs(chg):.1f}°"
            detail = f"温度 {v:.0f}°，处于{t(zone_key, lang)}区间{chg_phrase}。"
        else:
            chg_phrase = ""
            if chg is not None and not pd.isna(chg) and abs(chg) >= 0.5:
                direction = "up" if chg > 0 else "down"
                chg_phrase = f", {direction} {abs(chg):.1f}° this week"
            detail = f"Temperature {v:.0f}°, in the {t(zone_key, lang)} zone{chg_phrase}."
        out.append(
            {
                "icon": "thermostat",
                "accent": accent,
                "headline": (
                    f"{meta['label']} {market_name} 最值得关注"
                    if lang == "zh"
                    else f"{meta['label']} {market_name} is the standout"
                ),
                "detail": detail,
            }
        )

    # 2. Strongest divergence.
    if div_cards:
        top = div_cards[0]
        out.append(
            {
                "icon": "call_split",
                "accent": top["accent"],
                "headline": top["pair_plain"],
                "detail": top["plain_explanation"] or top["headline"],
            }
        )

    # 3. Percentile extreme.
    if not data.percentile.empty:
        pct = data.percentile.copy()
        pct["date"] = pd.to_datetime(pct["date"])
        latest_dt = pct["date"].max()
        snap = pct[pct["date"] == latest_dt].dropna(subset=["pct_10y"])
        extreme = snap[(snap["pct_10y"] >= 90) | (snap["pct_10y"] <= 10)]
        if not extreme.empty:
            extreme = extreme.assign(_dist=lambda d: (d["pct_10y"] - 50).abs())
            top = extreme.sort_values("_dist", ascending=False).iloc[0]
            ind = top["indicator"]
            plain = indicator_plain_name(ind, lang)
            pct_val = float(top["pct_10y"])
            is_high = pct_val >= 90
            accent = "coral" if is_high else "navy"
            if lang == "zh":
                headline = f"{plain} 处于 10 年 {pct_val:.0f}% 分位"
                detail = f"当前值 {top['value']:.4g}，{'已到极端高位' if is_high else '已到极端低位'} — `{ind}`。"
            else:
                headline = f"{plain} sits at {pct_val:.0f}-pct (10y)"
                detail = f"Current value {top['value']:.4g}, {'extreme high' if is_high else 'extreme low'} — `{ind}`."
            out.append({"icon": "monitoring", "accent": accent, "headline": headline, "detail": detail})

    return out[:3]


def _fig_to_json(fig) -> str:
    return json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)


def _i18n_namespace(lang: str) -> SimpleNamespace:
    """Pre-resolve every translation key for the template (`tx.foo` access)."""
    return SimpleNamespace(**{k: t(k, lang) for k in TRANSLATIONS})


def _render_one(
    env: Environment,
    data: DashboardData,
    lang: str,
    alt_href: str,
    archive_href: str,
    glossary_href: str,
) -> str:
    latest = data.latest_per_market()
    history_market = next(iter(latest), MARKETS[0])
    history_stats = _market_history_stats(data.temperature)

    # Per-market Plotly gauges (kept — the Plotly indicator looks better than
    # a hand-rolled CSS half-circle). Radar + attribution remain CSS-only in
    # the static template since the bento card already conveys those.
    gauges, data_quality = {}, {}
    for market, row in latest.items():
        gauges[market] = _fig_to_json(charts.gauge(market, row["overall"], row.get("overall_change_1w"), lang))
        data_quality[market] = row.get("data_quality", "ok")

    # Build one time-series figure per market that has any temperature history,
    # so the template can switch client-side without a re-render. `history_market`
    # is the initially-active tab.
    time_series_by_market: dict[str, str] = {}
    for market in MARKETS:
        if (data.temperature["market"] == market).any():
            time_series_by_market[market] = _fig_to_json(charts.time_series(data.temperature, market, lang))
    if history_market not in time_series_by_market and time_series_by_market:
        history_market = next(iter(time_series_by_market))
    divergence_json = _fig_to_json(charts.divergence_recent(data.divergence, lang=lang))

    cross_market_input = {
        market: {
            "valuation": row.get("valuation"),
            "sentiment": row.get("sentiment"),
            "liquidity": row.get("liquidity"),
        }
        for market, row in latest.items()
    }
    cross_market_json = _fig_to_json(charts.cross_market_radar(cross_market_input, lang))

    # Latest LLM-narrated brief, if any. Same brief.md is used for both lang
    # variants — the LLM-written paragraph is bilingual-friendly Chinese; we
    # don't auto-translate to keep the source of truth single (matches
    # README / config policy of avoiding translation drift).
    narrative_md, narrative_asof = load_latest_narrative()
    narrative_html = Markup(_md.markdown(narrative_md, extensions=["extra"])) if narrative_md else ""

    market_cards = _build_market_cards(latest, data_quality, lang, history_stats, data.latest_complete_date())
    divergence_cards = _build_divergence_cards(data.divergence, lang)
    key_takeaways = _build_key_takeaways(data, latest, divergence_cards, lang)

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
        archive_href=archive_href,
        glossary_href=glossary_href,
        repo_url=REPO_URL,
        clarity_project_id=_clarity_id(),
        page_type="dashboard",
        asof=data.asof().date().isoformat(),
        markets=MARKETS,
        gauges=gauges,
        data_quality=data_quality,
        history_market=history_market,
        time_series_by_market=time_series_by_market,
        market_meta=MARKET_META,
        cross_market_json=cross_market_json,
        divergence_json=divergence_json,
        narrative_html=narrative_html,
        narrative_asof=narrative_asof,
        market_cards=market_cards,
        divergence_cards=divergence_cards,
        key_takeaways=key_takeaways,
        health_summary=health_summary,
        health_table=health_table,
    )


# Filename convention:
#   zh dashboard  -> index.html (default landing), archive -> briefs.html
#   en dashboard  -> en.html,                       archive -> briefs.en.html
LANG_FILENAME = {"zh": "index.html", "en": "en.html"}
ARCHIVE_FILENAME = {"zh": "briefs.html", "en": "briefs.en.html"}
GLOSSARY_FILENAME = {"zh": "glossary.html", "en": "glossary.en.html"}


def _build_glossary_markets(weights: WeightsConfig, lang: str) -> list[dict]:
    """Flatten weights.yaml into the table shape the glossary template wants:
    one block per market, each carrying its 3 sub-temp weights + a row per
    indicator with (sub, weight-within-block, direction). Indicator names use
    the friendly i18n alias when available."""
    sub_keys = ("valuation", "sentiment", "liquidity")
    out = []
    for market in MARKETS:
        meta = MARKET_META[market]
        sub_w = weights.sub_weights.get(market, {})
        if not sub_w:
            continue
        rows: list[dict] = []
        for sub in sub_keys:
            block = weights.indicator_weights.get(f"{market}_{sub}", {})
            if not block:
                continue
            first = True
            for indicator, spec in block.items():
                rows.append(
                    {
                        "sub_label": t(sub, lang),
                        "sub_weight": float(sub_w.get(sub, 0.0)),
                        "is_first_in_block": first,
                        "indicator": indicator,
                        "indicator_plain": indicator_plain_name(indicator, lang),
                        "weight": float(spec.get("weight", 0.0)),
                        "direction": spec.get("direction", "+"),
                    }
                )
                first = False
        out.append(
            {
                "code": market,
                "label": meta["label"],
                "name_zh": meta["name_zh"],
                "name_en": meta["name_en"],
                "sub_weights": sub_w,
                "indicator_rows": rows,
            }
        )
    return out


def _build_glossary_history_rows(history_stats: dict[str, dict], lang: str) -> list[dict]:
    """Per market: today, all-time hot, all-time cold rows for the explainer
    table. Event labels come from `historical_events.event_label`."""
    rows: list[dict] = []
    for market in MARKETS:
        stats = history_stats.get(market)
        if not stats:
            continue
        meta = MARKET_META[market]
        market_label = f"{meta['label']} {meta['name_zh'] if lang == 'zh' else meta['name_en']}"
        for kind in ("today", "hot", "cold"):
            d = stats[f"{kind}_date"] if kind != "today" else stats["today_date"]
            temp = stats[f"{kind}_temp"] if kind != "today" else stats["today_temp"]
            rows.append(
                {
                    "market_label": market_label,
                    "kind": kind,
                    "date": d.isoformat(),
                    "temperature": temp,
                    "event": event_label(market, d, lang),
                    "is_today": kind == "today",
                    "history_pct": stats["today_pct"],
                }
            )
    return rows


def _render_glossary_pages(env: Environment, out_dir: Path, data: DashboardData, weights: WeightsConfig) -> list[Path]:
    """Render the bilingual glossary explainer pages (glossary.html /
    glossary.en.html). Each page is data-driven from weights.yaml + historical
    extremes from temperature_daily, so it stays in sync with config."""
    template = env.get_template("glossary.html.j2")
    history_stats = _market_history_stats(data.temperature)
    written: list[Path] = []
    for lang in SUPPORTED:
        alt_lang = next(other for other in SUPPORTED if other != lang)
        # Match the dashboard's "default lang lives at index.html" convention.
        alt_lang_href = GLOSSARY_FILENAME[DEFAULT_LANG] if alt_lang == DEFAULT_LANG else GLOSSARY_FILENAME[alt_lang]
        dashboard_href = "index.html" if lang == DEFAULT_LANG else LANG_FILENAME[lang]
        html = template.render(
            lang=lang,
            tx=_i18n_namespace(lang),
            alt_lang_href=alt_lang_href,
            dashboard_href=dashboard_href,
            repo_url=REPO_URL,
            asof=data.asof().date().isoformat() if data.asof() is not None else "",
            markets_meta=_build_glossary_markets(weights, lang),
            history_rows=_build_glossary_history_rows(history_stats, lang),
            clarity_project_id=_clarity_id(),
            page_type="glossary",
        )
        target = out_dir / GLOSSARY_FILENAME[lang]
        target.write_text(html, encoding="utf-8")
        written.append(target)
    return written


def _render_brief_pages(env: Environment, out_dir: Path, briefs: list[BriefMeta]) -> list[Path]:
    """For each brief on disk:
      1. copy raw .md to dist/brief/<date>.md (direct download / share URL)
      2. render dist/brief/<date>.html using the same site chrome

    Single-language only — brief content is Chinese, so chrome stays Chinese.
    English-speaking visitors arriving here from the EN archive still see
    the same content; the back-link is to the (zh) archive page.
    """
    if not briefs:
        return []
    brief_out = out_dir / "brief"
    brief_out.mkdir(parents=True, exist_ok=True)

    template = env.get_template("brief_single.html.j2")
    tx = _i18n_namespace(DEFAULT_LANG)
    written: list[Path] = []

    for b in briefs:
        # 1. raw md copy
        md_dest = brief_out / f"{b.asof}.md"
        shutil.copyfile(b.path, md_dest)
        written.append(md_dest)

        # 2. rendered html
        body_md = b.path.read_text(encoding="utf-8")
        body_html = Markup(_md.markdown(body_md, extensions=["extra"]))
        html = template.render(
            tx=tx,
            asof=b.asof,
            body_html=body_html,
            clarity_project_id=_clarity_id(),
            page_type="brief_daily",
            lang=DEFAULT_LANG,
        )
        html_dest = brief_out / f"{b.asof}.html"
        html_dest.write_text(html, encoding="utf-8")
        written.append(html_dest)

    return written


def _render_archive_index(env: Environment, out_dir: Path, briefs: list[BriefMeta], asof: str) -> list[Path]:
    """Render the bilingual /briefs.html (and /briefs.en.html) index page
    listing every brief on disk, newest first."""
    template = env.get_template("brief_archive.html.j2")
    written: list[Path] = []

    for lang in SUPPORTED:
        alt_lang = next(other for other in SUPPORTED if other != lang)
        alt_href = ARCHIVE_FILENAME[alt_lang]
        # Back-to-dashboard target depends on which lang we're rendering.
        dashboard_href = LANG_FILENAME[lang]

        html = template.render(
            lang=lang,
            tx=_i18n_namespace(lang),
            briefs=briefs,
            alt_lang_href=alt_href,
            dashboard_href=dashboard_href,
            repo_url=REPO_URL,
            asof=asof,
            clarity_project_id=_clarity_id(),
            page_type="brief_archive",
        )
        target = out_dir / ARCHIVE_FILENAME[lang]
        target.write_text(html, encoding="utf-8")
        written.append(target)

    return written


def render(out_dir: Path | str = "dist", data: DashboardData | None = None) -> list[Path]:
    """Render every page that lands on GitHub Pages:
      - dashboard (zh + en)
      - per-brief HTML pages + raw .md copies
      - bilingual brief archive index

    Returns the full list of written paths."""
    data = data or load()
    if data.temperature.empty:
        raise RuntimeError("No silver data. Run `finsynapse transform run --layer all` first.")

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    env = Environment(
        loader=FileSystemLoader(Path(__file__).parent / "templates"),
        autoescape=select_autoescape(["html"]),
    )

    briefs = list_briefs()
    weights = WeightsConfig.load()
    asof_str = data.asof().date().isoformat()

    written: list[Path] = []
    for lang in SUPPORTED:
        # Other lang's file is the alternate href for the toggle in this lang's page.
        alt_lang = next(other for other in SUPPORTED if other != lang)
        # Default lang lives at index.html; non-default at <lang>.html.
        if lang == DEFAULT_LANG:
            target = out_dir / "index.html"
            # Toggle link from default page must point to the alt-lang file
            alt_href = LANG_FILENAME[alt_lang]
        else:
            target = out_dir / LANG_FILENAME[lang]
            # Toggle link back to default goes to index.html (root), not zh.html
            alt_href = "index.html" if alt_lang == DEFAULT_LANG else LANG_FILENAME[alt_lang]

        archive_href = ARCHIVE_FILENAME[lang]
        glossary_href = GLOSSARY_FILENAME[lang]
        html = _render_one(env, data, lang, alt_href, archive_href, glossary_href)
        target.write_text(html, encoding="utf-8")
        written.append(target)

    written.extend(_render_glossary_pages(env, out_dir, data, weights))
    written.extend(_render_brief_pages(env, out_dir, briefs))
    written.extend(_render_archive_index(env, out_dir, briefs, asof_str))
    written.extend(_write_api_endpoints(data, out_dir))
    return written
