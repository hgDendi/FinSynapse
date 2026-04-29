from __future__ import annotations

import pandas as pd
import streamlit as st

from finsynapse.dashboard import charts
from finsynapse.dashboard.data import MARKETS, load
from finsynapse.dashboard.i18n import DEFAULT_LANG, SUPPORTED, t, translate_div


def main() -> None:
    # Page title needs lang set BEFORE we know user choice — use default,
    # then re-read after sidebar widget. Acceptable: only matters on first load.
    lang = DEFAULT_LANG
    st.set_page_config(page_title=t("page_title", lang), layout="wide", page_icon="🌡️")

    with st.sidebar:
        lang = st.radio(
            t("sidebar_lang_label", DEFAULT_LANG) + " / Language",
            options=list(SUPPORTED),
            index=list(SUPPORTED).index(DEFAULT_LANG),
            format_func=lambda c: {"zh": "中文", "en": "English"}[c],
            horizontal=True,
        )

    data = load()
    if data.temperature.empty:
        st.error(t("no_silver_data", lang))
        st.stop()

    asof = data.asof()
    st.title(t("header_title", lang))
    st.caption(f"{t('data_as_of', lang)} **{asof.date()}**  ·  silver: `{data.silver_dir}`")

    latest = data.latest_per_market()

    # --- Top: per-market gauge + radar + attribution -----------------------
    st.subheader(t("section_market_temps", lang))
    cols = st.columns(len(MARKETS))
    for col, market in zip(cols, MARKETS):
        with col:
            row = latest.get(market)
            if row is None:
                st.info(f"**{market.upper()}** — {t('no_data_card', lang)}")
                continue
            st.plotly_chart(
                charts.gauge(market, row["overall"], row.get("overall_change_1w"), lang),
                use_container_width=True,
                key=f"gauge_{market}",
            )
            sub = {
                "valuation": row.get("valuation"),
                "sentiment": row.get("sentiment"),
                "liquidity": row.get("liquidity"),
            }
            st.plotly_chart(charts.radar(market, sub, lang), use_container_width=True, key=f"radar_{market}")
            st.plotly_chart(charts.attribution_bars(row, lang), use_container_width=True, key=f"attrib_{market}")
            if row.get("data_quality", "ok") != "ok":
                st.warning(f"{t('data_quality_label', lang)}: {row['data_quality']}")

    st.divider()

    # --- 10Y temperature time series per market ----------------------------
    st.subheader(t("section_long_history", lang))
    market_for_history = st.radio(
        t("select_market", lang),
        options=[m for m in MARKETS if m in latest],
        horizontal=True,
        label_visibility="collapsed",
    )
    st.plotly_chart(charts.time_series(data.temperature, market_for_history, lang), use_container_width=True)

    st.divider()

    # --- Recent divergences -------------------------------------------------
    st.subheader(t("section_divergence", lang))
    st.plotly_chart(charts.divergence_recent(data.divergence, lang=lang), use_container_width=True)
    if not data.divergence.empty:
        recent_div = data.divergence[data.divergence["is_divergent"]].copy()
        recent_div["date"] = pd.to_datetime(recent_div["date"])
        recent_div = recent_div.sort_values("date", ascending=False).head(10)
        recent_div["description"] = recent_div["description"].map(lambda d: translate_div(d, lang))
        recent_div = recent_div.rename(columns={
            "date": t("th_date", lang),
            "pair_name": t("th_pair", lang),
            "a_change": t("th_a_change", lang),
            "b_change": t("th_b_change", lang),
            "strength": t("th_strength", lang),
            "description": t("th_signal", lang),
        })
        st.dataframe(
            recent_div[[t("th_date", lang), t("th_pair", lang),
                        t("th_a_change", lang), t("th_b_change", lang),
                        t("th_strength", lang), t("th_signal", lang)]],
            use_container_width=True,
            hide_index=True,
        )

    # --- Data health --------------------------------------------------------
    st.divider()
    st.subheader(t("section_data_health", lang))
    if data.health.empty:
        st.success(t("no_health_issues", lang))
    else:
        h = data.health.copy()
        n_fail = (h["severity"] == "fail").sum()
        n_warn = (h["severity"] == "warn").sum()
        st.metric(t("issues_metric", lang), f"{len(h)}", delta=f"{n_fail} fail / {n_warn} warn", delta_color="inverse")
        h_display = h.sort_values("date", ascending=False).head(50).rename(columns={
            "date": t("th_date", lang),
            "indicator": t("th_indicator", lang),
            "rule": t("th_rule", lang),
            "severity": t("th_severity", lang),
            "detail": t("th_detail", lang),
        })
        st.dataframe(h_display, use_container_width=True, hide_index=True)

    st.caption(f"{t('footer', lang)} · `config/weights.yaml`")


if __name__ == "__main__":
    main()
