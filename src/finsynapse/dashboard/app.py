from __future__ import annotations

import pandas as pd
import streamlit as st

from finsynapse.dashboard import charts
from finsynapse.dashboard.data import MARKETS, load


def main() -> None:
    st.set_page_config(page_title="FinSynapse — Macro Thermometer", layout="wide", page_icon="🌡️")

    data = load()
    if data.temperature.empty:
        st.error("No silver data yet. Run `uv run finsynapse transform run --layer all` first.")
        st.stop()

    asof = data.asof()
    st.title("🌡️ FinSynapse — Macro Thermometer")
    st.caption(f"Data as of **{asof.date()}**  ·  silver dir: `{data.silver_dir}`")

    latest = data.latest_per_market()

    # --- Top: per-market gauge + radar + attribution -----------------------
    st.subheader("Market temperatures")
    cols = st.columns(len(MARKETS))
    for col, market in zip(cols, MARKETS):
        with col:
            row = latest.get(market)
            if row is None:
                st.info(f"**{market.upper()}** — no data yet (Phase 1b: add AkShare).")
                continue
            st.plotly_chart(
                charts.gauge(market, row["overall"], row.get("overall_change_1w")),
                use_container_width=True,
                key=f"gauge_{market}",
            )
            sub = {
                "valuation": row.get("valuation"),
                "sentiment": row.get("sentiment"),
                "liquidity": row.get("liquidity"),
            }
            st.plotly_chart(charts.radar(market, sub), use_container_width=True, key=f"radar_{market}")
            st.plotly_chart(charts.attribution_bars(row), use_container_width=True, key=f"attrib_{market}")
            if row.get("data_quality", "ok") != "ok":
                st.warning(f"data_quality: {row['data_quality']}")

    st.divider()

    # --- 10Y temperature time series per market ----------------------------
    st.subheader("Long-history temperature")
    market_for_history = st.radio(
        "Market", options=[m for m in MARKETS if m in latest], horizontal=True, label_visibility="collapsed",
    )
    st.plotly_chart(charts.time_series(data.temperature, market_for_history), use_container_width=True)

    st.divider()

    # --- Recent divergences -------------------------------------------------
    st.subheader("Divergence signals (last 90 days)")
    st.plotly_chart(charts.divergence_recent(data.divergence), use_container_width=True)
    if not data.divergence.empty:
        recent_div = data.divergence[data.divergence["is_divergent"]].copy()
        recent_div["date"] = pd.to_datetime(recent_div["date"])
        recent_div = recent_div.sort_values("date", ascending=False).head(10)
        st.dataframe(
            recent_div[["date", "pair_name", "a_change", "b_change", "strength", "description"]],
            use_container_width=True,
            hide_index=True,
        )

    # --- Data health --------------------------------------------------------
    st.divider()
    st.subheader("Data health")
    if data.health.empty:
        st.success("No health issues recorded.")
    else:
        h = data.health.copy()
        n_fail = (h["severity"] == "fail").sum()
        n_warn = (h["severity"] == "warn").sum()
        st.metric("Issues", f"{len(h)}", delta=f"{n_fail} fail / {n_warn} warn", delta_color="inverse")
        st.dataframe(
            h.sort_values("date", ascending=False).head(50),
            use_container_width=True,
            hide_index=True,
        )

    st.caption("FinSynapse · Plan §11 thermometer · weights in `config/weights.yaml`")


if __name__ == "__main__":
    main()
