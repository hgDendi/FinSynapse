from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from finsynapse.dashboard.i18n import DEFAULT_LANG, t, translate_div

# Shared palette aligned with the redesigned UI (Stitch reference).
#   navy  — cold / valuation / CN accent
#   gold  — neutral / sentiment / HK accent
#   coral — hot / liquidity-or-risk / US accent
# All three are visually distinct on glass-morphism cards (background
# is near-white tinted rgba). Keep these constants the only source so
# Plotly figures and Tailwind tokens never drift.
COLOR_COLD = "#1E3A8A"  # navy
COLOR_MID = "#FBBF24"  # gold
COLOR_HOT = "#F87171"  # coral
COLOR_VALUATION = "#6366F1"  # indigo (sub-temp distinct from market accent)
COLOR_SENTIMENT = "#FBBF24"
COLOR_LIQUIDITY = "#10B981"
COLOR_BG = "rgba(0,0,0,0)"  # transparent — sits on glass cards
COLOR_PLOT_BG = "rgba(255,255,255,0.0)"

ZONE_BANDS = [
    (0, 30, "rgba(30,58,138,0.10)"),
    (30, 70, "rgba(251,191,36,0.08)"),
    (70, 100, "rgba(248,113,113,0.10)"),
]

# Manrope first to match the new design system; CJK fallback ensures
# Chinese labels render correctly when Manrope doesn't ship those glyphs.
FONT_FAMILY = (
    'Manrope, -apple-system, BlinkMacSystemFont, "PingFang SC", "Hiragino Sans GB", '
    '"Microsoft YaHei", "Noto Sans CJK SC", "Segoe UI", Roboto, sans-serif'
)


def temp_color(value: float) -> str:
    if pd.isna(value):
        return "#D1D5DB"
    if value < 30:
        return COLOR_COLD
    if value < 70:
        return COLOR_MID
    return COLOR_HOT


def gauge(market: str, value: float, change_1w: float | None = None, lang: str = DEFAULT_LANG) -> go.Figure:
    """Single-market thermometer gauge. Used by the Streamlit app; the static
    HTML site renders a CSS gauge instead so there is no Plotly bundle weight
    inside each market card."""
    color = temp_color(value)
    delta_dict = {"reference": value - (change_1w or 0), "valueformat": ".1f"} if change_1w is not None else None
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number+delta" if delta_dict else "gauge+number",
            value=float(value) if not pd.isna(value) else 0,
            number={"suffix": "°", "font": {"size": 44}},
            delta=delta_dict,
            title={"text": f"<b>{market.upper()}</b>", "font": {"size": 18}},
            gauge={
                "axis": {"range": [0, 100], "tickwidth": 1, "tickcolor": "#374151"},
                "bar": {"color": color, "thickness": 0.65},
                "bgcolor": "rgba(255,255,255,0.5)",
                "borderwidth": 0,
                "steps": [
                    {"range": [0, 30], "color": "rgba(30,58,138,0.15)"},
                    {"range": [30, 70], "color": "rgba(251,191,36,0.12)"},
                    {"range": [70, 100], "color": "rgba(248,113,113,0.18)"},
                ],
                "threshold": {
                    "line": {"color": "#111827", "width": 3},
                    "thickness": 0.85,
                    "value": float(value) if not pd.isna(value) else 0,
                },
            },
        )
    )
    fig.update_layout(
        height=260,
        margin=dict(t=40, b=10, l=20, r=20),
        paper_bgcolor=COLOR_BG,
        font=dict(family=FONT_FAMILY),
    )
    return fig


def radar(market: str, sub_temps: dict[str, float], lang: str = DEFAULT_LANG) -> go.Figure:
    """Three-axis radar of sub-temperatures."""
    cats = [t("valuation", lang), t("sentiment", lang), t("liquidity", lang)]
    vals = [sub_temps.get(k, 0) or 0 for k in ("valuation", "sentiment", "liquidity")]
    color = temp_color(sum(v for v in vals if v) / max(1, sum(1 for v in vals if v)))

    fig = go.Figure()
    fig.add_trace(
        go.Scatterpolar(
            r=[30] * 4,
            theta=[*cats, cats[0]],
            mode="lines",
            line=dict(color="rgba(30,58,138,0.35)", dash="dot"),
            showlegend=False,
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scatterpolar(
            r=[70] * 4,
            theta=[*cats, cats[0]],
            mode="lines",
            line=dict(color="rgba(248,113,113,0.35)", dash="dot"),
            showlegend=False,
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scatterpolar(
            r=[*vals, vals[0]],
            theta=[*cats, cats[0]],
            fill="toself",
            line=dict(color=color, width=2),
            opacity=0.45,
            name=market.upper(),
        )
    )
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100], tickfont=dict(size=10))),
        height=240,
        margin=dict(t=20, b=20, l=20, r=20),
        paper_bgcolor=COLOR_BG,
        showlegend=False,
        title=dict(text=f"<b>{market.upper()}</b> {t('chart_sub_temps_short', lang)}", font=dict(size=13), x=0.5),
        font=dict(family=FONT_FAMILY),
    )
    return fig


def cross_market_radar(latest_per_market: dict[str, dict[str, float]], lang: str = DEFAULT_LANG) -> go.Figure:
    """Overlay every market on the same valuation/sentiment/liquidity axes.

    Single per-market radar (above) shows shape; this view shows *relative*
    standing — at a glance which market is hottest in valuation vs sentiment
    vs liquidity. Same axis scale and zone bands as the per-market radar so
    the visual vocabulary is consistent.
    """
    cats = [t("valuation", lang), t("sentiment", lang), t("liquidity", lang)]
    theta = [*cats, cats[0]]
    market_colors = {"cn": COLOR_COLD, "hk": COLOR_MID, "us": COLOR_HOT}

    fig = go.Figure()
    # Cool/hot zone reference rings (30 / 70).
    for r_val, color in ((30, "rgba(30,58,138,0.35)"), (70, "rgba(248,113,113,0.35)")):
        fig.add_trace(
            go.Scatterpolar(
                r=[r_val] * 4,
                theta=theta,
                mode="lines",
                line=dict(color=color, dash="dot"),
                showlegend=False,
                hoverinfo="skip",
            )
        )

    for market, sub_temps in latest_per_market.items():
        vals = [sub_temps.get(k) for k in ("valuation", "sentiment", "liquidity")]
        # NaN axes can't be drawn — sub in a tiny value but reflect it in hover.
        plot_vals = [v if (v is not None and not pd.isna(v)) else 0 for v in vals]
        hover_vals = ["—" if (v is None or pd.isna(v)) else f"{v:.1f}°" for v in vals]
        color = market_colors.get(market, "#6B7280")
        fig.add_trace(
            go.Scatterpolar(
                r=[*plot_vals, plot_vals[0]],
                theta=theta,
                fill="toself",
                opacity=0.30,
                line=dict(color=color, width=2),
                name=market.upper(),
                customdata=[[hv] for hv in [*hover_vals, hover_vals[0]]],
                hovertemplate="%{theta}: %{customdata[0]}<extra>" + market.upper() + "</extra>",
            )
        )

    fig.update_layout(
        polar=dict(
            bgcolor="rgba(255,255,255,0.0)",
            radialaxis=dict(
                visible=True,
                range=[0, 100],
                tickfont=dict(size=10, color="#6B7280"),
                gridcolor="rgba(0,0,0,0.06)",
                linecolor="rgba(0,0,0,0.08)",
            ),
            angularaxis=dict(tickfont=dict(size=11, color="#374151"), linecolor="rgba(0,0,0,0.08)"),
        ),
        height=360,
        margin=dict(t=20, b=20, l=20, r=20),
        paper_bgcolor=COLOR_BG,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.12, xanchor="center", x=0.5, font=dict(size=12)),
        font=dict(family=FONT_FAMILY, color="#1c1b1b"),
    )
    return fig


def time_series(
    temp_df: pd.DataFrame,
    market: str,
    lang: str = DEFAULT_LANG,
    bootstrap_df: pd.DataFrame | None = None,
) -> go.Figure:
    """Long-history overall + sub-temperature time series for one market.
    Phase 2: adds overall_short / overall_long dashed lines and divergence.
    Phase 3: adds optional bootstrap CI band."""
    df = temp_df[temp_df["market"] == market].copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    has_short = "overall_short" in df.columns and df["overall_short"].notna().any()
    has_long = "overall_long" in df.columns and df["overall_long"].notna().any()
    has_div = has_short and has_long

    n_rows = 3 if has_div else 2
    row_heights = [0.55, 0.25, 0.20] if has_div else [0.65, 0.35]
    v_spacing = 0.04

    fig = make_subplots(
        rows=n_rows,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=v_spacing,
        row_heights=row_heights,
    )

    for lo, hi, color in ZONE_BANDS:
        fig.add_hrect(y0=lo, y1=hi, fillcolor=color, line_width=0, row=1, col=1)

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["overall"],
            name=t("overall", lang),
            line=dict(color="#1c1b1b", width=2),
            hovertemplate=f"%{{x|%Y-%m-%d}}<br>{t('overall', lang)}: %{{y:.1f}}°<extra></extra>",
        ),
        row=1,
        col=1,
    )

    if bootstrap_df is not None and not bootstrap_df.empty:
        bdf = bootstrap_df[bootstrap_df["market"] == market].copy()
        if not bdf.empty:
            bdf["date"] = pd.to_datetime(bdf["date"])
            bdf = bdf.sort_values("date")
            x_band = list(bdf["date"]) + list(bdf["date"])[::-1]
            y_band = list(bdf["lower"]) + list(bdf["upper"])[::-1]
            fig.add_trace(
                go.Scatter(
                    x=x_band,
                    y=y_band,
                    fill="toself",
                    fillcolor="rgba(99,102,241,0.12)",
                    line=dict(width=0),
                    name=t("val_bootstrap_ci", lang),
                    showlegend=True,
                    hovertemplate=f"{t('val_bootstrap_ci', lang)}<extra></extra>",
                ),
                row=1,
                col=1,
            )

    if has_short:
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["overall_short"],
                name=t("overall_short", lang),
                line=dict(color=COLOR_HOT, width=1, dash="dot"),
                opacity=0.7,
                hovertemplate=f"%{{x|%Y-%m-%d}}<br>{t('overall_short', lang)}: %{{y:.1f}}°<extra></extra>",
            ),
            row=1,
            col=1,
        )
    if has_long:
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["overall_long"],
                name=t("overall_long", lang),
                line=dict(color=COLOR_COLD, width=1, dash="dot"),
                opacity=0.7,
                hovertemplate=f"%{{x|%Y-%m-%d}}<br>{t('overall_long', lang)}: %{{y:.1f}}°<extra></extra>",
            ),
            row=1,
            col=1,
        )

    for sub_key, col in [
        ("valuation", COLOR_VALUATION),
        ("sentiment", COLOR_SENTIMENT),
        ("liquidity", COLOR_LIQUIDITY),
    ]:
        if sub_key in df.columns:
            label = t(sub_key, lang)
            fig.add_trace(
                go.Scatter(
                    x=df["date"],
                    y=df[sub_key],
                    name=label,
                    line=dict(color=col, width=1, dash="solid"),
                    opacity=0.85,
                    hovertemplate=f"%{{x|%Y-%m-%d}}<br>{label}: %{{y:.1f}}°<extra></extra>",
                ),
                row=2,
                col=1,
            )

    if has_div:
        for sign_color, lo, hi in [(COLOR_HOT, 0, float("inf")), (COLOR_COLD, float("-inf"), 0)]:
            if lo < hi and lo != float("-inf"):
                fig.add_hrect(y0=lo, y1=hi, fillcolor=sign_color.replace(")", ",0.06)"), line_width=0, row=3, col=1)

        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["divergence"],
                name=t("divergence", lang),
                line=dict(color="#1c1b1b", width=1.5),
                fill="tozeroy",
                fillcolor="rgba(99,102,241,0.12)",
                hovertemplate=f"%{{x|%Y-%m-%d}}<br>{t('divergence', lang)}: %{{y:.1f}}°<extra></extra>",
            ),
            row=3,
            col=1,
        )
        fig.add_hline(y=0, line=dict(color="rgba(0,0,0,0.15)", width=1), row=3, col=1)

    fig.update_yaxes(
        range=[0, 100], title_text="°", row=1, col=1, gridcolor="rgba(0,0,0,0.06)", zerolinecolor="rgba(0,0,0,0.08)"
    )
    fig.update_yaxes(
        range=[0, 100], title_text="°", row=2, col=1, gridcolor="rgba(0,0,0,0.06)", zerolinecolor="rgba(0,0,0,0.08)"
    )
    if has_div:
        fig.update_yaxes(title_text="Δ°", row=3, col=1, gridcolor="rgba(0,0,0,0.06)", zerolinecolor="rgba(0,0,0,0.08)")
    fig.update_xaxes(gridcolor="rgba(0,0,0,0.04)", linecolor="rgba(0,0,0,0.08)")

    height = 500 if has_div else 460
    fig.update_layout(
        height=height,
        margin=dict(t=44, b=30, l=46, r=20),
        paper_bgcolor=COLOR_BG,
        plot_bgcolor=COLOR_PLOT_BG,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=11)),
        hovermode="x unified",
        font=dict(family=FONT_FAMILY, color="#1c1b1b", size=12),
    )
    return fig


def attribution_bars(latest_row: pd.Series, lang: str = DEFAULT_LANG) -> go.Figure:
    contribs = {
        t("valuation", lang): latest_row.get("valuation_contribution_1w", 0) or 0,
        t("sentiment", lang): latest_row.get("sentiment_contribution_1w", 0) or 0,
        t("liquidity", lang): latest_row.get("liquidity_contribution_1w", 0) or 0,
    }
    overall_change = latest_row.get("overall_change_1w", 0) or 0
    fig = go.Figure(
        go.Bar(
            x=list(contribs.values()),
            y=list(contribs.keys()),
            orientation="h",
            marker=dict(color=[COLOR_VALUATION, COLOR_SENTIMENT, COLOR_LIQUIDITY]),
            text=[f"{v:+.1f}°" for v in contribs.values()],
            textposition="outside",
        )
    )
    fig.add_vline(x=0, line=dict(color="rgba(0,0,0,0.25)", width=1))
    fig.update_layout(
        title=dict(
            text=f"<b>{t('chart_1w_contribution', lang)}</b> — {t('chart_overall_change', lang)} {overall_change:+.1f}°",
            font=dict(size=13),
        ),
        height=200,
        margin=dict(t=40, b=20, l=80, r=40),
        paper_bgcolor=COLOR_BG,
        plot_bgcolor=COLOR_PLOT_BG,
        xaxis=dict(title="°", zeroline=True, gridcolor="rgba(0,0,0,0.06)"),
        showlegend=False,
        font=dict(family=FONT_FAMILY, color="#1c1b1b"),
    )
    return fig


def divergence_recent(div_df: pd.DataFrame, n: int = 15, lang: str = DEFAULT_LANG) -> go.Figure:
    if div_df.empty:
        fig = go.Figure()
        fig.add_annotation(text=t("no_divergence", lang), x=0.5, y=0.5, showarrow=False, font=dict(size=14))
        fig.update_layout(height=240, paper_bgcolor=COLOR_BG, font=dict(family=FONT_FAMILY))
        return fig

    df = div_df[div_df["is_divergent"]].copy()
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text=t("no_divergence", lang), x=0.5, y=0.5, showarrow=False, font=dict(size=14))
        fig.update_layout(height=240, paper_bgcolor=COLOR_BG, font=dict(family=FONT_FAMILY))
        return fig

    df["date"] = pd.to_datetime(df["date"])
    cutoff = df["date"].max() - pd.Timedelta(days=90)
    df = df[df["date"] >= cutoff].nlargest(n * 4, "strength")
    df["description_localized"] = df["description"].map(lambda d: translate_div(d, lang))

    fig = go.Figure()
    palette = {
        "sp500_vix": COLOR_HOT,
        "us10y_dxy": "#F59E0B",
        "gold_real_rate": COLOR_MID,
        "sp500_us10y": COLOR_VALUATION,
        "hsi_dxy": COLOR_LIQUIDITY,
        "csi300_volume": "#06B6D4",
        "hsi_southbound": "#EC4899",
    }
    for pair, group in df.groupby("pair_name"):
        fig.add_trace(
            go.Scatter(
                x=group["date"],
                y=group["strength"],
                mode="markers",
                name=pair,
                marker=dict(size=8, color=palette.get(pair, "#6B7280"), line=dict(width=0.5, color="#374151")),
                hovertemplate=("%{x|%Y-%m-%d}<br>" + pair + "<br>strength=%{y:.4f}<br>%{customdata}<extra></extra>"),
                customdata=group["description_localized"],
            )
        )

    fig.update_layout(
        title=dict(text=f"<b>{t('chart_recent_div', lang)}</b>", font=dict(size=13, color="#1c1b1b")),
        height=280,
        margin=dict(t=44, b=30, l=50, r=20),
        paper_bgcolor=COLOR_BG,
        plot_bgcolor=COLOR_PLOT_BG,
        xaxis=dict(title=None, gridcolor="rgba(0,0,0,0.04)", linecolor="rgba(0,0,0,0.08)"),
        yaxis=dict(title=t("th_strength", lang), gridcolor="rgba(0,0,0,0.06)", linecolor="rgba(0,0,0,0.08)"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=11)),
        font=dict(family=FONT_FAMILY, color="#1c1b1b"),
    )
    return fig


def validation_hit_rate_bar(hit_rate_table: dict, lang: str = DEFAULT_LANG) -> go.Figure:
    """Bar chart comparing directional hit rates across controllers × markets."""
    controllers = list(hit_rate_table.keys())
    market_order = ["us", "cn", "hk"]
    display_names = [c.replace("single-factor", "SF").replace("single-point", "SP") for c in controllers]
    market_colors_map = {"us": COLOR_HOT, "cn": COLOR_COLD, "hk": COLOR_MID}

    fig = go.Figure()
    for market in market_order:
        rates = []
        for ctrl in controllers:
            m = hit_rate_table.get(ctrl, {}).get(market, {})
            rates.append(m.get("directional_rate", 0) * 100)
        fig.add_trace(
            go.Bar(
                name=market.upper(),
                x=display_names,
                y=rates,
                marker_color=market_colors_map.get(market, "#6B7280"),
                text=[f"{r:.0f}%" for r in rates],
                textposition="outside",
            )
        )

    fig.update_layout(
        barmode="group",
        title=dict(text=f"<b>{t('val_hit_rate_title', lang)}</b>", font=dict(size=14, color="#1c1b1b")),
        height=360,
        margin=dict(t=50, b=40, l=50, r=20),
        paper_bgcolor=COLOR_BG,
        plot_bgcolor=COLOR_PLOT_BG,
        yaxis=dict(title="%", range=[0, 110], gridcolor="rgba(0,0,0,0.06)", zerolinecolor="rgba(0,0,0,0.08)"),
        xaxis=dict(gridcolor="rgba(0,0,0,0.04)"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=11)),
        font=dict(family=FONT_FAMILY, color="#1c1b1b"),
    )
    return fig


def validation_forward_scatter(
    forward_rows: list[dict],
    market: str,
    horizon: str = "3m",
    lang: str = DEFAULT_LANG,
) -> go.Figure:
    """Scatter plot: temperature vs forward return for one market+horizon."""
    xs = [r["temperature"] for r in forward_rows if r["market"] == market and r[f"return_{horizon}"] is not None]
    ys = [r[f"return_{horizon}"] for r in forward_rows if r["market"] == market and r[f"return_{horizon}"] is not None]

    if not xs:
        fig = go.Figure()
        fig.add_annotation(text=t("no_data_card", lang), x=0.5, y=0.5, showarrow=False, font=dict(size=14))
        fig.update_layout(height=300, paper_bgcolor=COLOR_BG, font=dict(family=FONT_FAMILY))
        return fig

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=xs,
            y=ys,
            mode="markers",
            marker=dict(size=6, color=[temp_color(x) for x in xs], opacity=0.65, line=dict(width=0.5, color="#374151")),
            hovertemplate=f"temp=%{{x:.1f}}°<br>{horizon} return=%{{y:.2%}}<extra></extra>",
        )
    )
    fig.add_hline(y=0, line=dict(color="rgba(0,0,0,0.15)", width=1))
    if len(xs) >= 30:
        df_smooth = pd.DataFrame({"x": xs, "y": ys}).sort_values("x")
        df_smooth["y_smooth"] = df_smooth["y"].rolling(50, min_periods=20, center=True).mean()
        fig.add_trace(
            go.Scatter(
                x=df_smooth["x"],
                y=df_smooth["y_smooth"],
                mode="lines",
                line=dict(color="#1c1b1b", width=2),
                name="trend",
                hovertemplate="temp=%{x:.1f}°<br>smoothed=%{y:.2%}<extra></extra>",
            )
        )
    fig.update_layout(
        title=dict(
            text=f"<b>{market.upper()} \u2014 {t('val_temp_vs_return', lang)} ({horizon})</b>",
            font=dict(size=13, color="#1c1b1b"),
        ),
        height=360,
        margin=dict(t=50, b=40, l=50, r=20),
        paper_bgcolor=COLOR_BG,
        plot_bgcolor=COLOR_PLOT_BG,
        xaxis=dict(
            title=t("overall", lang) + " (\u00b0)", range=[-5, 105], gridcolor="rgba(0,0,0,0.04)", zeroline=False
        ),
        yaxis=dict(tickformat=".0%", title=horizon, gridcolor="rgba(0,0,0,0.06)", zerolinecolor="rgba(0,0,0,0.08)"),
        showlegend=False,
        font=dict(family=FONT_FAMILY, color="#1c1b1b"),
    )
    return fig


def external_anchor_comparison(
    anchor_data: list[dict],
    lang: str = DEFAULT_LANG,
) -> go.Figure:
    """Scatter: multi-factor temperature vs external anchor (e.g. CNN F&G).

    anchor_data: list of {label, date, mf_temperature, cnn_fg, direction_aligned}
    """
    if not anchor_data:
        fig = go.Figure()
        fig.add_annotation(text=t("no_data_card", lang), x=0.5, y=0.5, showarrow=False, font=dict(size=14))
        fig.update_layout(height=300, paper_bgcolor=COLOR_BG, font=dict(family=FONT_FAMILY))
        return fig

    xs = [d["mf_temperature"] for d in anchor_data]
    ys = [d["cnn_fg"] for d in anchor_data]
    labels = [d["label"][:30] for d in anchor_data]
    aligned = [d.get("direction_aligned", True) for d in anchor_data]

    colors = ["#10B981" if a else "#F87171" for a in aligned]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=xs,
            y=ys,
            mode="markers+text",
            marker=dict(size=10, color=colors, line=dict(width=1, color="#374151")),
            text=labels,
            textposition="top center",
            textfont=dict(size=9),
            hovertemplate=f"{t('overall', lang)}: %{{x:.1f}}°<br>CNN F&G: %{{y:.1f}}°<br>%{{text}}<extra></extra>",
        )
    )

    # Diagonal (perfect agreement line)
    fig.add_trace(
        go.Scatter(
            x=[0, 100],
            y=[0, 100],
            mode="lines",
            line=dict(color="rgba(0,0,0,0.15)", dash="dash", width=1),
            showlegend=False,
            hoverinfo="skip",
        )
    )

    # Zone rectangles
    for x_lo, x_hi, color in [(0, 50, "rgba(30,58,138,0.06)"), (50, 100, "rgba(248,113,113,0.06)")]:
        fig.add_shape(type="rect", x0=x_lo, x1=x_hi, y0=0, y1=100, fillcolor=color, line_width=0, layer="below")

    fig.update_layout(
        title=dict(text=f"<b>{t('val_anchor_title', lang)}</b>", font=dict(size=13, color="#1c1b1b")),
        xaxis=dict(title=f"FinSynapse {t('overall', lang)} (°)", range=[-5, 105], gridcolor="rgba(0,0,0,0.04)"),
        yaxis=dict(title="CNN Fear & Greed Index", range=[-5, 105], gridcolor="rgba(0,0,0,0.06)"),
        height=400,
        margin=dict(t=50, b=50, l=60, r=20),
        paper_bgcolor=COLOR_BG,
        plot_bgcolor=COLOR_PLOT_BG,
        font=dict(family=FONT_FAMILY, color="#1c1b1b"),
    )
    return fig


def validation_zone_heatmap(zone_distribution: dict, lang: str = DEFAULT_LANG) -> go.Figure:
    """Heatmap of mean returns by temperature zone × time horizon."""
    zones = list(zone_distribution.keys())
    horizons = ["1m", "3m", "6m", "12m"]
    z_matrix: list[list[float | None]] = []
    z_text: list[list[str]] = []

    for zone in zones:
        z_data = zone_distribution.get(zone, [])
        row: list[float | None] = []
        text_row: list[str] = []
        for h in horizons:
            entry = next((e for e in z_data if e.get("horizon") == h), {})
            mr = entry.get("mean_return")
            row.append(mr)
            text_row.append(f"{mr:.2%}" if mr is not None else "N/A")
        z_matrix.append(row)
        z_text.append(text_row)

    fig = go.Figure(
        go.Heatmap(
            z=z_matrix,
            x=horizons,
            y=zones,
            text=z_text,
            texttemplate="%{text}",
            textfont=dict(size=12),
            colorscale=[
                [0.0, "#1E3A8A"],
                [0.3, "#93C5FD"],
                [0.5, "#F8FAFC"],
                [0.7, "#FCA5A5"],
                [1.0, "#DC2626"],
            ],
            zmid=0,
            showscale=True,
            colorbar=dict(title="mean return", tickformat=".0%"),
        )
    )

    fig.update_layout(
        title=dict(text=f"<b>{t('val_zone_heatmap_title', lang)}</b>", font=dict(size=13, color="#1c1b1b")),
        height=320,
        margin=dict(t=50, b=30, l=120, r=40),
        paper_bgcolor=COLOR_BG,
        plot_bgcolor=COLOR_PLOT_BG,
        font=dict(family=FONT_FAMILY, color="#1c1b1b"),
    )
    return fig


def validation_pivot_table(pivot_results: list[dict], lang: str = DEFAULT_LANG) -> go.Figure:
    """Table of pivot results with color-coded pass/fail cells.

    Returns a Plotly table figure. `pivot_results` is a list of dicts with:
    label, market, date, expected_zone, controllers: [{name, overall, ...}].
    """
    header = [
        f"<b>{t('th_date', lang)}</b>",
        f"<b>{t('select_market', lang).upper()}</b>",
        "<b>Pivot</b>",
        "<b>Expected</b>",
        "<b>Multi-factor</b>",
        "<b>PE SF</b>",
        "<b>VIX SP</b>",
        "<b>60d Mom</b>",
    ]
    cells: list[list] = [[] for _ in header]

    for pr in pivot_results:
        cells[0].append(pr["date"])
        cells[1].append(pr["market"].upper())
        cells[2].append(pr["label"])
        cells[3].append(pr["expected_zone"])
        ctrl_map = {c["name"]: c for c in pr.get("controllers", [])}
        for idx, c_name in enumerate(["multi-factor", "PE single-factor", "VIX single-point", "60d momentum"]):
            c = ctrl_map.get(c_name)
            if c:
                status = "✓" if c["directional_pass"] else "✗"
                cells[4 + idx].append(f"{c['overall']:.0f}° {status}")
            else:
                cells[4 + idx].append("—")

    fill_colors: list[list[str]] = []
    for _col_idx, col_cells in enumerate(cells):
        col_fills = []
        for val in col_cells:
            if "✗" in str(val):
                col_fills.append("rgba(248,113,113,0.15)")
            elif "✓" in str(val):
                col_fills.append("rgba(16,185,129,0.15)")
            else:
                col_fills.append("rgba(255,255,255,0.5)")
        fill_colors.append(col_fills)

    fig = go.Figure(
        go.Table(
            header=dict(
                values=header,
                fill_color="rgba(0,0,0,0.04)",
                font=dict(size=11, color="#1c1b1b"),
                align="left",
                height=32,
            ),
            cells=dict(
                values=cells,
                fill_color=fill_colors,
                font=dict(size=11, color="#1c1b1b"),
                align="left",
                height=28,
            ),
        )
    )

    fig.update_layout(
        title=dict(text=f"<b>{t('val_pivot_table_title', lang)}</b>", font=dict(size=13, color="#1c1b1b")),
        height=max(240, 30 * len(pivot_results) + 60),
        margin=dict(t=50, b=20, l=10, r=10),
        paper_bgcolor=COLOR_BG,
        font=dict(family=FONT_FAMILY),
    )
    return fig
