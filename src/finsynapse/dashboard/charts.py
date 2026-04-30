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


def time_series(temp_df: pd.DataFrame, market: str, lang: str = DEFAULT_LANG) -> go.Figure:
    """Long-history overall + sub-temperature time series for one market."""
    df = temp_df[temp_df["market"] == market].copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # Subplot titles intentionally omitted — the section heading already says
    # "长期温度走势 (CN)", and the legend identifies the lines, so the inline
    # "CN — 综合温度 / 分量温度" labels would be redundant chart-junk.
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        row_heights=[0.65, 0.35],
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

    fig.update_yaxes(
        range=[0, 100],
        title_text="°",
        row=1,
        col=1,
        gridcolor="rgba(0,0,0,0.06)",
        zerolinecolor="rgba(0,0,0,0.08)",
    )
    fig.update_yaxes(
        range=[0, 100],
        title_text="°",
        row=2,
        col=1,
        gridcolor="rgba(0,0,0,0.06)",
        zerolinecolor="rgba(0,0,0,0.08)",
    )
    fig.update_xaxes(gridcolor="rgba(0,0,0,0.04)", linecolor="rgba(0,0,0,0.08)")
    fig.update_layout(
        height=460,
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
