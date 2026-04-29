from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from finsynapse.dashboard.i18n import DEFAULT_LANG, t, translate_div

# Shared color scheme. Cold→Hot maps to blue→grey→red. Keep these consistent
# across every chart so the user builds a fixed mental model: blue = cheap/cold.
COLOR_COLD = "#2E86AB"
COLOR_MID = "#9CA3AF"
COLOR_HOT = "#E63946"
COLOR_BG = "#FAFAFA"

ZONE_BANDS = [
    (0, 30, "rgba(46,134,171,0.15)"),
    (30, 70, "rgba(156,163,175,0.10)"),
    (70, 100, "rgba(230,57,70,0.15)"),
]

# Use a CJK-capable font stack first so Chinese labels don't fall back to
# system default that may render as boxes inside Plotly charts.
FONT_FAMILY = (
    '-apple-system, BlinkMacSystemFont, "PingFang SC", "Hiragino Sans GB", '
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
    """Single-market thermometer gauge. Renders 0-100° with zone bands."""
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
                "bgcolor": "white",
                "borderwidth": 1,
                "bordercolor": "#E5E7EB",
                "steps": [
                    {"range": [0, 30], "color": "rgba(46,134,171,0.20)"},
                    {"range": [30, 70], "color": "rgba(156,163,175,0.12)"},
                    {"range": [70, 100], "color": "rgba(230,57,70,0.20)"},
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
    fig.add_trace(go.Scatterpolar(r=[30] * 4, theta=cats + [cats[0]], mode="lines",
                                   line=dict(color="rgba(46,134,171,0.4)", dash="dot"), showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatterpolar(r=[70] * 4, theta=cats + [cats[0]], mode="lines",
                                   line=dict(color="rgba(230,57,70,0.4)", dash="dot"), showlegend=False, hoverinfo="skip"))
    fig.add_trace(
        go.Scatterpolar(
            r=vals + [vals[0]],
            theta=cats + [cats[0]],
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


def time_series(temp_df: pd.DataFrame, market: str, lang: str = DEFAULT_LANG) -> go.Figure:
    """Long-history overall + sub-temperature time series for one market."""
    df = temp_df[temp_df["market"] == market].copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.06,
        row_heights=[0.65, 0.35],
        subplot_titles=(
            t("chart_overall_temp", lang).format(market=market.upper()),
            t("chart_sub_temps", lang),
        ),
    )

    for lo, hi, color in ZONE_BANDS:
        fig.add_hrect(y0=lo, y1=hi, fillcolor=color, line_width=0, row=1, col=1)

    fig.add_trace(
        go.Scatter(x=df["date"], y=df["overall"], name=t("overall", lang),
                   line=dict(color="#111827", width=1.6),
                   hovertemplate=f"%{{x|%Y-%m-%d}}<br>{t('overall', lang)}: %{{y:.1f}}°<extra></extra>"),
        row=1, col=1,
    )

    for sub_key, col in [("valuation", "#7C3AED"), ("sentiment", "#F59E0B"), ("liquidity", "#10B981")]:
        if sub_key in df.columns:
            label = t(sub_key, lang)
            fig.add_trace(
                go.Scatter(x=df["date"], y=df[sub_key], name=label,
                           line=dict(color=col, width=1, dash="solid"),
                           opacity=0.85,
                           hovertemplate=f"%{{x|%Y-%m-%d}}<br>{label}: %{{y:.1f}}°<extra></extra>"),
                row=2, col=1,
            )

    fig.update_yaxes(range=[0, 100], title_text="°", row=1, col=1)
    fig.update_yaxes(range=[0, 100], title_text="°", row=2, col=1)
    fig.update_layout(
        height=480,
        margin=dict(t=50, b=30, l=50, r=20),
        paper_bgcolor=COLOR_BG,
        plot_bgcolor="white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
        font=dict(family=FONT_FAMILY),
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
            marker=dict(color=["#7C3AED", "#F59E0B", "#10B981"]),
            text=[f"{v:+.1f}°" for v in contribs.values()],
            textposition="outside",
        )
    )
    fig.add_vline(x=0, line=dict(color="#374151", width=1))
    fig.update_layout(
        title=dict(text=f"<b>{t('chart_1w_contribution', lang)}</b> — {t('chart_overall_change', lang)} {overall_change:+.1f}°",
                   font=dict(size=13)),
        height=200,
        margin=dict(t=40, b=20, l=80, r=40),
        paper_bgcolor=COLOR_BG,
        plot_bgcolor="white",
        xaxis=dict(title="°", zeroline=True),
        showlegend=False,
        font=dict(family=FONT_FAMILY),
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
        "sp500_vix": "#E63946", "us10y_dxy": "#F59E0B", "gold_real_rate": "#FBBF24",
        "sp500_us10y": "#7C3AED", "hsi_dxy": "#10B981",
    }
    for pair, group in df.groupby("pair_name"):
        fig.add_trace(
            go.Scatter(
                x=group["date"], y=group["strength"], mode="markers", name=pair,
                marker=dict(size=8, color=palette.get(pair, "#6B7280"), line=dict(width=0.5, color="#374151")),
                hovertemplate=(
                    "%{x|%Y-%m-%d}<br>" + pair + "<br>strength=%{y:.4f}<br>"
                    "%{customdata}<extra></extra>"
                ),
                customdata=group["description_localized"],
            )
        )

    fig.update_layout(
        title=dict(text=f"<b>{t('chart_recent_div', lang)}</b>", font=dict(size=14)),
        height=260,
        margin=dict(t=40, b=30, l=50, r=20),
        paper_bgcolor=COLOR_BG,
        plot_bgcolor="white",
        xaxis=dict(title=None),
        yaxis=dict(title=t("th_strength", lang)),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        font=dict(family=FONT_FAMILY),
    )
    return fig
