"""Curated catalog of major market events for the glossary explainer page.

Each entry maps a (market, date-window) to a human-readable label. When the
glossary renders the historical hot/cold extremes per market, it tries to
attach a label by checking whether the extreme date falls inside any of these
windows; otherwise the row shows the date alone.

Hand-maintained — keep entries to widely-recognised regime markers (peaks,
crises, policy shocks). Avoid editorial commentary.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class EventEntry:
    market: str  # "cn" | "hk" | "us"
    start: date  # inclusive
    end: date  # inclusive
    label_zh: str
    label_en: str


# Ordered roughly chronologically. Windows are intentionally wide enough
# (~30-90 days) so a temperature extreme that lands a few days off the
# textbook headline date still matches.
EVENTS: tuple[EventEntry, ...] = (
    # ------------------------ US ------------------------
    EventEntry(
        "us",
        date(2011, 7, 1),
        date(2011, 10, 31),
        "2011 美债评级下调 + 欧债危机",
        "2011 US debt downgrade + Eurozone crisis",
    ),
    EventEntry(
        "us",
        date(2015, 8, 1),
        date(2016, 2, 29),
        "2015–16 人民币贬值 + 中国增长担忧",
        "2015–16 RMB devaluation + China growth scare",
    ),
    EventEntry(
        "us",
        date(2018, 12, 1),
        date(2019, 1, 15),
        "2018 圣诞抛售（加息/贸易战）",
        "2018 Christmas selloff (rate hikes + trade war)",
    ),
    EventEntry("us", date(2020, 2, 20), date(2020, 4, 15), "2020 COVID 崩盘", "2020 COVID crash"),
    EventEntry(
        "us",
        date(2020, 11, 1),
        date(2021, 3, 31),
        "2020–21 财政刺激与疫苗反弹高峰",
        "2020–21 stimulus + vaccine rally peak",
    ),
    EventEntry(
        "us", date(2021, 8, 1), date(2021, 12, 31), "2021 ARK / 成长股泡沫顶部", "2021 ARK / growth bubble peak"
    ),
    EventEntry(
        "us", date(2022, 1, 1), date(2022, 12, 31), "2022 美联储激进加息周期", "2022 Fed aggressive hiking cycle"
    ),
    EventEntry(
        "us",
        date(2022, 9, 15),
        date(2022, 11, 15),
        "2022 加息底部 / Fed Pivot 前夜",
        "2022 hiking-cycle trough / pre-Fed-pivot low",
    ),
    EventEntry("us", date(2023, 3, 1), date(2023, 5, 31), "2023 SVB 银行危机", "2023 SVB regional banking crisis"),
    EventEntry(
        "us", date(2024, 6, 1), date(2025, 12, 31), "2024–25 AI 主题驱动估值新高", "2024–25 AI-led valuation new highs"
    ),
    # ------------------------ CN ------------------------
    EventEntry("cn", date(2015, 5, 1), date(2015, 7, 15), "2015 杠杆牛顶 + 股灾", "2015 leveraged-bull top + crash"),
    EventEntry(
        "cn", date(2016, 1, 1), date(2016, 3, 31), "2016 熔断 + 人民币贬值", "2016 circuit-breaker + RMB devaluation"
    ),
    EventEntry(
        "cn", date(2017, 4, 1), date(2017, 6, 30), "2017 金融去杠杆 / 股债双杀", "2017 financial deleveraging shock"
    ),
    EventEntry("cn", date(2018, 6, 1), date(2019, 1, 31), "2018 中美贸易战", "2018 US-China trade war"),
    EventEntry("cn", date(2020, 2, 1), date(2020, 4, 15), "2020 COVID 冲击", "2020 COVID shock"),
    EventEntry(
        "cn",
        date(2020, 12, 1),
        date(2021, 2, 28),
        "2021 核心资产抱团高峰（白酒/医药）",
        "2021 core-asset huddle peak (baijiu / pharma)",
    ),
    EventEntry(
        "cn",
        date(2022, 3, 1),
        date(2022, 5, 31),
        "2022 上海封城 / 互联网监管",
        "2022 Shanghai lockdown + tech crackdown",
    ),
    EventEntry(
        "cn",
        date(2022, 10, 1),
        date(2022, 11, 30),
        "2022 二十大前/防疫政策低点",
        "2022 pre-20th-Congress / zero-COVID trough",
    ),
    EventEntry(
        "cn", date(2024, 9, 15), date(2024, 11, 15), "2024 9·24 政策转向反弹", "2024 Sept-24 policy-pivot rally"
    ),
    # ------------------------ HK ------------------------
    EventEntry("hk", date(2015, 4, 1), date(2015, 7, 31), "2015 沪港通牛市 + 股灾", "2015 connect-driven bull + crash"),
    EventEntry(
        "hk", date(2018, 6, 1), date(2019, 1, 31), "2018 贸易战 + 港币挤兑保卫战", "2018 trade war + HKD peg defence"
    ),
    EventEntry("hk", date(2019, 6, 1), date(2019, 12, 31), "2019 香港社会运动", "2019 HK social unrest"),
    EventEntry("hk", date(2020, 2, 1), date(2020, 4, 15), "2020 COVID 冲击", "2020 COVID shock"),
    EventEntry(
        "hk",
        date(2020, 12, 1),
        date(2021, 2, 28),
        "2021 南向资金井喷 / 科技股狂热",
        "2021 southbound surge + tech mania peak",
    ),
    EventEntry(
        "hk",
        date(2021, 3, 1),
        date(2022, 12, 31),
        "2021–22 中概互联网监管 + 教培清零",
        "2021–22 China internet regulation + edu wipeout",
    ),
    EventEntry(
        "hk",
        date(2022, 10, 1),
        date(2022, 11, 30),
        "2022 恒指 25 年新低（地产 + 流动性）",
        "2022 HSI 25-year low (property + liquidity)",
    ),
    EventEntry(
        "hk", date(2024, 4, 1), date(2024, 6, 30), "2024 红利 + 国央企重估反弹", "2024 dividend / SOE re-rating rally"
    ),
)


def find_event(market: str, when: date) -> EventEntry | None:
    """Return the first event window containing `when`, or None."""
    for ev in EVENTS:
        if ev.market != market:
            continue
        if ev.start <= when <= ev.end:
            return ev
    return None


def event_label(market: str, when: date, lang: str = "zh") -> str:
    ev = find_event(market, when)
    if ev is None:
        return ""
    return ev.label_zh if lang == "zh" else ev.label_en
