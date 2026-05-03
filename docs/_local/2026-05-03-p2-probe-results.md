# P2 探针结果汇总

日期：2026-05-03
读者：未来做 Phase 3/4/5 权重改进的 agent。

## HK 备选估值（probe_hk_alt_valuation.py）

| 候选 | 结果 | 备注 |
|---|---|---|
| yfinance ^HSI dividend yield | 待运行 | 需网络，CI 环境不可用 |
| HSCEI ETF (2828.HK) yield | 待运行 | 同上 |
| HSTECH ETF (3033.HK) yield | 待运行 | 同上 |
| AkShare AH 溢价 | 待运行 | 同上 |

**推荐**：待 probe 实际运行后填写

## HKMA Aggregate Balance（probe_hkma_balance.py）

- AkShare：待运行
- HKMA Public API：待运行

**推荐**：待填写

## CN PMI（probe_cn_pmi.py）

| 函数 | 结果 |
|---|---|
| macro_china_pmi | 待运行 |
| macro_china_non_man_pmi | 待运行 |
| macro_china_caixin_pmi | 待运行 |

**推荐**：待填写

## US T10Y3M（probe_us_yield_curve.py）

- 可获取性：待运行（需 FRED_API_KEY + 网络）
- 工程难点：需要非单调 stress transform；T10Y3M 不在 FinSynapse 的 FredProvider SERIES 列表里，需走独立 FRED API 调用或新增 series

## 下一步

把以上发现作为输入，写一份 Phase 3/4/5 的具体实施计划。**不要直接改 weights.yaml**。
