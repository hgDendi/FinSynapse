# 验证审计文件入库策略

日期：2026-05-03

## 决策

以下文件**应当入库**（git-tracked）：

- `scripts/champion_baseline.json` — 每次 PASS 的 validation run 都会追加，按 (date, algo_version) 去重
- `scripts/grid_search_results.json` — 每次 grid search 追加，包含 run_hash

以下文件**保持 ignored**：

- `scripts/validation_report.json` — 每次 run 完整覆盖，体积大、纯派生
- `scripts/oos_results.json` — 同上

## 理由

- champion + grid 是"在某个 algo_version 下最佳已知性能"的 audit trail
- 任何调整 `config/weights.yaml` 的 commit，message 必须引用 grid_search_results.json 的 run_hash 或 champion_baseline.json 的 (date, algo_version)
- 避免后置合理化：先有证据，再调权重
