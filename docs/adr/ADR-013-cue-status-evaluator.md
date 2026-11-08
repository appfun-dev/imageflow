# ADR-013：CUE Status Evaluator 实现（v1.3）

- 状态：Accepted
- 日期：2026-11-09
- 决策人：项目负责人（v1.3 grill Wave A Q1-Q3、Wave B Q4-Q6、Wave C Q7-Q9）
- 关联：ADR-005 §4（合约冻结）；`docs/spec/imageflow-v1.3-cue-status-evaluator.md`

## 背景

ADR-005 §4 冻结了 CUE Status Evaluator 的存储（ConfigMap + statusPolicyRef）、I/O（输入/输出契约）和安全失败语义，但将实现推迟到 v1.1。v1.1 聚焦 BuildRun Watcher，v1.2 聚焦 imageReferenceFormat，v1.3 兑现 CUE evaluator 承诺。

## 决策

1. **运行时 = CUE Go API**：直接 import `cuelang.org/go/cue`，零额外依赖，编译期类型检查。CUE 版本锁定到 controller 二进制。WASM/subprocess 隔离在出现具体安全问题前是 YAGNI。
2. **评估超时 = 100ms**：单次评估最大执行时间。CUE 是声明式配置语言，不应有复杂计算；100ms 足够覆盖典型 rollout/health 判定。超限 → Timeout 错误。
3. **Rollout/Health = 两个独立策略**：`statusPolicyRef.rolloutKey` + `statusPolicyRef.healthKey`。职责单一，可独立演进。符合 ADR-005 §8.2 "rollout 与 health 使用两个独立结果"。
4. **输入 Schema = CUE 内联**：策略文件顶层定义输入类型约束，零额外文件。CUE 本身就是 schema 语言。评估前用 `cue.Value.Validate()` 校验。
5. **变更检测 = Watch ConfigMap + 缓存失效**：Watch 机制保证策略变更及时生效，编译缓存避免重复编译。按 ConfigMap resourceVersion + key hash 缓存。
6. **错误分类 = CompileError/EvalError/Timeout**：三类足够区分"策略写错"vs"输入不匹配"vs"超时"，便于运维排查。所有错误 → Unknown + 明确 Condition Reason。
7. **测试机制 = ConfigMap 带 test fixtures**：同一 ConfigMap 的 data 中可放 `test-cases.yaml` key，evaluator 提供 test mode。符合 ADR-005 §8.3 "同一策略版本建立可重复测试"。
8. **输入大小限制 = 256KB**：序列化后的输入 JSON 最大 256KB。target.status 可能很大，256KB 足够覆盖典型 workload；超限 → EvalError。
9. **RBAC = 仅 imageflow-system namespace**：Status Policy ConfigMap 仅允许在 `imageflow-system` namespace 创建。策略属平台配置，集中管理降低信任面。符合 ADR-005 §8.3 来源信任要求。

## 备选方案

- **CUE WASM** —— 被否：增加复杂度，性能开销，YAGNI。
- **外部进程 (subprocess)** —— 被否：IPC 开销大，部署复杂，YAGNI。
- **500ms/1s 超时** —— 被否：可能导致 reconcile 堆积，100ms 足够。
- **单策略复合输出** —— 被否：违反 ADR-005 §8.2 独立结果要求。
- **独立 JSON Schema 文件** —— 被否：CUE 本身就是 schema 语言，零额外文件更简。
- **每次 reconcile 重新读取 ConfigMap** —— 被否：性能差，Watch + 缓存更优。
- **统一 EvaluationFailed 错误** —— 被否：无法区分策略错误 vs 输入错误 vs 超时。
- **任意 namespace 策略引用** —— 被否：安全边界要求集中管理。

## 影响

- `StatusPolicyRef` 新增 `rolloutKey` / `healthKey` 字段（向后兼容，v1/v1.2 对象无此字段时保持 Unknown）；
- 新增 CUE 依赖 (`cuelang.org/go`)；
- 新增 ConfigMap Watch informer；
- Admission webhook 扩展校验 `statusPolicyRef.namespace == "imageflow-system"`；
- 新增 CUE evaluator 包 (`internal/controller/cueeval/`)；
- 新增 Condition Reasons: `CompileError`, `EvalError`, `Timeout`, `InputTooLarge`。

## 未解决问题

（无）

## 修订日志

| 日期 | 变更 | 理由 |
|------|------|------|
| 2026-11-09 | 初始版本，Accepted | v1.3 grill Wave A/B/C 决策 |
