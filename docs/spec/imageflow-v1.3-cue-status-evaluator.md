# Spec: ImageFlow v1.3 — CUE Status Evaluator

> 来源：v1.3 grilling Wave A（Q1-Q3 运行时与接口）+ Wave B（Q4-Q6 schema 与错误）+ Wave C（Q7-Q9 测试与安全）。决策源 = ADR-005 §4（合约冻结）+ ADR-013（Accepted 2026-11-09）。本 spec 与 ADR 冲突时以 ADR 为准；改决策必须先改 ADR。

## Problem Statement

ImageFlow v1/v1.1/v1.2 对第三方 Workload 的 rollout/health 状态如实报告 `Unknown`（ADR-005 §4），因为 CUE Status Evaluator 被推迟。这导致使用 `statusPolicyRef` 的订阅无法获得有意义的状态反馈，运维人员必须手动检查目标 workload 的原生 status。

ADR-005 §4 冻结了 CUE evaluator 的存储（ConfigMap + statusPolicyRef）、I/O（输入/输出契约）和安全失败语义，v1.3 兑现此承诺，实现完整的 CUE 状态评估能力。

## Solution

ImageFlow v1.3 实现 CUE Status Evaluator，按 ADR-005 §4 合约执行外部配置的 CUE 状态策略：

- **运行时**：CUE Go API (`cuelang.org/go/cue`)，直接 import，零额外依赖；
- **超时**：单次评估最大 100ms，超限 → Timeout 错误；
- **接口**：rollout 和 health 是两个独立 CUE 策略，通过 `statusPolicyRef.rolloutKey` 和 `statusPolicyRef.healthKey` 引用；
- **Schema**：CUE 内联 schema，策略文件自带输入约束；
- **变更检测**：Watch ConfigMap + 编译缓存，策略更新自动失效缓存；
- **错误分类**：CompileError（策略语法错误）/ EvalError（输入不匹配或运行时错误）/ Timeout；
- **测试**：同一 ConfigMap 的 data 中可放 `test-cases.yaml` key，evaluator 提供 test mode；
- **输入限制**：单次评估最大 256KB，超限 → EvalError；
- **RBAC**：Status Policy ConfigMap 仅允许在 `imageflow-system` namespace 创建。

核心约束（来自 ADR-005 §4）：
- ConfigMap 缺失、key 缺失、编译失败、输入缺字段、评估超时一律不得视为成功 → `Unknown` 或明确 `EvaluationFailed` Condition；
- 限制输入大小、执行时间与能力面（无网络/文件）；
- 策略属可执行判定配置 → RBAC + 变更审计 + 来源信任；
- 同一策略版本建立可重复测试。

## User Stories

1. 作为平台工程师，我想为第三方 Workload 配置 CUE 状态策略，以便 ImageFlow 能自动评估其 rollout/health 状态，而不是报告 Unknown。
2. 作为平台工程师，我想在策略文件中定义输入 schema，以便策略使用者清楚需要提供哪些字段，错误信息明确指出缺失字段。
3. 作为值班工程师，我想在策略评估失败时看到明确的错误类型（CompileError/EvalError/Timeout），以便快速定位是策略写错、输入不匹配还是性能问题。
4. 作为平台工程师，我想更新 ConfigMap 后策略自动生效，无需重启 controller。
5. 作为安全审计员，我想确认只有 imageflow-system namespace 的 ConfigMap 可被用作状态策略，防止恶意策略注入。
6. 作为平台工程师，我想在 ConfigMap 中包含测试用例，以便在部署前验证策略逻辑正确。
7. 作为 ImageFlow 维护者，我想确认 CUE 评估器不会访问网络或文件系统，保证沙箱安全。

## Implementation Decisions

**决策溯源**：全部架构决策已冻结于 ADR-005 §4 + ADR-013；本节是实现约束汇编。

1. **API 变更 — StatusPolicyRef 扩展**：
   - 新增 `rolloutKey` 字段（string, required when statusPolicyRef is set）；
   - 新增 `healthKey` 字段（string, required when statusPolicyRef is set）；
   - 位置：`StatusPolicyRef.RolloutKey` / `StatusPolicyRef.HealthKey`，与现有 `Name`/`Namespace`/`Key` 同级；
   - 向后兼容：v1/v1.2 对象无此字段时，evaluator 不启用，保持 Unknown 行为。

2. **CUE 运行时**：
   - 使用 `cuelang.org/go/cue` 官方 Go API；
   - 编译结果缓存（按 ConfigMap resourceVersion + key hash）；
   - Watch ConfigMap 变更，失效对应缓存条目。

3. **评估超时**：
   - 单次评估最大 100ms（context.WithTimeout）；
   - 超时 → 返回 `Unknown` + Condition `StatusPolicyEvaluated=False, Reason=Timeout`。

4. **输入 Schema**：
   - CUE 策略文件顶层定义输入类型约束；
   - 评估前用 `cue.Value.Validate()` 校验输入符合 schema；
   - 校验失败 → EvalError + 详细字段路径。

5. **错误分类与 Condition**：
   - `CompileError`：CUE 语法错误或类型错误 → Condition `Reason=CompileError`；
   - `EvalError`：输入不匹配、字段缺失、运行时 panic → Condition `Reason=EvalError`；
   - `Timeout`：超过 100ms → Condition `Reason=Timeout`；
   - 所有错误 → `rolloutStatus=Unknown` 或 `healthStatus=Unknown`，绝不默认成功。

6. **测试机制**：
   - ConfigMap data 中可选 `test-cases.yaml` key；
   - 格式：YAML 数组，每项 `{input: {...}, expected: {status: ..., reason: ...}}`；
   - evaluator 提供 `RunTests(configMap) ([]TestResult, error)` 方法；
   - 测试结果可通过 annotation 或 Event 暴露（待定）。

7. **输入大小限制**：
   - 序列化后的输入 JSON 最大 256KB；
   - 超限 → EvalError + Condition `Reason=InputTooLarge`。

8. **RBAC 与信任**：
   - Admission webhook 校验 `statusPolicyRef.namespace == "imageflow-system"`；
   - 非 imageflow-system namespace 的引用 → admission 拒绝；
   - ConfigMap 变更审计通过 Kubernetes Audit Log。

9. **沙箱安全**：
   - CUE 评估禁用 `import`（除标准库）；
   - 无网络访问、无文件系统访问；
   - 评估在独立 goroutine 中运行，panic 不影响 controller。

## Out of Scope

- CUE WASM 或外部进程隔离（YAGNI，Go API 足够）；
- 策略热更新期间的 in-flight 评估中断（当前 reconcile 完成后再加载新策略）；
- 跨 namespace 策略引用（安全边界要求集中管理）；
- 策略版本管理（ConfigMap resourceVersion 即为版本标识）；
- 策略 marketplace 或共享库（后续迭代）。

## Testing Decisions

1. **好测试的标准**：只断言外部可见行为（Condition 类型/原因、status 值、Events）；不断言内部缓存命中或编译细节。
2. **缝 S1 = Kubernetes API 对象面**：envtest 覆盖 admission webhook 校验 + status update；kind e2e 覆盖真实 ConfigMap watch。
3. **缝 S2 = CUE 评估器**：单元测试覆盖三类错误、超时、输入大小限制；集成测试覆盖完整 evaluate 流程。
4. **验收映射**：§User Stories 7 条各至少对应一个命名用例；Story 7（沙箱安全）通过代码审查 + 负面测试验证。

## Open Questions

（无）

## Further Notes

- **钉版**：Kubernetes 1.37.x；operator-sdk v1.42.3；CUE v0.12.x；
- **向后兼容**：v1.3 不改变 v1/v1.1/v1.2 行为；无 statusPolicyRef 的订阅保持原有 native observation；
- **仓库痕迹**：ADR-005 §4（合约冻结）、ADR-013（Accepted）已入库；本 spec 引用路径均可解析。
