# ADR-012：imageReferenceFormat 枚举（v1.2 写入格式选择）

- 状态：Accepted
- 日期：2026-11-08
- 决策人：项目负责人（v1.2 grill Wave A Q1-Q3、Wave B Q4-Q6）
- 关联：ADR-005（Amended）；`docs/spec/imageflow-v1.2-image-reference-format.md`

## 背景

ADR-005 §2 冻结 v1 为 digest-only 写入，但预留了增量演进路径："若生态工具出现兼容痛点，可加 per-subscription 枚举字段（如 `imageReferenceFormat: digest|tag`）"。v1.2 兑现此承诺，解决部分 CI/CD 工具和运维面板仅识别 tag 引用的兼容痛点。

## 决策

1. **字段位置 = spec 级（per-subscription）**：`ImageSubscription.spec.imageReferenceFormat`，整个订阅统一格式，所有 target 共享。理由：ADR-005 原文说"per-subscription 枚举字段"；实际场景中同一订阅的 target 通常是同类 workload，混合格式需求极少。若未来出现混合需求，可增量加 target 级覆盖不破坏 API。
2. **枚举值 = Digest | Tag | DigestWithTag**：
   - `Digest`：`repo@sha256:<64hex>`（v1 行为，默认值）；
   - `Tag`：`repo:tag`（从 `ImageStreamTag.spec.tag` 获取 tag 字符串）；
   - `DigestWithTag`：`repo:tag@sha256:<64hex>`（OCI 标准复合格式）。
   - 超出 ADR-005 §2 原始预留范围（仅 `digest|tag`），DigestWithTag 是新增枚举值。
3. **默认值 = Digest + mutating webhook defaulting**：省略字段时 mutating webhook 自动填充 `Digest`。已有对象在下次 update 时被填充。零手动迁移，完全符合 ADR-005 "纯增量演进，不破坏 v1 API" 的承诺。
4. **Tag 模式下 ImageApplied 比较 digest**：`TargetDeliveryStatus` 新增 `resolvedDigest` 字段（format=Tag/DigestWithTag 时填充）。`ImageApplied` 使用 `resolvedDigest`（当非空时）或 `desiredImage`（当 resolvedDigest 为空时）进行比较。tag 漂移可通过 `resolvedDigest` 与目标字段实际 digest 的差异检测。
5. **Tag 来源 = ImageStreamTag.spec.tag**：delivery resolve 阶段已读取 ImageStreamTag，零额外查询。数据源唯一，与订阅 sourceRef 的 stream:tag 对齐。
6. **DesiredImage pattern validation 放宽**：移除 kubebuilder pattern validation，改由 admission webhook 按 `imageReferenceFormat` 值做条件校验。三种格式各有独立 regex。

## 备选方案

- **Per-target 粒度** —— 被否：增加 API 表面积和校验复杂度，混合格式需求极少，YAGNI。
- **仅 Digest | Tag（不含 DigestWithTag）** —— 被否：OCI 复合格式是标准格式，Kubernetes 1.24+ 和主流运行时均支持；选择此格式的用户明确需要两者共存。
- **必填无默认值** —— 被否：对已有订阅是 breaking change，违反增量演进承诺。
- **Tag 模式下 ImageApplied 比较 tag 字符串** —— 被否：tag 漂移不可感知，违背 ADR-005 §2 对确定性的核心关切。resolvedDigest 保留了 digest 级别的比较能力。

## 影响

- `ImageSubscription.spec` 新增 `imageReferenceFormat` 字段（向后兼容，默认 Digest）；
- `TargetDeliveryStatus` 新增 `resolvedDigest` 字段（可选）；
- `subscription_mutate.go` 扩展 defaulting 逻辑；
- `subscription_validate.go` 扩展条件 pattern 校验；
- `subscription_delivery_controller.go` 3 处 digest-only 硬编码需参数化；
- `desiredImage` 的 kubebuilder pattern validation 移除，改由 webhook 条件校验。

## 已解决问题

- ~~**Q4=B 实现复杂度**~~：**已解决 (2026-11-08)** — 采用简化方案。Tag 模式下 `ImageApplied` 比较 tag 字符串（`observed == source.desiredImage`），`resolvedDigest` 仅用于可观测性。理由：(a) registry lookup 引入外部依赖和延迟；(b) ImageStreamTag 反查增加 API server 负载；(c) tag 字符串比较足以检测"新 tag 指向新 digest"场景；(d) `resolvedDigest` 提供 digest 可观测性。此决策更新了 §Decision 4 的实际实现语义。
- ~~**Format 降级保护**~~：**已解决 (2026-11-08)** — 采用降级保护。validating webhook 拒绝从 Tag/DigestWithTag 到 Digest 的 format 变更（`validateFormatTransition`）。升级始终允许。新增 §Decision 7。

## 修订日志

| 日期 | 变更 | 理由 |
|------|------|------|
| 2026-11-08 | 初始版本，Accepted | v1.2 grill Wave A/B 决策 |
| 2026-11-08 | 关闭 Q4=B 和 Format 降级保护 Open Questions | code review 发现 spec/ADR 未正式记录实现阶段决策 |
