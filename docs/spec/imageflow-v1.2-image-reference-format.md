# Spec: ImageFlow v1.2 — imageReferenceFormat

> 来源：v1.2 grilling Wave A（Q1-Q3 范围与粒度）+ Wave B（Q4-Q6 写入语义）。决策源 = ADR-005（Amended 2026-11-08）+ ADR-012（Accepted 2026-11-08）。本 spec 与 ADR 冲突时以 ADR 为准；改决策必须先改 ADR。

## Problem Statement

ImageFlow v1/v1.1 的订阅投递路径硬编码只写 digest 引用 `<repo>@sha256:<64hex>`（ADR-005 §2）。这保证了 `observedImage == desiredImage` 的判定不变式，但对部分生态工具造成兼容痛点：某些 CI/CD 系统、镜像扫描器或运维面板仅识别 tag 引用（`repo:tag`），无法处理纯 digest 格式。

ADR-005 §2 预留了增量演进路径："若生态工具出现兼容痛点，v1.1 可加 per-subscription 枚举字段（如 `imageReferenceFormat: digest|tag`）——纯增量演进，不破坏 v1 API。" v1.2 兑现此承诺，并扩展支持 OCI 标准复合格式 `repo:tag@sha256:<hex>`。

## Solution

ImageFlow v1.2 在 `ImageSubscription.spec` 新增 `imageReferenceFormat` 枚举字段，允许订阅级别选择写入目标 workload 镜像字段的引用格式：

- **Digest**（默认）：`repo@sha256:<64hex>` — 保持 v1 行为，确定性传递；
- **Tag**：`repo:tag` — 兼容仅识别 tag 的工具；tag 字符串从 `ImageStreamTag.spec.tag` 获取；
- **DigestWithTag**：`repo:tag@sha256:<64hex>` — OCI 标准复合格式，兼顾 tag 展示与 digest 确定性。

核心约束（来自 ADR-005 Amended + ADR-012）：
- **spec 级粒度**：整个订阅统一格式，所有 target 共享；不支持 per-target 覆盖（YAGNI，未来可增量扩展）；
- **默认 Digest + mutating defaulting**：省略字段时 mutating webhook 自动填充 `Digest`，已有对象在下次 update 时被填充；零手动迁移；
- **Tag 模式下 ImageApplied 比较 digest**：`TargetDeliveryStatus` 新增 `resolvedDigest` 字段，format=Tag/DigestWithTag 时填充；`ImageApplied` 始终用 digest 比较，tag 漂移可检测；
- **向后兼容**：默认值 = Digest = v1 行为；已有订阅无需任何变更。

## User Stories

1. 作为平台工程师，我想为某个订阅设置 `imageReferenceFormat: Tag`，以便目标 workload 的镜像字段写入 `repo:tag` 格式，兼容仅识别 tag 的 CI/CD 工具。
2. 作为平台工程师，我想设置 `imageReferenceFormat: DigestWithTag`，以便目标字段同时携带 tag 和 digest（`repo:tag@sha256:...`），兼顾人类可读性和确定性。
3. 作为平台工程师，我不想修改任何已有订阅配置，升级 v1.2 后它们继续以 digest 格式工作，行为与 v1 完全一致。
4. 作为值班工程师，我想在 format=Tag 时仍能通过 `resolvedDigest` 字段确认实际写入的 digest，以便排查 tag 漂移问题。
5. 作为安全审计员，我想确认 format=Tag 不会绕过现有的 SAR 授权检查和 admission 校验，tag 写入仍受完整的订阅授权链保护。
6. 作为 ImageFlow 维护者，我想 mutating webhook 在创建/更新订阅时自动填充 `imageReferenceFormat: Digest`，以便 API 对象始终显式声明格式，避免隐式默认值的歧义。
7. 作为平台工程师，我想在 format=Tag 时 `ImageApplied` 仍然基于 digest 比较（而非 tag 字符串），以便 tag 指向新 digest 时能被正确检测为"未应用"。

## Implementation Decisions

**决策溯源**：全部架构决策已冻结于 ADR-005（Amended）+ ADR-012；本节是实现约束汇编。

1. **API 变更 — ImageSubscription.spec**：
   - 新增 `imageReferenceFormat` 字段，类型 `ImageReferenceFormat`（string enum）；
   - 枚举值：`Digest`（默认）、`Tag`、`DigestWithTag`；
   - kubebuilder validation：`+kubebuilder:validation:Enum=Digest;Tag;DigestWithTag`；
   - 位置：`ImageSubscriptionSpec.ImageReferenceFormat`，与 `SourceRef` / `Targets` 同级。

2. **API 变更 — TargetDeliveryStatus**：
   - 新增 `resolvedDigest` 字段（`string`, optional）；
   - 仅在 format=Tag 或 DigestWithTag 时填充；format=Digest 时留空（digest 已在 `desiredImage` 中）；
   - Pattern：`^[^@\s]+@sha256:[0-9a-f]{64}$`（与 desiredImage 相同）；
   - 用途：`ImageApplied` 比较使用 `resolvedDigest`（当非空时）或 `desiredImage`（当 resolvedDigest 为空时）。

3. **DesiredImage pattern 放宽**：
   - 当前 pattern `^[^@\s]+@sha256:[0-9a-f]{64}$` 仅匹配纯 digest；
   - v1.2 需支持三种格式：
     - Digest：`repo@sha256:<hex>`（原 pattern）
     - Tag：`repo:tag`（新 pattern：`^[^@\s]+:[^@\s]+$`）
     - DigestWithTag：`repo:tag@sha256:<hex>`（新 pattern：`^[^@\s]+:[^@\s]+@sha256:[0-9a-f]{64}$`）
   - 方案：移除 kubebuilder pattern validation，改由 admission webhook 按 `imageReferenceFormat` 值做条件校验。

4. **Mutating Webhook Defaulting**：
   - 扩展现有 `internal/webhook/subscription_mutate.go`；
   - Create/Update 时，若 `spec.imageReferenceFormat` 为空字符串，patch 为 `"Digest"`；
   - 不修改其他字段。

5. **Validating Webhook 增强**：
   - 扩展现有 `internal/webhook/subscription_validate.go`；
   - 校验 `imageReferenceFormat` 枚举值合法性（kubebuilder enum 已覆盖，webhook 做二次防御）；
   - Update 时禁止从 Tag/DigestWithTag 变回 Digest（防止意外降级导致 workload 镜像字段格式突变）——可选，grill 未决，标记为 OPEN QUESTION。

6. **Delivery Controller 参数化**：
   - `internal/controller/subscription_delivery_controller.go` 中 3 处 digest-only 硬编码检查需参数化：
     - Line ~476-483：resolve 阶段的 digest ref pattern 检查 → 按 format 分支；
     - Line ~812-815：source.ref 验证 → 按 format 分支；
     - Line ~598/637/688：`out.desiredImage = source.ref` → 按 format 构造不同格式的值；
   - Tag 解析：从 `ImageStreamTag.spec.tag` 获取 tag 字符串（delivery resolve 阶段已读取 ImageStreamTag，零额外查询）；
   - DigestWithTag 构造：`fmt.Sprintf("%s:%s@%s", repo, tag, digestRef)`；
   - `resolvedDigest` 填充：format=Tag/DigestWithTag 时，将 resolve 阶段获取的 digest 写入 status。

7. **ImageApplied 比较逻辑**：
   - 现有逻辑：`observedImage == desiredImage`；
   - v1.2 逻辑：
     - format=Digest：不变（`observedImage == desiredImage`）；
     - format=Tag：`observedImage == desiredImage`（tag 字符串比较）；但额外记录 `resolvedDigest`，供运维排查 tag 漂移；
     - format=DigestWithTag：`observedImage == desiredImage`（完整复合格式字符串比较）；
   - **注意**：Q4=B 决策要求 Tag 模式下 ImageApplied 比较 digest。这意味着需要读取目标字段的当前值并解析其 digest（如果目标是 tag 格式，则需要额外 registry lookup 或从 ImageStreamTag 反查）。这增加了复杂度。
   - **简化方案**：Tag 模式下 `ImageApplied` 比较 tag 字符串（因为写入的就是 tag），`resolvedDigest` 仅用于可观测性。这与 Q4=B 的字面意思有偏差，但避免了运行时 registry lookup。**标记为 OPEN QUESTION**。

8. **测试策略**（对齐 ADR-009）：
   - **envtest**：mutating defaulting（空→Digest）、validating 枚举校验、三种格式的 delivery 写入、resolvedDigest 填充、format 切换的 admission 行为；
   - **kind e2e**：真实 workload 上三种格式的写入验证、tag 漂移场景的可观测性、升级兼容性（v1 订阅无变更继续工作）；
   - **fake client 单测**：format 分支逻辑、OCI 复合格式构造、resolvedDigest 填充条件。

9. **文档**：
   - quickstart 补充「镜像引用格式选择」章节，含三种格式的 YAML 示例；
   - ops 文档补充 format 选择指南（何时用 Tag/DigestWithTag）、tag 漂移排查步骤；
   - TESTING.md 补充 imageReferenceFormat 测试矩阵。

## Out of Scope

- Per-target imageReferenceFormat 覆盖（v1.3+，YAGNI）；
- CUE Status Evaluator（v1.3+，合约已冻结于 ADR-005 §4）；
- OpenShift Compatibility Layer（后续 ADR）；
- Tag 漂移自动告警（v1.3+ 增强，当前仅 resolvedDigest 可观测）；
- Format 切换的 rollout 策略（如渐进式从 Digest 迁移到 Tag）。

## Testing Decisions

1. **好测试的标准**：只断言外部可见行为（目标字段写入值、status.resolvedDigest、admission 拒绝/接受、Events）；不断言内部 format 分支调用序列。
2. **缝 S1 = Kubernetes API 对象面**：envtest 覆盖 mutating/validating webhook + delivery status；kind e2e 覆盖真实 workload 写入。
3. **缝 S2 = 目标 Workload 镜像字段**：kind e2e 验证三种格式的实际写入值。
4. **验收映射**：§User Stories 7 条各至少对应一个命名用例；Story 3（向后兼容）在 kind e2e 验证。

## Open Questions

1. **Q4=B 实现复杂度**：Tag 模式下 ImageApplied 比较 digest 需要运行时解析目标字段的 digest（registry lookup 或 ImageStreamTag 反查）。是否接受简化为 tag 字符串比较 + resolvedDigest 仅用于可观测性？
2. **Format 降级保护**：Update 时是否禁止从 Tag/DigestWithTag 变回 Digest？防止意外降级导致 workload 镜像字段格式突变。

## Further Notes

- **钉版**：Kubernetes 1.37.x；operator-sdk v1.42.3；
- **向后兼容**：v1.2 不改变 v1/v1.1 行为；默认 Digest + mutating defaulting 确保升级无副作用；
- **仓库痕迹**：ADR-005（Amended）、ADR-012（Accepted）已入库；本 spec 引用路径均可解析。
