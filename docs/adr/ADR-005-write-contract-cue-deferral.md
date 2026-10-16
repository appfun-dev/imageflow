# ADR-005：第三方写入合约（imageFields[] 文法 + digest-only 写入）与 CUE 状态策略推迟

- 状态：Accepted
- 日期：2026-10-16
- 决策人：项目负责人（grilling 第 1 轮 Q4=B、Wave B Q20=A）
- 关联：D-010 / D-016 / FR-010 / FR-012；设计文档 §7.3 / §7.4 / §7.5 / §8.1–8.3；ADR-006；ADR-008（待落）

## 背景

§7.3 只给出受限路径语法的建议稿（D-010 未决），且全链路从未定义「往字段里写什么值」；CUE evaluator 原为 P0，Q4=B 决定推迟到 v1.1，前提是 v1 把合约冻结到实现方无需再谈判的程度。§7.5 明言聚合规则待决策（Q22 已决，落 ADR-008）。

## 决策

1. **imageFields[] 文法（v1 冻结）**：
   - 以 `.` 开始的 JSON-like 字段路径；对象按字段名寻址；数组仅允许按稳定标识键做**单层等值过滤**，如 `.spec.template.spec.containers[name=app].image`；
   - 禁止：通配符、数字下标（不稳定）、多层/嵌套过滤、任意脚本或表达式；
   - Admission webhook 校验文法合法性；reconcile 前校验目标字段存在、类型为字符串且可写，任一不满足 → 明确 Condition，不猜测；
   - 同一对象内路径重叠、重复或冲突 → 配置整体拒绝（admission 期）；
   - 写入使用最小字段更新（Server-Side Apply 或等价机制），不覆盖无关字段；
   - 不声称与 OpenShift `fieldPath` 注解语法兼容（§7.3 立场维持），兼容子集留给 OpenShift Compatibility Layer。
2. **写入值格式（内建 Kind 与第三方 Kind 一律适用）**：只写 **digest 引用** `<repo>@sha256:<64hex>`。tag 引用写入被拒绝。理由：订阅驱动更新的本质是 digest 确定性传递；tag 写入字段后仍可再变，破坏 `observedImage == desiredImage` 的判定不变式。若生态工具出现兼容痛点，v1.1 可加 per-subscription 枚举字段（如 `imageReferenceFormat: digest|tag`）——纯增量演进，不破坏 v1 API。
3. **§7.5 每目标状态模型 v1 冻结**：`desiredImage` / `observedImage` / `imageApplied` / `rolloutStatus`(Progressing|Succeeded|Failed|Unknown) / `healthStatus`(Healthy|Unhealthy|Unknown) / `reason` / `message` / `observedGeneration` + 时间戳与重试信息。`imageApplied=true` 不得被解释为 rollout 成功。跨目标聚合 = 最坏态（Failed > Unknown > Progressing > Succeeded），无跨目标回滚（Q22，落 ADR-008）。
4. **CUE evaluator 推迟 v1.1（D-016），合约 v1 冻结为规范**：
   - 存储（§8.1）：ConfigMap + `statusPolicyRef{name, namespace, key}`；不新增 WorkloadDefinition CRD；
   - I/O（§8.2）：输入 = `target.metadata/spec/status`、`desiredImage`、`observedImage`/`imageApplied`、订阅与目标 generation、必要 rollout 观察上下文；输出 = `Healthy|Progressing|Unhealthy|Unknown` + `reason` + `message`；rollout 与 health 使用两个独立结果，不由单个 CUE 结果承担全部语义；
   - 安全与失败（§8.3）：ConfigMap 缺失、key 缺失、编译失败、输入缺字段、评估超时一律不得视为成功 → `Unknown` 或明确 `EvaluationFailed` Condition；限制输入大小、执行时间与能力面（无网络/文件）；策略属可执行判定配置 → RBAC + 变更审计 + 来源信任；同一策略版本建立可重复测试。
   - v1.1 实现按本合约接入，零合约变更。
5. **v1 第三方目标行为**（§7.4 第 4 行落地）：授权（ADR-003 SAR）与路径校验通过 → 照常写入 digest；无可用 Status Policy → rollout/health 如实报告 `Unknown` + 明确 Condition，绝不默认成功。内建 Kind 的 rollout 观察按 ADR-006 走原生 status。

## 备选方案

- **完整 JSONPath** —— 被否：引入通用表达式求值面与错误语义复杂度，真实需求只是「指向若干字符串字段」。
- **CEL** —— 被否：同上，且求值成本更高、与 CUE 策略的能力边界需要额外定义。
- **CUE evaluator 进 v1（Q4=A）** —— 被否：独立安全子系统（沙箱/超时/失败语义/信任链）会拖慢核心闭环；`Unknown` 是合法终态（§7.4），推迟不构成砍功能。
- **tag 引用写入（Q20b=B/C）** —— v1 被否：破坏 digest 确定性；保留为 v1.1 增量选项。

## 影响

- Admission webhook 成为必备部署组件（文法校验 + 路径冲突检测 + ImageArtifact 写保护）；
- 文法冻结后 v1.1+ 只增不改，改文法必须立新 ADR；
- digest-only 意味着目标对象的 image 字段不再携带 tag 语义，按 tag 展示的外部工具会看到 `repo@sha256:...`；
- D-010、D-016 关闭。

## 未解决问题

- v1.1 `imageReferenceFormat` 枚举的具体字段名与默认值 —— v1.1 细化，必须增量兼容。
