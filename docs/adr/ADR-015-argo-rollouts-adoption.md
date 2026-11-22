# ADR-015：Argo Rollouts 采纳评估（渐进交付负载）（v1.8）

- 状态：Accepted
- 日期：2026-11-20
- 决策人：项目负责人（v1.8 grill Wave A Q1-Q6、Wave B Q7-Q12、Wave C Q13-Q15）
- 关联：ADR-006（未解决问题「v1.7+ 渐进交付负载」由本 ADR 裁决；清单修订在 ADR-006 落笔）；ADR-003（SAR 授权沿用）；ADR-005（第三方写入合约，CloneSet 对照）；ADR-008（投递语义）；ADR-009（测试分层）；ADR-010（metrics 受控标签集）；ADR-014（Selector Target 正交）

## 背景

ADR-006（v1.6 修订）留下唯一未解决问题：是否纳入 Argo Rollouts / OpenKruise 等渐进交付负载作为内建 Workload Kind，并规定「扩充内建 Kind 必须修订本 ADR，不得默认继承」。v1.8 grill 在上游实证调研（argo-rollouts types.go / 官方文档 / issue #3281；openkruise CloneSet CRD marker / kruise.io 文档）与三轮题波（15 题）基础上做出裁决：v1.8 采纳 Argo Rollouts 为内建 Kind，OpenKruise 全系暂不采纳并记录硬理由。本 ADR 记录评估结论与接入形态决策；具体实现由独立 spec/tickets 交付（同 v1.7 节奏）。

## 决策

### 范围层（Wave A）

1. **评估结论先行落账**：本轮 grill 的交付物是采纳结论与接入形态决策（本 ADR + ADR-006 修订），Rollout 支持的实现细节走独立 `/to-spec` → `/to-tickets`。
2. **分批采纳**：v1.8 只接入 Argo Rollouts；Kruise CloneSet 视 Rollout 落地成本与真实诉求再启一轮评估。
3. **接入形态 = 内建 adapter 注册**：Rollout 满足 ADR-006 双标准——镜像字段可写（`.spec.template.spec.containers[name].image`，同 Deployment）+ 原生 rollout 可观察（phase / stableRS / currentPodHash / pauseConditions）。按 adapter 注册表 init 登记，不碰引擎代码。
4. **策略自治（职责边界）**：ImageFlow 只 SSA 写 pod template image 字段并观察终态；canary 步骤、pause、promote 等策略字段永不由 ImageFlow 写入，rollouts-controller 拥有 rollout 过程。
5. **出范围**：OpenKruise ImagePullJob / SidecarSet / Advanced StatefulSet / UnitedDeployment、Argo AnalysisTemplate / Experiment / TrafficRouting 集成、OpenShift DeploymentConfig。不配 trafficRouting/analysis 的 Rollout 退化为带步骤的 Deployment，零干扰，属默认支持面。
6. **落账形态**：本 ADR 新建 + ADR-006 修订（内建 Kind 清单与未解决问题指针）+ GLOSSARY 补词条。

### 实现契约层（Wave B）

7. **终态判定 = 三合一**：`status.observedGeneration`（**字符串**，types.go 警告仅其与 generation 相等时其余 status 字段可信）== generation 且 `phase == Healthy` 且 `stableRS == currentPodHash` → Succeeded；`phase` 为 Degraded / ReplicaFailure / ProgressDeadlineExceeded → Failed。
8. **Paused 映射 = AwaitingPromotion**：`pauseConditions` 非空（主要为 CanaryPauseStep——`pause: {}` 无限期步等人工 promote；blue-green pause 等同理等待人工恢复；pauseConditions 由 rollouts-controller 写入、人工清除后恢复）→ Progressing + 新 Condition Reason `AwaitingPromotion`——合法稳态，非故障，不计 failure 指标词汇。（Amended 2026-11-21：由「含 CanaryPauseStep」放宽为「非空」判定，对齐实现语义——任何 pause 均为等待人工 promote 的合法稳态。）
9. **SSA 冲突 = 先礼后兵**：首发不 force；遇 409（rollouts-controller 在字段规范化场景夺走 `.spec.template.spec.containers` ownership，上游 issue #3281）升级 force 重试一次并记 Event。
10. **未装 CRD = 沿用既有 WHY**：集群未装 Rollout CRD 时复用引擎既有 `KindNotInstalled` 报告路径，不加 feature gate、零新机制。
11. **status 读取 = unstructured 提取**：observer 以 unstructured 提取约 8 个判定字段（observedGeneration/phase/stableRS/currentPodHash/pauseConditions/updatedReplicas/replicas/readyReplicas），pod template 经 converter 转 `corev1.PodSpec` 定位容器；**零新 Go 依赖**，不引 argo-rollouts module。
12. **冻结语义 = 不冻结**：同 Deployment——Healthy 后持续观察，后续翻 Degraded 如实上报。

### 落账与文档面（Wave C）

13. **Kruise CloneSet 本轮不采纳（记录硬理由）**：其 `spec.template` 标注 `PreserveUnknownFields + Schemaless`，属原子字段——SSA 无法对 `containers[].image` 获得细粒度 field ownership，SSA 写入路径不可行；可行替代路线（整 template apply / JSON Patch）改变写入模型，需独立评估。留待下轮。
14. **术语落账**：GLOSSARY 新增「Progressive Delivery Workload（渐进交付负载）」词条（与批处理负载对仗，Rollout 为首个内建实例）；`AwaitingPromotion` 登记进 Reason 词汇文档（ops.md / metrics 合约）。
15. **E2E 策略**：kind 集群安装 argo-rollouts（install.yaml），CI e2e job 增加安装步骤；场景集 = 无 pause 自动 canary 收敛至 Succeeded、`pause: {}` 步停在 AwaitingPromotion、SSA 409 冲突升级路径（单测层）。envtest 无 rollouts-controller，只能验证字段读取，验证不了交付闭环，不作替代。

## 备选方案

- **Rollout 与 CloneSet 同批接入** —— 被否：CloneSet 的 SSA 不可行是硬阻塞，同批会把 v1.8 拖入 JSON Patch 路线设计；分批让 Rollout 先行（Q2）。
- **Rollout 走 Generic Resolver + imageFields** —— 被否：第三方路径状态如实 Unknown（ADR-005），拿不到 Healthy/Degraded 语义，损失 ImageFlow 核心价值（同 ADR-006 备选 D 的判据）（Q3）。
- **ImageFlow 代管 canary 步骤** —— 被否：越权替用户管策略，与 rollouts-controller 双写冲突（Q4）。
- **终态仅看 phase == Healthy** —— 被否：types.go 警告 observedGeneration 不等时 status 不可信；且需 stableRS 确认新 ReplicaSet 已稳定（Q7）。
- **Paused 映射 Failed** —— 被否：等人工 promote 是合法稳态，报 Failed 误告警（Q8）。
- **SSA 首发即 force** —— 被否：无谓夺取 rollouts-controller 的字段 ownership，冲突面最大（Q9）。
- **未装 CRD 加 feature gate** —— 被否：新增配置维度与文档成本，既有 KindNotInstalled 已如实报告（Q10）。
- **引入 argo-rollouts Go module** —— 被否：重型依赖树 + 版本绑定；unstructured 提取约 8 字段已够用（Q11）。
- **Succeeded 后冻结**（Job 式）—— 被否：Rollout 是长期运行负载，Healthy 后可能翻 Degraded，冻结会漏报（Q12）。
- **CloneSet 记「待评估」不写理由** —— 被否：schemaless 发现是本轮最有价值的调研产出，不落账下轮从零重查（Q13）。
- **术语只在 ADR 内行文、不进 GLOSSARY** —— 被否：跨 spec/代码/文档三处使用，单点定义防漂移（Q14）。
- **E2E 仅 envtest 装 CRD + fake status** —— 被否：无 rollouts-controller 时 phase/stableRS 永不变，验证不了「写 image → canary 收敛」闭环——正是本特性核心价值（Q15）。

## 影响

- `internal/delivery`：builtin.go 注册 Rollout adapter（`argoproj.io/v1alpha1, Kind=Rollout`，standardApplyNest 路径同 Deployment）；SSA 409 → force 重试一次 + Event。
- `internal/controller`（observer）：observableBuiltinKind 白名单 + getWorkloadFacts 新增 Rollout case（unstructured 提取 + converter）；完成判定实现决策 7/8/12；新 Condition Reason `AwaitingPromotion`。
- 文档：GLOSSARY 新增 Progressive Delivery Workload 词条；ops.md / metrics 合约登记 `AwaitingPromotion` Reason；ADR-006 修订清单与未解决问题指针。
- RBAC：config/rbac 增加 `rollouts.argoproj.io` 的 get/list/watch + update/patch。
- 测试：单测（判定矩阵、SSA 409 升级）、envtest（字段读取）、kind e2e（argo-rollouts 安装 + 决策 15 场景集）；TESTING.md / scenario-map.md 登记。
- 实现 spec 由 `/to-spec` 产出（`docs/spec/imageflow-v1.8-argo-rollouts-support.md`）。

## 未解决问题

- OpenKruise 全系（CloneSet 优先）：是否以整 template apply / JSON Patch 路线接入——需独立评估（决策 13 硬理由已锚定 SSA 不可行）。
- Argo AnalysisTemplate / Experiment / TrafficRouting 集成：ImageFlow 是否感知 analysis 结果——出范围，将来有诉求再评估。
- Kruise 若接入后 InPlace 更新（Pod 不重建、原地改 image）对 Delivery/observer 语义的冲击——随 Kruise 评估一并处理。

## 修订日志

| 日期 | 变更 | 理由 |
|------|------|------|
| 2026-11-20 | 初始版本，Accepted | v1.8 grill Wave A/B/C 决策（15 题共识） |
| 2026-11-21 | 决策 8 措辞放宽：「`pauseConditions` 含 CanaryPauseStep」→「`pauseConditions` 非空」（决策内附 Amended 注记） | v1.8 双轴 code-review C1（Spec 轴）：实现按非空判定；语义上任何 pause 均为等待人工 promote 的合法稳态，采放宽而非收窄 |
