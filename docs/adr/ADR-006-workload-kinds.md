# ADR-006：内建 Workload 内建 Kind —— Deployment / StatefulSet / DaemonSet / Job / CronJob

- 状态：Amended (v1.6)
- 日期：2026-10-16（初版）；2026-11-12（修订）
- 决策人：项目负责人（grilling 第 2 轮 Q12=B）；v1.6 修订由架构评审通过
- 关联：D-009 / FR-010 / FR-013；`ImageFlow-Project-Startup-Design.md`（首批清单须立新 ADR 的要求，编号 006 为本轮补位）；`docs/research/2026-10-16-upstream-fact-check.md` §4；ADR-001；ADR-005（Outcome Stages）

## 背景

设计文档要求内建 Workload 适配器的「内建 Kind 清单」以独立 ADR 落档。OpenShift 原生 `image.openshift.io/triggers` 注解覆盖 7 种 Kind（CronJob / Deployment / StatefulSet / DaemonSet / Job / ReplicationController / Pod），但 ADR-001 已把该清单降级为兼容层参考资料；ImageFlow 自有内建 Kind 需按「镜像字段可写 + 完成/rollout 可观察」双标准独立确定。

v1 初版仅纳入 Deployment / StatefulSet / DaemonSet（长期运行服务）。v1.6 扩展至 Job / CronJob（批处理工作负载），因其镜像字段可写且完成状态可观察，满足内建 Kind 双标准。

## 决策

### 内建 Kind 清单

v1.6 内建 Workload Adapter = **Deployment、StatefulSet、DaemonSet、Job、CronJob**：

1. **长期运行服务 Kind**（Deployment / StatefulSet / DaemonSet）具原生 rollout 状态可观察性——Deployment（observedGeneration / conditions / updatedReplicas）、StatefulSet（updateRevision / currentRevision）、DaemonSet（desiredNumberScheduled / updatedNumberScheduled）——支撑 Outcome Stages 的 Rollout 阶段（FR-013）。
2. **批处理 Kind**（Job / CronJob）具原生完成状态可观察性——Job（conditions.type=Complete/Failed、active/succeeded/failed 计数、completionTime）、CronJob（lastScheduleTime、lastSuccessfulTime、active Job 引用）——支撑 Outcome Stages 的 Completion 阶段（见下文语义区分）。
3. 定位方式 = 按容器名，写入 pod template 的 container image 字段。CronJob 嵌套路径为 `.spec.jobTemplate.spec.template.spec.containers[name].image`，其余 Kind 为 `.spec.template.spec.containers[name].image`。
4. 非内建 Kind（Pod / ReplicationController / 任意第三方 CRD）：内建适配器拒绝处理，如实报告 `Unsupported` Condition，不猜测语义；第三方 Kind 可走 Generic Image Field Resolver + `imageFields[]`（FR-010，v1 仅字段写入，状态默认 `Unknown`，见 ADR-005）。
5. 扩充内建 Kind（如 Argo Rollouts、Kruise CloneSet）必须修订本 ADR，不得默认继承。

### Completion 与 Rollout 语义区分

内建 Kind 按结果语义分为两类，Outcome Stages 对其建模不同：

| 类别 | Kind | 终态判定依据 | Outcome Stage 命名 |
|------|------|-------------|-------------------|
| 长期运行 | Deployment / StatefulSet / DaemonSet | replica-based rollout（updatedReplicas == desired, observedGeneration 匹配） | Rollout（Succeeded/Failed/Unknown） |
| 批处理 | Job / CronJob | condition-based completion（Complete=True 或 Failed=True） | Completion（Succeeded/Failed/Unknown） |

- **Rollout** 描述长期运行服务的期望副本集替换过程；Health 在 Rollout Succeeded 后评估。
- **Completion** 描述批处理任务的执行终结状态；Health 对 Job 无意义（任务完成后 pod 终止），Delivery Result 以 Completion 为终态。
- CronJob 本身不直接执行，其 Delivery Result 跟踪最近一次 Job 实例的 Completion 状态；CronJob 级 status 仅反映调度健康（lastScheduleTime 停滞告警等）。

### Label Selector 多目标支持

Label Selector 多目标（一个 Subscription 通过 label selector 匹配多个同 Kind Workload）**推迟至 v1.7**。v1.6 仍要求 Subscription Target 显式指定单个 Workload name。此功能未被拒绝，仅因 v1.6 范围聚焦于 Job/CronJob 内建支持而延后。

## 备选方案

- **A. 仅 Deployment** —— 被否：长期运行服务的 P0 场景包含 STS/DS，覆盖面过窄。
- **C. 内建对齐原生注解 7 种（+ Pod/RC）** —— 被否：Pod 运行中改 image 字段对已启动容器无效；RC 属遗留 API；混入会污染 Outcome Stages 模型。
- **D. Job/CronJob 走 Generic Resolver 而非内建** —— 被否：Job/CronJob 的完成语义是 ImageFlow 核心价值（区别于裸字段 patch），Generic Resolver 无法提供 Completion 观察，降级为 Unknown 损失关键信息。

## 理由

- 「可写」与「可观察」是两种独立能力；只有两者兼备的 Kind 才能给出完整的 ImageApplied → Rollout/Completion → Health 链，这是 ImageFlow 区别于「裸字段 patch」的核心价值（调研笔记 §4）。
- 长期运行三种 Kind 覆盖单一组织内部平台（Q2 前提）中绝大多数常驻服务负载。
- 批处理两种 Kind（Job/CronJob）覆盖定时任务、数据管道、CI 触发器等场景；其完成语义与 rollout 本质不同，需独立建模而非强行复用 rollout 概念。
- Label Selector 多目标虽有价值，但与 Job/CronJob 内建支持正交；v1.6 聚焦后者，前者在 v1.7 独立交付。

## 影响

- Adapter 接口按可注册/可扩展设计，后续新增 Kind 不破坏 API。
- KindAdapter 注册表新增 Job / CronJob 两个内建条目；CronJob 使用嵌套 pod template 路径。
- Observer 新增 batch/v1 Kind 分类与 completion-based 事实提取逻辑。
- Delivery Result 的 Outcome Stages 对 Job/CronJob 使用 Completion 而非 Rollout 命名。
- kind e2e 必须覆盖五种内建 Kind 各自的更新与状态观察用例。
- D-009 关闭。

## 未解决问题

- v1.7 Label Selector 多目标支持（一个 Subscription 匹配多个同 Kind Workload）。
- v1.7+ 是否纳入 Argo Rollouts / Kruise 等渐进交付负载 —— 届时评估并修订本 ADR。

## 修订记录

### v1.6 修订（2026-11-12）

**变更内容：**

1. 术语统一：全文「首批 Kind」替换为「内建 Kind」，消除"首批"暗示的临时性，明确这些 Kind 是 ImageFlow 持久支持的内建能力。
2. 内建 Kind 扩展：新增 Job（batch/v1）与 CronJob（batch/v1）为内建 Workload Adapter 支持的 Kind。
3. 语义区分文档化：新增「Completion 与 Rollout 语义区分」小节，明确批处理 Kind 使用 condition-based completion 而非 replica-based rollout 作为终态判定依据，Outcome Stages 对应命名为 Completion。
4. Label Selector 推迟记录：明确 Label Selector 多目标功能推迟至 v1.7（非拒绝），避免后续误认为该功能被否决。
5. 备选方案补充：新增选项 D（Job/CronJob 走 Generic Resolver）及其被否理由。
6. 影响更新：补充 Job/CronJob 相关的 Adapter、Observer、Delivery Result、e2e 影响条目。

**修订理由：**

v1.6 核心目标是扩展内建 Kind 覆盖至批处理工作负载。Job/CronJob 的镜像字段可写性与完成状态可观察性均满足内建 Kind 双标准，且其完成语义是 ImageFlow 区别于裸字段 patch 的关键价值点，不应降级为 Generic Resolver 的 Unknown 状态。术语从"首批"到"内建"的调整反映这些 Kind 是持久支持的能力基线，而非阶段性试验。

**兼容性：**

本次修订向后兼容。已有 Deployment/StatefulSet/DaemonSet 的行为不变；新增 Job/CronJob 支持不影响现有 Subscription。Label Selector 多目标功能原未在 v1 实现，推迟记录不改变任何现有行为。
