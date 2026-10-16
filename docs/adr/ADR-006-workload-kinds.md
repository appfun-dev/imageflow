# ADR-006：内建 Workload 首批 Kind —— Deployment / StatefulSet / DaemonSet

- 状态：Accepted
- 日期：2026-10-16
- 决策人：项目负责人（grilling 第 2 轮 Q12=B）
- 关联：D-009 / FR-010 / FR-013；`ImageFlow-Project-Startup-Design.md`（首批清单须立新 ADR 的要求，编号 006 为本轮补位）；`docs/research/2026-10-16-upstream-fact-check.md` §4；ADR-001

## 背景

设计文档要求内建 Workload 适配器的「首批支持 Kind 清单」以独立 ADR 落档。OpenShift 原生 `image.openshift.io/triggers` 注解覆盖 7 种 Kind（CronJob / Deployment / StatefulSet / DaemonSet / Job / ReplicationController / Pod），但 ADR-001 已把该清单降级为兼容层参考资料；ImageFlow 自有首批需按「镜像字段可写 + rollout 可观察」双标准独立确定。

## 决策

v1 内建 Workload Adapter 首批 = **Deployment、StatefulSet、DaemonSet**：

1. 三者均具原生 rollout 状态可观察性——Deployment（observedGeneration / conditions / updatedReplicas）、StatefulSet（updateRevision / currentRevision）、DaemonSet（desiredNumberScheduled / updatedNumberScheduled）——支撑 Outcome Stages 的 Rollout 阶段（FR-013）。
2. 定位方式 = 按容器名，写入 pod template 的 container image 字段。
3. 非首批 Kind（Job / CronJob / Pod / ReplicationController / 任意第三方 CRD）：内建适配器拒绝处理，如实报告 `Unsupported` Condition，不猜测语义；第三方 Kind 可走 Generic Image Field Resolver + `imageFields[]`（FR-010，v1 仅字段写入，状态默认 `Unknown`，见 ADR-005 待落）。
4. 扩充首批（如 Job/CronJob、Argo Rollouts、Kruise CloneSet）必须立新 ADR 修订本清单，不得默认继承。

## 备选方案

- **A. 仅 Deployment** —— 被否：长期运行服务的 P0 场景包含 STS/DS，覆盖面过窄。
- **C. 首批对齐原生注解 7 种（+ Job/CronJob/Pod/RC）** —— 被否：三者无 rollout 语义可观察（Job 完成 ≠ rollout；运行中 Pod 改 image 字段对已启动容器无效；CronJob 只影响后续实例；RC 属遗留 API），混入会污染 Outcome Stages 模型。

## 理由

- 「可写」与「可观察」是两种独立能力；只有两者兼备的 Kind 才能给出完整的 ImageApplied → Rollout → Health 链，这是 ImageFlow 区别于「裸字段 patch」的核心价值（调研笔记 §4）。
- 三种 Kind 覆盖单一组织内部平台（Q2 前提）中绝大多数长期运行服务负载。

## 影响

- Adapter 接口按可注册/可扩展设计，后续新增 Kind 不破坏 API。
- kind e2e 必须覆盖三种 Kind 各自的更新与 rollout 观察用例。
- D-009 关闭。

## 未解决问题

- v1.1+ 是否纳入 Argo Rollouts / Kruise 等渐进交付负载 —— 届时评估并立新 ADR。
