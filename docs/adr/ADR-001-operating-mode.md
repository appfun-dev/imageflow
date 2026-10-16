# ADR-001：项目运行模式 —— v1 以 Vanilla Kubernetes 为先

- 状态：Accepted
- 日期：2026-10-16
- 决策人：项目负责人（grilling 第 1 轮 Q1=B）
- 关联：D-001 / D-002 / D-003 / D-016 / D-017；`ImageFlow-Project-Startup-Design.md` §3.3、§17 Phase 1；`docs/research/2026-10-16-upstream-fact-check.md`

## 背景

设计文档 §3.3 同时定义了 OpenShift 模式与标准 Kubernetes 模式，并警告「复用原生 API」与「自建 CRD」并存会形成双重事实源；D-002/D-003 把「v1 以哪种模式为主交付、stream 模型资源是否自建」留待决策。grilling 第 1 轮将其作为设计树树根问题（Q1）提出，选项为 A（OpenShift-first）/ B（Vanilla-K8s-first）/ C（双模式 day-one）。

## 决策

**v1 以 Vanilla Kubernetes 为先（选项 B）**：

1. v1 仅在标准 Kubernetes（1.37.x）上交付与验证；stream 模型资源族（ImageStream、ImageArtifact、ImageSubscription 等）为 ImageFlow 自有 CRD（自有 API group；group 名与逐类型分类由 ADR-002 定稿）。
2. OpenShift 模式日后以 **OpenShift Compatibility Layer** 落地：届时优先复用原生 `image.openshift.io/v1`（ImageStream / Image），避免双重事实源；原生 trigger 注解与 ImageFlow 的共存问题（D-016）一并推迟到该阶段。
3. 不做双模式 day-one；v1 不含任何 OpenShift 专有 API 依赖，架构保持接口分层，兼容层可后补而不返工。
4. v1 前提（Q2）：单一组织、可信租户的内部平台；除 namespace RBAC 外不做多租户隔离硬化。
5. D-001 同步关闭（Q3）：v1 不自建 Registry；只存元数据与引用，永不存 blob；`ImageStreamLayers`、FR-016 留在 v1 外。

## 备选方案

- **A. OpenShift-first**：v1 复用原生 ImageStream/ImageArtifact，标准 K8s 模式仅保留接口约束不交付。——被否。
- **C. 双模式 day-one**：v1 同时交付并验证两种模式。——被否。

## 理由

- 主干链路（Image Change 检测 → 订阅解析 → 镜像字段更新 → rollout 观察）与平台模式无关，选 B 不牺牲任何主干设计。
- 双模式会把 API 设计与测试矩阵直接翻倍，v1 阶段不可承受。
- Vanilla-first 使全量 e2e 可在 kind 中运行（无需 OpenShift 集群），与 Q7 工具链决策（operator-sdk + kind + envtest）天然互补。
- 「优先复用原生 API」保留为兼容层方向，尊重文档 §3.3 对双重事实源的警告。
- 上游事实核查确认「构建输出变化 → 订阅驱动 Workload 更新」上游无人实现（Shipwright Triggers 无此能力），差异化立论在 vanilla 形态下同样成立。

## 影响

- D-003 → 已决：ImageStream 等为自有 CRD；D-002 → 推迟至兼容层阶段；D-016 → 推迟。
- 版本与工具 pin（Q6/Q7，均经核实）：Kubernetes **1.37.x**（主战场）、operator-sdk **v1.42.3**（脚手架主，kubebuilder 补）、Shipwright Build **v0.21.9**（集成对象）、OpenShift **4.22**（仅兼容层参照，v1 不交付）；e2e 以 **kind** 为主、轻量测试用 **envtest**（controller-runtime）。
- OpenShift 原生 `image.openshift.io/triggers` 注解的 7-Kind 清单降级为兼容层/D-016 参考资料，不再是 v1 D-009（内建 Kind 首批）的输入。
- v1 的全部验证环境不需要 OpenShift 集群。

## 未解决问题

- 兼容层具体形态：桥接/镜像（把原生 ImageStream 同步进 ImageFlow CRD）vs 对原生 API 直接实现 stream 存储接口 —— 后续 ADR。
- ImageArtifact 在 OpenShift 模式下的映射策略（§12.2 警告不得无条件复制镜像实体）—— 后续 ADR。
- ~~v1 最低 Kubernetes 兼容版本线是否仅承诺 1.37.x~~ 已决（第 2 轮 Q14=是）：v1 仅承诺 K8s 1.37.x，不做历史版本兼容矩阵，v1.1 再评估放宽。
- ~~D-017（是否支持多集群/联邦）~~ 已决（第 2 轮 Q14=是）：v1 单集群——source / target / stream 同集群，不做联邦。
