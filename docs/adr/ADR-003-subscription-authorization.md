# ADR-003：ImageSubscription 模式与跨 namespace 授权

- 状态：Accepted
- 日期：2026-10-16
- 决策人：项目负责人（grilling 第 2 轮 Q11=B、第 3 轮 Q15=A / Q16-R3 / Q18）
- 关联：ADR-001、ADR-006、ADR-002（待落）；`ImageFlow-Project-Startup-Design.md` §7.2

## 背景

Q11=B 决定订阅的 source 与 target 均可跨 namespace：平台目录位于 `imageflow-system`（与控制器共用，Q18），应用团队在自己 namespace 里订阅并可驱动跨 ns 的 Workload 更新。这让「谁有权把镜像字段写进哪个 ns 的 workload」成为 v1 唯一真实需要设防的授权面；Q15=A 选择 RBAC 原生模型，不发明平行授权体系。

## 决策

1. **Schema**：ImageSubscription 为 namespace 级 CRD：
   - `spec.source`：SourceRef，v1 仅 `kind="ImageStreamTag"`（Q5=A），携带逻辑字段 `stream` + `tag`，由控制器解析到具体对象（字段最终形态随 ADR-002 物化模型定稿）；
   - `spec.targets[]`：`{kind, namespace, name}` + 内建 Kind（ADR-006）按 `containerName` 定位、第三方 Kind 按 `imageFields[]` 定位（语法待 ADR-005）；
   - `spec.policyRef`：引用 Status Policy（v1 仅冻结合约，evaluator 推迟，ADR-005 待落）。
2. **跨 ns 授权 = RBAC 原生（Q15=A）**：
   - 控制器以 SubjectAccessReview 校验**订阅创建者**对每个 target 资源持 `update` 权限；校验时机 = 创建 admission、targets 变更、定期复验（默认 10 分钟，flag 可调）；
   - 不满足 → 订阅报 `Forbidden` Condition 且对该目标零写入，不静默降级；
   - 创建者身份由 admission 写入 annotation `imageflow.appfun.dev/created-by`（user / serviceAccount）；
   - 控制器 ServiceAccount 的 RBAC 面：仅 ADR-006 白名单 Kind 的集群级 update/patch + events create/patch + 自有资源族全权；无通配 verb。
3. **目录访问（Q18）**：平台团队 = stream 族全权 ClusterRole（管理域 `imageflow-system`）；应用团队 = ImageStream/ImageArtifact 组织内全局只读 ClusterRole + 本 ns 内 ImageSubscription/Mapping 任务全权 Role；不做逐 ns RoleBinding 扩散。
4. **审计**：每次投递在订阅侧与目标侧两个 namespace 都写 Event。

## 备选方案

- **B. 目标 ns opt-in 注解** —— 被否：平行于 RBAC 的第二套授权体系，语义必然漂移，管理员要学两套。
- **C. A+B 双闸** —— 被否：单组织可信租户（Q2）下 B 无增量安全，只有运维噪音。
- **D. 不做限制** —— 被否：放掉了唯一要防的误配置（订阅误写他人 workload）。
- **目录读经逐 ns RoleBinding**（Q18 备选）—— 被否：绑定数随 namespace 线性增长，组织级只读语义下无增量收益。

## 理由

- 完全复用组织已有 RBAC 词汇：授予跨 ns 投递能力 = 在目标 ns 做一次标准 RoleBinding 授权，零 ImageFlow 私有概念；
- SAR 是 controller-runtime 现成能力，admission + reconcile 双保险覆盖「订阅创建时 target 尚不存在」的场景；
- 集中式写入（控制器 SA 持集群级 apps 写）是订阅驱动设计的固有形态，与 OpenShift image trigger controller 同款爆炸半径，缓解 = SAR 前置 + Kind 白名单 + Conflict Condition（ADR-008 待落）+ 双 ns Event。

## 影响

- 应用团队要驱动某 ns 的更新，必须先在该 ns 获得标准 RBAC `update` 授权——这是特性而非缺陷；
- SAR 调用量 ≈ 订阅数 × 目标数 × 复验频率，实现需结果缓存与限流；
- 未来若引入多租户，本模型是基座（可叠加 quota / opt-in），无需推翻。

## 未解决问题

- SAR 校验的 admission 载体：ValidatingAdmissionPolicy vs webhook —— spec 阶段实现选型；
- `containerName` 必填 vs 缺省语义（首容器 / 按 repo 名匹配）—— spec 定稿。
