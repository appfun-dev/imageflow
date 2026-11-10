# ImageFlow

ImageFlow 是云原生镜像生命周期与 Workload 镜像更新控制平面：管理镜像流与 tag、追踪镜像变化、按订阅关系更新 Workload 镜像字段，并观测 rollout 与健康状态。它复用 Shipwright 等现有构建能力，不替代镜像构建，也不是 Registry。

## Language

### 核心领域对象

**ImageArtifact**:
以不可变 digest 为主要身份的镜像实体及其元数据；对齐 OpenShift `image.openshift.io/v1` 的 `Image` 概念。v1 为 cluster 级去重存储（同 digest 全局一条）：spec/status 均由控制器写入（从 Import/Mapping 登记路径同步），用户唯一可写面 = metadata.annotations（ADR-002）。
_Avoid_: 镜像字符串、仓库地址、裸用 "image"

**Image Reference**:
指向镜像的值对象，如 `registry.example.com/team/app:1.2`（可变引用）或 `registry.example.com/team/app@sha256:...`（不可变引用）；不是实体，不是 Kind。
_Avoid_: 镜像地址、image URL

**ImageStream**:
组织 tag 与镜像关联的镜像流资源；不存储镜像层，不是 Registry。v1 起为 ImageFlow 自有持久化 CRD（vanilla-Kubernetes-first，见 ADR-001），语义对齐 OpenShift ImageStream；原生 API 复用推迟到 OpenShift Compatibility Layer。
_Avoid_: 镜像仓库、image repository

**ImageStreamTag**:
某个 ImageStream 下一个 tag 的当前镜像关联视图对象；不是单纯的 tag 字符串。v1 为**只读镜像 Kind**（ADR-002）：控制器从 ImageStream.status + ImageArtifact 合成（ownerRef 级联 GC），spec/status 全控制器所有，用户唯一可写面 = metadata.annotations；对象名编码 `<stream>--<tag>`（RFC 1123），订阅 SourceRef 用逻辑冒号形式 `<stream>:<tag>`；status 承载变化历史与投递事件摘要（有界）。语义对齐 OpenShift `image.openshift.io/v1` ImageStreamTag，但非虚拟投影。
_Avoid_: 标签、tag 字符串

**ImageTag**:
某个 ImageStream 下一个 digest 的视图对象；v1 为**只读镜像 Kind**，机制同 ImageStreamTag（ADR-002）；对象名编码 `<stream>--sha256-<hex>`；retag = 写新 Mapping（无独立 mutation 路径）；对齐 OpenShift `image.openshift.io/v1` ImageTag。
_Avoid_: 裸用 digest 指称该对象

**ImageStreamImage**:
通过 stream + digest 精确定位镜像的查询/引用概念；不建独立 Kind，其对外物化形态由 ImageTag 对象承担。
_Avoid_: digest 引用（指值对象时用 Image Reference）

**Image Change**:
镜像来源的引用或 digest 发生变化时产生的内部事件/逻辑对象；v1 默认不是 CRD。
_Avoid_: trigger、webhook、镜像更新

**ImageSubscription**:
镜像来源与一个或多个 Workload 目标镜像字段之间的声明式订阅关系；source 与 target 均可跨 namespace，授权为 RBAC 原生——SubjectAccessReview 校验订阅创建者对每个 target 的 update 权限（ADR-003）；不等于 OpenShift `image.openshift.io/triggers` 注解，也不是控制器。
_Avoid_: 触发注解、订阅器

**Subscription Target**:
一条 ImageSubscription 要更新的 Workload 及其镜像字段定位方式：内建 Kind 按容器名，第三方 Kind 按 imageFields[] 路径；写入值仅为 digest 引用（`repo@sha256:...`，ADR-005）。独占粒度 = **image 字段**（SSA fieldManager，ADR-008）：同一 workload 的不同容器可分属不同订阅，同字段冲突先到先得（后来者 Conflict Condition、零写入）。
_Avoid_: 裸用"目标资源"

**Delivery Result**:
一次镜像变化应用到一个目标的结果记录，横跨 ImageApplied / Rollout / Health 各独立阶段；v1 不是 CRD，存于 ImageSubscription.status 与 Events，其事件信息同时浮现到相关 ImageStreamTag / ImageTag 的 status 上。订阅整体态 = 跨目标最坏态聚合（Failed > Unknown > Progressing > Succeeded），无跨目标回滚；latest-wins：rollout 进行中新 tag 到达立即转投，旧投递记 Superseded；status 保留有界 3/5/5（ADR-008），完整历史交给 kube Events 与审计管道。
_Avoid_: 发布结果、部署记录

**Outcome Stages**:
端到端结果中必须分开建模与报告的阶段链：ImageResolved → ImageApplied → Rollout（Succeeded/Failed/Unknown）→ Health → BusinessValidation；禁止合并为单个 `Ready=True`。
_Avoid_: 裸用 Ready 作总体状态

**Business Validation**:
由外部验证/交付系统提供的端到端业务验收；不等同于 Kubernetes 健康状态。
_Avoid_: 健康检查、readiness

### 运行模式

**Vanilla Kubernetes**:
不带 OpenShift 扩展的标准 Kubernetes 集群；v1 的唯一交付与验证环境（ADR-001），e2e 基于 kind、轻量测试用 envtest。
_Avoid_: 裸 K8s、原生 K8s（歧义）

**OpenShift Compatibility Layer**:
日后把 ImageFlow stream 模型对接 OpenShift 原生 `image.openshift.io/v1` 资源（优先复用原生 ImageStream/Image，避免双重事实源）的交付形态；不在 v1 内。
_Avoid_: OpenShift 模式（不加限定时）、双模式

**Catalog Namespace**:
`imageflow-system` —— 平台镜像目录（平台团队管理的 ImageStream/ImageArtifact）与 ImageFlow 控制器共用的 namespace；应用团队经 ClusterRole 获得 ImageStream/ImageArtifact 的组织内全局只读（ADR-003）。
_Avoid_: image-catalog、裸用 "catalog ns"

### 逻辑组件

组件是逻辑职责划分，不意味着独立部署。

**Image Change Reconciler**:
识别、去重镜像变化并触发下游协调的逻辑组件；不是领域实体。
_Avoid_: 变化检测器（作组件名时）

**ImageSubscription Controller**:
协调订阅声明并更新目标 Workload 镜像字段的控制器；与 ImageSubscription 资源本身严格区分。

**Built-in Workload Adapter**:
在明确支持并测试过的内建 Workload Kind 中按容器名定位镜像字段的适配器；不得声称支持未实现/未测试的 GVK。v1 首批 = Deployment / StatefulSet / DaemonSet（ADR-006）；其余 Kind 如实报告 `Unsupported`。

**Generic Image Field Resolver**:
按配置的字段路径解析第三方 Workload 镜像字段的组件；不推断任意对象的语义正确性。

**Image Field Path**:
ImageFlow 自定义的受限 JSON-like 路径语法（`imageFields[]` 条目），用于定位第三方对象中可写的字符串字段；不等同于 OpenShift fieldPath，也不是 JSONPath。v1 文法 = 以 `.` 开始的受限点路径 + 数组稳定键单层等值过滤（如 `.spec.template.spec.containers[name=app].image`），禁止通配符/数字下标/任意表达式；写入值仅为 digest 引用（ADR-005）。
_Avoid_: JSONPath、fieldPath（不加限定时）

**Workload Observer**:
读取原生 status、判断 rollout 进展与容器健康状态的观察组件；不执行发布，不改写期望镜像。v1.5 起为 first-batch kinds 提供 native health determination（基于 pod container readiness + restart count），仅在 rollout Succeeded 后评估，CUE healthKey 优先。

**Native Health Determination**:
Workload Observer 对 first-batch kinds（Deployment/StatefulSet/DaemonSet）的容器级健康推导：基于 pod containerStatus.Ready 与 RestartCount，仅在 rollout = Succeeded 时评估，未配置 CUE healthKey 时自动启用。区别于 CUE Status Evaluator 通过 statusPolicyRef.healthKey 提供的自定义健康评估。

**CUE Status Evaluator**:
执行外部配置的 CUE 状态策略的逻辑组件；不是 KubeVela Definition 的原生运行时。

**Status Policy**:
存于 ConfigMap、被 `statusPolicyRef` 引用的可复用 CUE 状态定义。未配置 Status Policy 的目标，状态如实报告 `Unknown`，不得虚构 `Ready`；v1 冻结其存储（§8.1）与 I/O、安全失败合约（§8.2/§8.3），evaluator 实现推迟 v1.1（ADR-005）。
_Avoid_: WorkloadDefinition

**Image Import Handler**:
访问外部 Registry、读取镜像元数据并更新导入结果的组件；不长期存储镜像层。

**Image Mapping Handler**:
把已推送镜像登记到目标 stream/tag 关联的组件；不执行构建或 push。
