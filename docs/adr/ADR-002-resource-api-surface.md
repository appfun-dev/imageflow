# ADR-002: 资源族 API 表面与物化模型

- 状态：accepted（2026-10-16，grilling Q8/Q16/Q19）
- 相关：ADR-001（vanilla-first 使本表面成为 v1 交付面）、ADR-003（订阅授权）、ADR-004（登记路径）、ADR-005（写入合约）、ADR-008（status 保留上限）
- 决策台账：D-003/D-004/D-005/D-006/D-007/D-008 全部关闭；Q19 物化模型定案

## Context

ADR-001 选择 vanilla-K8s-first 后，ImageFlow 必须拥有整套资源表面：group `imageflow.appfun.dev/v1alpha1`（Q8=A，组织持有 appfun.dev），Kind 名沿用 §1.2 确认表（与 OpenShift 同名不同 group 可共存，`kubectl` 以 `资源.group` 消歧；语义对齐是刻意设计）。

物化模型的核心约束：**CRD 无法实现虚拟投影**。OpenShift 的 ImageStreamTag/ImageTag 是 openshift-apiserver 内部 REST storage 策略现场合成的虚拟资源（etcd 无独立对象）；CRD 对象必然存储于 etcd。真投影只有聚合 API server 一条路（Q16-R6 已因同一权衡否掉过一次，且为两个只读视图 Kind 引入 `k8s.io/apiserver` 全套运维与 Q7 工具链决策正面冲突）。Q19 定案 **A′：控制器所有的只读镜像（mirror）CRD**。

## Decision

### 命名编码（R4/R9 mirror 对象）

CRD 对象名不能含 `:` 和 `@`（RFC 1123）：

- ImageStreamTag：`<stream>--<tag>`；ImageTag：`<stream>--sha256-<64hex>`
- admission 约束 stream 名不含 `--`，保证按第一个 `--` 切分是单射；spec 字段为准，名字只是索引
- 订阅 SourceRef 使用逻辑冒号形式（`kind="ImageStreamTag"`, `name="<stream>:<tag>"`），用户永不手写编码名

### R1–R10 全表

| # | 类型 | v1 形态 |
|---|---|---|
| R1 | ImageStream | ns 级持久 CRD，**唯一事实源**。`spec.tags[]` 内嵌 importPolicy（R8）；`status.tags[]` 承载 tag→digest 关联与有界历史（ADR-008）。写路径仅两种：spec 编辑 + Import/Mapping 任务 |
| R2 | ImageArtifact | **cluster 级**去重存储（同 digest 全局一条），内容寻址；spec/status 控制器所有，**用户唯一可写面 = metadata.annotations**（控制器保留不覆盖）；写权限 = 控制器 SA + 平台管理员 RBAC |
| R3 | ImageSubscription | ns 级 CRD。`spec = source(SourceRef) + targets[]（kind/ns/name + 内建按容器名 / 第三方按 imageFields[]，ADR-005）+ policyRef`；source/target 可跨 ns（授权 = ADR-003 SAR） |
| R4 | ImageStreamTag | **只读镜像 CRD**：控制器从 `ImageStream.status` + ImageArtifact 合成，ownerRef → stream 级联 GC；spec/status 全控制器所有，用户唯一可写面 = annotations；status 承载 R10 富化（变化历史、投递事件摘要、消费者引用，有界） |
| R5 | ImageStreamImage | 不建 Kind；「内部视图」职责由 R9 ImageTag mirror 对象自然承担 |
| R6 | ImageStreamImport | 短生命周期任务 CRD（spec=请求 / status=结果），完成后 TTL GC（**成功 1h / 失败 24h**，失败留排查）+ Event 审计；按需或 scheduled 周期触发 |
| R7 | ImageStreamMapping | 同 R6 形态与 TTL；**幂等：同 digest 重复登记 = no-op 成功**；v1 构建产物登记的唯一桥（CI/Tekton 步骤创建，ADR-004） |
| R8 | ImageImportPolicy | `spec.tags[].importPolicy` 内嵌字段，非 Kind；v1 最小子集 = `scheduled` + `intervalSeconds`（默认 900s）；OpenShift 的 tagPolicy/importMode 等推迟 |
| R9 | ImageTag | **只读镜像 CRD**，机制同 R4；retag = 写新 Mapping（无独立 mutation 路径） |
| R10 | Image Change / Delivery Result | 均非 CRD。Change = 内部事件；Delivery Result = `subscription.status` 逐 target Outcome Stages + 最坏态聚合（ADR-008）+ Events + 审计记录；事件摘要**同时上浮**到对应 ImageStreamTag/ImageTag mirror 的 status（有界） |

### 红线守护（§3.3）

控制器所有的只读 mirror 是**派生数据**，不是第二事实源：用户不可写 spec、写路径唯一（stream + 任务），不存在需要仲裁的双写。ImageArtifact 的 annotations 可写面不触碰内容寻址的 spec/status。

## Rejected alternatives

- **真虚拟投影（聚合 API server，Q19-B′）**：行为 100% 对齐 OpenShift（零存储、名字可含 `:`），但 `k8s.io/apiserver` 技术栈完全在 operator-sdk 生态外、delegated authn/authz 与聚合层证书自运维、APIService 故障 = 整个 group 的 `kubectl get` 集群性失败，且与 Q7 单一 controller-manager 决策正面冲突。
- **用户可写 ImageStreamTag（PUT = `oc tag` 等价，Q19-B）**：与 Mapping 形成双写路径，需发明仲裁规则——§3.3 警告的双事实源成为现实。
- **mirror + 用户可写 spec 子集（Q19-C）**：可写子集日后从 A′ 无损演进（加字段不破坏兼容），无需现在预付复杂度。
- **ns 级 ImageArtifact（Q9-R2 备选）**：同 digest 跨 ns 重复登记、元数据冗余；单组织可信租户下 cluster 级去重更简。多租户阶段若需隔离再重新审视（已知演进点）。

## Consequences

- mirror 滞后为秒级（status 携带 observedGeneration 可溯源）；tag 变化时 mirror 对象使 etcd 写放大（单组织规模可忽略）。
- watch mirror Kind 可用（真 CRD 对象）——比 OpenShift 原生 ImageStreamTag 的 watch 语义更强。
- OpenShift 兼容层来日：mirror Kind 与原生虚拟资源 1:1 映射，SourceRef 逻辑形式不变。
- admission 必须强制：stream 名不含 `--`；mirror 对象 spec/status 拒绝用户写（CEL 校验或 webhook）。
