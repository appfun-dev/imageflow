# ADR-008: 投递语义——字段级独占、最坏态聚合、Superseded 与 status 保留

- 状态：accepted（2026-10-16，grilling Q21/Q22/Q23/Q24）
- 相关：ADR-005（写入合约与文法）、ADR-003（订阅授权，SAR 与 Conflict 语义同源）、ADR-002（mirror status 承载 R10 富化）
- 决策台账：D-011/D-012/D-013/D-014 全部关闭

## Context

一条订阅可指向 N 个目标；同一 workload 可能被多条订阅管理（典型场景：app 容器吃业务 stream、sidecar 吃平台 stream——**合理且必须允许**）；tag 更新可能在 rollout 进行中到达；所有 status 数组必须有界（etcd 单对象 ~1.5MB 红线）。Q21 追问暴露了对象级独占的错误粒度：真正会冲突的资源是「具体 image 字段」，对象级占位会误杀多容器分属不同订阅的场景。

## Decision

### 1. 字段级独占 + Server-Side Apply（D-011=A，粒度细化）

- 每条订阅一个独立 fieldManager：`imageflow.appfun.dev/sub-<ns>-<name>`，以 SSA 只申明自己拥有的 image 字段
- **同一字段**被两条订阅申明 → Apply 冲突 → 后来者得 `Conflict` Condition、零写入（先到先得）
- **不同字段**（不同容器 / 不同 imageFields 路径）→ 天然共存：一个 Pod 的多个容器可分属不同订阅
- `managedFields` 即独占登记表；workload 注解 `imageflow.appfun.dev/managed-by` 降级为面向审计的摘要
- 独占回收 = 删除/修改占用者订阅；reconcile 重放 Apply 保证「目标后建」场景下所有权仍确定
- 附带红利：SSA 最小字段更新兑现 §7.3「不覆盖无关字段」；无 resourceVersion 盲写竞态

### 2. 最坏态聚合（D-012=A）

- 逐 target 独立 Outcome Stages（§7.5 词汇，含 `Superseded`/`Conflict` 终态）
- `subscription.status` 整体态 = 跨目标最坏：`Failed > Unknown > Progressing > Succeeded`
- **无跨目标回滚**：回滚安全域属于原生 rollout controller（它们持有 revision 历史）

### 3. latest-wins + Superseded（D-013=A）

- rollout 进行中新 tag 到达 → 立即转投新 tag，旧投递记 `Superseded` 终态，不重试不回滚，原生控制器自行收敛
- patch 风暴防护 = 原生 progressDeadlineSeconds + Conflict Condition；真出现抖动 v1.1 加 debounce（flag 可加，不破坏 API）

### 4. status 保留 3/5/5（D-014=C，用户点名数字）

| 数组 | 上限 |
|---|---|
| `ImageStream.status.tags[].items`（每 tag 历史深度） | 3 |
| subscription 保留最近投递 | 5 |
| mirror（ImageStreamTag/ImageTag）status 事件 | 5 |

- 溢出统一落 kube Events + 审计日志；全部为 controller flags，v1 不做 per-resource override
- **如实声明**：kube Events 默认 TTL 仅 ~1h；完整历史由集群日志/审计管道承接，超出 ImageFlow 边界

## Rejected alternatives

- 对象级独占注解：粒度粗于冲突单元，误杀多容器多订阅场景（Q21 追问确认）
- last-write-wins / 显式 priority：订阅互相拉扯 flapping / 单组织下调参是纯运维负担
- all-or-nothing + 跨目标回滚：越权原生控制器 + 状态机爆炸
- 串行队列等 rollout 完成：需超时守护，复杂度不值（latest-wins 语义更诚实）
- 更大保留深度（10/20/10 原建议）：用户点名 3/5/5，历史交给原生日志系统

## Consequences

- fieldManager 命名必须稳定：改名 = 所有权迁移，属破坏性变更，需进 API 演进规则
- SSA 要求控制器对目标字段持 patch 权限（与 ADR-001/003 的集群级 apps 写权限一致）
- 3/5/5 偏小：排障依赖 Events 与日志管道；若 v1 用户反馈历史不够，调 flag 即可（非破坏）
- `Superseded` 为 §7.5 既有终态，状态机不新增状态
