# ADR-007：外部 Registry = Quay；不自建；仅标准 OCI 面

- 状态：Accepted
- 日期：2026-10-16
- 决策人：项目负责人（grilling 第 1 轮 Q3、第 2 轮 Q13=Quay、第 3 轮 Q17=C）
- 关联：D-001 关闭；`ImageFlow-Project-Startup-Design.md` §2 边界；`docs/research/2026-10-16-upstream-fact-check.md` §1

## 背景

设计文档把「是否自建 Registry」留为 D-001 待决。Q3/Q13 确认不自建、目标 Registry 为 Quay；Q17=C 确认双形态并存：dev/test 用 quay.io SaaS，prod 用自托管 Quay。

## 决策

1. **ImageFlow 永不存 blob**：只存元数据与引用（digest、size、manifest 摘要）；ImageStreamLayers 与 FR-016 整体留在 v1 外（D-001 关闭）。
2. **集成面 = 仅标准 OCI / Docker Registry v2 API**（manifest 获取、digest 解析、tag list）+ dockerconfigjson pull secret；**零 Quay 专有 API 依赖**——robot account 只是一种凭据形态，天然兼容。
3. **双形态单实现**：quay.io 与自托管 Quay 只差 base URL 与凭据，同一 client 代码覆盖；e2e test double = kind 旁起 `projectquay/quay` 容器；集成测试对两种形态各跑一遍（Phase 2）。
4. **凭据边界**：Import / 元数据读取用 pull secret（只读）；push 永不经过 ImageFlow。

## 备选方案

- **自建 Registry** —— 被否：blob 存储/GC/复制/鉴权全部偏离使命，v1 范围爆炸，且与「不是 Registry」定位冲突。
- **集成 Quay REST API** —— 被否：厂商锁定，破坏 ADR-001 的可移植性承诺；OCI 标准面已满足 v1 全部需求。

## 理由

- OCI Distribution Spec 是所有主流 Registry 的最大公约数，只依赖它意味着未来更换 Registry = 重跑一致性测试，无代码分叉预期；
- 双形态（SaaS dev + 自托管 prod）是单一组织平台的真实拓扑，单 client 避免环境特判。

## 影响

- dev/CI e2e 需注意 quay.io 匿名拉取限速（使用认证 robot 凭据）；
- 自托管 Quay 的版本差异被「仅 OCI 标准面」约束兜底；
- BuildRun digest 条件性（依赖策略 system results）等上游事实已录调研笔记，供 ADR-004 引用。

## 未解决问题

- prod 自托管 Quay 的版本基线 —— Phase 0 部署时确认并回填调研笔记。
