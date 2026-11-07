# ImageFlow BizDevOps 模型手册

**Current status: ACCEPTED** (human review 2026-11-07)

本种子描述仓库中已有的架构；不是将 ImageFlow 拆分为微服务的提案。它于 2026-10-27 从已接受的 ADR、Go API 类型、控制器和生成的 CRD 派生而来，并于 2026-11-07 经人工评审标记为 ACCEPTED（含 v1.1 BuildRunWatcher 模块）。

[bizdevops-modeling.md](../agents/bizdevops-modeling.md) 中的工作流现已生效。模型已 ACCEPTED，适用的 `ERROR` 严重级别规则在工作流的 A 或 B 结果解决前阻塞完成。

## 产物清单

| 文件 | 作用 |
|---|---|
| [context-map.cml](context-map.cml) | CML 源文件：上下文、逻辑模块、聚合根、逻辑事件/服务和外部关系 |
| [contracts/imageflow-api.mdsl](contracts/imageflow-api.mdsl) | 技术中立的逻辑 API 职责和载荷引用 |
| [diagrams/context-map.mmd](diagrams/context-map.mmd) | 由 CML 上下文映射派生的 Mermaid 评审视图 |
| [architecture-rules.json](architecture-rules.json) | 机器可读的 SOP-12 候选规则及其代码/模型作用域 |
| [review-template.md](review-template.md) | 最终代码 Model 轴及可选跨模型报告模板 |
| [model_consistency_test.go](../../test/model/model_consistency_test.go) | 聚焦的可执行完整性和清单检查 |

CML、MDSL、Mermaid、规则和状态变更须在同一提交中完成。评审可附在 PR 上；对于已接受的架构变更，将完成的报告存放在 `docs/model/reviews/` 下并与变更关联。

## 事实源边界

| 关注点 | 权威来源 |
|---|---|
| 共享语言 | [GLOSSARY.md](../../GLOSSARY.md) |
| 持久决策与理由 | [ADR](../adr/) |
| 变更的期望行为 | 发起的已评审规范/工单 |
| 已接受的上下文/模块/聚合结构 | CML（ACCEPTED 状态后） |
| 已接受的逻辑交互职责 | MDSL（ACCEPTED 状态后） |
| 字段级 Kubernetes API schema 与校验 | [Go API 类型](../../api/v1alpha1/) 及[生成的 CRD](../../config/crd/bases/) |
| 实现当前的实际行为 | 最终代码和可执行测试 |

任何一行不得静默覆盖另一行。特别地：

- 不要将每个 Go 字段复制到 MDSL 中制造两个 schema 权威；
- 不要将 Mermaid 视为独立模型；
- 不要因为当前代码存在就推断它是正确的；
- 不要为了事后合理化未评审的实现而将模型标记为 ACCEPTED。

## 当前建模基线

- 存在一个 `ImageFlow` 限界上下文。`Catalog`、`Subscription`、`Delivery` 和 `Observation` 是逻辑模块，不是独立部署单元。
- `ImageStream` 拥有标签到摘要的关联和有界历史。`ImageArtifact` 是集群作用域的内容寻址元数据；ImageFlow 不存储镜像 blob。
- `ImageStreamImport` 和 `ImageStreamMapping` 是短生命周期的注册任务。两者通过持久化、幂等的目录注册收敛，而非仅内存事件。
- `ImageStreamTag` 和 `ImageTag` 是控制器拥有的只读镜像，不是独立的目录真相。
- `ImageSubscription` 将一个源绑定到独立目标。`imageApplied`、rollout 和健康是不同的结果阶段；未知状态绝不伪造为成功。
- OCI Registry、Shipwright 和 Kubernetes 是外部系统。ImageFlow 不会变成 registry 或构建引擎，原生 Kubernetes 控制器执行工作负载 rollout。

CML 对每个资源 Kind 使用一个聚合根，以便机械地检查资源清单和所有权。这一建模选择不意味着每个 Kubernetes 对象都是富领域实体。

## 模型生命周期

1. `DRAFT`：派生或提议的模型；需要评审发现，但模型本身不是已接受的合并否决。
2. `ACCEPTED`：经人工评审的架构；适用的 `ERROR` 规则在工作流的 A 或 B 结果解决前阻塞完成。
3. `SUPERSEDED`：仅作为历史上下文保留，不再用于实现评审。

在本 README、`context-map.cml`、受影响的 MDSL 文件、`context-map.mmd` 和 `architecture-rules.json` 中同步更改状态。一致性测试会拒绝意外的状态偏移。

## 规则激活

`architecture-rules.json` 按 SOP-12 要求记录每条规则的 `statement`、`scope`、`because` 和 `severity`。初始文件覆盖限界上下文边界、聚合所有权/持久化和资源/服务刻板印象。

Go 测试验证规则形态和引用；它不是通用架构规则引擎。在声称某条规则已自动化强制执行之前，需增加真正的适应度函数或解析器支持的检查，并在该规则的验证证据中注明。对于无法机械判定的行为，Agent 评审仍然必要。

## 验证

```bash
go test ./test/model
```

此聚焦测试在现有 `make test` CI 路径下运行。它刻意不引入第三方依赖。它检查结构完整性，而非完整的 CML/MDSL 语法或运行时语义。如果后续引入官方解析器，需额外运行并注明其版本。
