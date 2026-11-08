# AGENTS.md

ImageFlow 仓库的 Agent 配置文件。

## 基础交互规范

### 格莱斯合作原则（Communication Protocol）

Agent 交互须遵循格莱斯合作原则的四大准则（Quantity、Quality、Relation、Manner），以及冲突处理优先级和持续协作规范。详见 `docs/agents/grices-cooperative-principle.md`。

#### Harness 集成补充

在合作原则基础上，补充 Agent 全生命周期交互约束、意图与上下文管理、证据与事实管理、工具调用治理、交互质量自检、Skill 组合与编排等执行层规范。详见 `docs/agents/agent-harness-integration.md`。

## Agent 技能

### Issue 跟踪

Issue 托管在 `appfun-dev/imageflow` 的 GitHub Issues，通过 `gh` CLI 操作。详见 `docs/agents/issue-tracker.md`。

### 分诊标签

默认五角色词汇表，标签字符串等于角色名（`needs-triage`、`needs-info`、`ready-for-agent`、`ready-for-human`、`wontfix`）。详见 `docs/agents/triage-labels.md`。

### 领域文档

单上下文模式：仓库根目录一份共享 `GLOSSARY.md` + `docs/adr/` 存放 ADR。详见 `docs/agents/domain.md`。

### Grill 问题澄清 UI

向用户呈现 grill/brainstorming 的 Frontier 题波请求拍板，或用户要求以可视化交互形式出题（如 `/genui` + Frontier）时，使用「题波卡」：一题一卡（radio 单选 + 「其他」textarea + 建议 callout），轮尾 submit 收整轮；回传后先与已入账 ADR 对账，再批量修订落地。详见 `docs/agents/grill-ui.md`。

### BizDevOps 建模

涉及架构、领域/API 或控制器边界变更以及合并前漂移审查时，遵循 `docs/agents/bizdevops-modeling.md`。`docs/model/` 中已接受的 CML/MDSL 制品约束实现；最终代码是审查证据，绝不构成静默重写模型的许可。

### 参考代码工作流 (XERJ)

实现任何非简单功能前，先搜索已索引的参考语料库，了解同类项目如何解决相同问题。完整说明见 `docs/agents/reference-code-workflow.md`。快速上手：

```bash
export XERJ_API_KEY="$(cat .xerj-data/admin.key)"
xerj search --prefix ref-* "<问题描述>"
xerj def --prefix ref-controller-runtime <符号名>
```

已索引参考：`controller-runtime`、`sample-controller`、`kubebuilder`。本仓库代码：`--prefix imageflow`。

## 子 Agent 派发路由（Matt 技能族）

使用 Matt 技能族（`dispatching-parallel-agents`、`subagent-driven-development` 或任何子 Agent 委派）时，按以下路由表为每个子任务选择专家或模型。按任务关键词或文件路径匹配，首次命中即生效。用户在提示词中显式指定 `@专家名` 或 `切换模型 provider/model` 始终覆盖此表。

| 触发条件 | 专家 / 模型 | 调用方式 |
|---------|-----------|---------|
| TDD、纯测试子任务：单元测试、集成测试、验收测试 | 🎯 Kent Beck·极限编程专家 — provider `kimi-coding`，model `k3`，reasoning `max` | 优先 `wf_run_node`/`summon_expert`（支持 thinking 参数）；若仅可用 `subagent_fork`，见下方工具限制说明 |
| 涉及特性开发的代码实现 | `/ub-mode` — provider `kimi-coding`，model `k3`，reasoning `max` | `subagent("/ub-mode\n" + task, { provider: "kimi-coding", model: "k3" })`；首行 `/ub-mode` 激活精简原则人设 |
| 默认（无匹配） | 继承父会话模型 | `subagent(task)` 不覆盖参数 |

### ⚠️ 工具限制：reasoning 参数透传

`subagent_fork` 的工具签名**不包含 `reasoning`/`thinking` 字段**（仅接受 `description`、`prompt`、`cwd`），因此无法通过该工具强制设定推理等级。路由表中 `reasoning: max` 的要求在不同工具下的行为：

| 派发工具 | provider/model 覆盖 | reasoning 覆盖 | 备注 |
|---------|-------------------|---------------|------|
| `wf_run_node` | ✅ | ✅ `thinking` 参数 | **推荐**用于需要强制 reasoning 的路由任务 |
| `summon_expert` | ✅ | ✅ 内置 reasoning 配置 | 适用于已注册的 Agency 专家 |
| `subagent_fork` | ❌ 不支持 | ❌ 不支持 | 仅能传递 prompt/cwd；provider/model/reasoning 均无法覆盖 |

**当仅可使用 `subagent_fork` 时的降级策略：**
- provider/model/reasoning 覆盖退化为**软约束**：在 task prompt 中声明期望的运行时配置，要求子 Agent 自检并报告实际生效值
- 子 Agent 必须在返回消息中包含 `runtime-check` 段，列出实际 provider/model/reasoning，以便父级验证
- 若子 Agent 报告的运行时与路由表不一致，父级应记录偏差并在任务结果中标注

### 预派发规则（强制 — 在技能步骤之前执行）

此规则是**执行门控**，不是建议。在进入任何 Matt 技能族工作流（`dispatching-parallel-agents`、`subagent-driven-development` 或任何子 Agent 委派）之前，必须作为 Step 0 运行：

**Step 0 — 路由分类（在技能执行步骤 1 之前运行）：**
1. 对每个候选任务，检查上方路由表。
2. 若任何任务命中非默认触发器 → 立即在当前层级调用 `subagent("@专家名\n" + task, { provider, model })`，使用路由表中指定的模型。task 首行必须为 `@专家名` 以激活专家人设。不要为这些任务继续进入技能的执行步骤。
3. 仅未命中任何触发器的任务（落入默认）→ 按技能的正常 `subagent(task)` 流程派发，不覆盖 provider/model。

路由任务使用带显式 `provider`/`model` 参数的 `subagent()` — task 文本中的 `@专家名` 前缀激活人设。绝不能用父模型静默内联处理路由任务。

### 规则
1. 任务命中触发器时，使用 `subagent("@专家名\n" + task, { provider, model })` — `@专家名` 首行激活人设，`provider`/`model` 参数强制使用正确模型。路由任务不得使用不带覆盖参数的普通 `subagent(task)`。
2. 多个独立的命中任务 → 并行派发多个 `subagent()` 调用，各自携带 `@专家名` 前缀和 provider/model。
3. 未命中任何触发器的任务 → 落入默认 `subagent(task)`，不覆盖 provider/model。
4. 用户提示词包含 `@专家名` → 在 subagent task 中使用该专家的 `@专家名` 前缀，跳过路由表查找。
5. 用户提示词包含 `切换模型 provider/model` → 作为 `provider`/`model` 选项传入 `subagent()`。
6. 任务涉及 K8s/OAM 配置文件时，始终以 `./config` 作为 cwd。
7. **深度回退**：若当前委派深度下无法使用带 provider/model 的 `subagent()`，在子 Agent 的开场消息中记录回退信息以便父级检测。绝不能用父模型静默内联处理任务。
