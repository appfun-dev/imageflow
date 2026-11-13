# loop-me 技能与 Workflow Spec 深度研究报告

> **调研日期**：2026-11-09  
> **调研范围**：Matt Pocock `mattpocock/skills` 仓库中的 `loop-me` 技能、Workflow Spec 结构、相关 grilling 技能族  
> **成熟度声明**：`loop-me` 当前位于 `skills/in-progress/` 目录，属于**进行中（in-progress）** 状态，尚未正式发布

---

## 1. 概述

`loop-me` 是 Matt Pocock 的 Agent Skills 集合中的一个实验性技能，其核心使命是：**通过结构化的盘问（grilling）会话，帮助用户将生活中反复出现的模式（loops）规格化为可被 Agent 实现的工作流规范（workflow specs）**。

该技能的独特之处在于它引入了 **"循环视角"（loop lens）**——一种将人的日常活动视为嵌套循环的认知框架，从而识别出哪些活动值得被自动化委托。它不直接构建工作流，而是产出一份足够精确的规格文档，使得"实施者 Agent 无需再问任何问题即可构建它"。

**来源**：
- 本地安装文件：`/Users/rangerhuang/.agents/skills/loop-me/SKILL.md`
- GitHub 仓库位置：`mattpocock/skills` → `skills/in-progress/loop-me/SKILL.md`（[链接](https://github.com/mattpocock/skills/blob/main/skills/in-progress/loop-me/SKILL.md)）
- 中文翻译版：`devcxl/mattpocock-skills-zh`（[链接](https://github.com/devcxl/mattpocock-skills-zh/blob/main/skills/in-progress/loop-me/SKILL.md)）

---

## 2. 技能定义

### 2.1 SKILL.md 全文解析

以下为 `/Users/rangerhuang/.agents/skills/loop-me/SKILL.md` 的完整内容分析（共 32 行）：

#### Frontmatter

```yaml
name: loop-me
description: Grill me about specs for the workflows I want to build, within this workspace.
disable-model-invocation: true
argument-hint: "A workflow to design, or nothing to go find one"
```

关键配置项：
- **`disable-model-invocation: true`**：禁止模型自动调用此技能，必须由用户显式触发
- **`argument-hint`**：提示用户可以传入一个待设计的工作流描述，或不传参让技能主动发现

#### 核心指令

> Run a stateful `/grilling` session whose only output is **workflow** specs. Use the grilling discipline (relentless, a round of questions at a time, a recommended answer attached to each) aimed at the vocabulary and goal below. Create, edit, and delete specs as the grilling resolves things.

**来源**：`/Users/rangerhuang/.agents/skills/loop-me/SKILL.md:8`

这段定义了 loop-me 的运行机制：
1. 它是一个**有状态的 grilling 会话**（不是单次问答）
2. **唯一输出**是 workflow specs（不是代码、不是文档、不是建议）
3. 遵循 grilling 纪律：无休止、一轮一轮地问、每题附推荐答案
4. 在 grilling 过程中动态创建、编辑、删除 spec 文件

### 2.2 The Loop Lens（循环视角）

> A **loop** is a recurring pattern in the user's life: their career, their week, their morning, a single repeated activity. Picturing a life as loops within loops reveals how predictable its activities really are, which is what makes them worth **delegating**. Use the lens to find loops worth specifying, and propose ones the user hasn't noticed.

**来源**：`/Users/rangerhuang/.agents/skills/loop-me/SKILL.md:12`

**核心洞察**：
- **Loop = 反复出现的模式**：可以是职业层面的（每周代码审查）、日常层面的（晨间邮件处理）、或单个重复活动（数据录入）
- **Loops within loops**：生活是嵌套循环的结构，大循环包含小循环
- **可预测性 = 可委托性**：正是因为活动是可预测的，才值得被委托给 Agent
- **主动发现**：Agent 应该用这个透镜去发现用户还没注意到的循环

> A **workflow** is the spec of one loop, made real. You run a workflow on a loop: the loop is its running instantiation. Workflows live in `workflows/*.md` and are the source of truth.

**来源**：`/Users/rangerhuang/.agents/skills/loop-me/SKILL.md:14`

**概念区分**：
| 概念 | 定义 | 存储位置 |
|------|------|----------|
| Loop（循环） | 现实中反复出现的模式 | 存在于用户生活中 |
| Workflow（工作流） | 一个循环的规格化表达 | `workflows/*.md` |
| Workflow Run | 工作流的一次执行实例 | 运行时产生 |

### 2.3 Vocabulary（共享词汇表）

> A shared language, reached for only when a workflow calls for it: never a checklist. **Mandate nothing structural**: a workflow needs no AI, no checkpoint, and no schedule unless the grilling shows it does.

**来源**：`/Users/rangerhuang/.agents/skills/loop-me/SKILL.md:18`

**重要原则**：词汇表是**按需取用**的工具箱，不是必须填满的模板。一个工作流可能完全不需要 AI、不需要检查点、不需要定时调度。

#### Trigger（触发器）

> What fires each run, an **event** (a new email, a new issue) or a **schedule** (every morning). Event-triggering is usually the more efficient.

**来源**：`/Users/rangerhuang/.agents/skills/loop-me/SKILL.md:20`

两种触发模式：
- **事件驱动**：新邮件到达、新 Issue 创建、Webhook 回调等（通常更高效）
- **时间驱动**：每天早上、每周一、每小时等

#### Checkpoint（检查点）

> A human-in-the-loop point where the user is asked to verify or decide. Some workflows have none and run autonomously; some use no AI at all.

**来源**：`/Users/rangerhuang/.agents/skills/loop-me/SKILL.md:21`

- 检查点是**可选的**人工介入点
- 有些工作流完全自主运行，没有检查点
- 有些工作流甚至不使用 AI

#### Push Right（向右推）

> Defer the checkpoint as far as it will go. Do maximal work before involving the human, so they are asked once, late, with everything prepared.

**来源**：`/Users/rangerhuang/.agents/skills/loop-me/SKILL.md:22`

这是 loop-me 最核心的设计哲学之一：
- **最大化自动化工作量**：在请求人工介入之前，尽可能多地完成工作
- **延迟决策点**：把检查点推到最右边（最晚的位置）
- **一次询问，万事俱备**：用户只被问一次，且所有信息都已准备好

#### Brief（简报）

> What a checkpoint presents, a tight, decision-ready summary (what was produced, why, and a link down to the asset itself), never the raw output. The user reads a brief, not a draft. Speed of review is imperative.

**来源**：`/Users/rangerhuang/.agents/skills/loop-me/SKILL.md:23`

Brief 的设计要求：
- **紧凑**：不是原始输出的复制粘贴
- **决策就绪**：包含"产出了什么"、"为什么"、"指向产物的链接"
- **审阅速度至上**：用户读的是简报，不是草稿

### 2.4 Definition of Done（完成标准）

> A workflow spec is done when an implementer agent could build it without asking a single question. Grill until then; nothing is done while a question remains.

**来源**：`/Users/rangerhuang/.agents/skills/loop-me/SKILL.md:27`

这是一个极其严格的标准：**零问题可构建**。只要还有一个问题未解决，spec 就不算完成。

### 2.5 Workspace 约定

- **`workflows/*.md`**：每个工作流一个 spec 文件
- **`NOTES.md`**：关于用户世界的原始笔记——他们使用的工具、处理的渠道、自己的术语。当 NOTES.md 为空或内容稀薄时，应先采访用户的世界再开始规格化。随着模糊术语浮现，将其锐化为规范术语并记录于此。

**来源**：`/Users/rangerhuang/.agents/skills/loop-me/SKILL.md:29-32`

### 2.6 agents/openai.yaml

```yaml
interface:
  display_name: "Loop Me"
  short_description: "Spec the workflows you want to build"
policy:
  allow_implicit_invocation: false
```

**来源**：`/Users/rangerhuang/.agents/skills/loop-me/agents/openai.yaml`

- `allow_implicit_invocation: false`：与 `disable-model-invocation: true` 一致，确保只有用户显式调用才会激活

---

## 3. Workflow Spec 结构

### 3.1 从 SKILL.md 推导的结构要求

SKILL.md **没有规定固定的 spec 模板**。这是刻意的设计选择——"Mandate nothing structural"。但从词汇表和完成标准可以推导出，一份合格的 workflow spec 至少需要回答以下问题：

```dsh-ui
{"title":"Workflow Spec 最小必要元素","gap":12,"items":[{"type":"list","items":[{"content":"📌 循环描述：这个循环是什么？它在用户生活中的位置和频率"},{"content":"⚡ Trigger：什么触发每次运行？事件还是时间表？"},{"content":"🔄 步骤序列：从触发到产出，具体发生什么？"},{"content":"🛑 Checkpoint（如有）：在哪里需要人工介入？为什么不能自动化？"},{"content":"📋 Brief 格式（如有 Checkpoint）：检查点呈现什么样的决策就绪摘要？"},{"content":"✅ 完成信号：一次运行如何被判定为成功完成？"},{"content":"❌ 失败策略：当无法产出预期结果时怎么办？"},{"content":"🔗 依赖与上下文：需要读取/写入什么资源？与其他循环的关系？"}]}]}
```

### 3.2 Issue #657 提出的扩展词汇

GitHub Issue [#657](https://github.com/mattpocock/skills/issues/657)（由 @qshanx 于 2026-07-24 提出，已关闭为 "not planned"）提议为 loop-me 增加两个可选术语：

| 提议术语 | 定义 | 状态 |
|---------|------|------|
| **Success Signal** | 一次运行达成预期目标的可观察证据。优先使用可运行的检查、退出状态、记录的状态转换或外部对账，而非 Agent 的定性判断 | 未采纳（closed as not planned） |
| **Failure Policy** | 当 Success Signal 无法产生时的处理方式：有界重试、跳过、暂停或升级到人工 | 未采纳 |

**未被采纳的原因推测**：这些概念虽然有价值，但可能被认为超出了 loop-me 作为"规格化工具"的范围，或者与"Mandate nothing structural"原则冲突。Issue 中提到这些概念在 `loop-design-check` 技能中有更完整的探索，但该技能的仓库路径已不可访问（404）。

**来源**：[GitHub Issue #657](https://github.com/mattpocock/skills/issues/657)

### 3.3 Spec 完成的标志

根据 SKILL.md 的定义，spec 完成有三个层次：

1. **语义完成**：所有 grilling frontier 问题都已解决
2. **实用完成**：实施者 Agent 可以零问题构建
3. **文件完成**：spec 已写入 `workflows/*.md`，NOTES.md 中的术语已锐化

---

## 4. Workflow Spec 范例

> ⚠️ **声明**：在 mattpocock/skills 仓库及其社区中**未找到官方发布的 workflow spec 范例**。以下范例基于 SKILL.md 定义和 MindStudio 博客文章的概念推导构造，标注为 **[推导]**。

### 4.1 [推导] 每日 PR Review Digest 工作流

```markdown
# Daily PR Review Digest

## Loop Description
每个工作日早晨，检查前一天新增的 Pull Requests，生成一份按优先级排序的 review digest，
帮助团队 lead 快速决定先审哪个 PR。这是一个"周循环"内的"日循环"。

## Trigger
- **类型**：Schedule
- **表达式**：每个工作日 09:00（Asia/Shanghai）
- **备选**：当 GitHub webhook 通知有新 PR 合并到 main 时也触发增量更新

## Steps
1. 从 GitHub API 拉取过去 24h 内 opened/updated 的 PRs
2. 对每个 PR 提取：标题、作者、变更文件数、diff 行数、关联 Issue、CI 状态
3. 按风险评分排序（diff 大小 × 涉及核心模块权重 × 无 reviewer 天数）
4. 生成 Brief（见下方格式）
5. 发送到 Slack #pr-digest 频道

## Checkpoint
- **位置**：Step 4 之后、Step 5 之前（Push right：所有分析已完成，只在发送前确认）
- **条件**：仅当存在高风险 PR（评分 > 阈值）时触发
- **超时**：30 分钟无响应则自动发送并标记为"未经确认"

## Brief Format
📊 **PR Digest - {date}**
- 待审 PR 总数：{count}
- 🔴 高风险：{high_count}（列出 top 3，附链接）
- 🟡 中风险：{mid_count}
- 🟢 低风险：{low_count}
- ⏱️ 预计总 review 时间：{estimate}
- 🔗 [查看完整列表]({dashboard_url})

## Completion Signal
Slack 消息成功发送（HTTP 200 from Slack API）

## Failure Policy
- GitHub API 不可达：等待 5 分钟后重试，最多 3 次；全部失败则发送降级 digest（仅含缓存数据 + 告警）
- Slack 发送失败：写入本地 fallback 文件 + 邮件通知团队 lead

## Dependencies
- GitHub Personal Access Token（read:org, read:repo）
- Slack Bot Token（chat:write）
- 核心模块列表配置文件：`config/core-modules.yaml`

## Notes
- "风险评分"是本项目自定义术语，已记录于 NOTES.md
- 此工作流不使用 AI；纯规则引擎
```

### 4.2 [推导] 每周 Newsletter 策展工作流

```markdown
# Weekly Newsletter Curation

## Loop Description
每周五下午，从多个信息源收集本周值得分享的内容，生成一期 newsletter 草稿。
这是一个"周循环"，嵌套在更大的"月度主题规划循环"内。

## Trigger
- **类型**：Schedule
- **表达式**：每周五 14:00（Asia/Shanghai）

## Steps
1. 扫描 RSS feeds（5 个技术博客 + 3 个行业媒体）
2. 检索本周保存的 bookmarks / highlights
3. AI 辅助摘要：对候选文章生成一句话摘要和相关度评分
4. 人工选择最终入选文章（Checkpoint）
5. AI 生成 newsletter 初稿
6. 格式化并保存到 `drafts/newsletter-{YYYY-MM-DD}.md`

## Checkpoint
- **位置**：Step 4（Push right：AI 已完成筛选和摘要，人工只做最终选择）
- **呈现方式**：Brief 展示 top 15 候选文章的一句话摘要 + 相关度评分
- **交互**：用户勾选入选文章（可多选），或输入"全部接受"/"全部拒绝"
- **超时**：24 小时无响应则暂停工作流并提醒

## Brief Format
📰 **Newsletter 候选 - Week {week_num}**
从 {source_count} 个源中筛选出 {candidate_count} 篇候选：
| # | 标题 | 来源 | 摘要 | 相关度 |
|---|------|------|------|--------|
| 1 | ... | ... | ... | ⭐⭐⭐ |
| ... | | | | |
请选择要纳入的文章，或回复"全部接受"。

## Completion Signal
`drafts/newsletter-{YYYY-MM-DD}.md` 文件存在且非空

## Failure Policy
- RSS 源全部不可达：使用上周缓存 + 告警
- AI 摘要服务不可用：保留原文标题和链接，跳过摘要
- 用户 24h 未响应：暂停，下周一重新触发

## Dependencies
- RSS feed URLs：`config/newsletter-feeds.yaml`
- AI API key（用于摘要生成）
- Bookmarks API 访问权限
```

---

## 5. 与其他 Grilling 技能的关系

### 5.1 Grilling 技能族对比

```dsh-ui
{"title":"Grilling 技能族定位对比","gap":14,"items":[{"type":"table","columns":[{"key":"skill","label":"技能"},{"key":"trigger","label":"触发方式"},{"key":"output","label":"输出物"},{"key":"scope","label":"适用范围"},{"key":"relation","label":"与 loop-me 关系"}],"rows":[{"skill":"grilling","trigger":"任何 grill 触发词","output":"共享理解（design tree 遍历完成）","scope":"通用：计划、决策、想法","relation":"loop-me 的底层引擎"},{"skill":"grill-me","trigger":"显式调用","output":"同 grilling","scope":"通用（grilling 的别名入口）","relation":"loop-me 间接依赖"},{"skill":"grill-with-docs","trigger":"显式调用","output":"共享理解 + ADR + Glossary","scope":"需要领域建模的场景","relation":"loop-me 不产出 ADR，但可组合"},{"skill":"loop-me","trigger":"显式调用 + 工作流描述","output":"workflows/*.md spec 文件","scope":"仅限工作流规格化","relation":"本技能"}]},{"type":"callout","content":"💡 关键区别：grilling 追求「共享理解」，loop-me 追求「零问题可构建的 spec」。前者是认知对齐，后者是工程交付物。","tone":"info"}]}
```

### 5.2 Grilling 引擎的核心机制

loop-me 依赖的 grilling 引擎（`/Users/rangerhuang/.agents/skills/grilling/SKILL.md`）具有以下关键特性：

1. **Design Tree（设计树）**：每个决策分支出依赖于它的后续决策
2. **Frontier（前沿）**：所有前置条件已满足的待决问题集合
3. **Round-by-Round**：每轮问完整个 frontier，附带推荐答案
4. **Fact-Finding Sub-Agent**：查找事实由子 Agent 完成，不占用用户的决策带宽
5. **完成条件**：frontier 为空 = 设计树的每个分支都已遍历

**来源**：`/Users/rangerhuang/.agents/skills/grilling/SKILL.md:6-30`

### 5.3 PR #593 的 Grilling 重构对 loop-me 的影响

PR [#593](https://github.com/mattpocock/skills/pull/593)（Release v1.2）对 grilling 进行了重大重构：
- 从"一次一个问题"改为"每轮问完整个 frontier"
- 引入后台子 Agent 进行事实查找
- 同步更新了 loop-me draft 以适配新的 grilling 接口

这意味着当前版本的 loop-me 使用的是**新版 grilling 引擎**，具有更高的效率和更好的并行探索能力。

**来源**：[GitHub PR #593](https://github.com/mattpocock/skills/pull/593)

### 5.4 Loop Lens 的独特价值

与其他 grilling 技能相比，loop-me 的独特价值在于：

| 维度 | 通用 Grilling | Loop-me |
|------|--------------|---------|
| 认知框架 | 设计树（决策依赖图） | 循环视角（嵌套重复模式） |
| 发现能力 | 被动响应用户提出的话题 | 主动发现用户未注意到的循环 |
| 输出形态 | 共享理解（无形） | `workflows/*.md` 文件（有形） |
| 完成标准 | Frontier 为空 | 零问题可构建 |
| 世界建模 | 不涉及 | NOTES.md 记录用户世界 |
| 术语管理 | 不涉及 | 模糊术语→规范术语的锐化 |

---

## 6. 应用指南

### 6.1 适用场景

```dsh-ui
{"title":"loop-me 适用 vs 不适用场景","gap":14,"items":[{"type":"row","items":[{"type":"col","width":1,"items":[{"type":"card","accent":"success","items":[{"type":"text","content":"✅ **适用场景**"},{"type":"list","items":[{"content":"每日/每周重复的信息处理流程（邮件分类、PR review、报告生成）"},{"content":"跨工具的自动化管道（GitHub → Slack → Notion）"},{"content":"需要人工检查点但希望最小化介入的高频任务"},{"content":"用户感到'总是在做同样的事'但说不清楚具体模式的场景"},{"content":"为新项目建立标准化的开发/运维工作流"}]}]}]},{"type":"col","width":1,"items":[{"type":"card","accent":"error","items":[{"type":"text","content":"❌ **不适用场景**"},{"type":"list","items":[{"content":"一次性任务（迁移、重构、初始搭建）"},{"content":"纯创意性工作（写作、设计探索）——缺乏可预测的循环结构"},{"content":"已有成熟 SaaS 解决方案的流程（除非需要深度定制）"},{"content":"实时交互式任务（客服对话、即时调试）"},{"content":"不包含重复模式的线性项目"}]}]}]}]}
```

### 6.2 如何判断一个循环值得被规格化

基于 SKILL.md 的 loop lens 原则，以下是判断标准：

1. **频率 × 耗时**：高频 + 高耗时 = 最高优先级。每周花 3 小时的重复工作比每天花 5 分钟的更值得规格化
2. **可预测性**：步骤是否可以用规则描述？如果每次都需要完全不同的判断，可能不适合
3. **错误成本**：出错后的修复代价越高，越需要明确的 spec（尤其是 Checkpoint 和 Brief 的设计）
4. **委托意愿**：用户是否愿意把这个循环交给 Agent？有些循环虽然可预测，但用户享受过程本身
5. **嵌套关系**：这个循环是否是更大循环的一部分？如果是，可能需要先规格化父循环

### 6.3 Spec 完成的实操检查清单

尽管 SKILL.md 说"不要清单"，但从完成标准可以推导出以下验证点：

- [ ] 一个不了解上下文的 Agent 读完 spec 后，能否独立实现？
- [ ] Trigger 是否明确到可以直接配置（cron 表达式或 webhook URL）？
- [ ] 每个步骤的输入/输出是否清晰？
- [ ] Checkpoint（如有）的 Brief 格式是否具体到可以直接渲染？
- [ ] Push right 原则是否已被应用？（检查点是否已推到最右？）
- [ ] NOTES.md 中的术语是否已在 spec 中使用规范形式？
- [ ] 失败场景是否有明确的处理策略？

### 6.4 已知限制与成熟度

| 方面 | 状态 | 说明 |
|------|------|------|
| 技能成熟度 | 🟡 In-Progress | 位于 `skills/in-progress/` 目录 |
| 官方范例 | ❌ 缺失 | 仓库中无 `workflows/` 示例目录 |
| Run 完成边界 | ⚠️ 已讨论未采纳 | Issue #657 提议 Success Signal / Failure Policy，closed as not planned |
| 多循环协调 | ❌ 未覆盖 | loop-of-loops 模式在社区文章中有讨论，但不在 loop-me 范围内 |
| 中文支持 | ✅ 社区翻译 | `devcxl/mattpocock-skills-zh` 提供完整中文翻译 |
| Grilling 引擎 | ✅ 已升级 | PR #593 重构后使用 frontier-based 轮询 |

---

## 7. 开放问题

1. **Workflow Spec 的最佳实践模板**：由于官方未提供范例，社区正在自行探索。是否需要一个"最小可行 spec"模板来降低入门门槛？这与"Mandate nothing structural"原则如何平衡？

2. **Run-Level 语义的归属**：Issue #657 提出的 Success Signal / Failure Policy 被关闭，但这些概念对于实施者 Agent 至关重要。它们应该属于 loop-me 的 spec 词汇表，还是属于另一个专门的"workflow-runtime"技能？

3. **NOTES.md 的结构化程度**：当前 NOTES.md 被定义为"原始笔记"，但随着术语积累，是否需要更结构化的格式（如 YAML frontmatter + 术语表）？

4. **Loop Discovery 的系统化方法**：SKILL.md 说"propose ones the user hasn't noticed"，但没有给出系统化的发现方法。是否可以结合日历分析、shell history 挖掘、或 IM 消息模式识别来自动发现候选循环？

5. **与现有编排系统的集成**：loop-me 产出的 spec 如何映射到具体的运行时（如 DSH Workflow、n8n、Temporal、Airflow）？是否需要一个"spec-to-runtime"的编译层？

6. **Loop-of-Loops 的规格化**：MindStudio 博客描述的嵌套循环协调模式（coordinator + workers + shared state）如何在 loop-me 的词汇表中表达？当前的 Trigger / Checkpoint / Push right / Brief 四元组是否足够？

7. **版本演进**：当用户的循环发生变化时，已有的 `workflows/*.md` 如何演进？是否需要 changelog 或 migration 机制？

8. **SDWC-LE 整合验证**：§9 提出的 SDWC + Loop Engineering 整合架构尚未经过实证。最小验证路径：用 `/loop-me` 产出 spec → 手动转换为 LE pattern → `loop-init` scaffold → 跑一周 report-only → 对比 spec 与实际运行的偏差。

9. **Spec ↔ Pattern 映射规范**：§9.4 的映射表是初步推导，需要与 loop-engineering 的实际 schema 对账确认。特别是 STATE.md 字段与 spec 章节的对应关系、gate.yaml 规则与 Checkpoint/Failure Policy 的编译规则。

10. **Matt 命令在 loop-audit 中的评分权重**：§9.4 提议将 Matt 命令检查项纳入 loop-audit 评分维度，但具体权重和评分标准需要与 LE 作者协商或自行定义后实证校准。

---

## 9. 与 Loop Engineering 的整合架构（SDWC-LE）

> **状态**：架构推导，未经实证验证
> **日期**：2026-11-12
> **触发**：用户在调研过程中发现 cobusgreyling/loop-engineering 仓库，要求评估与 SDWC 的整合可能性

### 9.1 背景：两个互补的项目

在 §5 提出 SDWC 架构后，调研发现了 [cobusgreyling/loop-engineering](https://github.com/cobusgreyling/loop-engineering)（以下简称 LE），其定位恰好覆盖 SDWC 缺失的运行时治理层。两者的生态位对比如下：

| 维度 | SDWC（本研究报告提出） | Loop Engineering（cobusgreyling） |
|------|----------------------|----------------------------------|
| 核心口号 | （未定义） | "Stop prompting. Design the loop. Get a score." |
| 关注阶段 | 设计时：Spec → DSH Workflow Graph | 运行时：Pattern → STATE.md → 调度执行 |
| 产出物 | `workflows/*.md` spec + DSH workflow instance | STATE.md + LOOP.md + loop-budget.md + gate.yaml |
| 状态管理 | DSH Visual Workflow 节点间数据流 | STATE.md 文件作为 memory spine |
| 验证机制 | Matt 命令作为检查项 + `wf_ask_user` | loop-audit 评分(0-100) + loop-sync 漂移检测 |
| 成本控制 | ❌ 未涉及 | ✅ loop-cost + loop-budget + budget-negotiator |
| 安全机制 | Matt 技能的 verification-before-completion | gate.yaml + circuit breaker + human handoff |
| 工具支持 | DSH + Matt 技能族 | claude/grok/codex/opencode/cursor/windsurf/openclaw |
| 成熟度 | 概念架构 | 生产级 CLI + npm 包 + 多 pattern 库 |

### 9.2 互补关系图

```
┌─────────────────────────────────┐     ┌──────────────────────────────────┐
│       SDWC (设计时)              │     │    Loop Engineering (运行时+治理)  │
│                                 │     │                                  │
│  loop-me grilling               │     │  loop-init scaffold              │
│       ↓                         │     │       ↓                          │
│  Workflow Spec                  │────→│  STATE.md + LOOP.md              │
│  (workflows/*.md)               │     │       ↓                          │
│       ↓                         │     │  loop-audit score                │
│  SDWC Compiler                  │     │       ↓                          │
│       ↓                         │     │  Scheduler / Cron                │
│  DSH Visual Workflow Instance   │     │       ↓                          │
│                                 │     │  Circuit Breaker                 │
│                                 │     │       ↓                          │
│                                 │     │  Post-Run Critique ──→ (反馈回    │
│                                 │     │                      spec 修订)  │
└─────────────────────────────────┘     └──────────────────────────────────┘
         ↑                                        │
         └──────── critique 驱动 spec 迭代 ←───────┘
```

关键数据流：
- **正向**：SDWC 产出的 spec 作为 LE pattern 的设计输入，经 `loop-init --pattern custom` 生成可运行 scaffold
- **反向**：LE 的 post-run critique 输出作为 `/loop-me` grilling 的输入，驱动 spec 迭代修订
- **对账**：spec 作为 loop-audit 和 loop-sync 的基准文档，检测运行时是否偏离设计意图

### 9.3 LE 为 SDWC 补齐的能力

SDWC 在 §5 中识别出的缺口，LE 几乎全部覆盖：

| SDWC 缺口 | LE 对应能力 | 整合方式 |
|-----------|------------|---------|
| 无成本治理 | loop-cost 预估 + loop-budget 上限 + budget-negotiator | 在 spec 中增加 Cost Profile 章节，引用 loop-cost 输出作为 DSH workflow 节点的 token budget 配置 |
| 无安全守卫 | gate.yaml denylist/allowlist + circuit breaker + human handoff | spec 中的 Checkpoint + Failure Policy 编译为 gate.yaml + circuit breaker 配置 |
| 无就绪度评分 | loop-audit 0-100 分 + loop-sync 漂移检测 | 将 Matt 命令检查项纳入 audit 评分维度 |
| 无运行反馈 | post-run critique 自动记录误报/噪声/改进建议 | critique 输出作为 `/loop-me` grilling 输入，形成闭环学习 |
| 单工具绑定 | 支持 claude/grok/codex/opencode/cursor/windsurf/openclaw | SDWC compiler 可按目标工具生成不同 pattern 变体 |
| 无生产化路径 | score ≥ 80 时可升级为 harness-foundry 声明式运行时 | SDWC spec 达到阈值后自动进入 LE 的生产化管线 |

### 9.4 SDWC 为 LE 补齐的能力

| LE 缺口 | SDWC 如何补 |
|---------|------------|
| Pattern 是预定义的，自定义新 pattern 需手写 | `/loop-me` grilling 产出 spec → 自动生成 custom pattern |
| 缺少「设计时」的结构化需求澄清 | Matt 技能族的 grilling 体系提供完整的需求工程方法论 |
| Spec 与运行时之间没有形式化桥梁 | SDWC compiler 将 spec 编译为 DSH workflow graph |
| 步骤级别的可靠性依赖 Agent 自由解读 | Matt 命令作为执行原语，降低单步不确定性 |

### 9.5 具体结合点详述

#### 9.5.1 Spec → Pattern 映射

SDWC 产出的 `workflows/*.md` 可作为 `loop-init --pattern custom` 的输入源。映射规则（初步推导，待实证）：

| Spec 章节 | LE 文件 | 说明 |
|-----------|---------|------|
| 循环描述 + Trigger | LOOP.md | 定义循环的 what/when/why |
| 步骤序列 + Checkpoint | STATE.md 的 steps 结构 | 每个步骤对应一个 state entry |
| Brief 格式 | STATE.md 的 brief template | 人工介入点的信息摘要模板 |
| Failure Policy | gate.yaml + circuit breaker | 失败时的守卫规则和熔断条件 |
| Cost Profile（新增） | loop-budget.md | token/时间/API 调用的预算上限 |
| 完成信号 | STATE.md 的 completion criteria | 循环终止或进入下一轮的条件 |

#### 9.5.2 loop-audit 扩展 Matt 检查项

LE 的 loop-audit 当前评分维度聚焦于 pattern 结构完整性。整合后可扩展以下 Matt 特有维度：

- 是否有 `/verification-before-completion` 对应的验证步骤
- 是否有 `/retro` 对应的复盘检查点
- Checkpoint 是否包含足够的人工决策上下文（Brief 质量）
- Trigger 条件是否可被 Agent 自主检测（vs 必须人工触发）
- 步骤间的状态传递是否有显式契约（vs 隐式依赖 Agent 记忆）

#### 9.5.3 Post-Run Critique → Spec 修订闭环

LE 的 post-run critique 记录了每次运行的误报、噪声、遗漏和改进建议。这些数据可以作为 `/loop-me` grilling 会话的输入材料，使 spec 修订基于实际运行证据而非纯理论推演。

闭环流程：
1. LE 运行 N 轮后积累 critique 数据
2. 启动 `/loop-me` grilling，将 critique 作为 NOTES.md 的补充输入
3. Grilling 产出修订版 spec
4. 重新编译为 LE pattern，进入下一轮运行
5. 对比修订前后的 audit score 变化，验证改进效果

### 9.6 整合架构命名

建议名称：**SDWC-LE**（Spec-Driven Workflow Compiler with Loop Engineering Runtime）

或更简洁地：**SDWC+LE**

### 9.7 最小验证路径

1. 用 `/loop-me` 对一个真实重复任务产出 spec
2. 手动按 §9.5.1 映射表转换为 LE pattern
3. `loop-init --pattern custom` 生成 scaffold
4. 以 report-only 模式运行一周
5. 对比 spec 预期与实际运行的偏差，记录到 NOTES.md
6. 用偏差数据驱动 spec 修订，验证闭环可行性

### 9.8 开放问题（整合相关）

见 §8 第 8–10 条。

---

## 附录 A：来源索引

| 编号 | 来源 | 类型 | URL / Path |
|------|------|------|------------|
| S1 | loop-me SKILL.md（本地） | 一手 | `/Users/rangerhuang/.agents/skills/loop-me/SKILL.md` |
| S2 | loop-me openai.yaml（本地） | 一手 | `/Users/rangerhuang/.agents/skills/loop-me/agents/openai.yaml` |
| S3 | grilling SKILL.md（本地） | 一手 | `/Users/rangerhuang/.agents/skills/grilling/SKILL.md` |
| S4 | grill-me SKILL.md（本地） | 一手 | `/Users/rangerhuang/.agents/skills/grill-me/SKILL.md` |
| S5 | grill-with-docs SKILL.md（本地） | 一手 | `/Users/rangerhuang/.agents/skills/grill-with-docs/SKILL.md` |
| S6 | GitHub Issue #657 | 一手 | https://github.com/mattpocock/skills/issues/657 |
| S7 | GitHub PR #593 | 一手 | https://github.com/mattpocock/skills/pull/593 |
| S8 | 中文翻译版 SKILL.md | 二手 | https://github.com/devcxl/mattpocock-skills-zh/blob/main/skills/in-progress/loop-me/SKILL.md |
| S9 | MindStudio Blog | 二手 | https://www.mindstudio.ai/blog/what-is-loop-of-loops-ai-agents-recurring-work |
| S10 | GitHub 仓库目录 | 一手 | https://github.com/mattpocock/skills/tree/main/skills/in-progress/loop-me |
| S11 | cobusgreyling/loop-engineering | 一手 | https://github.com/cobusgreyling/loop-engineering |
| S12 | loop-engineering README | 一手 | https://github.com/cobusgreyling/loop-engineering/blob/main/README.md |
| S13 | loop-engineering patterns/daily-triage | 一手 | https://github.com/cobusgreyling/loop-engineering/tree/main/patterns/daily-triage |

## 附录 B：术语对照表

| English | 中文 | 出处 |
|---------|------|------|
| Loop | 循环 | SKILL.md:12 |
| Workflow | 工作流 | SKILL.md:14 |
| Trigger | 触发器 | SKILL.md:20 |
| Checkpoint | 检查点 | SKILL.md:21 |
| Push right | 向右推 | SKILL.md:22 |
| Brief | 简报 | SKILL.md:23 |
| Loop lens | 循环视角/透镜 | SKILL.md:10 |
| Design tree | 设计树 | grilling SKILL.md:6 |
| Frontier | 前沿 | grilling SKILL.md:8 |
| Definition of done | 完成标准 | SKILL.md:25 |
| Success signal | 成功信号 | Issue #657（未采纳） |
| Failure policy | 失败策略 | Issue #657（未采纳） |
| SDWC-LE | Spec-Driven Workflow Compiler with Loop Engineering Runtime | §9.6（本报告提出） |
| Memory spine | 记忆脊柱/状态主干 | LE STATE.md 设计概念 |
| Circuit breaker | 熔断器 | LE gate.yaml 安全机制 |
| Post-run critique | 运行后评审 | LE 反馈机制 |
| Budget negotiator | 预算协商器 | LE loop-budget 组件 |
