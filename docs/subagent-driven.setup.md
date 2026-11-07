# 子 Agent 派发路由 — 环境配置指南

本文档说明如何为当前项目的 **Matt 技能族子 Agent 派发路由** 配置 DSH Host 模型白名单，使 `AGENTS.md` 中定义的路由规则能够正确执行。

## 前置条件

- DeepSeek Harness (DSH) 已安装并运行（Web GUI / TUI / Desktop 任一）
- 拥有 `kimi-coding` 和 `qwen-token-plan-cn` 两个 provider 的有效 API 凭证

## 1. AGENTS.md 路由规则摘要

`AGENTS.md` 定义了子 Agent 派发路由表，按任务关键词匹配专家/模型：

| 触发条件 | Provider | Model | Reasoning | 调用方式 |
|---------|----------|-------|-----------|---------|
| TDD、纯测试子任务 | `kimi-coding` | `k3` | `max` | 优先 `wf_run_node`/`summon_expert`（支持 thinking 参数）；若仅可用 `subagent_fork`，见下方工具限制说明 |
| 默认（无匹配） | 继承父会话 | 继承父会话 | — | `subagent(task)` 不覆盖参数 |

> ⚠️ **reasoning 值必须小写**：API 接受 `max` 而非 `Max`。AGENTS.md 中已修正为小写。

### ⚠️ 工具限制：reasoning 参数透传

`subagent_fork` 的工具签名**不包含 `reasoning`/`thinking` 字段**（仅接受 `description`、`prompt`、`cwd`），因此无法通过该工具强制设定推理等级。不同派发工具的能力对比：

| 派发工具 | provider/model 覆盖 | reasoning 覆盖 | 备注 |
|---------|-------------------|---------------|------|
| `wf_run_node` | ✅ | ✅ `thinking` 参数 | **推荐**用于需要强制 reasoning 的路由任务 |
| `summon_expert` | ✅ | ✅ 内置 reasoning 配置 | 适用于已注册的 Agency 专家 |
| `subagent_fork` | ❌ 不支持 | ❌ 不支持 | 仅能传递 prompt/cwd；provider/model/reasoning 均无法覆盖 |

**当仅可使用 `subagent_fork` 时的降级策略：**
- provider/model/reasoning 覆盖退化为**软约束**：在 task prompt 中声明期望的运行时配置，要求子 Agent 自检并报告实际生效值
- 子 Agent 必须在返回消息中包含 `runtime-check` 段，列出实际 provider/model/reasoning，以便父级验证
- 若子 Agent 报告的运行时与路由表不一致，父级应记录偏差并在任务结果中标注

### 预派发规则（Step 0 门控）

进入任何 Matt 技能族工作流前，必须先执行路由分类：
1. 命中非默认触发器 → 立即用 `subagent("@专家名\n" + task, { provider, model })` 派发
2. 未命中 → 按技能正常流程 `subagent(task)` 派发，不覆盖 provider/model

## 2. DSH Host 模型白名单配置

### 配置文件位置

```
~/.dsh/profiles/<profile>/cordis.patch.yml
```

其中 `<profile>` 为你使用的 DSH 运行时：`web`、`desktop`、`dsh-tui` 或 `headless`。

### 配置项

在 `cordis.patch.yml` 中找到（或添加）`subagent-model-selection-settings` 条目：

```yaml
- id: subagent-model-selection-settings
  name: "@deepseek-ai/dsh-tool-subagent/model-selection-settings"
  config:
    enabled: true
    allowedModels:
      # ── kimi-coding provider ──
      - provider: kimi-coding
        model: k3
      - provider: kimi-coding
        model: k3-256k
      - provider: kimi-coding
        model: k3.1-pro
      - provider: kimi-coding
        model: k3.1-pro-highspeed
      - provider: kimi-coding
        model: kimi-for-coding
      - provider: kimi-coding
        model: kimi-for-coding-highspeed

      # ── qwen-token-plan-cn provider ──
      - provider: qwen-token-plan-cn
        model: qwen3.8-max
      - provider: qwen-token-plan-cn
        model: qwen3.8-flash
      - provider: qwen-token-plan-cn
        model: qwen3.7-max
      - provider: qwen-token-plan-cn
        model: qwen3.7-plus
      - provider: qwen-token-plan-cn
        model: qwen3.6-plus
      - provider: qwen-token-plan-cn
        model: qwen3.6-flash
      - provider: qwen-token-plan-cn
        model: deepseek-v4-pro
      - provider: qwen-token-plan-cn
        model: deepseek-v4-flash
      - provider: qwen-token-plan-cn
        model: kimi-k2.5
      - provider: qwen-token-plan-cn
        model: kimi-k2.6
      - provider: qwen-token-plan-cn
        model: kimi-k2.7-code
      - provider: qwen-token-plan-cn
        model: glm-5
      - provider: qwen-token-plan-cn
        model: glm-5.1
      - provider: qwen-token-plan-cn
        model: glm-5.2
      - provider: qwen-token-plan-cn
        model: MiniMax-M2.5
```

### 最小必要配置

如果只需要支持 AGENTS.md 当前路由表中的 TDD 任务，最小配置为：

```yaml
- id: subagent-model-selection-settings
  name: "@deepseek-ai/dsh-tool-subagent/model-selection-settings"
  config:
    enabled: true
    allowedModels:
      - provider: kimi-coding
        model: k3
```

> 💡 建议保留完整列表，以便后续扩展路由表时无需再次修改配置。

## 3. 验证配置

### 检查白名单是否生效

在 DSH 会话中调用 `list_subagent_models` 工具：

- 无参数 → 返回已注册的 provider 列表，应包含 `kimi-coding` 和 `qwen-token-plan-cn`
- `provider: "kimi-coding"` → 返回该 provider 下的可用模型列表
- `provider: "qwen-token-plan-cn"` → 同上

### 端到端验证

发起一个 TDD 子任务，确认：
1. 子 agent 使用了 `kimi-coding/k3` 而非父会话模型
2. `@Kent Beck·极限编程专家` 人设被正确激活
3. 任务完成后报告中标注了正确的 provider/model/reasoning
4. 若使用 `subagent_fork` 派发，子 Agent 返回消息中包含 `runtime-check` 段且值与路由表一致

## 4. ⚠️ 重要提醒

> **修改 `cordis.patch.yml` 后，必须开启新会话才能生效。**
>
> DSH 在会话启动时加载模型白名单配置。当前正在运行的会话不会自动感知配置变更。请：
> 1. 保存 `cordis.patch.yml`
> 2. 关闭当前会话
> 3. 开启一个新会话
> 4. 在新会话中验证配置

同样，修改 `AGENTS.md` 中的路由规则后，也需要开启新会话才能使 Agent 重新加载最新的指令文件。

## 5. 故障排查

| 症状 | 原因 | 解决方案 |
|------|------|---------|
| `subagent()` 报错 "model not allowed" | 目标模型未在 `allowedModels` 中 | 在 `cordis.patch.yml` 中添加对应 provider/model 条目，开新会话 |
| 子 agent 使用了父模型而非指定模型 | 路由未命中或 `@专家名` 前缀缺失 | 检查 task 首行是否为 `@专家名`；检查 AGENTS.md 路由表触发条件 |
| reasoning 参数无效 | 使用了大写 `Max` 而非小写 `max` | AGENTS.md 中已修正为 `max`；如自定义路由请注意大小写 |
| reasoning 未生效 / 子 Agent 未报告 reasoning | `subagent_fork` 不支持 reasoning 透传 | 改用 `wf_run_node`（thinking 参数）或 `summon_expert`；若仅可用 `subagent_fork`，在 prompt 中要求子 Agent 自检并返回 `runtime-check` 段 |
| 配置修改后未生效 | 未开启新会话 | 关闭当前会话，开启新会话 |

## 参考

- [AGENTS.md](../AGENTS.md) — 子 Agent 派发路由规则定义
- [docs/agents/](./agents/) — Agent 技能详细文档
- DSH 文档：`@deepseek-ai/dsh-tool-subagent/model-selection-settings` 插件配置
