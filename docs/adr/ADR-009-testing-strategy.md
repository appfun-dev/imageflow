# ADR-009：测试策略与 Allure 可追溯性基线

- 状态：Accepted
- 日期：2026-10-16
- 决策人：项目负责人（grilling 第 1–5 轮全部采纳）
- 关联：Spec issue #1（`Spec: ImageFlow v1 — 镜像流目录、订阅投递与结果观察（vanilla Kubernetes）`）、ADR-001~008、TESTING.md

## 背景

ImageFlow v1 尚未写入任何测试（仓库中无 `*_test.go`、无 `go.mod`）。随着 ADR-001~008 陆续关闭，核心设计（Vanilla Kubernetes 为先、资源表面、订阅授权、字段写入合约、投递语义）已稳定，必须建立一套可重复的测试基线，使：

1. 单元 / 集成 / e2e 三层测试有明确边界与目录归属；
2. 每条测试都能追溯到 Spec（GitHub issue 以 `Spec:` 开头）、User Story 与测试用例 issue；
3. 复杂测试（尤其 integration / e2e）的内容能被非开发者读懂，参数与关键步骤外显到报告；
4. 运行入口保持 `go test ./...`，不引入额外 CLI 即可在 CI 中执行。

## 决策

### 1. 测试框架与 Allure 集成

- **测试组织**：`github.com/stretchr/testify/suite` 提供三层测试目录下的 fixture 生命周期与断言风格。
- **Allure 集成**：`github.com/allure-framework/allure-go/commons/gotest` 生成 Allure 结果；断言使用 `github.com/allure-framework/allure-go/testify/assert` 与 `require`，并将 `*allure.Context` 传入断言函数，使失败与步骤绑定到 Allure 报告。
- **运行入口**：保持 `go test ./...`；Allure JSON 结果默认写入 `allure-results/`（受 `ALLURE_RESULTS_DIR` 覆盖）。

### 2. 测试分层与目录

```text
test/
├── unit/          # 纯内存、无集群依赖；可在毫秒级完成
├── integration/   # 依赖 envtest / 轻量 fake；可启动 controller-runtime 环境
└── e2e/           # 依赖 kind 集群；验证端到端镜像流与 Workload 更新
```

- `unit`：验证纯函数、schema 校验、字段路径解析、授权判断等。
- `integration`：验证控制器 reconcile 循环、informer 行为、webhook 校验、跨组件交互。
- `e2e`：在 kind 中部署 ImageFlow，从镜像导入到 Workload rollout 的完整链路。

### 3. Allure 可追溯性 schema

每条需要文档 / TMS 追踪的测试必须携带：

| 维度 | Allure API | 取值规则 |
|---|---|---|
| 测试用例 ID | `allure.WithAllureID("IF-<issue-id>")` | 本测试用例对应的 GitHub Issue 号 |
| TMS | `allure.WithTMS("IF-<issue-id>", "<github-issue-url>")` | 指向同一测试用例 issue |
| Spec | `allure.WithTMS("SPEC-<id>", "<github-issue-url>")` | 标题以 `Spec:` 开头的 GitHub issue 编号（如 issue #1） |
| User Story（序号） | `allure.WithLabel("userstory", "US-<Seq>")` | Spec 中 User Stories 列表的序号 |
| User Story（独立 issue） | `allure.WithLink("<url>", "US-<issue-id>", "userstory")` | 故事拆分为独立 GitHub issue 时使用；可与 Label 同时存在 |
| 场景名 | `allure.WithLabel("scenario", "<场景名>")` | 具体测试场景 |
| Epic | `allure.WithEpic("Phase X: ...")` | 项目阶段 |
| Feature | `allure.WithFeature("<功能域>")` | 功能域 / ADR 编号 |
| Story | `allure.WithStory("US-<Seq>")` | 用户故事编号 |
| 文档 / ADR | `allure.WithLink("<url>", "ADR-###", "docs")` | 指向相关 ADR 或设计文档 |
| Bug 回归 | `allure.WithIssue(name, url)` | 仅用于回归某个 bug issue 时 |

### 4. 层级映射

| Allure 层级 | 来源 |
|---|---|
| `ParentSuite` | `unit` / `integration` / `e2e` |
| `Suite` | testify/suite 结构体名去掉 `Suite` 后缀 |
| `SubSuite` | 可选子分组 |
| `Epic` | 项目阶段 |
| `Feature` | Spec / 功能域 |
| `Story` | User Story |

### 5. 测试描述与参数外显

- **Integration / E2E 测试**：必须携带 `allure.WithDescription` 或 `allure.WithDescriptionHTML`，说明场景、前置条件、预期结果、Spec / User Story 追溯。
- **Unit 测试**：默认免描述；逻辑复杂或参数重时可补充。
- **HTML 描述触发条件**：描述含表格、列表、代码块或多行结构化内容时使用 `WithDescriptionHTML`。
- **测试参数**：表格驱动测试通过 `allure.WithParameter(name, value)` 把每组参数外显到 Allure Parameters 标签；测试显示名保持稳定，不在名字中拼接参数值。
- **步骤参数**：步骤涉及多组参数或关键特征参数验证时，使用 `allure.StepParameter` 外显。
- **步骤说明**：复杂或核心验证步骤使用 `allure.StepDescription` / `allure.StepDescriptionHTML` 补充「怎么做」。

### 6. 报告产物与 CI

- `allure-results/` 写入 `.gitignore`，不提交到仓库。
- CI 仅归档原始 JSON；HTML 报告由开发者本地通过 `allure serve` / `allure generate` 按需生成。
- 运行命令保持统一：`go test ./...`。

### 7. 强制范围豁免

- 所有 commit 的测试必须至少属于一个 testify/suite 并生成 Allure 层级。
- AllureID + Spec / TMS / User Story 链接仅对「追踪到 GitHub issue 或 Spec」的测试强制。
- 纯内部工具 / 辅助测试可省略 link，但需用 `allure.WithDescription` 说明用途。

## 备选方案

- **Ginkgo + Gomega**：被否。Ginkgo 的 `Describe/It` 层级与 Allure 层级映射需额外适配层；团队更熟悉 testify。
- **纯 go test，无 Allure**：被否。无法满足 Spec / User Story / 测试用例 issue 的多层追溯需求，也无法在报告中展示参数与步骤。
- **Allure 但不用 testify/suite**：被否。缺少 fixture 生命周期管理，integration / e2e 测试的 setup/teardown 会散落在 `TestMain` 或每个 `Test*` 函数中。

## 理由

- `go test` 是 Go 生态唯一原生入口，任何额外 runner 都会增加 CI 与本地开发的心智负担；Allure 以结果文件方式接入，不改变入口。
- testify/suite 提供轻量、显式的 fixture 组织；与 Allure 的 `allure.Test(s.T(), ...)` 嵌套调用天然兼容。
- Allure 的 `WithAllureID`、`WithTMS`、`WithLink`、`WithLabel`、`WithParameter` 等 API 直接覆盖 Spec / User Story / 测试用例 / 参数 / 步骤的可追溯需求。
- 通过 `ParentSuite = unit|integration|e2e` 与 `Suite = <suite name>`，报告天然按测试层与领域分组。

## 影响

- `go.mod` 新增依赖：`stretchr/testify`、`allure-framework/allure-go/commons/gotest`、`allure-framework/allure-go/testify/assert|require`。
- 测试编写成本增加（元数据标注、描述、参数外显），但换来可读报告与可追溯性。
- 未来新增 Kind 或 Status Policy 时，测试目录与标注模板已固定，可直接套用。
- `allure-results/` 不提交，需要 CI / 本地归档脚本配合。

## 未解决问题

- `allure-go` 具体 pin 版本与 `go.mod` 依赖解析细节 —— 随首个示例测试一起落地时确定。
- CI 中 Allure 结果的上传与失败策略（是否阻断合并、历史趋势存储）—— 后续 DevOps 决策。
- Allure Testplan 过滤（`ALLURE_TESTPLAN_PATH`）在 PR 流水线中的启用方式 —— 后续细化。
