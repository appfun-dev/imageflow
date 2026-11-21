# ImageFlow 测试约定

本文件是 `docs/adr/ADR-009-testing-strategy.md` 的可执行版本：日常命令、目录结构、Allure 标注模板与代码示例。

> **现状说明**：T12 期间落地了 Ginkgo + Gomega 的集成/e2e 套件。本文件按 ADR-009 把 **testify/suite + allure-go** 恢复为后续新增测试与重构的首要约定；既有 Ginkgo 套件继续维护，新增/拆分用例时优先按本文件执行。

## 目录结构

```text
imageflow/
├── internal/**/*.go              # 生产代码
├── internal/**/*_test.go         # 同包单元测试（可选，与代码共位）
├── test/
│   ├── unit/                     # 纯内存、无集群依赖；毫秒级完成
│   ├── integration/              # envtest / 轻量 fake；真实 API server + 控制器
│   ├── e2e/                      # kind 集群；端到端镜像流与 Workload 更新
│   └── registrydouble/           # 共享 OCI v2 registry double（TLS + basic auth + 故障注入）
├── allure-results/               # gitignore，Allure JSON 输出
├── TESTING.md                    # 本文件
└── docs/adr/ADR-009-testing-strategy.md
```

- **单元层**：纯函数、schema 校验、字段路径解析、授权判断等。
- **集成层**：真实 API server + etcd + 真实控制器 manager（需 `KUBEBUILDER_ASSETS`）。
- **e2e 层**：真实 kind 集群、Deployment 滚动、webhook/证书链、registrydouble。

## 快速开始

```bash
# 1. 安装 Allure 命令行（本地查看报告用）
brew install allure        # macOS
# 或见 https://allurereport.org/docs/install/

# 2. 运行全部测试（入口永远不变）
go test ./...

# 3. 只跑单元层
go test ./test/unit/...

# 4. 生成并查看 HTML 报告
allure serve allure-results
# 或生成静态站点
allure generate allure-results -o allure-report
```

## Allure 结果目录

- 默认输出到 `./allure-results`。
- 可通过环境变量覆盖：`ALLURE_RESULTS_DIR=/tmp/allure-results go test ./...`
- `allure-results/` 与 `allure-report/` 已加入 `.gitignore`，**不要提交**。

## 可追溯性 schema

每条需要追踪的测试必须携带：

| 维度 | Allure API | 取值规则 |
|---|---|---|
| 测试用例 ID | `allure.WithAllureID("IF-<issue-id>")` | 本测试用例对应的 GitHub Issue 号 |
| TMS | `allure.WithTMS("IF-<issue-id>", "<github-issue-url>")` | 指向同一测试用例 issue |
| Spec | `allure.WithTMS("SPEC-<id>", "<github-issue-url>")` | 标题以 `Spec:` 开头的 GitHub issue |
| User Story（序号） | `allure.WithLabel("userstory", "US-<Seq>")` | Spec 中 User Stories 列表序号 |
| User Story（独立 issue） | `allure.WithLink("<url>", "US-<issue-id>", "userstory")` | 故事拆分为独立 issue 时使用 |
| 场景名 | `allure.WithLabel("scenario", "<场景名>")` | 具体测试场景 |
| Epic | `allure.WithEpic("Phase X: ...")` | 项目阶段 |
| Feature | `allure.WithFeature("<功能域>")` | 功能域 / ADR 编号 |
| Story | `allure.WithStory("US-<Seq>")` | 用户故事编号 |
| 文档 / ADR | `allure.WithLink("<url>", "ADR-###", "docs")` | 指向相关 ADR 或设计文档 |
| Bug 回归 | `allure.WithIssue(name, url)` | 仅用于回归某个 bug issue 时 |

## 层级映射

| Allure 层级 | 来源 |
|---|---|
| `ParentSuite` | `unit` / `integration` / `e2e` |
| `Suite` | testify/suite 结构体名去掉 `Suite` 后缀 |
| `SubSuite` | 可选子分组 |
| `Epic` | 项目阶段 |
| `Feature` | Spec / 功能域 / ADR |
| `Story` | User Story |

## 标注模板

### 单元测试（三段式 + options 包）

```go
import (
    agt "github.com/allure-framework/allure-go/commons/gotest"
    ao "github.com/appfun-dev/imageflow/test/utils/allure/options"
    . "github.com/appfun-dev/imageflow/test/utils/should_dsl"
)

// TestAcceptsContainerNameFilter verifies imageFields path accepts container name filter.
func (s *FieldPathSuite) TestAcceptsContainerNameFilter() {
    metadata := []ao.Option{
        ao.AllureID("IF-101"),
        ao.TMS("IF-101", "https://github.com/appfun-dev/imageflow/issues/101"),
        ao.TMS("SPEC-1", "https://github.com/appfun-dev/imageflow/issues/1"),
        ao.Label("userstory", "US-3"),
        ao.Label("scenario", "合法路径通过校验"),
        ao.Epic("Phase 1: API and minimal image model"),
        ao.Feature("ADR-005 imageFields write contract"),
        ao.Story("US-3"),
        ao.Link("https://github.com/appfun-dev/imageflow/blob/main/docs/adr/ADR-005-write-contract-cue-deferral.md", "ADR-005", "docs"),
        ao.ParentSuite("unit"),
        ao.Suite("FieldPath"),
        ao.Description(`验证 imageFields 路径文法接受 [name=app] 单层容器名过滤。

合法路径格式：.spec.template.spec.containers[name=<name>].image
非法格式（通配符、数字索引）应被拒绝，见其他测试用例。`),
    }

    const validPath = ".spec.template.spec.containers[name=app].image"

    agt.Test(s.T(), "imageFields 路径接受 [name=app] 单层过滤",
        func(a *agt.Context) {
            err := subscription.ValidateImageFieldPath(validPath)
            AExpect(a, err).Should(BeNil(), "valid path must pass validation")
        },
        metadata...,
    )
}
```

### 集成 / e2e 测试（必须带描述）

```go
import (
    agt "github.com/allure-framework/allure-go/commons/gotest"
    ao "github.com/appfun-dev/imageflow/test/utils/allure/options"
)

// TestRejectsMissingUpdatePermission verifies RBAC enforcement on target update.
func (s *SubscriptionAuthorizationSuite) TestRejectsMissingUpdatePermission() {
    metadata := []ao.Option{
        ao.AllureID("IF-102"),
        ao.TMS("IF-102", "https://github.com/appfun-dev/imageflow/issues/102"),
        ao.TMS("SPEC-1", "https://github.com/appfun-dev/imageflow/issues/1"),
        ao.Label("userstory", "US-5"),
        ao.Label("scenario", "RBAC 不足时报告 Forbidden"),
        ao.Epic("Phase 1: API and minimal image model"),
        ao.Feature("ADR-003 cross-namespace authorization"),
        ao.Story("US-5"),
        ao.Link("https://github.com/appfun-dev/imageflow/blob/main/docs/adr/ADR-003-subscription-authorization.md", "ADR-003", "docs"),
        ao.ParentSuite("integration"),
        ao.Suite("SubscriptionAuthorization"),
        ao.Description(`验证：当 ImageSubscription 的创建者没有被授权更新目标 workload 时，
控制器应通过 SubjectAccessReview 识别并在该目标上产生 Forbidden Condition，
且不对目标做任何字段写入。

前置条件：
- envtest 已启动 controller-runtime 环境
- 创建 source ImageStreamTag 与 target Deployment
- 创建只读 target 的 ServiceAccount 作为订阅创建者

预期结果：
- subscription.status.targets[] 中对应项为 Forbidden
- Deployment image 字段未被修改`),
    }

    agt.Test(s.T(), "订阅创建者缺少 target update 权限时拒绝写入",
        func(a *agt.Context) {
            a.Step("创建只读 ServiceAccount 与 ImageSubscription", func(ctx *agt.Context) {
                // ...
            })
            a.Step("触发 reconcile 并断言 SAR 结果", func(ctx *agt.Context) {
                ctx.StepDescription("控制器调用 SubjectAccessReview 检查 update 权限")
                // ...
            })
        },
        metadata...,
    )
}
```

### 表格驱动 + 参数外显

```go
import (
    agt "github.com/allure-framework/allure-go/commons/gotest"
    ao "github.com/appfun-dev/imageflow/test/utils/allure/options"
    . "github.com/appfun-dev/imageflow/test/utils/should_dsl"
)

// TestValidateImageFieldPath verifies imageFields path grammar matrix.
func (s *FieldPathSuite) TestValidateImageFieldPath() {
    metadata := []ao.Option{
        ao.AllureID("IF-101"),
        ao.TMS("IF-101", "https://github.com/appfun-dev/imageflow/issues/101"),
        ao.TMS("SPEC-1", "https://github.com/appfun-dev/imageflow/issues/1"),
        ao.Label("userstory", "US-3"),
        ao.Label("scenario", "imageFields 路径文法矩阵"),
        ao.Epic("Phase 1: API and minimal image model"),
        ao.Feature("ADR-005 imageFields write contract"),
        ao.Story("US-3"),
        ao.ParentSuite("unit"),
        ao.Suite("FieldPath"),
        ao.Description(`验证 imageFields 路径文法的合法与非法格式。

合法：[name=<container>] 单层过滤
非法：通配符 [*]、数字索引 [0]`),
    }

    cases := []struct {
        name    string
        path    string
        wantErr bool
    }{
        {"container name filter", ".spec.template.spec.containers[name=app].image", false},
        {"wildcard rejected", ".spec.template.spec.containers[*].image", true},
        {"numeric index rejected", ".spec.template.spec.containers[0].image", true},
    }

    for _, tc := range cases {
        s.Run(tc.name, func() {
            agt.Test(s.T(), tc.name,
                func(a *agt.Context) {
                    err := subscription.ValidateImageFieldPath(tc.path)
                    if tc.wantErr {
                        AExpect(a, err).ShouldNot(BeNil())
                    } else {
                        AExpect(a, err).Should(BeNil())
                    }
                },
                append(metadata,
                    ao.Parameter("path", tc.path),
                    ao.Parameter("wantErr", fmt.Sprintf("%v", tc.wantErr)),
                )...,
            )
        })
    }
}
```

### 复杂步骤 + 步骤参数

```go
import (
    agt "github.com/allure-framework/allure-go/commons/gotest"
    ao "github.com/appfun-dev/imageflow/test/utils/allure/options"
)

// TestLatestWinsSupersedesOldTag verifies supersession during active rollout.
func (s *DeliverySuite) TestLatestWinsSupersedesOldTag() {
    metadata := []ao.Option{
        // ... traceability and hierarchy omitted for brevity
        ao.Description(`验证 rollout 进行中到达新 tag 时，旧投递被标记为 Superseded。

步骤：
1. 写入第一个 tag 并观察 ImageApplied
2. 在 rollout 进行中写入第二个 tag
3. 断言旧投递状态变为 Superseded`),
    }

    agt.Test(s.T(), "rollout 中到达新 tag 时旧投递记为 Superseded",
        func(a *agt.Context) {
            a.Step("写入第一个 tag 并观察 ImageApplied", func(ctx *agt.Context) {
                ctx.StepParameter("firstTag", "v1.0.0")
                ctx.StepParameter("firstDigest", "sha256:aaa...")
                // ...
            })
            a.Step("在 rollout 进行中写入第二个 tag", func(ctx *agt.Context) {
                ctx.StepDescription("模拟镜像源在原生 rollout 尚未完成时发生第二次变化")
                ctx.StepParameter("secondTag", "v1.1.0")
                // ...
            })
            a.Step("断言旧投递为 Superseded", func(ctx *agt.Context) {
                // ...
            })
        },
        metadata...,
    )
}
```

### should_dsl + AExpect/AEventually（推荐）

使用 `test/utils/should_dsl` 封装的 Gomega 原语，断言失败自动绑定 Allure Context：

```go
import (
    agt "github.com/allure-framework/allure-go/commons/gotest"
    ao "github.com/appfun-dev/imageflow/test/utils/allure/options"
    . "github.com/appfun-dev/imageflow/test/utils/should_dsl"
)

// TestFakeClockAdvancesDeterministically verifies Fake clock deterministic time advancement.
func TestFakeClockAdvancesDeterministically(t *testing.T) {
    metadata := []ao.Option{
        ao.AllureID("CLOCK-001"),
        ao.TMS("CLOCK-001", "https://github.com/appfun-dev/imageflow/issues/CLOCK-001"),
        ao.Label("userstory", "US-INFRA-1"),
        ao.Label("scenario", "deterministic time advancement"),
        ao.Epic("Infrastructure: Test Seams"),
        ao.Feature("ADR-009 Testing Strategy"),
        ao.Story("US-INFRA-1"),
        ao.ParentSuite("unit"),
        ao.Suite("Clock"),
        ao.Description(`验证 Fake clock 提供确定性时间控制。

测试用例：
1. 初始时间等于构造参数
2. Advance() 正确递增内部时间
3. Since() 计算从参考点经过的时长`),
    }

    const (
        advanceDuration  = 5 * time.Minute
        sinceTestAdvance = time.Hour
    )

    agt.Test(t, "Fake clock advances deterministically",
        func(a *agt.Context) {
            start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
            clk := NewFake(start)

            a.Step("Verify initial time equals start", func(ctx *agt.Context) {
                AExpect(a, clk.Now()).Should(Equal(start))
            })

            a.Step("Advance by configured duration", func(ctx *agt.Context) {
                clk.Advance(advanceDuration)
                want := start.Add(advanceDuration)
                AExpect(a, clk.Now()).Should(Equal(want))
            })

            a.Step("Verify Since computes elapsed duration", func(ctx *agt.Context) {
                before := clk.Now()
                clk.Advance(sinceTestAdvance)
                AExpect(a, clk.Since(before)).Should(Equal(sinceTestAdvance))
            })
        },
        metadata...,
    )
}
```

> **完整范例**：见 [`internal/clock/clock_test.go`](internal/clock/clock_test.go)。该文件包含三个测试用例，覆盖同步断言、异步断言和并发安全场景，是后续测试的参考样本。

## 命名约定

| 对象 | 约定 | 示例 |
|---|---|---|
| suite 结构体 | `<Domain><Scope>Suite` | `FieldPathSuite`、`SubscriptionAuthorizationSuite` |
| suite 文件 | `snake_case_test.go` | `subscription_fieldpath_test.go` |
| 测试方法 | `Test<Scenario>` | `TestAcceptsContainerNameFilter` |
| Allure 显示名 | 中文或英文完整句，不含参数值 | `imageFields 路径接受 [name=app] 单层过滤` |
| AllureID | `IF-<issue-id>` | `IF-101` |
| Spec ID | `SPEC-<issue-id>` | `SPEC-1` |
| User Story | `US-<Seq>` 或 `US-<issue-id>` | `US-3` |

## 描述与参数外显规则

- **Integration / E2E 测试**：必须携带 `ao.Description` 或 `ao.DescriptionHTML`。
- **HTML 描述触发条件**：描述含表格、列表、代码块或多行结构化内容时使用 `ao.DescriptionHTML`。
- **表格驱动测试**：通过 `ao.Parameter(name, value)` 外显参数；测试显示名保持稳定。
- **步骤参数/说明**：关键步骤使用 `ctx.StepParameter` / `ctx.StepDescription` 补充。

## 可追溯性 checklist

提交测试前逐条核对：

- [ ] 测试位于 `test/unit/`、`test/integration/`、`test/e2e/` 或生产代码同包 `_test.go`。
- [ ] 使用 `testify/suite` 组织；suite 结构体名以 `Suite` 结尾。
- [ ] 测试方法遵循三段式结构（metadata → params → body）。
- [ ] 测试方法内调用 `agt.Test(s.T(), ..., metadata...)`。
- [ ] 方法注释精简至单行 godoc，详细说明在 `ao.Description()` 中。
- [ ] `ao.AllureID("IF-<id>")` 与 `ao.TMS("IF-<id>", url)` 指向同一测试用例 issue。
- [ ] `ao.TMS("SPEC-<id>", url)` 指向标题以 `Spec:` 开头的 GitHub issue。
- [ ] `ao.Label("userstory", "US-<Seq>")` 已标注。
- [ ] `ao.Label("scenario", "<场景名>")` 已填写。
- [ ] `ao.Epic` / `ao.Feature` / `ao.Story` 已按层级映射填写。
- [ ] `ao.ParentSuite` = `unit|integration|e2e`；`ao.Suite` = 结构体名去掉 `Suite` 后缀。
- [ ] Integration / E2E 测试已携带 `ao.Description` 或 `ao.DescriptionHTML`。
- [ ] 表格驱动测试已使用 `ao.Parameter` 外显参数。
- [ ] 关键步骤已使用 `ctx.StepParameter` / `ctx.StepDescription` 补充说明。

## 测试结构约定

### 三段式结构

每个测试函数遵循统一的三段式结构，兼顾代码可读性与 Allure 报告完整性：

```go
func TestXxx(t *testing.T) {
    // 1. Allure 元数据（内联，紧邻测试主体）
    metadata := []ao.Option{
        ao.AllureID("XXX-001"),
        ao.TMS("XXX-001", "https://github.com/appfun-dev/imageflow/issues/XXX-001"),
        ao.Label("userstory", "US-XXX-1"),
        ao.Label("scenario", "场景名"),
        ao.Epic("..."),
        ao.Feature("..."),
        ao.Story("..."),
        ao.ParentSuite("unit"),
        ao.Suite("Component"),
        ao.Description(`详细说明...`),
    }

    // 2. 关键测试参数（const 提取，便于调整）
    const (
        paramA = valueA
        paramB = valueB
    )

    // 3. 测试执行主体
    agt.Test(t, "显示名称", func(a *agt.Context) {
        // ...
    }, metadata...)
}
```

### Code-as-Documentation 原则

- **方法注释精简至单行 godoc**：仅保留 `// TestXxx verifies ...` 形式的简短说明，确保 `go doc` 兼容。
- **详细说明移入 metadata.Description**：测试目的、前置条件、预期结果、配置说明等全部写入 `ao.Description()`，在 Allure 报告中完整呈现。
- **包级 godoc 声明约定**：测试文件的包注释应说明本文件采用的结构约定和文档策略。

> **完整范例**：见 [`internal/clock/clock_test.go`](internal/clock/clock_test.go)。该文件包含三个测试用例，覆盖同步断言、异步断言和并发安全场景，是后续测试的参考样本。

## Allure 导入约定

### gotest 包

Allure gotest 包采用**别名导入**：

```go
import (
    agt "github.com/allure-framework/allure-go/commons/gotest"    // agt.Test, agt.Context
)
```

- `agt.Test(...)` 用于创建测试
- `agt.Context` 用于步骤回调签名

### options 包（推荐）

使用 `test/utils/allure/options` 包获取更简洁的元数据构造函数（去除 `With` 前缀）：

```go
import (
    ao "github.com/appfun-dev/imageflow/test/utils/allure/options"  // ao.AllureID, ao.Label, ...
)
```

| 原函数 | options 包 | 说明 |
|--------|-----------|------|
| `WithAllureID` | `ao.AllureID` | 测试用例 ID |
| `WithTMS` | `ao.TMS` | TMS 链接 |
| `WithLabel` | `ao.Label` | 自定义标签 |
| `WithEpic` | `ao.Epic` | Epic 层级 |
| `WithFeature` | `ao.Feature` | Feature 层级 |
| `WithStory` | `ao.Story` | Story 层级 |
| `WithParentSuite` | `ao.ParentSuite` | 顶层 Suite |
| `WithSuite` | `ao.Suite` | 二级 Suite |
| `WithDescription` | `ao.Description` | Markdown 描述 |
| `WithLink` | `ao.Link` | 文档链接 |
| `WithParameter` | `ao.Parameter` | 测试参数 |

> **命名冲突处理**：当 `options.Clock` 与包内类型冲突时，使用短别名导入：`import ao "...options"` 然后 `ao.Clock(...)`。

### testify 包优先级

优先使用 Allure 封装的 testify 包，仅在必要时回退到原生 stretchr/testify：

| 优先级 | 包路径 | 使用场景 |
|--------|--------|----------|
| 1️⃣ | `github.com/allure-framework/allure-go/testify/assert` | 已导入 allure 时首选 |
| 1️⃣ | `github.com/allure-framework/allure-go/testify/require` | 已导入 allure 时首选 |
| 2️⃣ | `github.com/stretchr/testify/assert` | allure 版本缺少所需函数，或未导入 allure 时 |
| 2️⃣ | `github.com/stretchr/testify/require` | allure 版本缺少所需函数，或未导入 allure 时 |

> **判断流程**：已导入 allure → 用 `allure-go/testify`；所需函数缺失 → 回退 `stretchr/testify`（别名避免冲突）；未导入 allure → 直接用 `stretchr/testify`。详见 [CODING_STANDARDS.md](CODING_STANDARDS.md)。

## 依赖与版本

```text
require (
    github.com/allure-framework/allure-go/commons/gotest <version-with-testify>
    github.com/allure-framework/allure-go/testify/assert
    github.com/allure-framework/allure-go/testify/require
    github.com/stretchr/testify  // ← 仅当 allure 版本缺少所需函数时作为回退
)
```

> **注意**：`allure-go/testify` 包在部分早期 release 中不存在，请确保 pinned 到包含 `testify/assert|require` 的版本（可用 `go get github.com/allure-framework/allure-go@latest` 或主干版本，并在 `go.mod` 中记录明确版本）。

## CI 约定

- CI 中只执行 `go test ./...` 并归档 `allure-results/` 目录。
- 不生成 HTML 报告到仓库；HTML 生成留给本地或单独的 Allure Server。
- 失败截图 / 日志可通过 `a.GlobalAttachment` 或 `a.Attachment` 写入 `allure-results/`，随目录一起归档。

## 既有 Ginkgo/Gomega 套件

T12 之前已落地的 `test/integration/` 与 `test/e2e/` Ginkgo 套件继续维护：

- 新增用例或拆分旧用例时，优先按本文件使用 testify/suite + allure-go。
- 既有 Ginkgo 用例的重构可分批进行，不必一次性全量迁移。
- 在 Allure 报告未覆盖既有 Ginkgo 套件期间，可追溯性继续由 `docs/scenario-map.md` 与 ticket evidence comment 承担。

同理，`internal/controller/` 包的纯 stdlib 单测 lane（纯 `testing` + 表驱动，无 testify/suite、无 Allure 层级）按 [ADR-009 §7](docs/adr/ADR-009-testing-strategy.md) 的豁免/过渡条款继续维护：

- 该 lane 的可追溯性由本文件各版本测试矩阵节（用例 ↔ 断言对照表）与测试代码注释中的决策编号 citation（ADR-### / issue #）承担；
- 新增 `internal/controller/` 单测沿用同一惯例，直至 ADR-009 修订收回该豁免；
- 若要对存量单测引入 testify/suite + Allure 层级，须先修订 ADR-009 §7，再分批迁移。

## BuildRun Watcher 测试矩阵（v1.1，T7）

BuildRun Watcher 测试覆盖三层，对齐 ADR-009 + spec §Testing Decisions。

### envtest 单元测试（`internal/controller/buildrun_watcher_controller_test.go`）

| 用例 | 断言 |
|---|---|
| 合法注解 JSON 通过 admission | BuildRun 创建成功，注解值被接受 |
| 非法 JSON 注解被 admission 拒绝 | BuildRun 创建失败，rejection message 含 schema 错误 |
| 缺 stream/tag 字段被 admission 拒绝 | BuildRun 创建失败，rejection message 指明缺失字段 |
| 非 RFC 1123 值被 admission 拒绝 | BuildRun 创建失败，rejection message 指明值约束 |
| Succeeded + digest → Registered=True/Succeeded | BuildRun status condition 正确设置；ImageStream.status 更新；ImageArtifact 创建 |
| Succeeded + 空 digest → Registered=False/MissingDigest | BuildRun status condition reason=`MissingDigest`；ImageStream 不变 |
| Failed BuildRun 不触发登记 | 无 Registered condition 写入；ImageStream 不变 |
| 幂等登记：同 (stream, tag, digest) 重复 reconcile | 第二次 reconcile 为 no-op；无重复 Image Change / Event |
| CRD 缺失时 manager 启动失败 | `Register()` 返回 error；manager 不启动 |
| stream 不存在 → Registered=False/StreamNotFound | BuildRun status condition 报告具体失败原因 |

### kind e2e 场景（`test/e2e/`）

| 场景 | 描述 |
|---|---|
| 真实 Shipwright BuildRun → digest 登记 → ImageStream.status 更新 | 使用带 system results 的构建策略（kaniko/buildah），验证端到端自动登记链路 |
| 缺 digest 场景的 Condition 可观测性 | 使用不提供 system results 的策略或模拟空 digest，验证 `Registered=False/MissingDigest` 可被 `kubectl wait` 观测 |
| flag 关闭时 Watcher 不启动 | `--enable-buildrun-watcher=false`（默认），验证无 BuildRun informer 创建、无 reconcile 行为 |
| 注解 admission 端到端校验 | 在真实集群中验证 webhook 对非法注解的拒绝行为 |
| 自动登记后下游订阅投递 | BuildRun 自动登记 → ImageStream.status 更新 → 已有 ImageSubscription 自动投递到工作负载 |

## imageReferenceFormat 测试矩阵（v1.2，T6）

imageReferenceFormat 测试覆盖三层，对齐 ADR-009 + spec §Testing Decisions。

### envtest 单元/集成测试

| 用例 | 断言 |
|---|---|
| Mutating defaulting：空 → Digest | 创建省略 `imageReferenceFormat` 的订阅后，`spec.imageReferenceFormat == "Digest"` |
| Mutating defaulting：Update 时填充 | 已有对象 update 时若字段为空，自动填充 `Digest` |
| Validating：非法枚举值被拒绝 | `imageReferenceFormat: Invalid` → admission rejection |
| Delivery format=Digest：写入纯 digest 引用 | 目标镜像字段 = `repo@sha256:<hex>`；`resolvedDigest` 为空 |
| Delivery format=Tag：写入 tag 引用 | 目标镜像字段 = `repo:tag`；`resolvedDigest` 非空且为有效 digest |
| Delivery format=DigestWithTag：写入 OCI 复合格式 | 目标镜像字段 = `repo:tag@sha256:<hex>`；`resolvedDigest` 非空 |
| resolvedDigest 填充条件 | format=Tag/DigestWithTag 时非空；format=Digest 时为空 |
| ImageApplied 比较逻辑（Tag 模式） | tag 字符串相同但 resolvedDigest 变化 → 重新投递 |
| Format 切换：Digest → Tag | 目标镜像字段从 digest 引用变为 tag 引用；resolvedDigest 开始填充 |
| Format 切换：Tag → DigestWithTag | 目标镜像字段从 tag 引用变为复合格式 |
| 向后兼容：v1 订阅无变更 | 省略字段的已有订阅行为与 v1 一致（Digest 格式） |

### kind e2e 场景

| 场景 | 描述 |
|---|---|
| 三种格式的真实 workload 写入验证 | 分别创建 Digest/Tag/DigestWithTag 订阅，验证 Deployment 镜像字段实际写入值 |
| Tag 漂移可观测性 | format=Tag 时修改 ImageStream tag 指向新 digest，验证 resolvedDigest 更新、ImageApplied 重新投递 |
| 升级兼容性 | v1 订阅（无 imageReferenceFormat 字段）在 v1.2 集群中继续以 Digest 格式工作 |
| Mutating webhook defaulting 端到端 | 创建省略字段的订阅，验证 webhook 自动填充 Digest |
| DigestWithTag 运行时兼容性 | 验证目标 workload 能正常拉取 OCI 复合格式引用的镜像 |

## CUE Status Evaluator 测试矩阵（v1.3，T8）

CUE Status Evaluator 测试覆盖三层，对齐 ADR-009 + spec §Testing Decisions + [ADR-013](docs/adr/ADR-013-cue-status-evaluator.md)。

### 单元测试（`internal/controller/cueeval/`）

| 用例 | 文件 | 断言 |
|---|---|---|
| Happy path：合法策略 + 匹配输入 → Succeeded | `evaluator_test.go` TestHappyPath | `result.Status == "Succeeded"`，无 error |
| CompileError：语法错误策略 → CompileError | `evaluator_test.go` TestCompileError | `EvaluationError.Class == CompileError` |
| EvalError：输入不匹配 schema → EvalError | `evaluator_test.go` TestEvalError_InputMismatch | `EvaluationError.Class == EvalError`，message 含字段路径 |
| Timeout：超 100ms 评估 → Timeout | `evaluator_test.go` TestTimeout | `EvaluationError.Class == Timeout` |
| InputTooLarge：输入 JSON > 256KB → EvalError | `evaluator_test.go` TestInputSizeLimit | `EvaluationError.Reason == "InputTooLarge"` |
| Sandbox：import 非标准库 → CompileError | `evaluator_test.go` TestSandboxImportRejection | `EvaluationError.Class == CompileError`，禁止外部 import |
| Panic recovery：策略 panic → 不影响调用方 | `evaluator_test.go` TestPanicRecovery | 返回 error，goroutine 不崩溃 |
| Health policy：独立 health 策略评估 | `evaluator_test.go` TestHealthPolicy | health 结果正确，与 rollout 解耦 |
| Result fields：status/reason/message 完整输出 | `evaluator_test.go` TestResultFields | 三个字段均正确填充 |
| Cache hit：相同 resourceVersion+key → 缓存命中 | `evaluator_test.go` TestCacheHit | 第二次评估复用编译结果 |
| Clear cache：手动清除缓存 | `evaluator_test.go` TestClearCache | 清除后重新编译 |
| Invalidate cache：resourceVersion 变更 → 缓存失效 | `evaluator_test.go` TestInvalidateCache | 新版本自动重新编译 |
| Empty policy：空策略 → CompileError | `evaluator_test.go` TestEmptyPolicy | 明确错误，不默认成功 |
| Complex input：嵌套对象 + 数组输入 | `evaluator_test.go` TestComplexInput | 复杂结构正确解析和评估 |
| AsEvalError nil/wrong type：辅助函数边界 | `evaluator_test.go` TestAsEvalError* | nil 返回 false；非 EvaluationError 返回 false |
| EvaluationError.Error()：格式化输出 | `evaluator_test.go` TestEvaluationErrorMessage | 格式 `Class: Reason - Message` |

### Test Runner 单元测试（`internal/controller/cueeval/testrunner_test.go`）

| 用例 | 断言 |
|---|---|
| All pass：全部测试用例通过 | `len(results) == N`，全部 `Passed == true` |
| Some fail：部分用例失败 | 失败项 `Passed == false`，含 actual vs expected 差异 |
| Wildcard status/reason：`"*"` 通配匹配 | 通配符正确跳过对应字段比较 |
| Malformed YAML：非法 YAML → error | 返回解析错误，不 panic |
| Empty YAML/array：空内容 → 零结果 | 返回空结果集，不报错 |
| EvalError in test case：策略本身有错 | 测试结果标记为 failed，含 CompileError/EvalError 详情 |
| Index tracking：结果索引与输入对应 | 每条结果的 index 与 test-cases.yaml 行号一致 |
| Invalid policy + valid test cases：策略编译失败 | 所有测试用例标记为 failed，reason = CompileError |
| Mixed pass/fail：混合结果 | 正确区分通过和失败的用例 |
| Invalid structure：缺少 input/expected 字段 | 返回结构化错误 |

### Envtest 集成测试（待建设）

以下用例计划在后续 ticket 中落地于 `internal/controller/*_envtest_test.go`：

| 用例 | 断言 |
|---|---|
| statusPolicyRef 引用 imageflow-system ConfigMap → 评估生效 | `StatusPolicyEvaluated=True`；rolloutStatus ≠ Unknown |
| statusPolicyRef 引用非 imageflow-system namespace → admission 拒绝 | 创建 ImageSubscription 被 webhook 拒绝 |
| ConfigMap 缺失 → Unknown + Condition | `StatusPolicyEvaluated=False`，reason 指明 ConfigMap 不存在 |
| ConfigMap key 缺失 → Unknown + Condition | `StatusPolicyEvaluated=False`，reason 指明 key 不存在 |
| ConfigMap 更新 → 缓存失效 + 新策略生效 | 修改 ConfigMap 后下一个 reconcile 使用新策略 |
| CompileError 策略 → Condition Reason=CompileError | per-target rolloutStatus=Unknown + 明确错误信息 |
| Timeout 策略 → Condition Reason=Timeout | 超 100ms 策略触发 Timeout condition |
| InputTooLarge → Condition Reason=InputTooLarge | 大输入触发大小限制 |

### 覆盖率要点

- **编译缓存**：TestCacheHit / TestClearCache / TestInvalidateCache 覆盖缓存命中、清除、失效三条路径；
- **超时**：TestTimeout 使用故意超时的策略验证 100ms 硬限；
- **错误分类**：CompileError / EvalError / Timeout 三类各有独立用例，确保 Condition Reason 映射正确；
- **沙箱安全**：TestSandboxImportRejection 验证 import 禁止；TestPanicRecovery 验证 goroutine 隔离；代码审查确认无网络/文件系统调用；
- **测试 fixtures**：testrunner 测试覆盖 test-cases.yaml 的各种边界（空、畸形、通配、混合结果）。

## Native Health Determination 测试矩阵（v1.5，T5）

### 单元测试（`internal/controller/subscription_observer_test.go`）

Native health determination 的 verdict 逻辑由专用单元测试直接覆盖（rollout gate 确保 health 仅在 Succeeded 后评估），CUE 优先级同时有单元测试与 envtest 覆盖：

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestObserverDetermineDeploymentHealthStates` | Deployment verdict matrix（Healthy/Unhealthy/Unknown，含 spec D3 row 4：not Ready + restartCount=0 → Unhealthy） | 已实现 |
| `TestObserverDetermineStatefulSetHealthStates` | StatefulSet verdict matrix | 已实现 |
| `TestObserverDetermineDaemonSetHealthStates` | DaemonSet verdict matrix | 已实现 |
| `TestObserverHealthRolloutGateBlocksEvaluation` | rollout ≠ Succeeded → health Unknown，scanPodsForHealth 不被调用 | 已实现 |
| `TestObserverHealthCUEPriorityOverNative` | healthKey 存在时 CUE evaluator 优先，native 不运行 | 已实现 |

### Envtest 集成测试（`internal/controller/cueeval_envtest_test.go`）

- CUE health evaluation happy path：`TestCUEEvalEnvtestHappyPath` — healthKey 配置时 CUE 评估产出 HealthStatus=Healthy
- Backward compatibility：无 rolloutKey/healthKey 时保持 deferred，HealthStatus=Unknown

Run: `GOCACHE="$PWD/.gocache" go test ./internal/controller/... -run Health -skip 'Envtest'`

### 覆盖率要点

- **Rollout gate**：`TestObserverDetermine{Deployment,StatefulSet,DaemonSet}RolloutStates` 验证 rollout 状态判定，`TestObserverHealthRolloutGateBlocksEvaluation` 验证 health gate；
- **CUE priority**：envtest `TestCUEEvalEnvtestHappyPath` 与单元测试 `TestObserverHealthCUEPriorityOverNative` 验证 healthKey 存在时 CUE 评估生效；
- **Pod-level signals**：`scanPodsForHealth` 的 container Ready + RestartCount 逻辑由 per-kind verdict matrix 直接覆盖（含 zero-restart not-ready → Unhealthy，spec D3 row 4）；
- **Worst-case aggregation**：多 pod 场景的最差聚合由 Deployment verdict matrix 的 worst-case 用例覆盖。

## Target 规范键（v1.7-01，#78）

### 单元测试（`api/v1alpha1/common_types_test.go`）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestCanonicalTargetKeyFormat` | name 形态键编码为 `apiVersion/kind/namespace/name` 四元组（`/` 连接） | 已实现 |
| `TestCanonicalTargetKeyUniqueness` | 四字段逐一替换均产出不同键（单射） | 已实现 |
| `TestCanonicalTargetKeyStability` | 同输入多次调用输出一致（稳定性） | 已实现 |

### 覆盖率要点

- **改造点收敛**：生产代码全部 `TargetReference` map key / `==` 比较（12 处）统一走 `CanonicalTargetKey`，终扫 grep 无残留；
- **既有指纹兼容**：`mirror_synthesis.go` 的 `targetRefKey` 委托 `CanonicalTargetKey`，编码逐字节一致，事件去重指纹不变；
- **回归兜底**：12 处改造点的外部行为由既有 controller/authz/webhook 单测与 envtest 套件保持绿来兜底。

## Selector Target API + 准入校验（v1.7-02，#79）

### 单元测试（`api/v1alpha1/common_types_test.go`）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestCanonicalTargetKeySelectorFormat` | selector 形态键编码为 `apiVersion/kind/namespace/selector(<body>)`；body = matchLabels 按 key 排序 `k=v` + matchExpressions 按 (key, operator, values) 排序 `key Op v1,v2`（`;` 连接）；LabelSelector 非 nil 时恒编码 selector 形态 | 已实现 |
| `TestCanonicalTargetKeySelectorUniqueness` | 四元组字段与 selector 子句（matchLabels 值、matchExpressions values/operator）逐一替换均产出不同键；同四元组的 name 形态与 selector 形态永不冲突 | 已实现 |
| `TestCanonicalTargetKeySelectorStability` | matchExpressions 切片顺序与 matchLabels map 迭代顺序不改变键（排序后编码，确定性） | 已实现 |
| `TestCanonicalTargetKeyFormsDisjoint` | 两形态键永不撞键：`selector(...)` 包装的括号被 Name 的 DNS-subdomain pattern 排除，`name: "selector(app=x)"` 过不了 schema 校验（review #79 c2 修复） | 已实现 |

### 准入校验单元测试（`internal/webhook/webhook_test.go`）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestValidateSubscriptionShapeAccepts/selector target by matchLabels (ADR-014)` | 合法 selector-only target（内建 Kind + containerNames）通过 | 已实现 |
| `TestValidateSubscriptionShapeAccepts/selector target by matchExpressions (ADR-014)` | 仅 matchExpressions 的合法 selector target 通过 | 已实现 |
| `TestValidateSubscriptionShapeAccepts/third-party selector target by imageFields (ADR-014)` | selector 形态与 imageFields 正交组合（第三方 Kind）通过 | 已实现 |
| `TestValidateSubscriptionShapeRejects/name and labelSelector both set` | name/labelSelector 同设拒绝（互斥，ADR-014） | 已实现 |
| `TestValidateSubscriptionShapeRejects/neither name nor labelSelector set` | name/labelSelector 同缺拒绝（二选一必填） | 已实现 |
| `TestValidateSubscriptionShapeRejects/empty labelSelector` | 空 selector（matchLabels 与 matchExpressions 均空）拒绝 | 已实现 |
| `TestValidateSubscriptionShapeRejects/labelSelector with empty maps only` | 仅含空 map 的 selector 同样按空 selector 拒绝 | 已实现 |

### 覆盖率要点

- **API 形态**：`TargetReference.LabelSelector *metav1.LabelSelector` 可选；`Name` kubebuilder 校验由 Required 改 Optional；CRD 清单（imagesubscriptions / imagestreamtags / imagetags 三处内嵌 targetRef）与 `zz_generated.deepcopy.go` 经 `make generate manifests` 重新生成；
- **隐藏验收点（#78 扩展点兑现）**：selector target 进控制器前（#81），`CanonicalTargetKey` 已能区分两种定位形态且键确定性由排序保证；
- **校验落点**：name/labelSelector 校验与 T3 containerNames/imageFields 互斥同处 `validateTarget`，风格一致（field.ErrorList 聚合、Invalid 422 面），CRD 层不加 CEL（沿用 spec decision 6 分工）；
- **已知环境限制**：`test/integration` 的 `Admission webhooks [BeforeAll]`（webhook server StartedChecker 20s 超时）在 base commit 55c3e07e 上同样失败，属本机环境既有问题，与本改动无关；e2e 需 Podman，本机不跑。

## Selector Target 的 namespace 级 SAR（v1.7-03，#80）

### 准入校验单元测试（`internal/webhook/webhook_test.go`）

selector 语法深度校验（#79 review c3 移交），复用 apimachinery `ValidateLabelSelector`，与 API server 同一套规则：

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestValidateSubscriptionShapeRejects/selector with unknown operator` | 未知 operator 拒绝（"not a valid selector operator"） | 已实现 |
| `TestValidateSubscriptionShapeRejects/selector In with empty values` | In 空 values 拒绝 | 已实现 |
| `TestValidateSubscriptionShapeRejects/selector NotIn with empty values` | NotIn 空 values 拒绝 | 已实现 |
| `TestValidateSubscriptionShapeRejects/selector Exists carrying values` | Exists 带 values 拒绝 | 已实现 |
| `TestValidateSubscriptionShapeRejects/selector DoesNotExist carrying values` | DoesNotExist 带 values 拒绝 | 已实现 |
| `TestValidateSubscriptionShapeRejects/selector matchLabels with invalid key` | matchLabels 脏 key 拒绝 | 已实现 |
| `TestValidateSubscriptionShapeRejects/selector matchLabels with invalid value` | matchLabels 脏 value 拒绝 | 已实现 |
| `TestValidateSubscriptionShapeRejects/selector matchExpressions with invalid key` | matchExpressions 脏 key 拒绝 | 已实现 |
| `TestValidateSubscriptionShapeRejects/selector In with invalid value` | matchExpressions 脏 value 拒绝 | 已实现 |
| `TestValidateSubscriptionShapeAccepts/selector target using all four operators validly (v1.7-03, #80)` | In/NotIn/Exists/DoesNotExist 合法混用通过 | 已实现 |

### SAR 授权单元测试（`internal/webhook/subscription_authorize_test.go`）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestValidateCreateSelectorTargetChecksNamespaceScope` | selector target 产出不点名 SAR（`ResourceAttributes.Name==""`、verb=update、resource=deployments），allow 零警告 | 已实现 |
| `TestValidateCreateSelectorTargetDeniedRejectsAdmission` | selector deny → 拒入（IsForbidden），消息点名创建者、deny 原因与 selector 规范键 | 已实现 |
| `TestValidateCreateSelectorTargetsShareCachedNamespaceSAR` | 同 subject+GVK+namespace 的不同 selector 共享一次 SAR（缓存键含 `name=""`）；第二个订阅命中缓存零新增 SAR | 已实现 |
| `TestValidateUpdateSelectorTargetsUnchangedSkipsSAR` | spec.targets 未变化的更新零 SAR（selector 形态钉住既有短路） | 已实现 |
| `TestValidateUpdateSelectorTargetChangeDeniedRejects` | targets 变更后 deny → 拒入，消息含新 selector 规范键 | 已实现 |
| `TestValidateCreateMixedDeniedNamedWarnsSelectorRejects` | 混合 targets：named deny 保留 ADR-003 警告语义（"ZERO writes"），selector deny 拒入 | 已实现 |
| `TestValidateCreateSelectorTargetUnresolvedWarnsWithoutRejection` | selector SAR 未决（限流）只警告不拒入，交控制器周期复核兜底 | 已实现 |

### envtest（`internal/webhook/subscription_authorize_envtest_test.go`）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestSelectorTargetSARAdmission/denied namespace-level SAR rejects the admission as Forbidden` | 真实 RBAC 下创建者无 deployments update → 403 Forbidden（vimagesubscription.kb.io 路由，消息点名创建者与 selector 规范键） | 已实现 |
| `TestSelectorTargetSARAdmission/allowed namespace-level SAR admits the subscription` | 授予 deployments update 后准入通过、对象持久化、mutating 钉入创建者身份 | 已实现 |

### 覆盖率要点

- **deny 拒入只作用于 selector 形态**：named target 维持 ADR-003 警告语义（运行期 per-object 闸仍在）；selector target 面向动态对象集，准入是唯一不点名闸口（ADR-014 决策 4），未决（限流/SAR 失败/Kind 未装）维持警告 + 控制器周期复核 fail closed；
- **SAR 复用既有 TTL 缓存**：selector 形态以 `name=""` 入既有缓存键，同 subject+GVK+namespace 的不同 selector 共享一次 SAR，未新增缓存路径；
- **语法校验复用 apimachinery**：`metav1validation.ValidateLabelSelector`（零 options）落 `validateTarget` 同缝，field.ErrorList 风格、Invalid 422 面，与 T3/v1.7-02 校验一致；脏 key/value 不再流入规范键；
- **envtest 缝扩展**：共享环境暴露 `envtestEnv`/`envtestCfg` 包级 var，`AddUser` 产非 admin 身份，deny/allow 用不同用户隔离 SAR 缓存键；
- **已知环境限制**：同 v1.7-02（`test/integration` webhook BeforeAll 20s 超时系本机既有问题；e2e 需 Podman 不跑）。envtest 需 `KUBEBUILDER_ASSETS`（`make test` 经 setup-envtest 提供）；本机以 `./bin/setup-envtest use 1.33` 备齐后两方向用例实跑通过。

## 匹配解析 + 投递核心（v1.7-04，#81）

### 匹配解析单元测试（`internal/controller/subscription_delivery_match_resolver_test.go`）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestMatchResolverExcludesTerminalPodsAndForeignObjects` | 服务端过滤工厂收到 `pods\|<ns>\|app=checkout` 元组；Succeeded/Failed Pod、异 label、异 namespace 对象全部排除出匹配集（ADR-014 决策 13、spec D5） | 已实现 |
| `TestMatchResolverExcludesCompletedJobs` | Job `Complete=True` / `Failed=True` 排除，`False` 保留 | 已实现 |
| `TestMatchResolverReusesInformerPerCanonicalTuple` | 同 (GVK, namespace, selector) 元组复用同一 informer（factory 只调 1 次、informers map 长度 1）；异 selector、异 namespace 各新建 | 已实现 |
| `TestNewDynamicListerWatcherCarriesSelectorServerSide` | dynamic fake + reactor 捕获 List/Watch options：`LabelSelector == "app=checkout"`、namespace 正确——selector 进 ListWatch options 而非客户端过滤（ADR-014 决策 12） | 已实现 |
| `TestMatchResolverOnChangeForwardsEventsIncludingTombstones` | Add/Modify/Delete 事件到达 onChange sink；`DeletedFinalStateUnknown` tombstone 解包后转发；非对象载荷丢弃 | 已实现 |
| `TestMatchResolverGuards` | named 形态 ref 拒绝（"selector-form"）；Start 前 resolve 拒绝（"not started"）；坏 selector 报 "invalid label selector" | 已实现 |
| `TestIsCompletedObject` | 表驱动 10 例：Pod phase Succeeded/Failed → true；Job Complete/Failed=True → true、False → false；Deployment 无结束态语义 → false | 已实现 |
| `TestTargetRefMatchesObject` / `TestSelectorMatchesLabels` | named 精确匹配、selector 按 labels 匹配、ns/kind/apiVersion 差异不匹配、selector+Name 并存按 named、坏/nil selector fail closed | 已实现 |

### 投递扇出单元测试（`internal/controller/subscription_delivery_controller_test.go`，"Selector targets: per-object fan-out delivery" 节）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestSubscriptionDeliverySelectorFanOutPerObjectSSA` | 匹配集 2 对象各 1 次 SSA apply（applies=2、statusWrites=1、otherWrites=0）；status 条目为逐对象 name 形态（永不出现 selector 形态条目）；records/事件/指标按对象计数；稳态零写入 | 已实现 |
| `TestSubscriptionDeliverySelectorConflictSkipsOnlyContestedObject` | web-1 被 rival 独占 → 零写入、entry Conflict（消息含冲突方订阅名）、Conflict condition True、Warning `DeliveryConflict` 事件；web-2 照常投递（applies=1）；聚合 worst-state Conflict（ADR-014 决策 9） | 已实现 |
| `TestSubscriptionDeliverySelectorZeroMatchesZeroSurface` | 空匹配集：零条目、零 records、零事件、聚合空、对象零写入（NoMatches Condition 属 #82） | 已被 #82 更替（→ v1.7-05 节 `TestSubscriptionDeliverySelectorZeroMatchesNoMatchesSurface`） |
| `TestSubscriptionDeliverySelectorWithoutResolverIsHonest` | 未接线 resolver → entry InvalidTarget、聚合 Unknown、零写入（registration bug 如实上报） | 已实现 |
| `TestSubscriptionDeliverySelectorResolveErrorMapping` | `NoKindMatchError` → KindNotInstalled + waitingStateRequeue；一般解析错误 → ApplyFailed transient（RetryCount=1）+ reconcile 报错 | 已实现 |
| `TestSubscriptionDeliverySelectorOverlapDeduplicatesEntry` | 两个 selector target 匹配同一对象：resolve 各跑一次、apply 只 1 次；原断言「1 个条目、1 条 record（spec 顺序 first-wins 去重）」已被 #82 反转——聚合条目按 target 独立、重叠 selector target 各保留一条（新断言见 v1.7-05 节） | 已被 #82 更替（→ v1.7-05 节 `TestSubscriptionDeliverySelectorOverlapKeepsPerTargetEntries`） |
| `TestSubscriptionDeliverySelectorLabelRemovalPrunesEntryKeepsImage` | 摘 label 后条目被 stale-pruning 剪除、已写镜像保留（不回收、零回滚写入）、records append-only 不变（ADR-014 决策 3） | 已被 #82 更替（→ v1.7-05 节 `TestSubscriptionDeliverySelectorLabelRemovalShrinksMatchedKeepsImage`） |
| `TestSubscriptionDeliverySelectorForbiddenEntryPreservedPerObject` | 逐对象 T8 冻结契约：预置 web-1 name 形态 Forbidden 条目 → 该对象零写入、条目 DeepEqual 保留，web-2 正常投递 | 已实现 |

> **#82 更替说明**：上表中涉及 status 条目形态的断言（FanOut 的「逐对象 name 形态条目」、ZeroMatches 的「零条目」、Overlap 的「1 个条目」、LabelRemoval 的「条目剪除」、Forbidden 的「不出现 selector 形态条目」）已由 v1.7-05 的聚合语义更替（见下节）；SSA 扇出、冲突跳过、resolver 纪律等 per-object 写路径断言仍然有效。

### Watch 映射单元测试（`internal/controller/subscription_delivery_watches_test.go`）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestSubsForMatchedObjectMapsCoveringSelectorSubs` | match-informer 通道事件只映射 selector 形态覆盖它的订阅（同 GVK+ns+labels）；named 订阅、异 ns、异 GVK 不误映射；摘 label/无 label 对象映射空（DELETE tombstone 诚实性）；typed 载荷与无 GVK 载荷拒绝 | 已实现 |
| `TestSubsForWorkloadSelectorFormMapsWholeClass` | typed workload 事件：selector 订阅映射整个类（labels 满足即映射）、named 订阅保持精确名匹配、两者可同映射、无 label 陌生人映射空 | 已实现 |

### Observer 单元测试（`internal/controller/subscription_observer_test.go`）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestObserverSelectorEntriesObservedPerObject` | 引擎写入的 name 形态条目经合成 target（selector 的 locators/policy + 条目的 name ref）逐对象独立观察：web-1 Succeeded（record 治愈、双命名空间事件、指标）、web-2 仍 Progressing、聚合 worst-state Progressing + 有界轮询 requeue | 已被 #82 更替 |
| `TestObserverSelectorThirdPartyEntryHonestUnknown` | 第三方 CRD（EdgeService）selector 条目如实 Unknown：不轮询、不伪造裁决、Reason/Message 保持引擎所有、聚合 Unknown、仅订阅侧 1 条 RolloutUnknown 事件（ADR-014 决策 6、spec D11） | 已被 #82 更替 |

> **#82 更替说明**：observer 的 selector 观察在 #82 改为 per-target 聚合（name 形态合成条目观察被删除），上表两用例由 v1.7-05 小节的 `TestObserverSelectorTargetAggregateWorstCase` / `TestObserverSelectorThirdPartyAggregateHonestUnknown` 取代。

### envtest 集成（`test/integration/subscription_delivery_test.go`，manager lane）

| 用例 | 断言 | 状态 |
|---|---|---|
| `t81s-envtest-selector-fan-out-per-object-ssa-conflict-skip-and-label-driven-reconcile` | 生产接线（`RegisterSubscriptionDelivery` 含真实 matchResolver + dynamic informer）：rival 先独占 web-2 → selector 订阅扇出，web-1 真实 SSA 投递（真实 managedFields 归 selector 订阅所有）、web-2 冲突跳过（rival 所有权不变、镜像不变、聚合 entry Conflict 含冲突方名与被争对象名、matched=2、Warning `DeliveryConflict` 事件落在订阅上）；给 web-3 打 label → 服务端过滤 informer 事件驱动 reconcile、新成员即时投递且 matched 增至 3；全程 status 只有 1 条 selector 形态聚合条目（#82） | 已实现 |

### 覆盖率要点

- **服务端过滤**：selector 进 ListWatch options（reactor 捕获证明）；informer 按 `CanonicalTargetKey` 元组复用，事件经 `source.Channel` + `EnqueueRequestsFromMapFunc(subsForMatchedObject)` 驱动 reconcile；
- **逐对象纪律**：SSA/冲突/T8 冻结/stale-pruning 全部按对象独立（把 named 路径的「单 ref」泛化为「解析出的对象列表」，复用既有写契约）；
- **诚实性**：结束态对象（Completed Pod/Job）在匹配解析层排除；零匹配零 status 面；未接线 resolver 如实 InvalidTarget；第三方 CRD 观察如实 Unknown；
- **范围边界**：status 聚合精化/matched 计数/NoMatches Condition 属 #82，匹配数上限属 #83，本票不实现；
- **已知环境限制**：`test/integration` webhook lane 的 `[BeforeAll]` 超时与 e2e Podman 依赖为本机既有环境问题（同 v1.7-02 小节说明）。

## Status 聚合 + matched 计数 + NoMatches（v1.7-05，#82）

### API 面（`api/v1alpha1/imagesubscription_types.go`）

- `TargetDeliveryStatus` 新增 `MatchedObjects *int32`（json `matchedObjects,omitempty`，`Minimum=0`）：selector target 专属的匹配集计数（ADR-014 决策 5/8、spec v1.7 D8）。指针语义：named target 恒为 `nil`，selector target 零匹配为 `&0`。**只由投递引擎写入**（成员变化经 match-informer 驱动 reconcile 刷新），observer 只读不写。CRD 清单与 deepcopy 经 `make manifests generate` 再生，diff 无无关混入。

### 聚合折叠单元测试（`internal/controller/subscription_delivery_controller_test.go`）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestAggregateSelectorOutcomeFoldsWorstState` | 纯折叠函数 `aggregateSelectorOutcome` 表驱动 10 例：delivered 集折 Progressing + 合成 Applied 消息 + resetRetry；converged 集不强制 rollout（observer 车道）；conflict 压过 delivered 兄弟（rolloutSet=Conflict、reason=Conflict、消息点名对象）；首个失败按匹配序定 reason；T8 冻结对象只计数不投票（单 active 退化 passthrough、全冻结零事实）；observed 非 uniform 诚实写空、部分观察保留 persisted；一个确认未 applied 破 AND 保证；单对象退化消息/locator passthrough | 已实现 |

### 投递扇出单元测试（#81 用例按 #82 语义重写）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestSubscriptionDeliverySelectorFanOutPerObjectSSA` | 2 对象各 1 次 SSA apply（applies=2、statusWrites=1）；status 恒 1 条 selector 形态聚合 entry（matched=2、Progressing/Applied）；records=1（per-target，D8）；ImageApplied 事件 per-object 双命名空间共 4 条；applies{success}=2、latency 2 样本；稳态零写零事件 | 已重写 |
| `TestSubscriptionDeliverySelectorConflictSkipsOnlyContestedObject` | 1 条聚合 entry：Reason/RolloutStatus=Conflict、ImageApplied=false、消息含冲突方订阅名与被争对象名、matched=2；record 1 条 Conflict；Warning `DeliveryConflict` 1 条；delivered 兄弟保留 2 条 ImageApplied 事件；failures{conflict}=1、applies{success}=1 | 已重写 |
| `TestSubscriptionDeliverySelectorConflictSteadyZeroChurnAndRecovery` | 冲突生命周期三段：稳态零写零事件零 record（contested 聚合永不到 freshConverged，不重复确认兄弟）；稳态冲突中新成员加入 → 照常投递 + 双命名空间审计事件 + matched=3，但不记 Applied record（聚合仍 Conflict）；被争对象离开匹配集 → 聚合恢复（Reason=Applied、RolloutStatus Conflict→Progressing 交回 observer 车道、matched=1、Conflict condition 翻 False、confirmed record + 事件恰一次） | 已实现（新增） |
| `TestSubscriptionDeliverySelectorZeroMatchesNoMatchesSurface` | 零匹配反转 #81 零表面：1 条 entry Unknown/NoMatches/matched=0（非 nil）+ 恰 1 条 Warning `NoMatches` 事件 + 订阅聚合 Unknown；对象零写、零 record；NoMatches 不进 failure 指标词汇表（failures{no_matches} 不存在）；不阻塞同订阅其它 target；稳态零写零事件 | 已重写 |
| `TestSubscriptionDeliverySelectorOverlapKeepsPerTargetEntries` | 两个 selector target 匹配同一对象：resolve 各跑一次、apply 只 1 次（SSA 幂等）；各得一条聚合 entry（matched 各 1）、2 条 record、4 条事件（A valueChanged + B confirmed）——聚合按 target 独立，不再 per-object 去重 | 已重写 |
| `TestSubscriptionDeliverySelectorLabelRemovalShrinksMatchedKeepsImage` | 摘 label：entry 恒 1 条，matched 2→1 触发恰一次 status 写；已投递镜像保留不回收；records append-only 无 churn；零事件 | 已重写 |
| `TestSubscriptionDeliverySelectorForbiddenEntryPreservedPerObject` | 冻结对象只计数不投票：预置 name 形态 Forbidden entry DeepEqual 原样保留（stale-pruning 豁免）+ selector 聚合 entry 出现（matched=2 含冻结对象、active-only 事实）；web-2 正常投递 | 已重写 |
| `TestSubscriptionDeliverySelectorWithoutResolverIsHonest` / `TestSubscriptionDeliverySelectorResolveErrorMapping` | #81 断言原样有效（InvalidTarget 如实上报 / NoKindMatchError→KindNotInstalled+requeue、一般错误→ApplyFailed transient） | 不变 |

## 匹配数硬上限（v1.7-06，#83）

### 配置单元测试（`internal/config/config_test.go`）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestSelectorMatchLimitFlag` | `--selector-match-limit` 默认值 = 50 = `config.DefaultSelectorMatchLimit`（ADR-014 决策 7）；`=7` 解析生效且 Validate 通过；0 / 负值被 Validate 拒绝（"must be positive"） | 已实现 |

### 引擎单元测试（`internal/controller/subscription_delivery_controller_test.go`，"Selector targets: match-count hard limit" 节）

> #82/#83 合并后，本节断言改写在聚合表面上：breach 压过 per-object 折叠（rolloutSet 强制 Failed、Reason=`MatchLimitExceeded`），matched 仍计**全量**观测成员（含被截断者——成员是事实，投递受限），record 恒 per-target 一条。

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestSubscriptionDeliverySelectorMatchLimitTruncatesAndReports` | limit=2、匹配 3（名字排序）：只投递前 2 个（applies=2、statusWrites=1；web-3 零写入）；status 恒 1 条 selector 聚合 entry：Failed/`MatchLimitExceeded`（消息含观测数 3 与上限 2）、matched=3（全量成员含截断者）、delivered prefix 聚合事实 ImageApplied=true/ObservedImage=DesiredImage=digest；record 1 条（selector 键、NewImage=digest）；聚合 Failed；恰好 1 条 Warning（含 flag 名与两个数量）；ImageApplied 事件 per-object 双命名空间 4 条；`failures{match_limit_exceeded}`=1；稳态第二次 pass 零写入零事件、条目 Message 不变 | 已按聚合语义重写 |
| `TestSubscriptionDeliverySelectorMatchLimitBoundaryNotExceeded` | 恰等于上限（2/2）不触发：正常扇出 applies=2；1 条聚合 entry（matched=2、`assertT9cDeliveredEntry`）；聚合 Progressing；无 Warning | 已按聚合语义重写 |
| `TestSubscriptionDeliverySelectorMatchLimitDefaultFifty` | 零值 `config.Config{}` 经构造器归一化回退默认 50：51 个匹配（web-000..web-050）只投递前 50、web-050 零写入；聚合 entry Failed/`MatchLimitExceeded`、matched=51、消息含 51/50 | 已按聚合语义重写 |
| `TestSubscriptionDeliverySelectorMatchLimitQuotaExcludesCompletedObjects` | 配额针对「有效匹配集」（ADR-014 决策 13）：Job 类 target、3 个匹配其一 `Complete=True`，fake-client lister 替身（resolver 接口注释钦定模式）排除后有效集 2 = limit → 不超限；聚合 entry 正常报告（Applied/Progressing/matched=2——Completed 从不进匹配集，不占配额）；Completed Job 零写入、无 Warning | 已按聚合语义重写 |

### Observer 单元测试（`internal/controller/subscription_observer_test.go`）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestObserverSelectorTargetAggregateWorstCase` | 表驱动 6 例覆盖票面全 verdict 路径（全 Succeeded、混 Failed、混 Unknown、Progressing、零匹配、单对象退化）：per-object verdict 永不落 status；聚合 worst-case（`AggregateRolloutStatus` 同排名）落 selector entry；transition 事件恰 1 条仅订阅侧，文案含有界摘要（`worst-case aggregate X over N matched object(s): web-1=…`，上限 5 个 + `+N more`）；record 按聚合治愈/保持 in-flight；observer 不写 matched（`MatchedObjects==nil`）；engine-owned Reason/Message 不被 observer 覆写；Progressing 聚合保持有界轮询 requeue；零匹配聚合 Unknown + 事件含「matched zero live objects」 | 已实现（取代 #81 per-object 观察用例） |
| `TestObserverSelectorThirdPartyAggregateHonestUnknown` | 第三方 CRD selector 聚合如实 Unknown：不轮询、不伪造裁决、Reason/Message 保持引擎所有、record in-flight、1 条订阅侧 RolloutUnknown 事件含 `edge-1=Unknown`、指标 {other,unknown}=1 | 已重写 |
| `TestObserverSkipsMatchLimitExceededSelectorEntry` | selector 键条目是引擎所有的 target 级终态报告：observer 在**门禁处跳过整个 target**（先于 match resolution——matcher 零调用、零轮询、零改写、零事件、RequeueAfter=0、ResourceVersion 不变）；interceptor 注入真实 API 对空 name Get 的 "resource name may not be empty" 非 NotFound 错误形态（裸 fake client 会退化为 NotFound，故无守卫时本测试为真 RED）；聚合保持 Failed | 已按聚合语义重写 |

### envtest 集成（`test/integration/subscription_delivery_test.go`，manager lane）

- `t81s-envtest-selector-fan-out-per-object-ssa-conflict-skip-and-label-driven-reconcile` 断言按 #82 更新：全程 status 只有 1 条 selector 形态聚合 entry（matched=2→3 随成员增长、Conflict 消息含冲突方与被争对象名）；per-object SSA/冲突跳过/label 驱动 reconcile 的生产接线断言不变。本机以 `KUBEBUILDER_ASSETS`（`bin/k8s/1.33.0-darwin-arm64`）实跑通过。

### 覆盖率要点

- **聚合位置**：engine 在 `deliverSelectorTarget` 内折叠出单条聚合 outcome（`aggregateSelectorOutcome` 纯函数，可单测）；observer 在 `observeSelectorTarget` 内对每对象复用 named 车道 `observeTarget` 合成 ephemeral verdict 后聚合，两侧共用 `AggregateRolloutStatus` 排名（Failed > Conflict > Unknown > Progressing > Superseded > Succeeded）；
- **matched 所有权**：只由 engine 写（informer 成员事件驱动刷新）；observer 只读——单元测试双侧 pin `MatchedObjects` 的写与不写；
- **per-object 明细只走 Events**（spec D8）：投递/确认/失败 Warning/冲突 Warning 全部按对象计划（`planDeliveryEvents`、per-object failure 循环），latency/applies/failures 指标按对象计数；status 有界 3/5/5 合约不变，record 恒 per-target 一条；
- **NoMatches 为合法态**（ADR-014 决策 8）：Warning 事件有、failure 指标无（词汇表刻意排除）；零匹配按 Unknown 参与订阅聚合，不阻塞其它 target；
- **Conflict 生命周期**：worst-state 压过 delivered 兄弟（record 跟随最坏态）；稳态零 churn；被争对象离开匹配集后聚合恢复 Conflict→Progressing（observer 的 Conflict 门禁会跳过 entry，engine 必须显式交还车道，否则永久卡死）；
- **上限判定时机（#83）**：上限检查位于 `deliverSelectorTarget` 中 matchResolver 返回有效匹配集（已排除结束态对象）之后、逐对象扇出之前；截断按名字排序取前 `limit` 个，行为确定；超限部分不投递、不 requeue、不报错（终态语义同 Conflict）；合并后 breach 覆写聚合折叠——单条聚合 entry Failed/`MatchLimitExceeded`，matched 仍计全量观测成员；
- **上限报告路径（#83）**：`planEffects` reasonChanged 路径发 Warning（消息含上限值与观测数），第二次 pass 持久化 reason 相同即不重发；`limitSteady` 期间 converged 车道与兄弟 confirmed 事件静默、真实写入照常审计；`deliveryFailureLabel` 新增 `match_limit_exceeded`（指标 reason 词汇 11→12）；
- **归一化单点（#83）**：`NewSubscriptionDeliveryReconciler` 对 `SelectorMatchLimit <= 0` 回退 `config.DefaultSelectorMatchLimit`——零值配置（存量测试、嵌入方）不会以 limit=0 误伤全部匹配；flag 层 Validate 另行拒绝非正值；
- **observer 与 engine-owned 终态**：`NoMatches`/`MatchLimitExceeded` 均列入 observer 的 selector 门禁与 `isEngineOwnedReason` 冻结集——observer 不改写、不与 engine 强制的 rollout 拉锯；成员缩回限内后 engine 翻转 Reason，observer 车道自动恢复观察（Failed 是可 transition 态，无卡死）；#83 时代 `observeTarget` 内的结构式守卫（`LabelSelector != nil && Name == ""` 跳过）保留为防御；v1.7-04 的「永不出现 selector 形态条目」断言自 #82 起被完全更替——selector target 恒一条聚合 entry；
- **已知环境限制**：e2e 需 Podman（本机 socket 拒连，不跑）；`test/integration` webhook lane `[BeforeAll]` 超时与 Manager metrics 单跑 flake 为本机既有环境问题（同 v1.7-02 小节说明）。

## 动态集行为：摘 label 保留 + per-object Superseded（v1.7-07，#84）

### 引擎单元测试（`internal/controller/subscription_delivery_controller_test.go`）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestSubscriptionDeliverySelectorDynamicSetLifecycle` | 单订阅动态集全生命周期六阶段（ADR-014 决策 3/10、spec D9）：phase 0 合法空集零写入（matched=0、NoMatches）；phase 1 成员加入 → 下一 pass 立即投递当前 digest（applies=1、web-1=digestA、非成员零写入、record 1 条 in-flight、ImageApplied 双命名空间）；phase 2 第二个成员加入已收敛目标 → 只投递新成员、同 digest 不翻转历史（窗口 2 条均 digestA 非 Superseded）；phase 3 web-1 摘 label → 镜像保留（不回收）、matched 缩至 1、零 churn 零事件；phase 4 新 tag → 只覆盖集内 web-2（web-1 保 digestA、web-3 未入集不动）、聚合 entry digestB/Progressing、窗口 [digestB in-flight, digestA Superseded ×2]（聚合 latest-wins）、DeliverySuperseded 事件 4 条（2 翻转 × web-2 × 双命名空间）全部点名 web-2 且无一提及 web-1；phase 5 tag 移动后新成员加入 → 立即获得**当前** digestB（web-2 零写入）、同 image 不翻转（窗口 4 条：2 条 digestB 非 Superseded + 2 条 Superseded 尾部保持终态）、事件仅 web-3 ImageApplied 双命名空间 | 已实现 |

### 状态写入器单元测试（`internal/controller/subscription_observer_test.go`）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestSupersededEventsPerObjectForSelectorTargets` | per-object Superseded 纯函数语义（`mergeRecentDeliveries`→`supersededEvents`，ADR-014 决策 10、spec D9）：selector 形态记录翻转保持聚合 latest-wins 不变；每个**实际被新 tag 覆盖写入**的对象（objectOutcomes.delivered）各得一条 DeliverySuperseded 事件，targetSide+targetObject 骑对应对象（双命名空间），消息含 latest-wins/ADR-008/ADR-014 decision 10/对象 ref/old+new image/源/no retry/no cross-target rollback；摘 label 对象不在本 pass objectOutcomes → 永不被点名（消息不含其名字）；冲突跳过对象（delivered=false）不记；零覆盖翻转（如 conflict 车道记录）→ 恰 1 条订阅侧聚合事件（!targetSide）诚实声明 zero overwritten；named 形态回归锚：恰 1 事件、targetSide、消息前缀逐字 `Delivery superseded (latest-wins, ADR-008):` 不突变 | 已实现 |

> 既有回归锚继续钉住 named 行为：`TestSupersededEventPlannedOnLatestWins`（消息词汇全量）与 `subscription_delivery_format_envtest_test.go` 的双命名空间计数（==2）。

### envtest 集成（`test/integration/subscription_delivery_test.go`，t84 lane）

- `t84-envtest-dynamic-set-label-removal-retention-and-per-object-superseded`（独立 Ordered Context，production registrar `subscription-delivery-t84`）：初始扇出双成员 digestA/matched=2；**摘 web-1 label 经服务端过滤 informer 的 DELETE 通道驱动 reconcile**（apiserver 对离开 selector 的对象发携带 PrevObject 旧 labels 的 DELETED 事件，`subsForMatchedObject` 客户端复核通过 → 订阅入队）→ matched 缩至 1、Reason 保持 Applied、web-1 镜像保留 digestA 不回收；retag digestC → 只覆盖集内 web-2、web-1 保 digestA 不算 Superseded、窗口首条 digestC、digestA 记录翻转 Superseded；DeliverySuperseded 事件（按 t84-stream 过滤）≥2 条（订阅侧+对象侧）、全部点名 web-2 无一提及 web-1、对象侧副本骑 web-2 Deployment（双命名空间实证）；tag 移动后新建 web-3 → 立即获得当前 digestC、matched=2。本机以 `KUBEBUILDER_ASSETS`（`bin/k8s/1.33.0-darwin-arm64`）实跑通过（7.2s）。

### 覆盖率要点

- **per-object Superseded 的实现位置**：`internal/controller/subscription_delivery_status.go` 新增 `supersededEvents`（`mergeRecentDeliveries` 翻转处调用）：named 形态原样委托既有 `supersededEvent`（T11 词汇逐字不变）；selector 形态从同 canonical key 聚合 outcome 的 `objectOutcomes` 过滤 `delivered==true`（=实际被新 tag 覆盖写入），每对象一条事件骑该对象；记录翻转本身（聚合 latest-wins）与翻转条件完全不动；
- **「覆盖写入」的判定**：`objectOutcomes[i].delivered` 仅在值变更 apply 落地时为 true（冲突跳过、已收敛零写入、Job immutable 特例均为 false）——Superseded 以**实际投递**为准（ADR-008），与决策 10 一致；
- **摘 label 不记 Superseded 的机制**：退出匹配集的对象根本不出现在本 pass 的 `objectOutcomes` 里（resolver 只报告当前成员），故任何 supersede 事件都点不到它的名；其已写镜像不回收（无 rollback 路径存在）、仅从 matched 计数与聚合折叠中消失；零覆盖翻转（全员冲突/收敛时聚合记录仍按 latest-wins 翻转）走订阅侧兜底事件，明确声明 zero objects overwritten；
- **新成员立即投递**：引擎侧无新代码——#81 的 informer 事件通道（ADD/UPDATE/DELETE → `subsForMatchedObject` → 入队）+ 每 pass 全量重解析匹配集已保证「下次 reconcile 投递当前 desired image」；本票用单测 phase 0→1/1→2/4→5 与 t84 lane（tag 移动后新建对象直接拿 digestC）把端到端行为钉死，未发现缺口，故未补实现；
- **事件词汇零新增**（与 #85 并行约束）：per-object 审计复用既有 `EventReasonDeliverySuperseded`/Normal/1024 截断与双命名空间发射机制（`emitPlannedEffects`），未新增指标、未动 `delivery_metrics.go` 标签集；
- **watch DELETE 语义实证**：t84 lane 首次用真实 API server（1.33）证明摘 label 触发链无缺口——DELETED 事件携旧（匹配时）labels 到达客户端复核，`subscription_delivery_watches_test.go` 的「non-matching labels map nothing」pin 与该行为一致，无需修改 watch 实现；
- **已知环境限制**：e2e 需 Podman（本机 socket 拒连，不跑）；`test/integration` webhook lane `[BeforeAll]` 超时为本机既有环境问题（同 v1.7-02 小节说明）。

## Metrics 基数收口（v1.7-08，#85）

### 基数回归单元测试（`internal/controller/metrics_cardinality_test.go`）

> #85 审计结论：T9/T11 现有标签集合规——对象名/命名空间/UID 从未进入任何标签键或标签值；selector 扇出的 latency/applies/failures 计数是 per-object 的（粒度合规），标签维度恒为 subscription+target 级。本节为**回归钉住**而非修复，三层设防（ADR-014 决策 11、spec v1.7 D10、ADR-010 受控标签集）。

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestMetricsCardinalityFrozenLabelKeySets` | 定义层：newDeliveryMetrics()+newObserverMetrics() 注册进私有 PedanticRegistry、各触碰一条合法 series 后 gather 恰为冻结的 6 个 family；每条 series 的标签键集合与契约**完全相等**（latency={}、applies={result}、failures={reason}、truncated={}、rollout={kind,result}、health={result}）；cardForbiddenLabelKeys 全表（name/namespace/uid/object/target/workload/pod/container/subscription/stream/tag/image 等）永不出现 | 已实现 |
| `TestDeliveryFailureLabelVocabularyIsClosed` | 词汇层：12 个 TargetReason 失败原因恰好映射到对应 snake_case 标签（ok=true）；其余一切——NoMatches/Applied 合法非失败态、空串、对象名/命名空间/订阅名/镜像引用形状的字符串、未来新增 reason——恒 `("", false)`，未登记原因永不计数 | 已实现 |
| `TestObserverLabelVocabulariesAreClosed` | 词汇层：observerResultLabel 把全部 RolloutStatus（含 Superseded/Conflict/空串/任意字符串）折叠进 4 值闭集；observerHealthLabel 同理 3 值；observableBuiltinKind 只映射 5 个 builtin GVK 到有界 kind 常量，第三方 GVK / imageFields / 无 containers 恒 `("", false)`（调用方折叠为 `other` 常量，raw kind 串永不作标签值） | 已实现 |
| `TestSelectorDeliveryMetricsNeverCarryObjectIdentity` | 行为层：表驱动 5 条 selector 车道真实 reconcile（全量匹配 3 对象、ghost 匹配、limit=2 截断、per-object 冲突、零匹配合法态），每车道以私有 registry gather 全部发射 series 并断言 family ⊆ 冻结契约、键集精确相等、标签值 ∈ 封闭词汇、键/值不含任何对象身份片段（web-/ghost/uid85/team-checkout/checkout/live/quay.io/sha256）；车道计数钉住非真空（applies{success}=3/2/2/1、latency (3,1800s)、failures{target_not_found\|match_limit_exceeded\|conflict}=1、零匹配车道 failures/applies 零 series——NoMatches 永不进标签） | 已实现 |
| `TestSelectorObserverMetricsNeverCarryObjectIdentity` | 行为层：双对象 selector worst-case 折叠（web-1 Succeeded + web-2 deadline Failed → 聚合 Failed）：对象名只出现在 Events（`web-1=Succeeded`/`web-2=Failed`——D10 per-object 可观测性由 Events 承担）；指标面恰 1 条 rollout{deployment,failed} series + 1 条 health series（per 聚合 transition，永不 per object）；同套冻结契约 + 身份片段断言 | 已实现 |

### 变异自证（mutation check，证明断言非真空；变异验证后均已还原）

- `deliveryFailureLabel` 默认分支放行未知 reason（`return reason, true`）→ 词汇层 12 例 FAIL + 行为层各车道 FAIL（`reason="Applied"`/`"NoMatches"` 越出封闭词汇）；
- per-object 失败标签拼入对象名（`label+"|"+obj.ref.Name`，subscription_delivery_status.go 的 objectOutcomes 循环）→ 行为层 ghost/冲突车道 FAIL（`reason="target_not_found|web-ghost"`、`"conflict|web-1"` 同时命中「越出封闭词汇」与「携带对象身份片段」两条断言）；
- `observerResultLabel` 默认分支透传原始状态串 → 词汇层 FAIL（Superseded/Conflict/空串/`web-1` 全部泄出为标签值）。

### 契约文档同步（`docs/metrics-contract.md`）

- §T9 基数段后新增 selector 维度说明：计数 per-object、标签恒 subscription+target 级、对象名/UID 永不进标签、NoMatches 不计数（ADR-014 决策 8）、match_limit_exceeded 为 target 级计数（每次截断 pass 计 1，不按被排除对象逐个计）；
- §T11 修正既有漂移：kind 词汇补 `job`/`cronjob`（v1.6 #74 引入的 builtin kind，文档此前未同步），series 上限 19→27（6 kinds × 4 results + 3 health）；补 selector 聚合折叠说明（每聚合 transition 恰一条 rollout sample + 一条 health sample）；
- `internal/controller/subscription_observer_metrics.go` rollout Help 字符串同步为 `(deployment|statefulset|daemonset|job|cronjob|other)`（Help 为元数据文案，不属于 §Stability 的 names/label-keys 契约面）。

### 覆盖率要点

- **三层设防**：定义层（collector 定义中不存在身份维度）→ 词汇层（未登记值拿不到标签）→ 行为层（真实车道 gather 结果无身份）——任何一层被将来的改动突破，测试都 RED；
- **计数粒度与标签维度分离**：per-object 计数（latency/applies/failures 按对象计）合规且被车道计数断言钉住；违规的只有标签维度出现对象身份——正是 ADR-014 决策 11 划定的边界；
- **私有 registry 收集缝**：`prometheus.NewPedanticRegistry()` + `collectors()`/`registerDeliveryMetrics`/`registerObserverMetrics` 既有缝，不污染进程级 `ctrlmetrics.Registry`；fixture 的 metrics 每测试独立构造，series 不串台；
- **与 #84 并行边界**：本小节零行为代码改动（唯一实现侧 diff 是 Help 字符串文案）；`observerResultLabel` 对 Superseded/Conflict 的折叠在词汇层已断言，#84 改动行为路径不影响本约束；
- **已知环境限制**：同 v1.7-02 小节（e2e 需 Podman，本机不跑；`test/integration` webhook lane `[BeforeAll]` 超时为本机既有环境问题）；本小节全部为纯单元测试，不依赖 envtest/Podman。

## Selector Target 全场景 E2E（v1.7-09，#86）

### kind E2E 车道（`test/e2e/selector_e2e_test.go`）

> 车道设计：`Describe(..., Label("t86"), Ordered)`，双命名空间（订阅侧 `t86-selector-e2e` + 工作负载侧 `t86-selector-e2e-wl`），完全沿用 t11 车道协议——生产 manager 镜像以裸 Pod 启动并绑定**生成的** `manager-role`（车道级 ClusterRoleBinding，绝不手写规则）、webhook 不活跃（无 `--webhook-cert-path`）故 `created-by` 注解手工注入、creator SA 在两个命名空间持有 update 级 Role（selector 的运行时 T8 SAR 是命名空间级、无 resourceName，ADR-014 决策 4，凭该 grant 得 Allowed）。manager 额外带 `--selector-match-limit=3`：US-9 的启动旋钮（出厂默认 50 由 `internal/config` 单测钉住），超限场景用 4 个对象即可触达 MatchLimitExceeded。全部工作负载为 replicas=0 Deployment + 虚构 digest（t12 硬化车道模式）：永不拉镜像，原生控制器确定性地把 rollout 收敛到 Succeeded。匹配域是对象 metadata 上的 `t86s-app=<场景>` 标签；Pod selector 用独立的 `t86s-name` 标签——摘 label 场景因此永不触碰不可变的 `spec.selector`。

| 场景 | 用例（It） | 关键断言 | 状态 |
|---|---|---|---|
| 多匹配投递 | `t86-e2e-selector-fanout-delivers-one-digest-to-every-matched-deployment` | 恰好 3 个成员（=limit，`observed > limit` 才超限，等值是合法边界，US-1）：3 个 Deployment 全部持有 digest；status 仅**一条**聚合条目（D8）——`targetRef.name` 为空、`labelSelector` 原样回显、无第二条 per-object 条目；`matchedObjects=3`（US-16）、Reason=Applied、imageApplied=true；observer 把 per-object 判定折叠为 Succeeded 聚合（US-15）；每个对象双命名空间 ImageApplied 审计、消息含 `previous field value: registry.k8s.io/pause:3.9`（US-17）；聚合 RolloutSucceeded 事件只在订阅侧、对象侧为空（selector 聚合无单一宿主对象） | 已实现（CI 已实跑） |
| 扩容拾取 | `t86-e2e-scale-out-new-member-picks-up-current-digest-without-a-trigger` | 2 个创始成员收敛后新建第 3 个同标签 Deployment：server-side-filtered informer 拾取（无轮询、无任何人工触发，US-2/US-25），自动拿到**当前** desired digest；`matchedObjects` 增长到 3；迟到者获双命名空间 ImageApplied；observer 对扩大的集合重新折叠回 Succeeded | 已实现（CI 已实跑） |
| 摘 label 保留 | `t86-e2e-label-removal-retains-image-shrinks-matched-never-supersedes-departed` | 3 成员收敛（**门控在 observer 的 Succeeded 治愈之后**）→ `kubectl label ... t86s-app-` 摘标签：`matchedObjects` 收缩为 2、Reason 保持 Applied（US-4）；离开者镜像**保留不回收**且 managedFields 仍持有订阅的 field manager（引擎无 recall 路径，US-3）；任何位置（事件/交付窗口）都**没有** Superseded（US-18：Superseded 只属于被实际覆盖的对象）；随后 retag 只覆盖在册成员——离开者保持旧 digest、在册者拿到新 digest、聚合窗口最新记录为 retag 且仍无 Superseded 翻转（创始记录已终态，latest-wins 无可翻转对象；**在途翻转**由单元车道（假时钟，`TestSubscriptionDeliverySelectorDynamicSetLifecycle` phase 4 窗口）+ envtest t84（真实时钟 `clock.Real{}`、delivery-only lane——无 observer，确定性由此而来）确定性覆盖，本车道不与之抢竞态） | 已实现（CI 已实跑） |
| 零匹配（NoMatches） | `t86-e2e-zero-match-reports-unknown-nomatches-warning-and-unblocks-siblings` | 组合订阅 `[具名 sibling, ghost selector]`（status.targets 按 spec 序）：sibling 正常投递且条目形如 `name\|\|Applied\|`（具名目标 matchedObjects 恒 nil——只有 selector 计数）；ghost 条目 `\|0\|NoMatches\|Unknown`（合法空集，US-11）；订阅级聚合折叠为 **Unknown 而非 Failed**（D7：NoMatches 不参与降级为失败）；订阅侧 **Warning** 事件 reason=NoMatches，消息含 `matched zero live objects`、`legal empty match set`、`ADR-014 decision 8`；ghost 不阻塞 sibling——sibling 治愈到 Succeeded 而聚合诚实保持 Unknown（US-12） | 已实现（CI 已实跑） |
| 超限（MatchLimitExceeded） | `t86-e2e-match-limit-breach-delivers-sorted-prefix-and-reports-failed` | 4 个成员 > limit 3：仅按名字典序前缀（flood-1..3）投递，第 4 个对象镜像保持 pause:3.9 且 **managedFields 中无订阅 field manager**（零写入用 SSA 所有权证明，不只看镜像字段；格式断言双侧锚定——已投递成员必含、越限成员必不含）；`matchedObjects=4` 报告**完整观测数**；Reason=MatchLimitExceeded、条目与聚合 rolloutStatus 均 **Failed**（Failed 是最坏折叠，US-10/US-15）；订阅侧 **Warning** 事件消息含 `matched 4 objects, over the hard limit of 3`、`--selector-match-limit`、`remaining 1 are NOT delivered`；前缀成员照常获双命名空间 ImageApplied 审计且越限对象的名字从不出现在任何投递审计里；observer 永不改写该判定（MatchLimitExceeded 条目在其 gate 之外） | 已实现（CI 已实跑） |

### Allure 可追溯标签（ADR-009）

既有 e2e 车道为 Ginkgo 套件、尚无 allure-go 运行器；按 TESTING.md §Allure 记录的过渡约定，标签映射以文件头与逐 It 注释块形式**全量声明**（套件迁移时机械附加），与 `ao.*`（`test/utils/allure/options`）词汇一一对应：

| Allure 维度 | 取值 |
|---|---|
| AllureID | `IF-86` |
| TMS | `IF-86` → https://github.com/appfun-dev/imageflow/issues/86；`SPEC-77` → https://github.com/appfun-dev/imageflow/issues/77 |
| Epic | `Phase 1.7: Label Selector Multi-Target` |
| Feature | `ADR-014 Selector Target` |
| ParentSuite / Suite | `e2e` / `t86-selector` |
| Link（docs） | `docs/spec/imageflow-v1.7-label-selector-multi-target.md`、`docs/adr/ADR-014-label-selector-multi-target.md` |
| Story（按场景） | fanout `US-1, US-15, US-16, US-17`；scale-out `US-2, US-25`；label-removal `US-3, US-4, US-18`；zero-match `US-11, US-12`；limit-breach `US-9, US-10` |

### 覆盖率要点

- **恰好等限是合法集**：引擎的越限判定是 `observed > limit`（严格大于），fanout 场景用 3 成员对 limit 3 钉住这个边界——少一个对象都不算覆盖；
- **聚合形态钉死（D8）**：每个 selector 目标恒一条 status 条目——无名、回显 selector、`matchedObjects` 计数；per-object 细节只走 Events（US-17），fanout 场景同时断言「无第二条条目」；
- **聚合 transition 事件只落订阅侧**：selector 聚合无单一宿主对象（`ev.ref.Name` 为空 → 对象侧拷贝跳过），fanout 场景以「订阅侧 RolloutSucceeded 非空 + 对象侧为空」双向钉住；
- **matchedObjects 语义分叉**：selector 条目计数（含合法 0）、具名条目恒 nil——零匹配场景的 targets 转储（`name|matched|reason|rolloutStatus` 逐行、序无关）同时验证两种形态；
- **摘 label 的确定性策略**：retag 门控在 observer Succeeded 治愈之后，使「latest-wins 无在途记录可翻」成为确定性语义而非竞态；在途 Superseded 翻转与双命名空间 DeliverySuperseded 审计归单元车道（假时钟，`TestSubscriptionDeliverySelectorDynamicSetLifecycle` phase 4 窗口）+ envtest t84（真实时钟、delivery-only lane，无 observer 注册——确定性由此而来）——两层互补、互不重复；
- **零写入的证明强度**：越限场景不止断言镜像未变，还断言 managedFields 中**不存在**订阅的 field manager（`imageflow.appfun.dev/sub-<ns>-<name>`，与 `internal/delivery.FieldManagerFor` 同构，<128 字符故无哈希后缀）；
- **NoMatches/MatchLimitExceeded 事件断言同时钉住类型与文案**：`type|message` 行断言 Warning 类型 + 引擎消息的稳定子串（`matched zero live objects`/`legal empty match set`/`ADR-014 decision 8`；`over the hard limit of 3`/`--selector-match-limit`/`remaining 1 are NOT delivered`），文案漂移会 RED；
- **本机未实跑声明**：本机 Podman socket 拒连、kind 集群无法创建（同 v1.7-02/07/08 小节），本车道**首跑在 CI/有 Podman 的环境**；本机验收为编译级证据——`go build ./...`、`go vet ./test/e2e/...`、`gofmt -l .` 全清洁，`go test -c ./test/e2e/ -o /dev/null` 编译通过，`go test ./test/e2e/ -run xxx -count=1` 返回 `ok ... [no tests to run]`。

## Argo Rollouts 内建 Kind 支持测试矩阵（v1.8，#91）

v1.8（ADR-015，`docs/spec/imageflow-v1.8-argo-rollouts-support.md`）把 Argo Rollout（`argoproj.io/v1alpha1`）
纳入内建 Kind。本节是 v1.8 全部测试 lane 的总览索引（对齐 spec §Testing Decisions 六条缝）；各票的完整用例表、
断言与覆盖率要点见紧随其后的 v1.8-01 / v1.8-02 小节，此处不重复。

| 票 | Lane | 层 · 文件 | 覆盖要点 |
|---|---|---|---|
| v1.8-01（#92） | Adapter 单测 | unit · `internal/delivery/delivery_test.go` | Rollout 精确 GVK 命中内建注册表；Plan 只声明具名容器字段（与 Deployment 同形 apply-nest）；Classify 走 KindAdapter 车道 |
| v1.8-01（#92） | SSA 升级单测 | unit · `internal/controller/subscription_delivery_controller_test.go` | 首发 409（foreign manager `rollouts-controller` 持有 image 字段，上游 argo/argo-rollouts#3281 形态）→ force 重试恰一次 + Warning `ApplyConflictEscalated`；持续冲突如实 Failed（预算 1/pass 用尽，无 retry bookkeeping）；named / selector 两投递形态端到端 |
| v1.8-01（#92） | envtest 双 lane | envtest · `internal/controller/subscription_delivery_rollout_envtest_test.go` | lane A：vendored Rollout CRD（无 rollouts-controller）真实 SSA 写路径 + 二次 pass 稳态零写入；lane B：无 CRD → `KindNotInstalled` 等待态（spec D9） |
| v1.8-02（#93） | D5 判定矩阵 18 例 | unit · `internal/controller/subscription_observer_test.go` | untrusted 三种 / 三合一 Succeeded（含两负例）/ Failed 三 phase / `pause: {}` → Progressing + `AwaitingPromotion` / pod 镜像失败 / Unknown 类；两跳 RS owner 链 pod 失败 |
| v1.8-02（#93） | D4 提取 | unit · `internal/controller/subscription_observer_test.go` | unstructured 逐字段提取断言 + `observedGeneration` 字符串防御解析边界表（空白/非数字/小数/溢出/缺失 → 不信任 status） |
| v1.8-02（#93） | envtest 5+1 例 | envtest · `internal/controller/subscription_observer_argorollout_envtest_test.go` | CRD 在、controller 不在（status 经 /status 子资源手工落库）：5 例 D5 决定性行端到端 + 1 例提取断言（含 API Server Create 剥离 status 的自证） |
| v1.8-02（#93） | 契约扩充 | unit · `internal/controller/metrics_cardinality_test.go`、`subscription_observer_test.go` | 失败 reason 闭集**负表**加 `AwaitingPromotion`（进 health 词汇、绝不进 failure 指标词汇，ADR-015 决策 8 / spec D10）；observer kind 闭集加 `rollout`（系列上限 27→31）；`isEngineOwnedReason` 守卫放行 |
| v1.8-04（#95） | kind e2e（t87 车道） | kind · `test/e2e/rollout_e2e_test.go`（CI 安装 argo-rollouts v1.10.2，pin install.yaml） | 两场景表与断言详见下文 v1.8-04 专节：spec D11 (a) 无 pause canary 收敛 Succeeded（真实 digest + 决策 7 三合一全兑现）；(b) `pause: {}` 步停 → Progressing + `AwaitingPromotion`（no-op 播种 Unknown，Consistently 70s 永不 Succeeded）；(c) SSA 409 升级明确只在 unit 层（已由 v1.8-01 覆盖）；envtest 无 controller 无法闭环，故收敛判定只在 kind e2e；本机未实跑（无 Podman）、首跑在 CI `E2E Tests` job |

## Argo Rollout 投递支持（v1.8-01，#92）

### Adapter 单元测试（`internal/delivery/delivery_test.go`）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestRolloutAdapterIsRegistered` | `argoproj.io/v1alpha1, Kind=Rollout` 精确 GVK 命中内建注册表（D2：init 注册，零引擎改动） | 已实现 |
| `TestBuiltInPlanDeclaresOnlyTheNamedContainerImages`（Rollout 行） | 表驱动沿用五内建 Kind 同一缝：Rollout fixture 的 Plan 只声明具名容器字段，路径为标准 apply-nest `.spec.template.spec.containers`（与 Deployment 同形） | 已实现（表加行） |
| `TestClassifyServesFirstBatchAndRefusesTheRest`（Rollout 行） | Classify 对 Rollout 走 KindAdapter 车道；不新增分类枚举 | 已实现（表加行） |

### 引擎单元测试（`internal/controller/subscription_delivery_controller_test.go`）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestSubscriptionDeliverySSAConflictEscalatesForceOnce` | 首发不 force 遇 409（foreign manager `rollouts-controller` 持有 image 字段，上游 argo/argo-rollouts#3281 形态）→ 升级 force 重试**恰一次**（applies=2：conflict 1 + success 1）→ digest 落盘且字段归订阅 field manager 所有；恰 1 条 Warning `ApplyConflictEscalated` 事件**命名目标与冲突 manager**；值变更投递的双命名空间 ImageApplied 审计不变（ADR-015 决策 9 先礼后兵） | 已实现 |
| `TestSubscriptionDeliverySSAConflictPersistentFailsHonestly` | 强制重试仍 409（budget 1/reconcile pass 用尽）：如实 Failed（`ApplyFailed`、消息承认 forced retry 预算耗尽）、**无 retry bookkeeping**（RetryCount=0，非 transient）、正常等待 requeue（`waitingStateRequeue`）而非 workqueue 退避风暴；对象零写入；`failures{apply_error}`=1；升级事件与失败 Warning 各 1 条 | 已实现 |
| `TestSubscriptionDeliveryDeliversToNamedRollout` | named 形态端到端：一次 apply + 一次 status write，digest 落盘、字段归订阅 manager、Applied 条目引用 Rollout kind | 已实现 |
| `TestSubscriptionDeliverySelectorFanOutToRollouts` | selector 形态自动通用（v1.7 路径零改动）：resolver 以 Rollout selector 键被查一次，2 成员各一次 SSA 写入，单条 selector 聚合 entry（matched=2、Rollout kind 回显） | 已实现 |

### envtest 集成（`internal/controller/subscription_delivery_rollout_envtest_test.go`）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestRolloutDeliveryEnvtestSSAWritePath`（lane A） | 共享环境装 vendored Rollout CRD（`test/testdata/thirdparty/argoproj.io_rollouts.yaml`，结构式 schema、containers 为 name 键 map-list）而**无 rollouts-controller**：CR 经真实 API server admission 原样往返 pod-template 巢；一次投递经真实 SSA 写入 digest 且 managedFields 含订阅 manager 的 Apply 条目；Applied 条目 + ImageApplied 事件；第二次 pass 稳态零写入（rollout 与订阅的 resourceVersion 均不变） | 已实现（本机 `KUBEBUILDER_ASSETS` 实跑通过） |
| `TestRolloutDeliveryEnvtestKindNotInstalled`（lane B） | 专用环境只装 imageflow CRD（无 Rollout CRD）：named Rollout target 的 REST-mapping miss 如实映射为 `KindNotInstalled` 等待态（spec D9，不新增枚举），正常等待 requeue、零写入 | 已实现（本机实跑通过） |

### 覆盖率要点

- **先礼后兵的预算语义**：force 升级只在「首发 409」单点发生，预算恰为 1/reconcile pass（ADR-015 决策 9）；orphan 接管（首发即 force）与升级重试共用同一 `IsConflict` 错误车道——持续冲突一律诚实 Failed，不做超预算重试；
- **升级审计无 reasonChanged 门控**：每次真实强制接管都发 Warning（真实集群 event correlator 聚合为 series）；稳态成功 churn 中每次升级都对应真实 ownership 争夺，必须可见；失败词汇表与指标基数均不增（复用 `TargetReasonApplyFailed`/`failureLabelApplyError`）；
- **foreign vs other-subscription**：首发 force 仍只对 `OwnershipOtherSubscription`（孤儿接管，v1.7 既有语义）触发；foreign manager（如 rollouts-controller）首发展望 409 升级车道——冲突竞态重分类（terminal Conflict、零写入）优先级仍高于强制重试；
- **RBAC 增量**：`+kubebuilder:rbac:groups=argoproj.io,resources=rollouts,verbs=get;list;watch;update;patch`（spec D8），`make manifests` 重生成 `config/rbac/role.yaml` 纯增量一个 rule 块；
- **selector 形态零改动**：resolver 与 match-informer 通道是 GVK 无关的（v1.7 #81 车道），Rollout selector 仅需注册表条目，单测与 envtest 双侧证明；
- **已知环境限制**：e2e 需 Podman（本机 socket 拒连，不跑），本票无 e2e 车道新增；`test/integration` webhook lane 既有环境限制同 v1.7-02 小节说明。
## Argo Rollout 观察与完成判定（v1.8-02，#93）

本票（ADR-015，docs/spec/imageflow-v1.8-argo-rollouts-support.md D4/D5/D6/D10）把 Argo Rollout（`argoproj.io/v1alpha1`）
纳入 observer 内建 Kind 白名单：unstructured 提取 `status.observedGeneration`（字符串，防御解析）/`status.phase`/
`status.stableRS`/`status.currentPodHash`/`status.pauseConditions` 与 4 个 replica 计数字段（D4，零新 Go 依赖，
`runtime.DefaultUnstructuredConverter` 把 `.spec.template.spec` 转成与 Deployment 同构的 `corev1.PodSpec` 复用容器
状态分析），D5 判定矩阵（untrusted→Progressing；三合一→Succeeded；Degraded/ReplicaFailure/ProgressDeadlineExceeded
→Failed；pause→Progressing+AwaitingPromotion；reconcile 永不冻结）。本车道两层：unit 判定表（假客户端）+ envtest
（CRD 在、controller 不在——真实 API Server，手工 status 经 /status 子资源落库）。

| 测试名 | 行为 | 状态 |
|---|---|---|
| `TestObserverDetermineArgoRolloutVerdictStates` | D5 判定矩阵 18 例（untrusted 三种：observedGeneration 空串/非数字/小于 generation；Succeeded 三合一：phase=Healthy + stableRS==currentPodHash + observedGeneration==generation，及 stableRS 为空、两者不等两个负例；Failed 三种 phase；Paused 无 pauseConditions 非 pause；pause→Progressing+AwaitingPromotion 且 message 含 "awaiting promotion"；pod ImagePullBackOff→Failed；镜像漂移/缺容器/缺 template→Unknown；Succeeded 后 health 走 v1.5 native health=NoMatchingPods） | 已实现（CI 已实跑） |
| `TestObserverGetWorkloadFactsArgoRolloutExtraction` | D4 提取逐字段断言：generation=3/observedGeneration 字符串 "3"→int64 3（parsed=true）、phase/stableRS/currentPodHash/pauseConditions→rolloutPaused、replicas 4 计数、spec.replicas、template 转换出 `corev1.PodSpec{Containers:[app@digest]}` | 已实现（CI 已实跑） |
| `TestParseArgoRolloutObservedGeneration` | 字符串防御解析边界表：`"3"`/`" 3 "`→(3,true)；`""`/`"abc"`/`"3.5"`/int64 溢出→(0,false)；字段缺失→(0,false) | 已实现（CI 已实跑） |
| `TestObserverDetermineArgoRolloutPodImageFailureAndReplicaSetChain` | Pod 枚举两跳 owner 链（Rollout→ReplicaSet→Pod）：RS 以 Rollout UID 为 owner、Pod 以 RS UID 为 owner 时被列出，pod ImagePullBackOff→Failed（消息 "cannot start image"） | 已实现（CI 已实跑） |
| `TestObserverArgoRolloutAwaitingPromotionHealthMessage` | health 语义：rollout Progressing+awaitingPromotion → Health Unknown + Reason=AwaitingPromotion，message 含 "awaiting promotion"（ADR-015 决策 8：合法稳态，非 NotReady） | 已实现（CI 已实跑） |
| `TestObserverArgoRolloutEnvtestVerdicts` | envtest（CRD 在、controller 不在）：5 例 D5 决定性行端到端走 observeTarget——succeeded 三合一（health=NoMatchingPods，D6 移交 v1.5 native health）、paused→Progressing+AwaitingPromotion、Degraded→Failed（health=WaitingForRollout）、observedGeneration 缺失→Progressing（untrusted）、stale→Progressing；断言 kindLabel=`rollout` 进指标词汇 | 已实现（CI 已实跑） |
| `TestObserverArgoRolloutEnvtestExtraction` | envtest 提取：断言 API Server 在 Create 时剥离 status（CRD 声明 status 子资源）、经 `client.Status().Update` 落库后逐字段读回（含 template→corev1.PodSpec 转换） | 已实现（CI 已实跑） |
| `TestObserverObservableBuiltinKindClassification`（既有，扩充） | 白名单分类表加 Rollout 行：`argoproj.io/v1alpha1, Kind=Rollout` + containerNames→(rollout, true) | 已实现（CI 已实跑） |
| `TestIsEngineOwnedReason`（既有，扩充） | 非引擎原因列表加 `AwaitingPromotion`——守卫 `isEngineOwnedReason` 判定为 false，允许其写入 entry.Reason | 已实现（CI 已实跑） |
| `TestDeliveryFailureLabelVocabularyIsClosed`（既有，扩充） | 失败 reason 闭集**负表**加 `HealthReasonAwaitingPromotion`/`"AwaitingPromotion"`：钉死「进 health 词汇、绝不进 failure 指标词汇」（ADR-015 决策 8 / spec D10） | 已实现（CI 已实跑） |
| `TestObserverLabelVocabulariesAreClosed`（既有，扩充） | 观察者 kind 闭集正表加 `argoRolloutGVK→observerKindRollout`；负表保持 third-party/imageFields/无 containerNames→other | 已实现（CI 已实跑） |

### 变异自证

- 判定表按 spec §D5 原文 5 步顺序构造，每例断言 status + message 稳定子串 + awaitingPromotion 标志三件套——
  删掉六步矩阵中任何一步（如跳过 untrusted 检查直接信 phase）至少 3 例 RED；
- `TestParseArgoRolloutObservedGeneration` 的溢出/小数/空白边界直接对着 `strconv.ParseInt` 的拒绝面，把
  「非数字视为不匹配（不信任 status）」从注释变成可证伪行为；
- envtest 的 `t93eCreateRollout` 先断言 API Server **剥离**了 Create 携带的 status——若 fixture CRD 漏声明
  status 子资源，该断言立即 RED，防止「测试自己骗自己」（手工 status 根本没有落库）；
- 负表断言（`AwaitingPromotion` 不属于 failure 指标词汇）是反向变异防护：将来若有人把它注册成失败原因，
  metrics 契约测试 RED 而不是静默污染告警指标。

### 契约文档同步

- `docs/metrics-contract.md`：`kind` 词汇行加 `rollout`（ADR-015 v1.8，#93）；观察者系列上限 6 kinds×4+3=27 →
  **7 kinds×4+3=31**；
- `docs/ops.md` Reason 词汇表：Health 原因行加 `AwaitingPromotion`（注明 ADR-015 决策 8 合法稳态、永不进 failure 词汇）；
- `docs/scenario-map.md` 场景 11（rollout 四态分别报告）补 Argo Rollout 车道两条用例（unit 判定表 + envtest）；
- `GLOSSARY.md`：AwaitingPromotion 词条在 ADR-015 wave C 已落地，本票无变更；
- `docs/spec/imageflow-v1.8-argo-rollouts-support.md` D12 要求的 ops/TESTING/scenario-map 三处登记至此全部落账。

### 覆盖率要点

- **CRD 在、controller 不在是本票指定形态**：envtest 只装 `test/testdata/thirdparty/argoproj.io_rollouts.yaml`
  （最小 CRD：根级 `x-kubernetes-preserve-unknown-fields` + status 子资源），不跑任何 Argo 控制器——所有 status
  由测试手工经 /status 子资源写入，正是「observer 读的就是上游控制器写的那个形状」的最小可信证明；
- **transition 语义决定断言面**：`out.detMessage` 只在计划了 rollout 状态翻转时携带——Progressing→Progressing
  不翻转、消息为空是**正确行为**，故 progressing 三例以 priorStatus=Unknown 播种（翻转携带消息），succeeded/failed
  两例以 Progressing 播种；这是断言形态而非测试绕行；
- **零新依赖验证**：`go.mod` 无 diff；unstructured/converter 全部来自已在依赖树里的 `k8s.io/apimachinery`；
- **本机实跑声明**：unit 层（`go test ./internal/controller/...`）与 envtest 层均本机实跑通过
  （`KUBEBUILDER_ASSETS` 指向主 checkout 的 `bin/k8s/1.33.0-darwin-arm64`，worktree 不带该目录）；
  e2e kind 车道本机无 Podman 不在本票范围（本票无 e2e 用例要求）。

## Argo Rollouts kind e2e 闭环（v1.8-04，#95）

本票（ADR-015 决策 15，docs/spec/imageflow-v1.8-argo-rollouts-support.md D11）补 v1.8 的最后一块：
在**真实 kind 集群 + 真实 rollouts-controller** 上验证 spec D11 的两个闭环场景。envtest 无法替代——
CRD 在、controller 不在时 native status 永远空白，收敛判定（决策 7 三合一）与步停映射（决策 8）
都依赖 rollouts-controller 真实写入 status.phase/stableRS/currentPodHash/pauseConditions。
CI 的 `E2E Tests` job（`.github/workflows/test-e2e.yml`）在建好 kind 集群后安装 **pin 的
argo-rollouts v1.10.2**（stable 通道，与 ADR-015 研究锚点一致；`kubectl create ns argo-rollouts` +
`kubectl apply -f …/releases/download/v1.10.2/install.yaml` + `rollout status` 等待就绪），
然后 `make test-e2e` 照常跑全量车道。

车道文件 `test/e2e/rollout_e2e_test.go`（`Describe(..., Label("t87"), Ordered)`，命名与结构沿用
t86 车道先例）：双命名空间（订阅侧 `t87-rollout-e2e` / 负载侧 `t87-rollout-e2e-wl`）、manager 裸
Pod + 生成 RBAC 绑定（`config/rbac/role.yaml`，含 #92 的 argoproj.io verbs）、created-by 注解手工
注入（webhook 在 e2e 不活跃）、事件断言沿用 t11/t86 的双 ns field-selector 模式。Allure 过渡标注
（ADR-009）：AllureID **IF-95** · TMS **IF-95**/**SPEC-91** · Epic *Phase 1.8: Argo Rollouts
Support* · Feature *ADR-015 Argo Rollouts Adoption* · Suite **t87-rollout**（文件头与逐 It 注释块）。

| 场景 | 用例 | 断言要点 |
|---|---|---|
| 无 pause 收敛（D11-a，US-8/US-20） | `t87-e2e-rollout-canary-without-pause-converges-succeeded-after-image-write` | `canary: {}` 基础金丝雀；真实 digest（registry.k8s.io/pause 多架构锚点，与 t11 同一 digest，kubelet 真拉取）经引擎写入 pod-template 标准路径（决策 4）；原生门控 phase=Healthy 且 stableRS==currentPodHash（决策 7 三合一全部兑现）；entry Succeeded + Reason=Applied（R3）+ health Healthy + 聚合 Succeeded；双 ns RolloutSucceeded 事件含审计四段与 `succeeded per native` |
| pause:{} 步停（D11-b，US-11/US-21） | `t87-e2e-rollout-canary-pause-step-holds-progressing-awaiting-promotion-never-succeeded` | Phase 1：Rollout 初始部署收敛 Healthy（v1.10.2 首次部署跳过 pause），订阅后到播种 Applied；Phase 2：新 stream 触发引擎写新 digest → rollouts-controller 创建新 RS 并停在 `pauseConditions[0].reason==CanaryPauseStep`；observer 判 Progressing（delivery re-seed 可能已置 entry=Progressing，steady-state 不重复发事件——subscription_observer.go:975）；验证通过 subscription status 字段（per-target rollout=Progressing + aggregate=Progressing + native pauseConditions=CanaryPauseStep）而非事件；health Unknown（ADR-005 §3 门）+ Reason 保持 Applied；Consistently 70s 断言永不 Succeeded、双 ns 无 RolloutSucceeded |

### 覆盖率要点

- **场景 b 的两阶段设计与事件不可断言**：Phase 1 让 Rollout 初始部署先收敛 Healthy（v1.10.2
  首次部署跳过 pause），再建订阅播种 Applied；Phase 2 通过新 stream 触发真正的 canary revision。
  由于 delivery re-seed 可能已将 entry 置为 Progressing，observer 的 Progressing→Progressing
  属 steady-state（subscription_observer.go:975），不发 transition 事件——AwaitingPromotion
  语义通过 subscription status 字段验证（per-target rollout + aggregate + native pauseConditions），
  而非依赖事件；
- **观察靠 30s 轮询**：observer 不 watch Rollout informer，Progressing 时按 `waitingStateRequeue`
  （30s）重排——场景 a 收敛 Eventually 预算 5m，场景 b 的 Consistently 70s 覆盖两个完整轮询周期；
- **真实收敛是真凭证**：注册镜像取 t11 的 pause:3.9 真实多架构 digest，kubelet 拉取、金丝雀 RS
  pod Ready、rollouts-controller 推进——observer 的 Succeeded/Progressing 判定全部挣得，非模拟；
- **本机未实跑声明**：本机无 Podman（kind 集群起不来），本票只做到编译级验收——
  `gofmt -l` 干净、`go vet ./test/e2e/...` 通过、`go build ./...` 通过；
- **CI 首跑声明**：t87 车道的首次真实运行发生在 CI 的 `E2E Tests` job（含 argo-rollouts v1.10.2
  安装步骤）；若首跑暴露时序问题，只允许调整 t87 车道的超时与门控预算，不允许放松断言面。
