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
| `TestSubscriptionDeliverySelectorZeroMatchesZeroSurface` | 空匹配集：零条目、零 records、零事件、聚合空、对象零写入（NoMatches Condition 属 #82） | 已实现 |
| `TestSubscriptionDeliverySelectorWithoutResolverIsHonest` | 未接线 resolver → entry InvalidTarget、聚合 Unknown、零写入（registration bug 如实上报） | 已实现 |
| `TestSubscriptionDeliverySelectorResolveErrorMapping` | `NoKindMatchError` → KindNotInstalled + waitingStateRequeue；一般解析错误 → ApplyFailed transient（RetryCount=1）+ reconcile 报错 | 已实现 |
| `TestSubscriptionDeliverySelectorOverlapDeduplicatesEntry` | 两个 selector target 匹配同一对象：resolve 各跑一次、apply 只 1 次、1 个条目、1 条 record（spec 顺序 first-wins 去重） | 已实现 |
| `TestSubscriptionDeliverySelectorLabelRemovalPrunesEntryKeepsImage` | 摘 label 后条目被 stale-pruning 剪除、已写镜像保留（不回收、零回滚写入）、records append-only 不变（ADR-014 决策 3） | 已实现 |
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

### Observer 单元测试（`internal/controller/subscription_observer_test.go`）

| 用例 | 断言 | 状态 |
|---|---|---|
| `TestObserverSelectorTargetAggregateWorstCase` | 表驱动 6 例覆盖票面全 verdict 路径（全 Succeeded、混 Failed、混 Unknown、Progressing、零匹配、单对象退化）：per-object verdict 永不落 status；聚合 worst-case（`AggregateRolloutStatus` 同排名）落 selector entry；transition 事件恰 1 条仅订阅侧，文案含有界摘要（`worst-case aggregate X over N matched object(s): web-1=…`，上限 5 个 + `+N more`）；record 按聚合治愈/保持 in-flight；observer 不写 matched（`MatchedObjects==nil`）；engine-owned Reason/Message 不被 observer 覆写；Progressing 聚合保持有界轮询 requeue；零匹配聚合 Unknown + 事件含「matched zero live objects」 | 已实现（取代 #81 per-object 观察用例） |
| `TestObserverSelectorThirdPartyAggregateHonestUnknown` | 第三方 CRD selector 聚合如实 Unknown：不轮询、不伪造裁决、Reason/Message 保持引擎所有、record in-flight、1 条订阅侧 RolloutUnknown 事件含 `edge-1=Unknown`、指标 {other,unknown}=1 | 已重写 |

### envtest 集成（`test/integration/subscription_delivery_test.go`，manager lane）

- `t81s-envtest-selector-fan-out-per-object-ssa-conflict-skip-and-label-driven-reconcile` 断言按 #82 更新：全程 status 只有 1 条 selector 形态聚合 entry（matched=2→3 随成员增长、Conflict 消息含冲突方与被争对象名）；per-object SSA/冲突跳过/label 驱动 reconcile 的生产接线断言不变。本机以 `KUBEBUILDER_ASSETS`（`bin/k8s/1.33.0-darwin-arm64`）实跑通过。

### 覆盖率要点

- **聚合位置**：engine 在 `deliverSelectorTarget` 内折叠出单条聚合 outcome（`aggregateSelectorOutcome` 纯函数，可单测）；observer 在 `observeSelectorTarget` 内对每对象复用 named 车道 `observeTarget` 合成 ephemeral verdict 后聚合，两侧共用 `AggregateRolloutStatus` 排名（Failed > Conflict > Unknown > Progressing > Superseded > Succeeded）；
- **matched 所有权**：只由 engine 写（informer 成员事件驱动刷新）；observer 只读——单元测试双侧 pin `MatchedObjects` 的写与不写；
- **per-object 明细只走 Events**（spec D8）：投递/确认/失败 Warning/冲突 Warning 全部按对象计划（`planDeliveryEvents`、per-object failure 循环），latency/applies/failures 指标按对象计数；status 有界 3/5/5 合约不变，record 恒 per-target 一条；
- **NoMatches 为合法态**（ADR-014 决策 8）：Warning 事件有、failure 指标无（词汇表刻意排除）；零匹配按 Unknown 参与订阅聚合，不阻塞其它 target；
- **Conflict 生命周期**：worst-state 压过 delivered 兄弟（record 跟随最坏态）；稳态零 churn；被争对象离开匹配集后聚合恢复 Conflict→Progressing（observer 的 Conflict 门禁会跳过 entry，engine 必须显式交还车道，否则永久卡死）；
- **已知环境限制**：e2e 需 Podman（本机 socket 拒连，不跑）；`test/integration` webhook lane `[BeforeAll]` 超时为本机既有环境问题（同 v1.7-02 小节说明）。
