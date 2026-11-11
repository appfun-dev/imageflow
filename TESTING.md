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
