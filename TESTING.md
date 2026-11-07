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

### 单元测试

```go
func (s *FieldPathSuite) TestAcceptsContainerNameFilter() {
    allure.Test(s.T(), "imageFields 路径接受 [name=app] 单层过滤",
        allure.WithAllureID("IF-101"),
        allure.WithTMS("IF-101", "https://github.com/appfun-dev/imageflow/issues/101"),
        allure.WithTMS("SPEC-1", "https://github.com/appfun-dev/imageflow/issues/1"),
        allure.WithLabel("userstory", "US-3"),
        allure.WithLabel("scenario", "合法路径通过校验"),
        allure.WithEpic("Phase 1: API and minimal image model"),
        allure.WithFeature("ADR-005 imageFields write contract"),
        allure.WithStory("US-3"),
        allure.WithLink("https://github.com/appfun-dev/imageflow/blob/main/docs/adr/ADR-005-write-contract-cue-deferral.md", "ADR-005", "docs"),
        allure.WithParentSuite("unit"),
        allure.WithSuite("FieldPath"),
        func(a *allure.Context) {
            err := subscription.ValidateImageFieldPath(".spec.template.spec.containers[name=app].image")
            require.NoError(a, err)
        },
    )
}
```

### 集成 / e2e 测试（必须带描述）

```go
func (s *SubscriptionAuthorizationSuite) TestRejectsMissingUpdatePermission() {
    allure.Test(s.T(), "订阅创建者缺少 target update 权限时拒绝写入",
        allure.WithAllureID("IF-102"),
        allure.WithTMS("IF-102", "https://github.com/appfun-dev/imageflow/issues/102"),
        allure.WithTMS("SPEC-1", "https://github.com/appfun-dev/imageflow/issues/1"),
        allure.WithLabel("userstory", "US-5"),
        allure.WithLabel("scenario", "RBAC 不足时报告 Forbidden"),
        allure.WithEpic("Phase 1: API and minimal image model"),
        allure.WithFeature("ADR-003 cross-namespace authorization"),
        allure.WithStory("US-5"),
        allure.WithLink("https://github.com/appfun-dev/imageflow/blob/main/docs/adr/ADR-003-subscription-authorization.md", "ADR-003", "docs"),
        allure.WithParentSuite("integration"),
        allure.WithSuite("SubscriptionAuthorization"),
        allure.WithDescription(`
验证：当 ImageSubscription 的创建者没有被授权更新目标 workload 时，
控制器应通过 SubjectAccessReview 识别并在该目标上产生 Forbidden Condition，
且不对目标做任何字段写入。

前置条件：
- envtest 已启动 controller-runtime 环境
- 创建 source ImageStreamTag 与 target Deployment
- 创建只读 target 的 ServiceAccount 作为订阅创建者

预期结果：
- subscription.status.targets[] 中对应项为 Forbidden
- Deployment image 字段未被修改
        `),
        func(a *allure.Context) {
            a.Step("创建只读 ServiceAccount 与 ImageSubscription", func(ctx context.Context) error {
                // ...
                return nil
            })
            a.Step("触发 reconcile 并断言 SAR 结果", func(ctx context.Context) error {
                a.StepDescription("控制器调用 SubjectAccessReview 检查 update 权限")
                // ...
                return nil
            })
        },
    )
}
```

### 表格驱动 + 参数外显

```go
func (s *FieldPathSuite) TestValidateImageFieldPath() {
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
            allure.Test(s.T(), tc.name,
                allure.WithAllureID("IF-101"),
                allure.WithTMS("IF-101", "https://github.com/appfun-dev/imageflow/issues/101"),
                allure.WithTMS("SPEC-1", "https://github.com/appfun-dev/imageflow/issues/1"),
                allure.WithLabel("userstory", "US-3"),
                allure.WithLabel("scenario", "imageFields 路径文法矩阵"),
                allure.WithParameter("path", tc.path),
                allure.WithParameter("wantErr", tc.wantErr),
                allure.WithEpic("Phase 1: API and minimal image model"),
                allure.WithFeature("ADR-005 imageFields write contract"),
                allure.WithStory("US-3"),
                allure.WithParentSuite("unit"),
                allure.WithSuite("FieldPath"),
                func(a *allure.Context) {
                    err := subscription.ValidateImageFieldPath(tc.path)
                    if tc.wantErr {
                        require.Error(a, err)
                    } else {
                        require.NoError(a, err)
                    }
                },
            )
        })
    }
}
```

### 复杂步骤 + 步骤参数

```go
func (s *DeliverySuite) TestLatestWinsSupersedesOldTag() {
    allure.Test(s.T(), "rollout 中到达新 tag 时旧投递记为 Superseded",
        // ... metadata omitted for brevity
        func(a *allure.Context) {
            a.Step("写入第一个 tag 并观察 ImageApplied", func(ctx context.Context) error {
                a.StepParameter("firstTag", "v1.0.0")
                a.StepParameter("firstDigest", "sha256:aaa...")
                // ...
                return nil
            })
            a.Step("在 rollout 进行中写入第二个 tag", func(ctx context.Context) error {
                a.StepDescription("模拟镜像源在原生 rollout 尚未完成时发生第二次变化")
                a.StepParameter("secondTag", "v1.1.0")
                // ...
                return nil
            })
            a.Step("断言旧投递为 Superseded", func(ctx context.Context) error {
                // ...
                return nil
            })
        },
    )
}
```

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

- **Integration / E2E 测试**：必须携带 `allure.WithDescription` 或 `allure.WithDescriptionHTML`。
- **HTML 描述触发条件**：描述含表格、列表、代码块或多行结构化内容时使用 `WithDescriptionHTML`。
- **表格驱动测试**：通过 `allure.WithParameter(name, value)` 外显参数；测试显示名保持稳定。
- **步骤参数/说明**：关键步骤使用 `a.StepParameter` / `a.StepDescription` 补充。

## 可追溯性 checklist

提交测试前逐条核对：

- [ ] 测试位于 `test/unit/`、`test/integration/`、`test/e2e/` 或生产代码同包 `_test.go`。
- [ ] 使用 `testify/suite` 组织；suite 结构体名以 `Suite` 结尾。
- [ ] 测试方法内调用 `allure.Test(s.T(), ...)`。
- [ ] `allure.WithAllureID("IF-<id>")` 与 `allure.WithTMS("IF-<id>", url)` 指向同一测试用例 issue。
- [ ] `allure.WithTMS("SPEC-<id>", url)` 指向标题以 `Spec:` 开头的 GitHub issue。
- [ ] `allure.WithLabel("userstory", "US-<Seq>")` 已标注。
- [ ] `allure.WithLabel("scenario", "<场景名>")` 已填写。
- [ ] `Epic` / `Feature` / `Story` 已按层级映射填写。
- [ ] `ParentSuite` = `unit|integration|e2e`；`Suite` = 结构体名去掉 `Suite` 后缀。
- [ ] Integration / E2E 测试已携带 `WithDescription` 或 `WithDescriptionHTML`。
- [ ] 表格驱动测试已使用 `allure.WithParameter` 外显参数。
- [ ] 关键步骤已使用 `a.StepParameter` / `a.StepDescription` 补充说明。

## 依赖与版本

```text
require (
    github.com/allure-framework/allure-go/commons/gotest <version-with-testify>
    github.com/allure-framework/allure-go/testify/assert
    github.com/allure-framework/allure-go/testify/require
    github.com/stretchr/testify
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
