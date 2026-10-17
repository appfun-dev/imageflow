# ImageFlow 测试约定

本文件是 ADR-009 的可执行版本：日常命令、目录结构、Allure 标注模板与代码示例。

## 目录结构

```text
imageflow/
├── go.mod                        # 项目 Go module（示例测试落地时初始化）
├── internal/                     # 生产代码
│   └── subscription/
│       └── fieldpath.go          # 示意：ADR-005 imageFields[] 路径校验
├── test/
│   ├── unit/                     # 纯内存、无集群
│   ├── integration/              # envtest / fake
│   └── e2e/                      # kind
├── allure-results/               # gitignore，Allure JSON 输出
├── TESTING.md                    # 本文件
└── docs/adr/ADR-009-testing-strategy.md
```

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
- `allure-results/` 已加入 `.gitignore`，**不要提交**。

## 标注模板

以下代码为模板示例，实际落地时需按项目真实包路径、issue 编号与 allure-go 版本调整 import。

### 单元测试（轻量）

```go
func (s *FieldPathSuite) TestAcceptsContainerNameFilter() {
    allure.Test(s.T(), "imageFields 路径接受 [name=app] 单层过滤",
        allure.WithAllureID("IF-42"),
        allure.WithTMS("IF-42", "https://github.com/appfun-dev/imageflow/issues/42"),
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

### Integration / E2E 测试（必须带描述）

```go
func (s *SubscriptionAuthorizationSuite) TestRejectsMissingUpdatePermission() {
    allure.Test(s.T(), "订阅创建者缺少 target update 权限时拒绝写入",
        allure.WithAllureID("IF-51"),
        allure.WithTMS("IF-51", "https://github.com/appfun-dev/imageflow/issues/51"),
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
                allure.StepDescription(ctx, "控制器调用 SubjectAccessReview 检查 update 权限")
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
                allure.WithAllureID("IF-42"),
                allure.WithTMS("IF-42", "https://github.com/appfun-dev/imageflow/issues/42"),
                allure.WithTMS("SPEC-1", "https://github.com/appfun-dev/imageflow/issues/1"),
                allure.WithLabel("userstory", "US-3"),
                allure.WithLabel("scenario", "imageFields 路径文法校验"),
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
                allure.StepParameter(ctx, "firstTag", "v1.0.0")
                allure.StepParameter(ctx, "firstDigest", "sha256:aaa...")
                // ...
                return nil
            })
            a.Step("在 rollout 进行中写入第二个 tag", func(ctx context.Context) error {
                allure.StepDescription(ctx, "模拟镜像源在原生 rollout 尚未完成时发生第二次变化")
                allure.StepParameter(ctx, "secondTag", "v1.1.0")
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

## 可追溯性 checklist

提交测试前逐条核对：

- [ ] 测试位于 `test/unit/`、`test/integration/` 或 `test/e2e/`。
- [ ] 使用 `testify/suite` 组织；suite 结构体名以 `Suite` 结尾。
- [ ] 测试方法内调用 `allure.Test(s.T(), ...)`，不要依赖 testify/suite 自动生成的匿名 Allure 结果。
- [ ] `allure.WithAllureID("IF-<id>")` 与 `allure.WithTMS("IF-<id>", url)` 指向同一测试用例 issue。
- [ ] `allure.WithTMS("SPEC-<id>", url)` 指向标题以 `Spec:` 开头的 GitHub issue。
- [ ] `allure.WithLabel("userstory", "US-<Seq>")` 已标注；若该 Story 有独立 issue，再补 `allure.WithLink(url, "US-<id>", "userstory")`。
- [ ] `allure.WithLabel("scenario", "<场景名>")` 已填写。
- [ ] `Epic` / `Feature` / `Story` 已按层级映射填写。
- [ ] `ParentSuite` = `unit|integration|e2e`；`Suite` = 结构体名去掉 `Suite` 后缀。
- [ ] Integration / E2E 测试已携带 `WithDescription` 或 `WithDescriptionHTML`。
- [ ] 表格驱动测试已使用 `allure.WithParameter` 外显参数。
- [ ] 关键步骤已使用 `allure.StepParameter` / `allure.StepDescription` 补充说明。

## 命名约定

| 对象 | 约定 | 示例 |
|---|---|---|
| suite 结构体 | `<Domain><Scope>Suite` | `FieldPathSuite`、`SubscriptionAuthorizationSuite` |
| suite 文件 | `snake_case_test.go` | `subscription_fieldpath_test.go` |
| 测试方法 | `Test<Scenario>` | `TestAcceptsContainerNameFilter` |
| Allure 显示名 | 中文或英文完整句，不含参数值 | `imageFields 路径接受 [name=app] 单层过滤` |
| AllureID | `IF-<issue-id>` | `IF-42` |
| Spec ID | `SPEC-<issue-id>` | `SPEC-1` |
| User Story | `US-<Seq>` 或 `US-<issue-id>` | `US-3` |

## 本地报告

```bash
# 运行后即时查看
go test ./... && allure serve allure-results

# 生成静态报告用于附件
allure generate allure-results --clean -o allure-report
```

## CI 约定

- CI 中只执行 `go test ./...` 并归档 `allure-results/` 目录。
- 不生成 HTML 报告到仓库；HTML 生成留给本地或单独的 Allure Server。
- 失败截图 / 日志可通过 `a.Attachment` 写入 `allure-results/`，随目录一起归档。
