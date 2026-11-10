# 编码标准

本文件定义 ImageFlow 项目的编码标准。Code Review 时作为 Standards 轴的审查依据。

> **原则**：机械性规则优先用自动化工具（linter/CI）执行；本文档专注于需要人工判断的标准。

## 注释准确性

**规则**：注释必须准确描述其所标注的代码行为。

- 重构改变行为时，必须在同一 commit 中更新相关注释
- 过时注释视为缺陷（defect），而非文档债务（documentation debt）
- 注释不得声称代码做了它实际没做的事（违反 Grice Quality §2 不编造事实）

**示例**：
```go
// ❌ 错误：注释声称单次获取，但代码实际调用了两次
// The pod list is fetched ONCE and shared by rollout and health determination.

// ✅ 正确：注释准确反映代码行为
// observeTarget fetches the pod list once per workload per reconcile pass
// and passes it to both determineRollout and determineHealth.
```

**审查要点**：当 diff 包含注释变更或新增注释时，验证注释与代码行为一致。

## Spec-代码一致性

**规则**：当 spec 存在编号决策（D1, D2, ...）时，实现必须逐条符合。

- 每个决策的实现应在 commit message 或代码注释中引用决策编号
- 偏离 spec 必须在 commit message 中明确说明理由
- 测试应断言 spec 要求的行为，而非仅断言当前实现

**审查要点**：Code Review Spec 轴应逐条核对 diff 是否符合 spec 中的每个编号决策。

## 测试文档时序

**规则**：TESTING.md 中的测试覆盖声明必须在测试实际存在并通过后才能标记为「已实现」。

- 禁止在测试落地前标记测试为「已实现」
- 新增测试后，同步更新 TESTING.md 的覆盖矩阵
- 测试被删除或跳过时，及时更新文档状态

**审查要点**：当 diff 修改 TESTING.md 时，验证所声称的测试确实存在于代码中。

## 语言约定

**规则**：项目以中文为主要交互语言，保留核心技术领域词汇。

- ADR、spec、ops.md 等面向 agent 的文档使用中文
- 代码注释、commit message 可使用英文（技术惯例）
- 用户面向的错误消息使用中文

## Allure 测试导入约定

### gotest 包导入

**规则**：Allure gotest 包采用匿名导入 + 别名导入并存。

```go
import (
    . "github.com/allure-framework/allure-go/commons/gotest"      // 匿名：With* 函数直接使用
    agt "github.com/allure-framework/allure-go/commons/gotest"    // 别名：agt.Test, agt.Context
)
```

- `agt.Test(...)` 替代 `allure.Test(...)`
- `agt.Context` 替代 `allure.Context`
- `With*(...)` 函数通过匿名导入直接使用（如 `WithAllureID`, `WithLabel`）

### testify 包导入优先级

**规则**：优先使用 Allure 封装的 testify 包，仅在必要时回退到原生 stretchr/testify。

| 优先级 | 包路径 | 使用场景 |
|--------|--------|----------|
| 1️⃣ | `github.com/allure-framework/allure-go/testify/assert` | 已导入 allure 包时首选 |
| 1️⃣ | `github.com/allure-framework/allure-go/testify/require` | 已导入 allure 包时首选 |
| 2️⃣ | `github.com/stretchr/testify/assert` | allure 版本缺少所需函数，或未导入 allure 时 |
| 2️⃣ | `github.com/stretchr/testify/require` | allure 版本缺少所需函数，或未导入 allure 时 |

**示例**：
```go
// ✅ 场景 1：已导入 allure，优先用 allure 封装的 testify
import (
    . "github.com/allure-framework/allure-go/commons/gotest"
    agt "github.com/allure-framework/allure-go/commons/gotest"
    "github.com/allure-framework/allure-go/testify/assert"
    "github.com/allure-framework/allure-go/testify/require"
)

// ✅ 场景 2：allure 版本缺少某函数，回退到 stretchr（别名避免冲突）
import (
    . "github.com/allure-framework/allure-go/commons/gotest"
    agt "github.com/allure-framework/allure-go/commons/gotest"
    "github.com/allure-framework/allure-go/testify/assert"
    strequire "github.com/stretchr/testify/require"  // ← 别名，仅当 allure require 缺少所需函数时
)

// ✅ 场景 3：未导入 allure 包，直接用 stretchr
import (
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)
```

**判断流程**：
1. 测试文件已导入 allure gotest 包？→ 用 `allure-go/testify/assert` 或 `require`
2. 所需函数在 allure 版本中不存在？→ 回退 `stretchr/testify`（别名导入避免冲突）
3. 未导入 allure 包？→ 直接用 `stretchr/testify`

## 引用

- 格莱斯合作原则：[docs/agents/grices-cooperative-principle.md](docs/agents/grices-cooperative-principle.md)
- BizDevOps 建模：[docs/agents/bizdevops-modeling.md](docs/agents/bizdevops-modeling.md)
- 测试策略：[docs/adr/ADR-009-testing-strategy.md](docs/adr/ADR-009-testing-strategy.md)
- ADR 索引：[docs/adr/INDEX.md](docs/adr/INDEX.md)
