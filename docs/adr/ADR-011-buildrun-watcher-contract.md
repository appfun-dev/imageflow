# ADR-011：BuildRun Watcher v1.1 实现契约（注解语法 + Feature Gate + Condition 语义）

- 状态：Accepted
- 日期：2026-11-07
- 决策人：项目负责人（v1.1 grilling Wave A Q1、Wave B Q2/Q3/Q4）
- 关联：ADR-004（Amended）；`docs/research/2026-10-16-upstream-fact-check.md` §2

## 背景

ADR-004 将 BuildRun Watcher 推迟至 v1.1 并冻结了核心设计，但留下注解锁定语法未决。v1.1 grilling 收敛了范围（仅 Watcher）与三个实现契约细节。

## 决策

1. **v1.1 范围 = 仅 BuildRun Watcher**：CUE Status Evaluator、imageReferenceFormat 等推迟至 v1.2+。Watcher 与其余候选无依赖关系，单独交付不产生技术债。
2. **注解锁定语法 = 单注解结构化 JSON**：Build/BuildRun 对象上设置 `buildrun.imageflow.appfun.dev/target`，值为 JSON `{"stream":"<name>","tag":"<name>"}`。admission webhook 须校验该注解的 JSON schema（必填 stream/tag 字段、值符合 RFC 1123）。kubectl annotate 体验通过文档示例弥补。
   - 备选 A（双注解分离）被否：虽更简单，但用户明确选择结构化以保留未来扩展空间。
   - 备选 B（单注解冒号形式）被否：stream 名含冒号时歧义，且引入不必要解析层。
3. **Feature Gate = 显式 flag + CRD 前置检测**：`--enable-buildrun-watcher=true`（默认 false）才启用 watcher controller；启用后启动时检测 `shipwright.io/v1beta1` Build CRD 存在性，不存在则报错退出。此设计修订了 ADR-004 §4 原「自动启用」语义（见 ADR-004 修订记录）。
4. **缺 digest 失败语义 = Registered=False / MissingDigest**：BuildRun Succeeded 但 `.status.output.digest` 为空时，Watcher 在 BuildRun status 上设置 `Registered=False`，reason=`MissingDigest`，message 引用「构建策略未提供 system results」。复用 Mapping 任务的 Condition 体系，保持登记路径语义一致。
   - 备选 B（Skipped=True / NoOutputDigest）被否：引入新状态概念但与 Mapping 体系不一致。
   - 备选 C（Event-only）被否：违反 ADR-004「不静默降级」原则，不可被 kubectl wait 观测。

## 理由

- 结构化 JSON 注解虽增加 admission 校验复杂度，但为未来可能的扩展（如多 tag、条件过滤）预留空间，且用户明确偏好；
- 显式 opt-in 比隐式自动启用更安全，避免集群中意外激活 watcher 导致未预期的 reconcile 行为；
- 复用 Mapping Condition 体系减少状态机分支，用户和监控工具可用统一语义查询所有登记路径的结果。

## 影响

- 新增 controller：`internal/controller/buildrun_watcher_controller.go`，feature-gated 注册；
- admission webhook 增加 BuildRun 注解校验逻辑；
- RBAC：controller SA 需 `shipwright.io/buildruns` get/list/watch + status patch 权限（feature-gated 条件注入）；
- 测试：envtest 覆盖注解校验、Condition 设置、CRD 缺失报错；kind e2e 覆盖真实 Shipwright BuildRun → digest 登记全链路；
- 部署清单：新增 `--enable-buildrun-watcher` flag、可选 RBAC ClusterRoleBinding；
- 文档：quickstart 补充 Shipwright BuildRun 注解示例、kubectl annotate 用法。
