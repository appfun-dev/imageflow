# Spec: ImageFlow v1.1 — BuildRun Watcher

> 来源：v1.1 grilling Wave A（Q1 范围收敛）+ Wave B（Q2-Q4 实现契约）。决策源 = ADR-004（Amended 2026-11-07）+ ADR-011（Accepted 2026-11-07）+ `docs/research/2026-10-16-upstream-fact-check.md` §2。本 spec 与 ADR 冲突时以 ADR 为准；改决策必须先改 ADR。

## Problem Statement

ImageFlow v1 的构建产物登记路径依赖 CI 流水线侧显式创建 ImageStreamMapping 任务（ADR-004 §1）。这对非 Shipwright 的 CI 系统是合理的通用桥，但对 Shipwright 用户引入了不必要的胶水步骤：BuildRun 已经携带了 `.status.output.digest`，却仍需流水线额外创建一个 Mapping CRD 来触发下游订阅投递。

Shipwright Triggers v0.20.0 不具备 Workload 镜像字段更新能力（调研笔记 §3 已核实），因此「BuildRun 完成 → 自动登记进 ImageFlow 目录」的能力缺口只能由 ImageFlow 自身填补。

## Solution

ImageFlow v1.1 新增 **BuildRun Watcher**：一个 feature-gated 可选 controller，watch Shipwright BuildRun 对象，在其 Succeeded 且携带 digest 时自动执行等价于 ImageStreamMapping 的登记逻辑，消除 Shipwright 用户的 CI 胶水步骤。

核心约束（来自 ADR-004 + ADR-011）：
- **显式 opt-in**：`--enable-buildrun-watcher=true`（默认 false）；启用后前置检测 `shipwright.io/v1beta1` Build CRD 存在性，不存在则报错退出；
- **注解锁定**：Build/BuildRun 通过 `buildrun.imageflow.appfun.dev/target = {"stream":"<name>","tag":"<name>"}` 注解声明目标 stream/tag；admission webhook 校验 JSON schema；
- **digest 门控**：`.status.output.digest` 非空 → 内部等价于一次 Mapping（幂等）；为空 → `Registered=False / MissingDigest` Condition，不静默降级；
- **零 API 变更**：不新增 CRD、不修改现有 CRD schema；仅新增 controller + admission 校验逻辑 + RBAC 扩展。

## User Stories

1. 作为 Shipwright 用户，我想在 BuildRun 上设置注解声明目标 stream/tag，以便构建成功后镜像自动登记进 ImageFlow 目录，无需 CI 流水线额外创建 Mapping 任务。
2. 作为平台工程师，我想通过 `--enable-buildrun-watcher` flag 控制 watcher 启停，以便在未安装 Shipwright 的集群上不产生无效 reconcile。
3. 作为平台工程师，我想在启用 watcher 但集群缺少 Shipwright CRD 时收到明确的启动失败信息，以便快速定位配置错误而非静默 no-op。
4. 作为值班工程师，我想在 BuildRun Succeeded 但缺 digest 时看到 `Registered=False / MissingDigest` Condition，以便定位构建策略未提供 system results 的问题。
5. 作为平台工程师，我想 watcher 的登记行为与手动创建 Mapping 任务语义完全一致（幂等、同 `(stream, tag, digest)` 重复 = no-op），以便两条路径可互换使用且不产生重复 Image Change。
6. 作为安全审计员，我想 watcher 仅在显式启用时才获得 Shipwright BuildRun 的读权限，以便 RBAC 最小权限原则不被默认配置破坏。
7. 作为 ImageFlow 维护者，我想 watcher 的注解锁定语法有 admission 校验（JSON schema、必填字段、RFC 1123 值约束），以便非法注解在写入时即被拒绝而非在 reconcile 时静默忽略。

## Implementation Decisions

**决策溯源**：全部架构决策已冻结于 ADR-004（Amended）+ ADR-011；本节是实现约束汇编。

1. **Controller 注册**：`internal/controller/buildrun_watcher_controller.go`，实现 `controller.Registrar` 接口；`Register(mgr, opts)` 内检查 `opts.Config.EnableBuildRunWatcher`，false 则跳过注册、不创建 informer；true 则检测 CRD 存在性（RESTMapper discovery），不存在返回 error 阻止 manager 启动。
2. **注解契约**：
   - Key：`buildrun.imageflow.appfun.dev/target`
   - Value：JSON `{"stream":"<rfc1123-name>","tag":"<rfc1123-name>"}`
   - Admission：validating webhook 对带该注解的 BuildRun 执行 JSON unmarshal + 必填字段检查 + 值 RFC 1123 校验；失败返回明确 rejection message。
   - 注解仅由用户/CI 设置在 BuildRun 上；watcher 只读、不写回。
3. **Reconcile 逻辑**：
   - Watch BuildRun，过滤条件：annotation 存在 + `.status.conditions` 含 `Succeeded=True`；
   - 提取 `.status.output.digest`；非空 → 构造内部 Mapping 等价操作（调用现有 ImageStreamMapping reconciler 的登记函数，或直接写 ImageStream.status + ImageArtifact）；幂等键 = `(stream, tag, digest)`；
   - digest 为空 → patch BuildRun status condition `Registered=False, reason=MissingDigest, message="BuildRun succeeded but .status.output.digest is empty; build strategy may not provide system results"`；
   - 登记成功 → patch `Registered=True, reason=Succeeded`；
   - 登记失败（stream 不存在、RBAC 拒绝等）→ patch `Registered=False, reason=<具体原因>`。
4. **RBAC**：
   - Controller SA 新增（feature-gated 条件注入）：`shipwright.io/buildruns` get/list/watch + status patch；
   - 不授予 Build/BuildStrategy 权限；watcher 仅消费 BuildRun status；
   - ClusterRole `imageflow-buildrun-watcher` 独立于主 controller ClusterRole，仅在 flag 启用时绑定。
5. **Flag**：`--enable-buildrun-watcher`（bool，默认 false）；加入 `internal/config.ControllerConfig`；`cmd/main.go` 绑定并传入 `controller.Options`。
6. **测试策略**（对齐 ADR-009）：
   - **envtest**：注解校验（合法/非法 JSON、缺字段、非 RFC 1123 值）、Condition 设置（Succeeded+digest / Succeeded-digest / Failed）、幂等登记、CRD 缺失启动失败；
   - **kind e2e**：真实 Shipwright BuildRun（带 system results 的构建策略）→ digest 登记 → ImageStream.status 更新 → 下游订阅投递触发；缺 digest 场景的 Condition 可观测性；flag 关闭时 watcher 不启动；
   - **fake client 单测**：reconcile 纯逻辑（digest 提取、Condition 构造、幂等判定）。
7. **部署清单变更**：
   - `config/default/kustomization.yaml`：新增 `--enable-buildrun-watcher` flag（默认 false）；
   - `config/rbac/`：新增 `buildrun_watcher_clusterrole.yaml` + 条件绑定的 ClusterRoleBinding；
   - Helm chart（如有）：values.yaml 新增 `buildrunWatcher.enabled: false`。
8. **文档**：
   - quickstart 补充「Shipwright BuildRun → ImageFlow 自动登记」章节，含 kubectl annotate 示例；
   - ops 文档补充 flag 说明、RBAC 前置条件、troubleshooting（MissingDigest 排查步骤）；
   - TESTING.md 补充 watcher 测试矩阵。

## Out of Scope

- CUE Status Evaluator（v1.2+，合约已冻结于 ADR-005）；
- imageReferenceFormat 枚举（v1.2+，ADR-005）；
- OpenShift Compatibility Layer（后续 ADR）；
- Build/BuildStrategy watch（仅消费 BuildRun status）；
- 多 stream/tag 单次登记（当前一 BuildRun = 一 target；未来可扩展注解 schema）；
- BuildRun 注解的 mutating admission 默认值注入（v1.1 要求用户显式设置）。

## Testing Decisions

1. **好测试的标准**：同 v1 spec——只断言外部可见行为（BuildRun status Condition、ImageStream.status 变化、ImageArtifact 创建、admission 拒绝、Events）；不断言内部调用序列。
2. **缝 S1 = Kubernetes API 对象面**：envtest + kind e2e 双层覆盖；kind 侧使用真实 Shipwright Build（带 kaniko/buildah 策略提供 system results）。
3. **缝 S4 = Shipwright BuildRun status**：envtest 直写 BuildRun status 子资源模拟 Succeeded+digest / Succeeded-digest / Failed；kind 侧用真实构建。
4. **验收映射**：§User Stories 7 条各至少对应一个命名用例；Story 5（幂等）在 envtest 和 kind 双层验证。

## Further Notes

- **钉版**：Shipwright Build v0.21.9（同 v1 spec）；Kubernetes 1.37.x；operator-sdk v1.42.3；
- **向后兼容**：v1.1 不修改任何现有 CRD schema、不改变 v1 行为；flag 默认 false 确保升级无副作用；
- **开放项**：多 target 注解扩展语法（v1.2+）；BuildRun 注解 mutating 默认值（v1.2+）；
- **仓库痕迹**：ADR-004（Amended）、ADR-011（Accepted）已入库；本 spec 引用路径均可解析。
