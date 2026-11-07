# ImageFlow 测试约定

本文件是测试策略的可执行说明（决策背景见 `docs/adr/ADR-009-testing-strategy.md`；
T12 按仓库现实重写：v1 实际采用 **Ginkgo + Gomega**，未引入 testify/suite 与
Allure——ADR-009 中相应工具章节以本文与实现为准；可追溯性由
`docs/scenario-map.md` 与各 ticket 的 evidence comment 承担）。

## 分层结构

```text
imageflow/
├── internal/**/*_test.go        # 单元层：与生产代码同包，纯内存/fake client
├── test/registrydouble/         # 共享 OCI v2 registry double（TLS+basic auth+故障注入）
├── test/integration/            # envtest 层：真实 API server + 真实控制器（Ginkgo 套件）
└── test/e2e/                    # kind 层：真实集群 + 真实镜像构建部署（Ginkgo 套件，CI 执行）
```

- **单元层**：控制器纯逻辑（状态机、路径文法、词汇表、TTL 判定、凭据不泄漏的
  单元面），无集群依赖。
- **envtest 层**（`test/integration/`，约 96 个命名用例）：真实 API server + etcd +
  真实控制器 manager（`RunSpecs` 在 `manager_test.go`，**没有** `suite_test.go`）。
  限制：envtest 无 kube-controller-manager——原生 GC 级联与原生 rollout 不存在，
  测试直接断言 ownerRef 契约、直接写 workload status 子资源模拟原生控制器行为。
  需要 `KUBEBUILDER_ASSETS`（`make envtest` 或 CI 自动准备）。
- **kind 层**（`test/e2e/`，24 个命名用例，CI 执行）：真实集群、真实 Deployment 滚动、
  真实 webhook/证书链、进程内 registrydouble 提供真实 OCI 语义（真实 sha256、
  schema2 manifest、TLS、basic auth、401/404/5xx/hang 故障注入）。本机无容器运行时，
  本地只做编译验证：`go vet ./test/e2e/`。

## 运行命令

```bash
# 全部非 e2e 测试（单元 + envtest；CI 的 make test 同款）
make test

# 手动等价（注意排除 e2e，且需要 envtest 资产）
export KUBEBUILDER_ASSETS=<path-to>/k8s/<version>-<os>-<arch>
go test -count=1 $(go list ./... | grep -v /e2e)

# kind e2e 全链（建集群 → 构建镜像 → 安装 → 跑 24 specs → 删集群）
make test-e2e

# lint（golangci-lint v2.1.0，0 issues 门槛）
make lint
```

CI（`.github/workflows/`）：`lint.yml`、`test.yml`（`go mod tidy` 校验 + `make test`）、
`test-e2e.yml`（kind + cert-manager + `make test-e2e`，Ginkgo 超时 30m；当前全绿用时
约 16 分钟墙钟）。e2e manager pod 一律绑定**生成的** manager-role（68acbdc 协议，
禁止手写规则清单——markers 变更后绑定自动跟随）。

## 命名与组织约定

- **命名用例**：`It("...")` 描述即用例名（kebab-case 长句，如
  `t9-e2e-deployment-digest-registration-delivers-and-starts-the-native-rollout`），
  ticket 前缀（t4/t7/t8/t9/t10/t11/t12）标识 lane；`docs/scenario-map.md` 按名字引用。
- **lane 隔离**：每条 kind lane 独占命名空间（`t7-import-e2e`、`t8-authz-e2e`、
  `t9-delivery-e2e`、`t10-imagefields-e2e`、`t11-observer-e2e`、`t12-hardening-e2e`），
  自建 SA/ClusterRoleBinding/manager pod（args 模式：
  `--sar-recheck-period=20s --leader-elect=false --metrics-bind-address=0`），
  AfterAll 清理；lane 间可共存于同一集群。
- **控制器名进程内全局唯一**：测试 manager 内注册的控制器名加 `-it` 后缀。
- **窗口纪律**（kind 层）：`Eventually` 2m/2s（慢路径 3m/2s–5m/5s），
  `Consistently` 30s/10s；保持全绿运行远离 30m 预算（超时上调需单独记录）。
- **kind 节点污点**：工作负载一律加 blanket toleration `operator: Exists`；
  StatefulSet 用 `podManagementPolicy: Parallel`；需要真实 Ready 的 pod 用
  `registry.k8s.io/pause:3.9`（digest `sha256:7031c1b2…21097`）。
- **lint 陷阱**：goconst 对包内 ≥3 次的字符串字面量报错（赋值/表达式上下文才计数，
  函数调用实参不计）；跨 ns RoleBinding 不能引用他 ns 的 Role（复制 Role 对象）。
- **registrydouble 复用**：需要真实 Registry 语义一律用 `test/registrydouble`
  （S2 一致性已验证），禁止另起 double。

## 冻结契约（测试不得破坏，改动需 orchestrator 裁定）

- T9 per-target Reason 词汇表与 §12.9 `recentDeliveries` 记录形状（引擎属主）。
- T11 R3：观察器**不写** per-target entry Reason/Message；其 Rollout* reason 只出现在
  它治愈的投递记录、事件与 `StatusPolicyEvaluated` condition 上。
- 聚合 rolloutStatus 优先序 `Failed > Conflict > Unknown > Progressing > Superseded > Succeeded`。
- SSA field manager 格式 `imageflow.appfun.dev/sub-<ns>-<name>`（超长截断+哈希）。
- 指标命名 `imageflow_<subsystem>_<metric>_<unit>`；`docs/metrics-contract.md` append-only。
- **已知瞬态**（有记录，勿"顺手修复"）：T9 kind specs 1–3 钉 `rolloutStatus=Progressing`
  ——诚实 Failed 翻转在 15–75s 后才落地，快于 2s 轮询窗口；两轮 CI 均有余量通过。
  若新增用例使其变 flake，先上报再处理（放宽方向 = 接受 Progressing-or-Failed）。

## 真实 Registry e2e 腿（可选，CI 默认 skip）

kind 套件默认全部跑在 registrydouble 上。`test/e2e/import_e2e_test.go` 中 3 条
**真实 Registry 腿**仅当环境变量齐备时执行：

| 变量 | 必填 | 语义 |
|---|---|---|
| `IMAGEFLOW_E2E_REGISTRY_HOST` | 是 | 真实 Registry 主机（S2 客户端强制 `https://`） |
| `IMAGEFLOW_E2E_REGISTRY_REPO` | 是 | 仓库路径（如 `org/repo`） |
| `IMAGEFLOW_E2E_REGISTRY_TAG` | 是 | 测试用 tag（按需腿会改写其指向，见下） |
| `IMAGEFLOW_E2E_REGISTRY_PUSH_USERNAME` / `_PUSH_PASSWORD` | 漂移腿必须 | 测试**自身**重指 tag 内容以制造漂移（ImageFlow 永不推送，ADR-007；无凭据时漂移腿显式 Skip，可用运维预发布 tag 走按需腿） |
| `IMAGEFLOW_E2E_REGISTRY_PULL_SECRET` | 否 | dockerconfigjson 文件路径（凭据腿） |
| `IMAGEFLOW_E2E_REGISTRY_CA` | 否 | PEM CA 证书路径（私有 CA；挂进 manager pod 并以 `SSL_CERT_FILE` 生效） |

语义：三者（HOST/REPO/TAG）齐备时真实腿**替换** double 成为默认源；缺推送凭据的
子能力各自显式 Skip 并说明所缺变量。**T12 裁定（issue #13）：CI 保持 skip**——
漂移腿需要推送凭据（我们不对任何公共 Registry 持有），纯匿名公共镜像不能覆盖三条腿，
且引入外部 Registry 可用性/限流会使 CI 因非代码原因变红。`docs/scenario-map.md`
的硬规则与此一致：场景映射只允许引用 **CI 实际运行**的命名用例，skip 腿至多作为
可选补充证据。

## 可追溯性

- §15 十七条场景 ↔ 命名用例映射：`docs/scenario-map.md`（每条场景 ≥1 个 CI 运行用例）。
- 每个 ticket 的验收证据（AC → 用例、CI run、偏差）以 evidence comment 形式记录在
  对应 GitHub issue 上（见 `docs/agents/issue-tracker.md`）。

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
