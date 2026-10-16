# Upstream Fact-Check（Phase 0 证据）

日期：2026-10-16（Asia/Shanghai）· 方式：子代理在线调研，全部核实到一手来源 · 未核实项：无

用途：为 `ImageFlow-Project-Startup-Design.md` §17 Phase 0「核验上游事实」提供事实基线；供 grilling 第 1 轮 Q6 与第 2 轮（D-009 / D-013 / 导入链路）引用。

## 1. 版本基线（截至 2026-10-16）

| 组件 | 最新稳定 | 备注 |
|---|---|---|
| OpenShift Container Platform | 4.22（GA 2026-06-09；补丁 4.22.17，2026-10-06） | 支持窗口内：4.19 / 4.20 / 4.21 / 4.22；4.18 标准维护已于 2026-08-25 结束（仅 EUS 付费扩展至 2027-02-25） |
| Kubernetes | v1.37.1（1.37.0 GA 2026-08-26；1.37.1 补丁 2026-09-23） | dl.k8s.io stable 通道与 GitHub latest release 一致 |
| Shipwright Build | v0.21.9（2026-10-14，release-v0.21） | BuildRun 输出 digest 字段确认，见 §2 |
| Shipwright Triggers | v0.20.0（2026-06-30） | 仅作参考、非依赖，见 §3 |

来源：endoflife.date/openshift；access.redhat.com/support/policy/updates/openshift；dl.k8s.io/release/stable.txt；github.com/shipwright-io/build/releases/latest；github.com/shipwright-io/triggers/releases/latest。

## 2. Shipwright BuildRun 输出字段

- `.status.output.digest`（Go：`BuildRunStatus.Output.Digest`），同级 `.status.output.size`、可选 `.status.output.vulnerabilities`。
- **条件**：digest/size 仅在构建策略提供 system results 时才被填充。
- → ImageFlow 从 BuildRun 做导入/登记的路径必须处理「无 digest」情形（grilling 第 2 轮问题）。
- 来源：shipwright-io/build `docs/buildrun.md`（"Step Results in BuildRun Status"）；`pkg/apis/build/v1beta1/buildrun_types.go`。

## 3. Shipwright Triggers 能力边界（设计前提验证 ✅）

- 事件类型（SHIP-0031，Build CRD `spec.triggers.when[].type`）：`GitHub`（Push / PullRequest，可限定 branches）、`Image`（监听构建的**输入**镜像变化，非输出）、`Pipeline`（Tekton PipelineRun 达到指定 status）。
- GitLab webhook：不支持（"only GitHub supported initially"，代码零 gitlab 引用）。
- **不存在「BuildRun 状态变化」触发源**；触发输出仅为「创建 BuildRun」。
- **无 Workload 镜像字段更新能力**：v0.20.0 release.yaml 的 ClusterRole/Role 仅含 `shipwright.io builds/buildruns` 与 `tekton.dev customruns/pipelineruns`，无任何 `apps/v1` 或 `image.openshift.io` 权限；全仓库 Go 代码零处引用 deployment/statefulset/daemonset。
- 实现现状注意：v0.20.0 `main.go` 只启动 Inventory / PipelineRun / CustomRun 三个 controller，无 HTTP webhook 服务器——GitHub webhook 接收端在当前发布代码中尚未实现（chart 中端口为遗留脚手架），只有 API 定义与设计文档。
- → 设计文档的差异化前提成立：「构建输出变化 → 订阅驱动 Workload 更新并观察 rollout」上游无人做，ImageFlow 不与 Triggers 重叠（Triggers 在构建触发侧，ImageFlow 在构建产出消费侧）。
- 来源：shipwright-io/community SHIP-0031；shipwright-io/build `docs/build.md`（"Defining Triggers"）、`trigger_types.go`；shipwright-io/triggers v0.20.0 release.yaml、v0.17.0 release notes、`docs/components.md`。

## 4. OpenShift 原生 `image.openshift.io/triggers` 注解（4.22 文档核实）

- 适用于「含 pod template 但无原生 trigger 字段」的资源；ImageStreamTag 变化后按注解 fieldPath 自动更新镜像。官方支持 **7 种**：CronJob、Deployment、StatefulSet、DaemonSet、Job、ReplicationController、Pod。
- DeploymentConfig / BuildConfig 使用 API 原生 ImageChange trigger，不走该注解。
- → 原生注解只做「字段 patch」：无订阅模型、无 rollout 观察、无冲突/聚合策略、无跨集群可移植性——这是 ImageFlow 的差异化落点。**ADR-001（vanilla-K8s-first）后更新**：该 7 种清单降级为 OpenShift Compatibility Layer 与 D-016 的参考资料，不再是 v1 D-009（内建 Kind 首批）的输入；v1 内建适配器首批另行决定。
- 来源：docs.redhat.com OpenShift 4.22 "Triggering updates on ImageStream changes"。

## 调研附注

- api.github.com 经 web_fetch 被本地 DNS 策略拦截，改用 curl 直连成功。
- docs.openshift.com/container-platform/latest 旧 URL 已 404，现行文档在 docs.redhat.com。

## 5. v1 钉版（grilling 第 1 轮 Q6/Q7，用户确认，2026-10-16）

| 组件 | 钉版 | 角色 |
|---|---|---|
| Kubernetes | 1.37.x | v1 主战场（ADR-001 vanilla-K8s-first）；e2e 以 kind 为主，轻量测试用 envtest（sigs.k8s.io/controller-runtime/pkg/envtest） |
| operator-sdk | v1.42.3 | 脚手架主工具（kubebuilder 作为补充）；GitHub tag 已核实存在且为当前 latest release |
| Shipwright Build | v0.21.9 | 集成对象；BuildRun 输出字段路径见 §2（digest 有条件填充） |
| OpenShift | 4.22 | 仅 OpenShift Compatibility Layer 参照，v1 不交付 |
| Shipwright Triggers | v0.20.0 | 仅参考、非依赖，见 §3 |
