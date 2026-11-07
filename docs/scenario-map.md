# §15 验收场景 ↔ 命名用例映射表

本表是 T12（issue #13 AC5）要求的入库映射：设计文档 `ImageFlow-Project-Startup-Design.md` §15「验收标准建议」
的全部场景（原文 18 条，v1 实现 17 条）逐条对应到**在 CI 中真实运行**的命名测试用例。

规则（与编排方裁定一致）：

- 每条场景至少映射 1 个命名用例；映射只允许引用 CI 常规运行（`make test` / `make test-e2e`）中**实际执行**的用例。
- 需要真实外部 Registry 凭据的 T7 附加腿（`IMAGEFLOW_E2E_REGISTRY_*` 环境变量族，见 `TESTING.md`）在 CI 中跳过，
  只能作为附加证据列出，**永远不作为任何场景的唯一映射**。
- 用例分三层：**kind**（`test/e2e/`，真实集群，CI job `E2E Tests`）、**envtest**（`test/integration/`，
  真实 API Server + 注入时钟，CI job `Tests`）、**unit**（`internal/...` 包内 Go 测试，CI job `Tests`）。
- 用例名即 Ginkgo `It` 文本（kind/envtest）或 `TestXxx/子测试` 名（unit）；文件路径同表给出。

## 两条范围裁定记录

- **场景 15（OpenShift 与原生 trigger 共存）不在 v1 交付**：ADR-001 确立 vanilla-K8s-first，
  spec（issue #1）测试决策 2 明确「§15 十八条去掉 #15，实现 17 条」；设计文档 §14 D-016 与
  `docs/research/2026-10-16-upstream-fact-check.md` §4 将 OpenShift 原生 `image.openshift.io/triggers`
  注解降级为未来 OpenShift Compatibility Layer 的参考资料。v1 没有 OpenShift 模式，因此不存在
  「与原生 trigger controller 重复更新同一字段」的运行时路径。若未来引入兼容层，需新 ADR 并补场景用例。
- **场景 12 按 v1 语义重述**：原文的 CUE ConfigMap 评估器按 ADR-005 §4 推迟到 v1.1；v1 的实现语义是
  **`statusPolicyRef` 不可用（缺失或指向不存在的 ConfigMap/key）时如实报告 Unknown，绝不虚构成功**
  （T11 行为，`StatusPolicyEvaluated=False` Condition 显式声明）。CUE 缺失/编译失败/超时/未知结果的
  分支语义由 v1.1 的评估器工单继续映射到同一「不视为成功」契约。

## 映射表

| §15 | 场景（原文摘要） | 命名用例（层 · 文件） |
|---|---|---|
| 1 | 创建镜像流与 tag，可查询当前关联 digest | `t7-e2e-imports-a-real-image-from-the-sidecar-registry-into-the-catalog`（kind · test/e2e/import_e2e_test.go）；`creates and reads back all seven catalog kinds through the API`（envtest · test/integration/catalog_crd_test.go） |
| 2 | 外部 Registry 导入后 ImageArtifact/Tag 状态一致 | `t7-imports-an-external-tag-on-demand-into-the-catalog`、`t7-imports-a-digest-reference-after-verifying-the-manifest`（envtest · test/integration/import_controller_test.go）；`synthesizes ImageStreamTag and ImageTag mirrors agreeing with the source of truth`（envtest · test/integration/mirror_controller_test.go）；kind 腿同场景 1 |
| 3 | 登记已推送镜像，更新目标 tag 关联 | `t4-registers-pushed-digest-with-artifact-dedup-and-bounded-history`（envtest · test/integration/mapping_controller_test.go）；`t9-e2e-deployment-digest-registration-delivers-and-starts-the-native-rollout`（kind · test/e2e/delivery_e2e_test.go，经真实 T4 登记链路） |
| 4 | 同一 tag 的 digest 变化触发变化检测 | `t4-detects-real-image-change-when-tag-stays-and-digest-changes`（envtest · test/integration/mapping_controller_test.go）；`t7-e2e-scheduled-import-detects-external-tag-drift-with-a-short-interval`（kind · test/e2e/import_e2e_test.go，CI 默认对 registry double 运行） |
| 5 | 一条 ImageSubscription 配置多个目标 | `t12-e2e-manager-kill-mid-delivery-recovers-to-eventual-consistency-from-persisted-state`（kind · test/e2e/t12_hardening_e2e_test.go，一条订阅 3 个 Deployment 目标）；`t11i-envtest-statefulset-daemonset-independent-targets-worst-state-aggregate`（envtest · test/integration/t11_observer_test.go，STS+DS 独立目标） |
| 6 | 内建 Workload 按 `containerNames[]` 更新指定容器 | `t9-e2e-deployment-digest-registration-delivers-and-starts-the-native-rollout`（kind）；`t9i-envtest-deployment-container-image-becomes-the-digest-reference`（envtest · test/integration/subscription_delivery_test.go）；三 Kind 全覆盖见 `t11-e2e-statefulset-daemonset-real-rollouts-succeed-per-target`（kind · test/e2e/observer_e2e_test.go） |
| 7 | 第三方 Workload 按 `imageFields[]` 更新多个字段 | `t10-e2e-multiple-imagefields-paths-deliver-digests-to-a-third-party-crd`（kind · test/e2e/imagefields_e2e_test.go）；`t10i-envtest-multiple-imagefields-paths-including-array-filter-become-digests`（envtest · test/integration/t10_imagefields_test.go） |
| 8 | `containerNames[]` 与 `imageFields[]` 同时出现被拒绝 | `rejects targets that set both containerNames and imageFields`（envtest · test/integration/webhook_test.go）；边界记录：`documents the webhook boundary: the CRD shape allows both locator fields`（envtest · test/integration/catalog_crd_test.go） |
| 9 | 无效字段路径 / 容器名缺失 / RBAC Forbidden / target NotFound 均报告明确原因 | 路径：`t10-e2e-invalid-field-gets-explicit-status-and-zero-writes`（kind）+ `t10i-envtest-every-invalid-field-class-gets-an-explicit-status-and-zero-writes`（envtest）；容器缺失：`TestSubscriptionDeliveryFailureSurfaces/container-absent-reported`（unit · internal/controller/subscription_delivery_controller_test.go）；Forbidden：`t12-e2e-delivery-stops-while-forbidden-and-resumes-when-regranted`（kind · test/e2e/t12_hardening_e2e_test.go）+ `t8-e2e-real-rbac-grant-and-revocation-flip-the-forbidden-state-within-the-recheck-period`（kind · test/e2e/sar_gate_e2e_test.go）；NotFound：`t9i-envtest-target-created-after-subscription-delivers-on-replay`（envtest）+ `TestSubscriptionDeliveryFailureSurfaces/target-missing-waits-without-writes`（unit） |
| 10 | 镜像字段已符合期望时不重复 patch | `t9i-envtest-equal-value-reconcile-produces-no-patch`、`t10i-envtest-equal-value-reconcile-of-imagefields-target-produces-no-patch`（envtest）；kind 腿：`t12-e2e-manager-kill-mid-delivery-recovers-to-eventual-consistency-from-persisted-state` 的 Consistently 断言（generation/image/事件数静止） |
| 11 | rollout progressing / succeeded / failed / unknown 分别报告 | `t11i-envtest-deployment-native-rollout-progression-observed-stage-by-stage`（envtest · progressing 逐阶段）；`t11-e2e-deployment-real-rollout-succeeds-cross-ns-events-mirror-floatup`、`t11-e2e-deployment-real-rollout-fails-and-image-applied-stays-true`（kind · succeeded/failed）；unknown 见场景 12 |
| 12 | （v1 重述）`statusPolicyRef` 不可用 ⇒ 如实 Unknown | `t11i-envtest-no-status-policy-and-policy-ref-targets-never-fabricate-success`（envtest · test/integration/t11_observer_test.go，含 policyRef 缺失与指向不存在两种腿）；`t11-e2e-deployment-real-rollout-succeeds-cross-ns-events-mirror-floatup`（kind · health Unknown + `StatusPolicyEvaluated=False` Condition）；第三方目标腿：`t10-e2e-multiple-imagefields-paths-deliver-digests-to-a-third-party-crd`（kind · rollout Unknown 永不虚构成功） |
| 13 | 多订阅修改同一字段触发冲突保护 | `t9-e2e-second-subscription-same-field-terminal-conflict-zero-writes`（kind）；`t10-e2e-same-path-second-subscription-conflicts-with-zero-writes`（kind）；`t9i-envtest-second-subscription-same-field-terminal-conflict-zero-writes`（envtest） |
| 14 | 控制器重启后重新 reconcile，不丢最终一致性 | `t12-e2e-manager-kill-mid-delivery-recovers-to-eventual-consistency-from-persisted-state`（kind · 投递中途强杀 manager pod → 同 spec 重建 → 纯持久化状态收敛，§12.9 记录不重不丢）；`t9i-envtest-manager-restart-converges-from-persisted-state-without-write-amplification`、`t9i-envtest-crash-before-status-write-recovers-from-persisted-ownership`（envtest）；`t4-rebuilds-registration-state-from-persisted-objects-after-crash-without-duplicate-image-change`（envtest · 登记侧重建） |
| 15 | OpenShift 模式与原生 trigger 不重复更新 | **v1 不交付（文档化推迟）**——见上文「范围裁定记录」第一条；无命名用例，出 v1 范围 |
| 16 | Shipwright 构建成功但镜像登记失败时，区分并报告两个阶段 | 构建阶段属外部系统（Shipwright 自有 `BuildRun.status.conditions`，见 `docs/quickstart.md` 管道示例）；登记阶段：`t4-ttl-gc-retains-failed-mapping-for-24h-with-diagnosable-reason`（envtest · Mapping `Complete=False/Failed` + 可诊断原因长期保留）；`t4-e2e-gc-retains-a-failed-mapping-for-the-longer-failure-ttl`（kind） |
| 17 | tag 不变、digest 变化时识别真实镜像变化 | `t4-detects-real-image-change-when-tag-stays-and-digest-changes`（envtest）；`t7-detects-external-tag-drift-and-attributes-the-image-change-to-the-import-source`（envtest · 导入侧漂移归因）；`t7-e2e-scheduled-import-detects-external-tag-drift-with-a-short-interval`（kind） |
| 18 | 凭据不泄漏到 status、事件、日志或错误消息 | `t12-e2e-pull-secret-canary-never-surfaces-across-import-and-delivery`（kind · 导入+投递全链 9 类表面扫雷）；`t7-e2e-credentials-from-a-pull-secret-never-surface-in-status-or-events`（kind · 导入侧）；`t7-credentials-from-a-pull-secret-never-surface-in-status-or-events`（envtest）；`TestImportPullSecretIsUsedAndNeverSurfaces`、`TestImportPullSecretFailuresAreDiagnosableAndLeakFree`（unit · internal/controller/import_controller_test.go） |

## 相关但非 §15 编号的补充覆盖

- **短周期计划导入（spec 测试决策 2）**：`t7-e2e-scheduled-import-detects-external-tag-drift-with-a-short-interval`
  （kind，`intervalSeconds: 15` + 短 manager flags）；`t7-scheduled-import-re-imports-the-tag-at-intervalseconds-under-the-injected-clock`
  （envtest，注入时钟精确周期）；`TestScheduledImportRunsAgainAfterTheIntervalUnderTheInjectedClock`（unit）。
- **真实 RBAC/SAR 拒绝路径**：场景 9 + `t8-envtest-*` 六条（test/integration/subscription_authz_controller_test.go）。
- **ADR-008 独占/聚合/Superseded/保留**：`t9i-envtest-recent-deliveries-bounded-and-superseded-latest-wins`、
  `t11i-envtest-latest-wins-superseded-terminal-across-the-engine-handoff`（envtest）；kind 冲突腿见场景 13。
- **跨 ns 投递与双 ns Events**：`t11-e2e-deployment-real-rollout-succeeds-cross-ns-events-mirror-floatup`（kind）。
- **TTL GC**：场景 16 的 kind/envtest 腿 + `t4-ttl-gc-deletes-successful-mapping-after-injected-clock-passes-success-ttl`、
  `t4-ttl-gc-applies-uniformly-to-the-import-task-kind`、`t7-ttl-gc-collects-completed-import-tasks-with-the-t4-machinery`（envtest）。

## 附加证据（CI 跳过、可选启用）

设置 `IMAGEFLOW_E2E_REGISTRY_HOST/_REPO/_TAG`（+可选 `_PULL_SECRET/_REGISTRY_CA/_PUSH_USERNAME/_PUSH_PASSWORD`）后，
T7 的三条 kind 用例改为打真实外部 Registry（环境契约见 `TESTING.md`）。它们为场景 2/4/17/18 提供
**额外**的真实-Registry 证据；CI 默认对 registry double 运行同名用例，因此上表映射不依赖该变量族。

## 运行方式

```bash
make test        # unit + envtest（test/integration，envtest 二进制经 KUBEBUILDER_ASSETS）
make test-e2e    # kind 全量（建集群 → 构建镜像 → 全部 lane → 拆集群）
```

CI 对应 `.github/workflows/test.yml`（Tests + Lint）与 `.github/workflows/test-e2e.yml`（E2E Tests）。
