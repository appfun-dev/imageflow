# Spec: Argo Rollouts Built-in Kind Support (v1.8)

- Status: Draft
- Date: 2026-11-20
- Author: Agent (grilling → to-spec workflow)
- Related: ADR-015（采纳评估，15 决策）, ADR-006（v1.8 修订，内建 Kind 清单）, ADR-003（SAR 授权沿用）, ADR-005（第三方写入合约，CloneSet 对照）, ADR-008（投递语义）, ADR-009（测试分层）, ADR-010（受控标签集）, ADR-014（Selector Target 正交）, GLOSSARY.md（Progressive Delivery Workload, Built-in Workload Adapter, Outcome Stages）

## Problem Statement

采用 Argo Rollouts 做渐进交付（canary / blue-green）的团队，其 Workload 不是 Deployment 而是 `Rollout`（argoproj.io/v1alpha1）。当前 ImageFlow 内建 Kind 只有 Deployment / StatefulSet / DaemonSet / Job / CronJob（ADR-006 v1.6），把 Rollout 写进 Subscription Target 会得到如实但无用的 `Unsupported` Condition；退到 Generic Image Field Resolver + `imageFields[]` 则状态永远 Unknown（ADR-005 合约），拿不到 Healthy / Degraded / Paused 语义——而这正是渐进交付团队最需要看到的信号。

ADR-006 决策 5 要求扩充内建 Kind 必须修订 ADR；ADR-015 已完成三轮 grilling（15 题）裁决：v1.8 采纳 Argo Rollouts 为内建 Kind，OpenKruise 全系暂不采纳（schemaless template 硬理由）。本 spec 将其落地为可实现的交付规格。

## Solution

Subscription Target（Named 与 Selector 两种定位形态均可）接受 `apiVersion: argoproj.io/v1alpha1, kind: Rollout`：

- **投递**：复用 pod template adapter 路径（`.spec.template.spec.containers[name].image`，与 Deployment 相同），SSA 写 image 字段；遇 SSA 409 冲突（rollouts-controller 字段规范化夺权，上游 issue #3281）升级 force 重试一次并记 Event；
- **观察**：observer 新增 Rollout 事实提取与完成判定——三合一终态（observedGeneration == generation 且 phase == Healthy 且 stableRS == currentPodHash → Succeeded；Degraded / ReplicaFailure / ProgressDeadlineExceeded → Failed）；`pause: {}` 无限期步映射 Progressing + Reason `AwaitingPromotion`（合法稳态，非故障）；
- **策略自治**：canary 步骤、pause、promote 等策略字段永不由 ImageFlow 写入，rollouts-controller 拥有 rollout 过程；
- **零新依赖**：status 以 unstructured 提取约 8 个字段 + converter 转 corev1.PodSpec，不引 argo-rollouts Go module；集群未装 Rollout CRD 时沿用既有 `KindNotInstalled` 报告路径，不加 feature gate。

## User Stories

1. As a platform operator, I want to target an Argo Rollout by name so that image updates flow into my canary workload like any built-in Kind.
2. As a platform operator, I want to target Rollouts by labelSelector so that a fleet of canary workloads receives the same image stream (composes with ADR-014).
3. As a platform operator, I want ImageFlow to write only the pod template image field so that my canary strategy (steps, pause, promote) stays owned by the Rollouts controller.
4. As a platform operator, I want the write to land at `.spec.template.spec.containers[name].image` so that Rollout-managed ReplicaSets pick up the new image on the next step.
5. As a platform operator, I want containerNames to select a subset of containers on a Rollout so that sidecars are not rewritten.
6. As a platform operator, I want SSA conflicts with the rollouts-controller resolved by a single forced retry so that field-normalization ownership grabs (upstream issue #3281) do not permanently block delivery.
7. As a platform operator, I want a forced SSA retry to emit an Event so that ownership contention is visible in the audit trail.
8. As a platform operator, I want a Rollout that has converged to Healthy with the new ReplicaSet stable to report Rollout=Succeeded so that I know delivery completed.
9. As a platform operator, I want status fields read only when observedGeneration matches generation so that ImageFlow never reports on a stale status snapshot (argo types.go warning).
10. As a platform operator, I want Degraded / ReplicaFailure / ProgressDeadlineExceeded phases to report Rollout=Failed so that a broken canary surfaces immediately.
11. As a platform operator, I want an indefinite `pause: {}` step to report Progressing with reason AwaitingPromotion so that waiting for human promotion is not mistaken for failure.
12. As a platform operator, I want AwaitingPromotion excluded from failure metric vocabularies so that on-call is not paged for a legal steady state.
13. As a platform operator, I want Rollout observation to stay live after Succeeded (no Job-style freeze) so that a later Degraded transition is reported honestly.
14. As a platform operator, I want container health on Rollout pods evaluated by the existing native health determination so that v1.5 health semantics apply unchanged.
15. As a platform operator, I want targeting a Rollout on a cluster without the Rollout CRD to report KindNotInstalled so that the misconfiguration is explicit.
16. As a platform operator, I want no new Go module dependency so that ImageFlow's dependency tree and version compatibility stay unchanged.
17. As a platform operator, I want SAR authorization for Rollout targets to work exactly like other built-in Kinds so that the ADR-003 contract needs no new gates.
18. As a platform operator, I want selector targets over Rollouts to respect the same match limit, zero-match, and per-object conflict semantics so that v1.7 invariants carry over unchanged.
19. As a platform operator, I want metrics for Rollout targets to keep the controlled label set (no object-name labels) so that cardinality stays bounded (ADR-010).
20. As a platform operator, I want end-to-end proof on a real kind cluster with the Rollouts controller installed so that the "write image → canary converges" loop is demonstrated, not simulated.
21. As a platform operator, I want a canary with a `pause: {}` step to be observable mid-flight at AwaitingPromotion in e2e so that the promotion gate mapping is proven against the real controller.
22. As a platform operator, I want documentation (glossary, ops reason vocabulary, scenario map) updated so that Rollout semantics are discoverable without reading the ADR.

## Implementation Decisions

### D1: API Surface — Zero Schema Change

`TargetReference.Kind` carries no enum restriction; `argoproj.io/v1alpha1, Kind=Rollout` is admissible today for both Named and Selector targets. No CRD schema or webhook change is required. Rollout targets compose with ADR-014 selector semantics unchanged (same namespace, same GVK, match limit, per-object conflict).

### D2: Delivery — Built-in Adapter Registration

Register a pod-template adapter for the Rollout GVK in the adapter registry (init-time registration, same shape-driven mechanism as Deployment/StatefulSet/DaemonSet: standard apply-nest path `.spec.template.spec.containers`). CronJob-style nested path is not needed. Plan stays a pure function.

### D3: Delivery — SSA Conflict Escalation

First apply attempt without force. On HTTP 409 apply-conflict (the rollouts-controller can claim `.spec.template.spec.containers` ownership during field normalization, upstream issue #3281), retry exactly once with force and record a Warning Event identifying the Rollout and the conflicting manager. A retry that still conflicts reports Failed honestly; no further retries beyond the controller's normal requeue. Retry budget = 1 per reconcile pass, not per event storm.

### D4: Observer — Unstructured Fact Extraction

Observer gains a Rollout case: read the object unstructured, extract `status.observedGeneration` (string per argo types.go — parse defensively, non-numeric → treat as unobserved), `status.phase`, `status.stableRS`, `status.currentPodHash`, `status.pauseConditions`, `status.updatedReplicas`, `status.replicas`, `status.readyReplicas`. Pod template converted to `corev1.PodSpec` via the existing converter for container-level inspection. No argo-rollouts Go module is imported.

### D5: Observer — Completion Verdict Matrix

Rollout is a long-running workload: it uses the Rollout outcome stage (not Completion) and is **never frozen** (same as Deployment — a later Degraded flip is reported).

- `observedGeneration != generation` → status untrusted → report Progressing (spec not yet observed).
- `observedGeneration == generation` + `phase == Healthy` + `stableRS == currentPodHash` → Succeeded.
- `phase` in {Degraded, ReplicaFailure, ProgressDeadlineExceeded} → Failed (phase reason surfaced in message).
- `pauseConditions` contains a canary pause step → Progressing with Reason `AwaitingPromotion` (steady legal state, not in failure vocabulary).
- Otherwise → Progressing.

### D6: Observer — Health Composition

After Rollout=Succeeded, container health reuses the v1.5 native health determination on the Rollout's pods (container readiness + restart count); CUE healthKey stays the priority override. No new health semantics.

### D7: Authorization — No New Gate

Rollout targets pass through the existing SAR paths unchanged: named targets use the ADR-003 named SAR; selector targets use the ADR-014 namespace-level unnamed SAR on the declared GVK. Rollout GVK is just another resource attribute.

### D8: RBAC Manifest

Manager role gains `get`/`list`/`watch` + `update`/`patch` on `rollouts.argoproj.io`. On clusters without the CRD the verbs are inert; no optional-manifest split.

### D9: Absent CRD — Reuse KindNotInstalled

Targeting a Rollout on a cluster without the CRD reports through the engine's existing `KindNotInstalled` WHY path (Classify / Unsupported taxonomy). No feature gate, no new configuration surface.

### D10: Metrics & Reason Vocabulary

`AwaitingPromotion` registered as a Progressing reason (not failure vocabulary) in the metrics contract and ops documentation. No new metric series; no per-object labels (ADR-010).

### D11: End-to-End Proof

kind-based e2e installs argo-rollouts (pinned install.yaml) as a CI e2e job step. Scenarios (ADR-015 decision 15): (a) canary without pause converges to Succeeded after image write; (b) canary with `pause: {}` step is observed at Progressing/AwaitingPromotion mid-flight; (c) SSA 409 escalation is covered at unit level (not e2e). envtest is not a substitute — without the rollouts-controller the delivery loop cannot close.

### D12: Documentation Surface

GLOSSARY terms are already landed (ADR-015 wave C). This spec drives: ops.md reason vocabulary (+AwaitingPromotion), TESTING.md scenario matrix registration, scenario-map.md entries, ADR-006 v1.8 amendment already references this rollout support.

## Testing Decisions

What makes a good test (unchanged): external behavior only, no implementation detail assertions; highest existing seam preferred.

Seams (all existing, one new CI step):

1. **Adapter unit tests** — same table-driven seam as the five existing built-in Kinds: Plan for a Rollout fixture produces the standard apply-nest plan; containerNames subset honored.
2. **SSA escalation unit tests** — fake client + interceptor injecting a 409 apply-conflict (prior art: v1.7 #83 interceptor injecting the real API error shape), asserting exactly one forced retry, the Warning Event, and honest Failed on persistent conflict.
3. **Observer verdict unit tests** — table-driven over the D5 matrix (prior art: v1.6 Job/Deployment verdict tables), including the string-typed observedGeneration edge (empty / non-numeric / mismatch).
4. **Observer extraction unit tests** — unstructured Rollout fixture → facts struct; converter path to corev1.PodSpec.
5. **envtest integration** — Rollout CRD installed (no controller): admission, SAR, field write, and status read paths against the real API server (prior art: v1.7 t81s lane). Explicitly cannot test the canary loop.
6. **kind e2e (t87-e2e lane)** — argo-rollouts installed in CI; scenarios per D11. Allure labeling per ADR-009; scenario-map.md registration (naming follows t86 precedent).

## Out of Scope

- OpenKruise entire family (CloneSet first): schemaless template blocks fine-grained SSA field ownership; the whole-template-apply / JSON-Patch route changes the write model and needs its own evaluation (ADR-015 decision 13, open question).
- Argo AnalysisTemplate / Experiment / TrafficRouting integration — ImageFlow neither reads analysis verdicts nor manipulates traffic weights.
- OpenShift DeploymentConfig.
- ImageFlow writing any Rollout strategy field (steps, pause, promote) — strategy autonomy is permanent (ADR-015 decision 4).
- Cross-namespace Rollout targeting (ADR-014 same-namespace rule carries over).
- In-place update semantics (Kruise-specific, irrelevant to Argo Rollouts).

## Further Notes

Upstream fact anchors used by this spec (from the v1.8 grill research pass, 2026-11-20):

- Argo Rollouts: `status.observedGeneration` is a **string**; argoproj/argo-rollouts types.go warns other status fields are trustworthy only when it equals generation. `pause: {}` waits indefinitely for human promotion; pauseConditions are written by the rollouts-controller and cleared by humans. Issue #3281 documents the controller claiming `.spec.template.spec.containers` SSA ownership during field normalization. Without trafficRouting/analysis a Rollout behaves as a stepped Deployment with zero interference. Apache-2.0; CI covers K8s 1.34–1.37.
- OpenKruise CloneSet: `spec.template` carries PreserveUnknownFields + Schemaless markers, making it an atomic field — fine-grained SSA on `containers[].image` is not possible; partition semantics and InPlace updates (kruise-daemon) add further semantic distance. Deferred with rationale recorded.
