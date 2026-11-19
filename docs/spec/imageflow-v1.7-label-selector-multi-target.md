# Spec: Label Selector Multi-Target (v1.7)

- Status: Draft
- Date: 2026-11-20
- Author: Agent (grilling → to-spec workflow)
- Related: ADR-014, ADR-006（v1.7 指针补注）, ADR-003（SAR 授权）, ADR-008（投递语义）, ADR-005（写入合约）, ADR-010（受控标签集）, GLOSSARY.md（Selector Target, Named Target, Subscription Target, Delivery Result, Outcome Stages）

## Problem Statement

平台/应用团队经常需要把同一条镜像流投递给**一组同构 Workload**——例如某个团队 namespace 下所有带 `app=payment` 的 Deployment，或一组金丝雀实例。当前 Subscription Target 只能按 `namespace + name` 显式指定单个对象（ADR-006 v1.6 模型），用户必须：

1. 为每个对象手写一条 target，订阅随对象增减而频繁改动；
2. 新建/扩容的对象在被手工加入订阅前收不到镜像更新，形成静默漂移；
3. 删除对象后残留 target 持续报 Unknown，污染订阅整体状态。

ADR-006 将 Label Selector 多目标推迟至 v1.7；ADR-014 已完成 15 题 grilling 共识（Wave A/B/C）。本 spec 将其落地为可实现的交付规格。

## Solution

`TargetReference` 新增可选 `labelSelector`（`metav1.LabelSelector`），与 `name` 互斥（webhook 强制二选一）。一条 Selector Target 在同 namespace 内动态匹配同 GVK 的一组对象：

- 控制器以**服务端 LabelSelector 过滤的 informer** 持续跟踪匹配集，新匹配对象立即投递当前 desired image；摘除 label 的对象保留已写镜像、不回收；
- 已 Completed 的 Pod/Job（错误退出、任务完成）完全排除出匹配集；
- 匹配数受硬上限保护（启动时可配置，默认 50），超限报 `Failed/MatchLimitExceeded`；
- 授权在 admission 时做 **namespace 级不点名 SAR**（创建者对该 namespace 内该 GVK 的 update 权限），运行期不逐对象校验；
- status 保持 per-target 一条记录：跨匹配对象最坏态聚合 + matched 计数，per-object 明细走 Events；冲突逐对象独立检测（先到先得、后来零写入逐对象延续）。

## User Stories

1. As a platform operator, I want one subscription target to deliver images to all Deployments matching a label selector so that I don't maintain one target per object.
2. As a platform operator, I want newly created matching objects to receive the current desired image immediately so that scaling out never leaves objects on stale images.
3. As a platform operator, I want objects whose labels are removed to keep their last delivered image so that label experiments never roll back running workloads.
4. As a platform operator, I want label-removed objects to disappear from the target's status detail so that the matched count reflects reality.
5. As a platform operator, I want `name` and `labelSelector` to be mutually exclusive at admission so that ambiguous targets are rejected before they reach the controller.
6. As a platform operator, I want selector matching confined to the target's namespace and declared Kind so that authorization and observation semantics stay unchanged from ADR-003/ADR-006.
7. As a platform operator, I want one namespace-level SAR at subscription admission to authorize selector targets so that I am not surprised by per-object denials at delivery time.
8. As a platform operator, I want the SAR to be re-verified only when spec.targets changes so that unrelated updates add no SAR load (existing webhook behavior preserved).
9. As a platform operator, I want a match count hard limit (default 50, configurable at controller startup) so that a runaway selector cannot fan out unboundedly.
10. As a platform operator, I want exceeded limits reported as Failed with reason MatchLimitExceeded plus a Warning Event so that quota breaches are visible in both status and audit trail.
11. As a platform operator, I want a selector matching zero objects to report Unknown with reason NoMatches so that the legitimate "subscribe first, label later" workflow does not look like a failure.
12. As a platform operator, I want NoMatches not to block other targets in the same subscription so that one empty selector never poisons the whole subscription.
13. As a platform operator, I want field-level conflicts detected per matched object so that one conflicting object never blocks delivery to the rest.
14. As a platform operator, I want conflicting objects reported via Conflict Events with the conflicting owner identified so that I can resolve first-come-first-served disputes.
15. As a platform operator, I want per-target status to aggregate matched objects by worst-case (Failed > Unknown > Progressing > Succeeded) so that a single failing object marks the target Failed, consistent with ADR-008.
16. As a platform operator, I want per-target status to include the matched object count so that I can see fan-out size at a glance.
17. As a platform operator, I want per-object delivery detail in Events rather than status so that the status object stays bounded regardless of match count.
18. As a platform operator, I want Superseded recorded per object only when that object was actually overwritten by a newer tag so that status reflects what really happened on each object.
19. As a platform operator, I want metrics aggregated by subscription + target without object-name labels so that Prometheus cardinality stays bounded (ADR-010).
20. As a platform operator, I want completed Pods/Jobs excluded from matching so that no writes are attempted against objects where an image update can never take effect.
21. As a platform operator, I want completed objects not to count against the match limit so that quota reflects actionable targets only.
22. As a platform operator, I want selector targets to combine with both containerNames and imageFields so that built-in kinds and third-party CRDs get the same multi-target capability.
23. As a platform operator, I want third-party CRD matches to report honest Unknown status so that ImageFlow never fabricates rollout semantics (ADR-005).
24. As a platform operator, I want the controller to watch only matching objects via server-side selector filtering so that memory usage does not scale with unrelated namespace content.
25. As a platform operator, I want label changes on watched objects to trigger reconcile naturally so that the dynamic match set stays live without polling.

## Implementation Decisions

### D1: API Schema — TargetReference.labelSelector

`TargetReference` gains optional `LabelSelector *metav1.LabelSelector`. `Name` becomes optional (anchored in v1.6 spec §D2). Exactly one of `name` / `labelSelector` must be set; enforced by admission webhook (not CEL), consistent with existing T3 mutual-exclusion validation for containerNames/imageFields. `Namespace`, `APIVersion`, `Kind` remain required.

### D2: Webhook Mutual Exclusion + Namespace-Level SAR

Two admission gates extend existing seams:

1. **Validation**: reject targets where both or neither of `name`/`labelSelector` is set; reject empty labelSelector (`matchLabels` and `matchExpressions` both empty).
2. **Authorization**: for selector targets, one SubjectAccessReview per target checks the created-by subject's `update` verb on the declared GVK within the declared namespace, without resourceName. SAR result cached per existing TTL mechanism (internal/config flag). Updates not touching spec.targets skip SAR entirely (existing behavior).

### D3: Match Resolution — Server-Side Filtered Informer

For each selector target, the controller establishes an informer on the target GVK with the label selector pushed into the ListWatch options (server-side filtering). The informer cache holds only matching objects. Label changes produce delete/add events on the filtered informer, naturally driving reconcile. Match resolution is re-evaluated on every reconcile from the informer cache; no polling.

### D4: Delivery — Per-Object SSA with Per-Object Conflict Detection

Delivery iterates the resolved match set and applies SSA per object with the existing field-manager discipline (ADR-008). Conflicts are detected per object: a conflicting object is skipped and reported via Conflict Event; remaining objects proceed. The first-come-first-served, zero-write-for-later-comer rule applies per object.

### D5: Completed-Object Exclusion

Objects in terminal completed state are excluded from the match set entirely: not matched, not delivered, not counted toward the match limit. For Pods this means phase Succeeded/Failed; for Jobs, conditions Complete=True or Failed=True. The exclusion is evaluated at match-resolution time from informer cache state.

### D6: Match Limit

A controller startup flag (internal/config pattern, default 50) bounds the match count per target. Matches beyond the limit are not delivered; the target reports Failed with reason `MatchLimitExceeded` and a Warning Event naming the limit and observed count.

### D7: Zero-Match Semantics

Zero matches is a legal empty set: the target reports Unknown with reason `NoMatches` and a Warning Event. It does not block other targets in the same subscription and does not count as Failed in subscription-level worst-case aggregation (Unknown > Progressing ordering applies per ADR-008).

### D8: Status Model — Per-Target Aggregate + Matched Count

`TargetDeliveryStatus` remains keyed by targetRef — one entry per selector target. It carries:

- worst-case aggregate across matched objects (Failed > Unknown > Progressing > Succeeded);
- a matched object count field;
- existing bounded history (3/5/5) unchanged.

Per-object detail (per-object outcome, conflict identity, superseded records) is emitted as Events only. No per-object status expansion.

### D9: Superseded Semantics Per Object

Superseded is recorded for an object only when that object was actually overwritten by a newer tag delivery. Label-removed objects retaining their last image are not Superseded — they simply leave the match set. Aggregate latest-wins semantics unchanged.

### D10: Metrics Without Object-Name Labels

Delivery/observer metrics for selector targets use subscription + target dimensions only. Object names never appear in metric labels. Per-object observability is served by Events, consistent with ADR-010's controlled label set.

### D11: Field-Location Orthogonality

Selector targets compose with both `containerNames` (built-in kinds) and `imageFields` (third-party kinds). Object-location (name vs selector) and field-location (containerNames vs imageFields) are independent dimensions. Third-party CRD matches deliver fields but report Unknown status per ADR-005.

### D12: Terminology and Documentation

GLOSSARY.md already landed (ADR-014 commit): Selector Target and Named Target entries; Subscription Target updated with both location forms. ADR-006 unresolved-question pointer landed. No further glossary changes.

## Testing Decisions

### What Makes a Good Test

Tests verify external behavior (admission accept/reject, delivered image fields, aggregate status, event content), not internal implementation details. Each test constructs a specific match-set state and asserts the resulting target status and events. Tests cover all verdict paths: normal multi-match, zero match, limit exceeded, partial conflict, label removal, completed-object exclusion.

### Test Seams

All work uses existing seams — no new seams introduced:

1. **Admission validation**: `internal/webhook/webhook_test.go` — name/labelSelector mutual exclusion, empty selector rejection.
2. **Admission SAR**: `internal/webhook/subscription_authorize_test.go` — namespace-level SAR allow/deny for selector targets, SAR skipped on non-target updates.
3. **Delivery controller**: `internal/controller/subscription_delivery_controller_test.go` — match resolution, per-object SSA, per-object conflict skip, limit exceeded, label-removal retention.
4. **Observer/status**: `internal/controller/subscription_observer_test.go` — worst-case aggregation across matched objects, matched count, NoMatches, per-object Superseded.
5. **Envtest**: existing `*_envtest_test.go` pattern for webhook SAR against envtest apiserver.
6. **E2E**: `test/e2e/` following the `t11-e2e-*` naming convention — selector target against real kind cluster: multi-match delivery, scale-out pickup, label removal, zero match.

### Prior Art

- `subscription_authorize_test.go` table-driven SAR allow/deny tests — same pattern for selector SAR.
- `TestObserverDetermineDeploymentRolloutStates` table-driven verdict tests — same pattern for multi-object aggregation.
- v1.6 `t11-e2e-*` cross-namespace event tests — template for selector E2E event assertions.
- ADR-009 testing strategy: Allure traceability tags on new cases.

## Out of Scope

- **Cross-namespace selector matching**: match domain is strictly the target's namespace. Cross-namespace requires redesigning ADR-003's authorization model.
- **New workload kinds**: Argo Rollouts / Kruise evaluation remains an ADR-006 unresolved question, independent of this feature.
- **Image rollback on label removal**: removed objects keep their last delivered image by design (ADR-014 Q3). No revert path.
- **Per-object status CRD or status expansion**: per-object detail lives in Events only.
- **Object-name metric labels**: permanently excluded (cardinality).
- **Runtime per-object SAR**: authorization is admission-time namespace-level only.
- **Selector on source side**: this feature concerns targets only; ImageStreamTag source selection is unchanged.

## Further Notes

- The v1.6 spec §D2 anchor ("Name becomes optional") is fulfilled by D1.
- The informer-per-selector design means N selector targets on the same GVK+selector share informer cost only if the cache is keyed accordingly; implementation should reuse informers where the (GVK, namespace, selector) tuple is identical.
- `MatchLimitExceeded` and `NoMatches` are new Condition Reasons introduced by ADR-014 and must be registered in the API package's reason constants.
- The match-limit flag follows the existing internal/config startup-flag pattern (same as SAR recheck period).
- After implementation, the v1.5/v1.6 spec header Status drift (Draft despite merged) should be corrected together with flipping this spec's status on merge — a known docs-hygiene item from the v1.7 盘点.
