# Spec: Job/CronJob Support (v1.6)

- Status: Draft
- Date: 2026-11-12
- Author: Agent (grilling → to-spec workflow)
- Related: ADR-006, ADR-005 §3/§4, GLOSSARY.md (Outcome Stages, Delivery Result, Subscription Target)

## Problem Statement

ImageFlow 当前仅支持 Deployment/StatefulSet/DaemonSet 三种内建 Workload Kind（ADR-006）。批处理场景（Job）和定时任务（CronJob）无法通过 ImageSubscription 进行镜像投递与状态观察，用户必须手动更新镜像或使用 imageFields 放弃 Outcome Stages 观察能力。

Job/CronJob 与 long-running workloads 有本质语义差异：Job 是一次性执行，"完成"而非"滚动更新"；CronJob 调度创建独立 Job 实例，镜像变更仅影响未来创建的 Job。这些差异要求新的观察模型，不能简单复用 Deployment 的 rollout 语义。

## Solution

为 Job 和 CronJob 实现内建适配器，扩展 Outcome Stages 模型以支持 completion 语义。Job 的 Rollout 阶段映射为 completion 结果（Succeeded=completed, Failed=failed），Health 阶段直接由 completion 结果决定（成功即健康）。CronJob 通过 ownerReferences 发现最近创建的 Job 并应用相同的 completion 观察逻辑。v1.6 保持单对象 target 模型（namespace+name），Label Selector 多目标能力拆分为 v1.7 独立特性。

## User Stories

1. As an ImageFlow operator, I want to deliver image changes to Job pod templates so that newly created Jobs use the updated image.
2. As an ImageFlow operator, I want to deliver image changes to CronJob jobTemplate so that future scheduled Jobs use the updated image.
3. As an ImageFlow operator, I want to observe Job completion status (Complete/Failed conditions) through the Rollout stage so that I know whether the delivered image ran successfully.
4. As an ImageFlow operator, I want to observe CronJob's most recent Job completion status so that I can monitor scheduled task outcomes.
5. As an ImageFlow operator, I want a completed Job to report Healthy automatically so that I don't need separate health configuration for batch workloads.
6. As an ImageFlow operator, I want a failed Job to report Unhealthy so that I am alerted to batch processing issues through the same Health stage used for long-running workloads.
7. As an ImageFlow operator, I want a running Job to report Unknown health so that I am not misled by premature health verdicts.
8. As an ImageFlow operator, I want my existing CUE statusPolicyRef configuration to work with Job/CronJob targets so that custom completion logic takes precedence over native observation.
9. As an ImageFlow operator, I want Job/CronJob targets without statusPolicyRef to get native completion observation automatically so that batch workloads have zero-config status coverage.
10. As an ImageFlow operator, I want ImageFlow to update only the Job template without deleting or recreating running Jobs so that the controller respects the never-deploys principle (ADR-005 §1).
11. As an ImageFlow operator, I want CronJob observation to use ownerReferences for Job discovery so that the observation is reliable across all Kubernetes versions.
12. As an ImageFlow operator, I want Job/CronJob completion events in both namespaces so that cross-namespace subscribers receive the same audit trail as Deployment targets.
13. As an ImageFlow operator, I want worst-case aggregation to work across mixed Job and Deployment targets so that a single failing Job marks the subscription as Failed.
14. As an ImageFlow operator, I want Job targets to report Progressing while the Job is actively running so that I can distinguish "still executing" from "unknown state".
15. As an ImageFlow operator, I want CronJob targets with no scheduled Jobs yet to report honest Unknown with an explanatory message so that I understand why there is no status.
16. As an ImageFlow operator, I want the delivery engine to locate Job containers by name using the same pod-template adapter pattern so that the write surface is consistent with Deployment/STS/DS.
17. As an ImageFlow operator, I want CronJob's nested jobTemplate path to be handled transparently so that I configure CronJob targets the same way as other kinds.
18. As an ImageFlow operator, I want Job completion messages to include the Job name and completion time so that I can correlate status with specific Job executions.
19. As an ImageFlow operator, I want third-party Job-like CRDs to remain honestly Unsupported so that ImageFlow does not fabricate completion semantics for unknown resources.
20. As an ImageFlow operator, I want Label Selector multi-target support to be planned as a separate v1.7 feature so that v1.6 delivers a focused, testable scope.

## Implementation Decisions

### D1: Completion Semantics Mapped to RolloutStatus

Job/CronJob completion results are expressed through the existing `RolloutStatus` enum (Succeeded/Failed/Progressing/Unknown). No new API fields are introduced.

| Job Condition | RolloutStatus | Rationale |
|---------------|---------------|-----------|
| conditions[Complete=True] | Succeeded | Job finished successfully |
| conditions[Failed=True] | Failed | Job terminated with failure |
| active > 0, no terminal condition | Progressing | Job is still running |
| No conditions, no active pods | Unknown | No observation available |

Rationale: Outcome Stages (GLOSSARY.md) already defines Rollout as the stage between ImageApplied and Health. Completion is the Job-equivalent of rollout — the question "did the new image deploy successfully?" maps to "did the Job complete successfully?". Worst-case aggregation (ADR-008) works unchanged across mixed target kinds.

### D2: Single-Object Target Model (Label Selector Deferred)

v1.6 retains the existing TargetReference model: each target specifies exactly one object by namespace + name. Label Selector / Label Matcher for multi-target matching is deferred to v1.7 as an independent feature requiring its own ADR.

Rationale: Label Selector introduces 1:N target semantics that affect delivery (multi-object SSA), observer (multi-workload aggregation per target), and status (per-match vs per-target reporting). This is orthogonal to completion semantics and should be designed independently.

### D3: Job Template Update Only (No Auto-Recreate)

When an ImageSubscription delivers a new image to a Job target, ImageFlow updates the Job's pod template container image field. It does NOT delete or recreate the Job. Kubernetes Jobs are immutable once created; the updated template only affects Jobs created after the update (e.g., by CronJob scheduling or manual recreation).

Rationale: Deleting Jobs is a side-effect operation that exceeds ImageFlow's never-deploys principle (ADR-005 §1). The delivery engine's responsibility ends at field update; Job lifecycle management belongs to the operator or external controllers.

### D4: CronJob Observes Most Recent Job by creationTimestamp

For CronJob targets, the observer lists Jobs owned by the CronJob (via ownerReferences with controller=true) and selects the most recent by creationTimestamp. The completion status of that Job becomes the CronJob target's RolloutStatus.

If no Jobs exist for the CronJob, RolloutStatus = Unknown with message "no Jobs scheduled yet".

Rationale: The most recent Job reflects the latest image delivery effect. Using creationTimestamp is deterministic and simple. Alternatives considered and rejected:
- Most recent *successful* Job: ignores failures (exactly the signal operators need)
- All active Jobs aggregated: noisy for high-frequency CronJobs, complex semantics

### D5: ownerReferences for CronJob → Job Discovery

The observer discovers Jobs belonging to a CronJob through standard Kubernetes ownerReferences (controller=true). This mechanism is available in all Kubernetes versions and is the same mechanism used by garbage collection.

The `batch.kubernetes.io/cronjob` annotation (K8s 1.28+) may be used as supplementary verification but is not the primary discovery mechanism.

### D6: Completion-Based Health Determination

Job/CronJob health is determined directly from completion status, without pod-level scanning:

| RolloutStatus | HealthStatus | Rationale |
|---------------|--------------|-----------|
| Succeeded | Healthy | Successful completion = healthy |
| Failed | Unhealthy | Failed completion = unhealthy |
| Progressing | Unknown | Running Jobs have no meaningful readiness signal |
| Unknown | Unknown | No observation = no health verdict |

Rationale: Unlike long-running workloads, Jobs do not serve traffic and have no meaningful readiness probe semantics. A Job pod's readiness is transient (it terminates on completion). The completion result IS the health signal for batch workloads. This avoids reusing scanPodsForHealth which was designed for persistent container readiness.

### D7: CronJob Nested Pod Template Adapter

CronJob image delivery uses a specialized adapter that navigates the nested path `.spec.jobTemplate.spec.template.spec.containers[name=X].image`. Job delivery reuses the existing pod-template adapter path `.spec.template.spec.containers[name=X].image`.

Both adapters share the same container-name location discipline (exact match, no default guessing, spec decision 6 from ADR-006).

### D8: ADR-006 Amendment

ADR-006 must be amended to:
- Replace "首批 Kind" terminology with "内建 Kind" to reflect the expanded set
- Add Job and CronJob to the built-in adapter registry
- Document completion semantics as distinct from rollout semantics
- Record that Label Selector multi-target is deferred (not rejected)
- Update status to Amended (v1.6)

### D9: Event Message Format for Completion

Completion events use completion-aware message format distinct from rollout messages:
- Succeeded: `"completion of <image> succeeded: Job <name> completed at <time>"`
- Failed: `"completion of <image> failed: Job <name> failed with reason <reason>"`
- Progressing: `"completion in progress: Job <name> active since <time>"`
- CronJob delegation: `"observing most recent Job <name> for CronJob <cronjob-name>: <status>"`
- No Jobs: `"no Jobs scheduled yet for CronJob <cronjob-name>; completion stays Unknown"`

### D10: CUE StatusPolicyRef Priority Preserved

The existing priority chain applies unchanged: if a Job/CronJob target has `statusPolicyRef` configured with evaluable keys, CUE evaluation takes precedence over native completion observation. Native completion is the fallback when no CUE policy is configured.

## Testing Decisions

### What Makes a Good Test

Tests verify external behavior (completion status output, health verdict, event content), not internal implementation details. Each test constructs a specific Job/CronJob state and asserts the resulting RolloutStatus/HealthStatus. Tests cover all verdict paths and verify CUE priority override.

### Modules Tested

- Observer completion determination logic (new function alongside determineHealth)
- Observer kind classification (observableBuiltinKind extension)
- Delivery adapter Plan generation for Job and CronJob paths
- CronJob → Job discovery via ownerReferences
- Health determination mapping from completion status
- Event message formatting for completion verdicts

### Prior Art

- `TestObserverDetermineDeploymentRolloutStates`: table-driven rollout verdict tests — same pattern for completion states
- `TestObserverDetermineDeploymentHealthStates`: table-driven health verdict tests — same pattern for completion-based health
- `TestObserverObservableBuiltinKindClassification`: kind classification tests — extend with Job/CronJob cases
- `TestObserverReconcileDeploymentSucceededDualNamespaceEvents`: dual-namespace event tests — same pattern for completion events
- E2E test `t11-e2e-deployment-real-rollout-succeeds-cross-ns-events-mirror-floatup`: real-cluster rollout observation — template for Job completion E2E

### Test Seam

All unit tests use the existing seam in `subscription_observer_test.go`, following the established table-driven pattern. New test functions:
- `TestObserverDetermineJobCompletionStates`
- `TestObserverDetermineCronJobCompletionStates`
- `TestObserverDetermineJobHealthFromCompletion`
- `TestObserverDetermineCronJobHealthFromCompletion`

Delivery adapter tests extend the existing `delivery_test.go` suite.

E2E tests follow the `t11e-*` naming convention in `observer_e2e_test.go`, adding Job and CronJob scenarios alongside existing Deployment/STS/DS tests.

## Out of Scope

- **Label Selector / multi-target**: One target matches multiple resources via labelSelector. Deferred to v1.7 as an independent feature requiring its own ADR and API design.
- **Job auto-recreate on image change**: ImageFlow updates templates only; it does not delete or recreate Jobs. Job lifecycle is operator-managed.
- **CronJob suspend/resume management**: ImageFlow does not modify CronJob scheduling state.
- **Job history/TTL management**: Job cleanup is cluster-managed via TTL controller or operator policy.
- **Parallel Job observation for CronJob**: Only the most recent Job is observed. Aggregating across multiple concurrent Jobs is out of scope.
- **Third-party Job-like CRDs**: Custom resources that behave like Jobs remain Unsupported without imageFields + statusPolicyRef.
- **Application-layer completion checks**: Database migrations, data validation, or business logic verification within Jobs. Native completion covers only Kubernetes Job status conditions.

## Further Notes

- This spec establishes the pattern for extending ADR-006 with new workload kinds. Future additions (Argo Rollouts, Kruise CloneSet) should follow the same grilling → to-spec → ADR amendment workflow.
- Completion semantics reuse RolloutStatus rather than introducing CompletionStatus to avoid API version bump. If future kinds require fundamentally different status dimensions, a CompletionStatus field may be reconsidered with proper migration planning.
- The Label Selector feature (v1.7) will need to address: API schema changes (Name becomes optional), delivery controller multi-object SSA, observer multi-workload aggregation per target, and status model expansion. These are non-trivial and justify independent design.
- Job/CronJob support validates that ImageFlow's Outcome Stages model generalizes beyond long-running workloads. The three-stage chain (ImageApplied → Rollout/Completion → Health) proves flexible enough for batch semantics.
