# Spec: Native Health Determination (v1.5)

- Status: Draft
- Date: 2026-11-10
- Author: Agent (grill session with user)
- Related: ADR-005 §3/§4/§8.2, ADR-006, ADR-013, Issue #46

## Problem Statement

所有 first-batch workload kinds（Deployment/StatefulSet/DaemonSet）的 health stage 始终为 Unknown。用户无法通过 ImageFlow 获得原生健康状态反馈，仅当配置了 CUE `statusPolicyRef.healthKey` 时才能获得 health 评估。这违反了 ADR-005 §3 定义的 `healthStatus(Healthy|Unhealthy|Unknown)` 合约——字段存在但从未被 native observer 填充。

## Solution

为 first-batch workload kinds 实现基于 pod-level signals 的 native health determination，作为 rollout determination 的后置步骤。仅在 rollout = Succeeded 时评估 health，CUE strategy（healthKey）优先于 native。未配置 healthKey 的 first-batch target 自动获得 native health 覆盖，无需任何配置变更。

## User Stories

1. As an ImageFlow operator, I want Deployment targets to report Healthy when all containers are Ready and have no restart loops, so that I can confirm the deployed image is running correctly without configuring a CUE strategy.
2. As an ImageFlow operator, I want StatefulSet targets to report Healthy based on pod container readiness, so that stateful workloads get the same native health coverage as Deployments.
3. As an ImageFlow operator, I want DaemonSet targets to report Healthy based on pod container readiness, so that node-level agents get native health coverage.
4. As an ImageFlow operator, I want a target to report Unhealthy when a container has restartCount > 0 and is currently not Ready, so that I am alerted to crash loops immediately.
5. As an ImageFlow operator, I want a target to report Healthy when a container has restarted but is now Ready, so that self-healed workloads are correctly reported as healthy.
6. As an ImageFlow operator, I want health to remain Unknown while rollout is Progressing, so that I don't receive misleading health signals during deployment.
7. As an ImageFlow operator, I want health to remain Unknown when rollout has Failed, so that health assessment is scoped to successfully deployed images only.
8. As an ImageFlow operator, I want my existing CUE healthKey configuration to take precedence over native health, so that v1.3 behavior is preserved and custom health logic is respected.
9. As an ImageFlow operator, I want third-party targets (non-first-batch kinds) without healthKey to continue reporting Unknown, so that the observer never fabricates health for unobservable workloads.
10. As an ImageFlow operator, I want health determination to share the same pod list operation as rollout determination, so that API server load is minimized.
11. As an ImageFlow operator, I want the health condition message to explain why a target is Healthy/Unhealthy/Unknown, so that I can diagnose issues from the Subscription status alone.
12. As an ImageFlow operator, I want health determination to be independent of rollout determination logic, so that changes to rollout rules don't inadvertently affect health reporting.

## Implementation Decisions

### D1: Health Signal Source — Pod-Level Only

Health determination uses **pod-level signals only**:
- Container readiness (`containerStatus.Ready`)
- Container restart count (`containerStatus.RestartCount`)
- Container waiting reason (CrashLoopBackOff detection)

Workload-level conditions (e.g., Deployment Available) are **not** used. This keeps the logic uniform across all three first-batch kinds and avoids Kind-specific branching.

### D2: Rollout Gate — Health Evaluated Only After Rollout Succeeded

Health determination executes **only when rollout status = Succeeded**. When rollout is Progressing or Failed, health = Unknown with a message explaining the gate.

Rationale: During Progressing, pods may legitimately be not-ready (still starting). After Failed, the image was not successfully deployed, so health assessment is meaningless. This enforces clean stage separation per ADR-005 §3.

### D3: Health Verdict Rules

For each container matching the desired image in pods owned by the workload:

| Condition | Verdict |
|-----------|---------|
| All containers Ready=true AND restartCount=0 | Healthy |
| All containers Ready=true AND some restartCount>0 | Healthy (self-healed) |
| Any container Ready=false AND restartCount>0 | Unhealthy (crash loop) |
| Any container Ready=false AND restartCount=0 AND rollout=Succeeded | Unhealthy (failed to become ready after successful rollout) |
| No pods found matching desired image | Unknown |
| Pod list fails | Unknown |

Aggregation: **worst-case wins** (Unhealthy > Unknown > Healthy), consistent with cross-target aggregation in ADR-005 §3.

### D4: CUE Strategy Priority

Priority chain for health determination:
1. `statusPolicyRef.healthKey` configured → use CUE evaluator (v1.3 behavior, unchanged)
2. First-batch kind AND no healthKey → use native health determination (NEW)
3. Otherwise → Unknown (existing behavior)

This is symmetric with rollout determination's priority chain (rolloutKey → CUE; else native; else Unknown). Existing v1.3 configurations are unaffected.

### D5: Shared Pod List Infrastructure

Health determination shares the **same pod list result** as rollout determination within a single observer reconcile pass. The pod list is fetched once and passed to both `determineRollout` and `determineHealth` functions.

Implementation: extract pod listing into a shared helper that returns filtered pods for the workload, then pass the result to both determination functions.

### D6: Health Condition Message Format

Messages follow the existing rollout message pattern:
- Healthy: `"health of <image> is healthy per native <Kind> status: all N containers ready, M total restarts"`
- Unhealthy: `"health of <image> is unhealthy: container <name> in pod <pod> has restartCount=N and is not ready (<reason>)"`
- Unknown (rollout gate): `"health stage is not evaluated: rollout is <Progressing|Failed>, health assessment requires rollout Succeeded (ADR-005 §3)"`
- Unknown (no pods): `"health of <image> is unknown: no pods found matching desired image"`

### D7: Observer Reconcile Loop Integration

In the observer reconcile loop, after `determineRollout`:
1. If rollout ≠ Succeeded → set health = Unknown with gate message
2. If rollout = Succeeded AND healthKey configured → use existing CUE path
3. If rollout = Succeeded AND first-batch kind AND no healthKey → call `determineNativeHealth`
4. Persist health verdict to per-target entry's `healthStatus` / `reason` / `message`

### D8: GLOSSARY.md Update

Add "Native Health Determination" term to GLOSSARY.md:
> **Native Health Determination**: The observer's derivation of workload health from pod-level container status (readiness, restart count) for first-batch kinds, applied only after rollout succeeds and only when no CUE healthKey is configured. Distinct from CUE-based health evaluation via statusPolicyRef.healthKey.

## Testing Decisions

### Test Seam

All tests use the **existing seam** in `internal/controller/subscription_observer_test.go`, following the established patterns:
- `TestObserverDetermineDeploymentRolloutStates` → add `TestObserverDetermineDeploymentHealthStates`
- `TestObserverDetermineStatefulSetRolloutStates` → add `TestObserverDetermineStatefulSetHealthStates`
- `TestObserverDetermineDaemonSetRolloutStates` → add `TestObserverDetermineDaemonSetHealthStates`

### What Makes a Good Test

- Tests external behavior (health status output), not internal implementation
- Each test constructs a specific pod/workload state and asserts the resulting health verdict
- Tests cover all verdict paths: Healthy, Unhealthy, Unknown (gate), Unknown (no pods)
- Tests verify CUE priority: healthKey configured → CUE path taken, native skipped

### Test Categories

1. **Healthy states**: All containers ready, zero restarts; all containers ready with prior restarts (self-healed)
2. **Unhealthy states**: Container not ready with restarts (crash loop); container not ready post-successful-rollout
3. **Unknown states**: Rollout Progressing gate; rollout Failed gate; no matching pods; pod list error
4. **CUE priority**: healthKey configured overrides native for first-batch kinds
5. **Cross-kind consistency**: Same pod state produces same verdict across Deployment/StatefulSet/DaemonSet
6. **Message format**: Verify human-readable messages for each verdict

### Prior Art

- `TestObserverDetermineDeploymentRolloutStates` (line 536): table-driven test with workload state → rollout verdict
- `TestObserverPodImageFailureMarksRolloutFailed` (line 731): pod scan test pattern
- `cueeval/evaluator_test.go:TestHealthPolicy` (line 268): CUE health evaluation tests (complementary, not replaced)

## Out of Scope

- **Application-layer health checks**: Database connections, HTTP endpoints, business metrics. Native health covers only container runtime signals. Custom health logic uses CUE strategies.
- **Time-window smoothing**: No restart rate tracking or sliding window. Instantaneous snapshot only, consistent with rollout determination's approach.
- **Non-first-batch kinds**: Job, CronJob, custom resources. These remain Unknown without healthKey. Covered by future D2 (Job/CronJob Support).
- **Readiness probe configuration**: Observer reads probe results but does not configure or modify probes. Probe tuning is a cluster operator concern.
- **Health history or trending**: No persistence of health transitions beyond current status. Event stream captures transitions.
- **Cross-target health aggregation**: Already defined in ADR-005 §3 (worst-case). This spec implements per-target health; aggregation is existing infrastructure.

## Further Notes

- This spec is **purely additive**: no existing behavior changes for targets with healthKey configured or for non-first-batch kinds.
- The implementation reuses the pod scan infrastructure from rollout determination, minimizing new code surface.
- CrashLoopBackOff is explicitly included in health (unlike rollout where it was excluded per line 23 of subscription_observer_determination.go). This is correct: CrashLoopBackOff is a health signal, not a rollout signal.
- ADR-005 may need amendment to record that native health determination is now implemented for first-batch kinds (previously deferred as "honestly Unknown in v1").
