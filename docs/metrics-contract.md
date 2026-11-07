# ImageFlow Metrics Contract

This document freezes the naming and label conventions for ImageFlow-owned metrics. It is written for platform engineers who operate the controller-manager and for future controller authors who add new instrumentation.

## Scope

- **ImageFlow-owned metrics** follow the convention `imageflow_<subsystem>_<metric>_<unit>`.
- **Controller-runtime metrics** (e.g. `controller_runtime_reconcile_total`, `workqueue_depth`) are emitted unchanged; do not rename or wrap them.
- **Process / Go runtime metrics** come from the Prometheus default collectors and are out of scope.

## Naming convention

```
imageflow_<subsystem>_<metric>_<unit>
```

- `subsystem` maps to a logical component: `manager`, `registry`, `import`, `mapping`, `subscription`, `mirror`, `observer`.
- `metric` is a short, snake_case description of what is being measured.
- `unit` is one of: `total`, `seconds`, `bytes`, `ratio`.

Examples for future controllers:

- `imageflow_import_requests_total`
- `imageflow_mapping_duration_seconds`
- `imageflow_subscription_delivery_total`
- `imageflow_mirror_status_age_seconds`

## Label convention

- `controller` – the controller name as passed to `Named()`.
- `result` – one of `success`, `error`, `requeue`, `requeue_after` for reconciliation counters.
- `name` / `namespace` – used only when the metric refers to a single Kubernetes object; prefer `controller` labels for aggregate metrics.

## Current T1 exposure

The T1 scaffold exposes the controller-runtime workqueue metrics, including:

- `workqueue_depth{name, controller}` – current queue depth.
- `workqueue_adds_total{name, controller}` – total items added.
- `workqueue_queue_duration_seconds{name, controller}` – time spent in queue.
- `workqueue_work_duration_seconds{name, controller}` – time spent processing.

These are visible on the metrics endpoint at `:8080/metrics` when the manager is deployed.

## T6 registry client metrics (seam S2)

The registry client (`internal/registry`, seam S2) exposes call duration and failure rate for every logical registry operation. One observation is recorded per seam call, including calls rejected before any network I/O (allowlist denial, invalid reference).

- `imageflow_registry_requests_total{operation, result}` – Counter. Total registry API calls made through the S2 client. Failure rate is derived from the `result` label, e.g.:

  ```promql
  sum(rate(imageflow_registry_requests_total{result!="success"}[5m]))
    /
  sum(rate(imageflow_registry_requests_total[5m]))
  ```

- `imageflow_registry_request_duration_seconds{operation, result}` – Histogram. Duration of one logical registry operation, including the token exchange and the single auth retry. Buckets (seconds): `0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30`.

Label vocabularies (bounded; this is the documented exception to the reconcile-oriented `result` values above):

- `operation` – one of `fetch_manifest`, `resolve_digest`, `list_tags` (the three frozen S2 operations).
- `result` – one of `success`, `error`, `not_found`, `unauthorized`, `denied`, `timeout`:
  - `denied` – registry host rejected by the `--registry-allowlist` guard (fail closed, zero network I/O);
  - `not_found` – HTTP 404 (`MANIFEST_UNKNOWN`, `NAME_UNKNOWN`);
  - `unauthorized` – HTTP 401/403 after the auth attempt;
  - `timeout` – operation deadline exceeded (client request timeout or caller context);
  - `error` – everything else (invalid reference, digest mismatch, 429/5xx, transport and protocol failures).

Cardinality is bounded by construction: 3 operations × 6 results = at most 18 series per metric. There is deliberately **no registry-host label** — hosts come from user-authored ImageStream references (and denied hosts from arbitrary input), so a host label would be unbounded. Per-registry breakdown belongs to structured logs and Events, not to these metrics.

Registration: the client registers these collectors with the `prometheus.Registerer` passed in `registry.Options.Registerer` (in the manager: `sigs.k8s.io/controller-runtime/pkg/metrics.Registry`); `nil` disables metrics.

## T4 mapping metrics

The Mapping push path (`internal/controller`: ImageStreamMapping reconciler + task TTL GC) exposes task duration, failure rate and garbage-collection activity. One observation is recorded per task that reaches a terminal state, and per GC deletion; re-reconciling a finished task records nothing.

- `imageflow_mapping_registrations_total{outcome}` – Counter. Total ImageStreamMapping task completions. Failure rate is derived from the `outcome` label, e.g.:

  ```promql
  sum(rate(imageflow_mapping_registrations_total{outcome="failed"}[5m]))
    /
  sum(rate(imageflow_mapping_registrations_total[5m]))
  ```

- `imageflow_mapping_task_duration_seconds{result}` – Histogram. Task duration (`status.completedAt - status.startedAt`, both stamped from the injected clock seam) observed when the task reaches its terminal state. Buckets (seconds): `0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300`.

- `imageflow_mapping_gc_deletions_total{kind}` – Counter. Task objects deleted by TTL garbage collection. GC applies uniformly to both task kinds (success TTL `--mapping-ttl-success`, failure TTL `--mapping-ttl-failure`); deletions are attributed per kind.

Label vocabularies (bounded; this is the documented exception to the reconcile-oriented `result` values in the label convention above):

- `outcome` – one of `changed`, `noop`, `failed`:
  - `changed` – the registration recorded a real Image Change (the tag's current association moved to the new digest); this series is also the Image Change count for the push path;
  - `noop` – idempotent re-registration of the current `(stream, tag, digest)`: no Image Change, no stream status write (pipeline-retry semantics, ADR-004);
  - `failed` – terminal task failure; the diagnosable code (`StreamNotFound`, `InvalidImageReference`) lives in the Complete condition message and the `MappingFailed` Event, not in a label.
- `result` – one of `success`, `failure` (mirrors the terminal Complete condition reason `Succeeded`/`Failed`).
- `kind` – one of `mapping` (ImageStreamMapping), `import` (ImageStreamImport).

Cardinality is bounded by construction: 3 outcomes + 2 results + 2 kinds = at most 7 series (plus histogram buckets). There are deliberately **no namespace/name labels** — task objects are short-lived and high-churn, so per-object attribution belongs to kube Events (`MappingRegistered`, `MappingFailed`, `ImageChanged`, `TagHistoryOverflow`, `ImageArtifactCreated`, `TaskGarbageCollected`) and structured logs, not to these metrics.

Registration: the T4 registration units register these collectors once per process against controller-runtime's global metrics registry (`sigs.k8s.io/controller-runtime/pkg/metrics.Registry`); duplicate registration attempts are tolerated so test managers can coexist.

## T5 mirror metrics

The mirror synthesis controller (`internal/controller`, controller name `mirror`) exposes write activity, bounded-window overflow and synthesis lag for the read-only ImageStreamTag/ImageTag views (ADR-002 R4/R9/R10, ADR-008).

- `imageflow_mirror_object_writes_total{kind, action, result}` – Counter. Mirror object writes performed by the synthesis loop. An idempotent no-op reconcile performs no write at all and is not counted, which makes this counter the observability surface of the no-write-amplification contract.
  - `kind` – one of `ImageStreamTag`, `ImageTag`.
  - `action` – one of `create`, `update` (main resource; e.g. restoring a drifted ownerRef), `status_update`, `delete` (stale-mirror pruning and the cascade sweep after stream deletion).
  - `result` – one of `success`, `error`.
  - Cardinality is bounded by construction: 2 kinds × 4 actions × 2 results = at most 16 series.

- `imageflow_mirror_events_truncated_total{kind}` – Counter. Mirror status event entries dropped by the bounded window (`--mirror-event-limit`, CRD cap `MirrorEventMaxItems` = 5), counted only when a status write occurred. Per ADR-008 the overflow additionally surfaces as an `EventSummaryTruncated` kube Event on the mirror object and in the audit log. Bounded: 2 series.

- `imageflow_mirror_synthesis_lag_seconds{kind}` – Histogram. Age of the catalog association at the moment it was written to the mirror (injected clock now minus the association's `created` timestamp), observed only on actual mirror status writes. Buckets (seconds): `0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300`. ADR-002 promises seconds-level mirror lag; alert on p99 of this histogram. Bounded: 2 series × buckets.

There are deliberately no `name`/`namespace` labels on these metrics — mirror identity would be unbounded; per-object detail belongs to structured logs and kube Events. Reconcile-level health is read from the unchanged controller-runtime metrics (`controller_runtime_reconcile_total{controller="mirror"}`, `workqueue_*{name="mirror"}`).

Registration: the collectors are registered at package init with `sigs.k8s.io/controller-runtime/pkg/metrics.Registry` (the manager's registry), so they appear on the same metrics endpoint as the T1 exposure.

## T7 import metrics

The Import pull path (`internal/controller`: ImageStreamImport reconciler + scheduled-import cadence controller) exposes import duration and failure rate, the catalog outcome of successful imports (drift detection) and scheduled task creation activity. One observation is recorded per task that reaches a terminal state and per scheduled task creation; re-reconciling a finished task records nothing.

- `imageflow_import_duration_seconds{result}` – Histogram. **Import duration**: the task lifetime (`status.completedAt - status.startedAt`, both stamped from the injected clock seam) observed when the task reaches its terminal state. Buckets (seconds): `0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300`. The registry round trip itself is measured separately by the T6 client metrics (`imageflow_registry_request_duration_seconds{operation="resolve_digest"|"fetch_manifest"}`); the difference between the two is controller and API-server overhead.

- `imageflow_import_tasks_total{result, reason}` – Counter. Total ImageStreamImport tasks that reached a terminal state. **Import failure rate** is derived from the `result` label, and the `reason` label breaks failures down by the same diagnosable code that prefixes the `Complete` condition message:

  ```promql
  sum(rate(imageflow_import_tasks_total{result="failure"}[5m]))
    /
  sum(rate(imageflow_import_tasks_total[5m]))
  ```

  ```promql
  sum by (reason) (rate(imageflow_import_tasks_total{result="failure"}[15m]))
  ```

- `imageflow_import_registrations_total{outcome}` – Counter. Catalog registrations performed by SUCCESSFUL imports. On the pull path `changed` is the drift-detection counter: the external tag pointed at a different digest than the catalog, so a real Image Change was recorded (the corresponding Events are `TagDriftDetected` on the stream and `ImageChanged` attributed to `ImageStreamImport`). `noop` means the tag already referenced the resolved digest: no Image Change, no stream status write.

- `imageflow_import_scheduled_creations_total{result}` – Counter. ImageStreamImport tasks created by the scheduled-import controller (`spec.tags[].importPolicy.scheduled`). `error` means a due task could not be created; the cause is reported on the stream as a `ScheduledImportError` Event. Not-due reconciles record nothing, which keeps the counter a clean "imports per period" signal.

Label vocabularies (bounded; this is the documented exception to the reconcile-oriented `result` values in the label convention above):

- `result` (tasks / duration) – one of `success`, `failure` (mirrors the terminal `Complete` condition reason `Succeeded`/`Failed`).
- `reason` – `Succeeded` for successes; for failures one of the frozen import failure codes: `NetworkUnreachable`, `RegistryTimeout` (network problems); `Unauthorized`, `PullSecretNotFound`, `PullSecretInvalid` (credential problems); `ManifestNotFound`, `DigestMismatch` (manifest problems); `RegistryDenied` (allowlist/SSRF guard), `RegistryError` (429/5xx and protocol violations), `InvalidImageReference`, `UnsupportedImageSourceKind`, `StreamNotFound` (request/catalog problems). Registry-side detail at the HTTP level belongs to the T6 `result` vocabulary (`denied`, `not_found`, `unauthorized`, `timeout`, `error`).
- `outcome` – one of `changed`, `noop` (same vocabulary as `imageflow_mapping_registrations_total`, minus `failed`: import failures are counted by `imageflow_import_tasks_total`).
- `result` (scheduled creations) – one of `created`, `error`.

Cardinality is bounded by construction: 2 + (2 results × 12 reasons, of which only 12 pairs occur) + 2 + 2 = at most ~18 series (plus histogram buckets). There are deliberately **no registry-host, namespace or name labels** — hosts come from user-authored references and task objects are short-lived and high-churn, so per-object attribution belongs to kube Events (`ImportSucceeded`, `ImportFailed`, `TagDriftDetected`, `ScheduledImportCreated`, `ScheduledImportError`, plus the shared `ImageChanged`, `ImageArtifactCreated`, `TagHistoryOverflow`, `TaskGarbageCollected`) and structured logs. Credential material can never reach a label: the label vocabularies are closed enumerations and the registry host is not a label.

Task GC of the Import kind is instrumented by T4 and is not duplicated here: `imageflow_mapping_gc_deletions_total{kind="import"}`.

Registration: the T7 registration units register these collectors once per process against controller-runtime's global metrics registry (`sigs.k8s.io/controller-runtime/pkg/metrics.Registry`); duplicate registration attempts are tolerated so test managers can coexist.
## T8 authorization metrics

The subscription authorization gate (ADR-003, `internal/authz` — consumed by the admission authorizer in `internal/webhook` and the `subscription-authz` controller in `internal/controller`) exposes the SAR cache hit rate required by the T8 ticket plus the verdict mix. One lookup is recorded per gate check of one (identity, target) pair; one verdict is recorded per check outcome, cache hits included. The webhook-side and controller-side gates of one process share a single collector set, so the counters describe the whole-process SAR surface.

- `imageflow_subscription_sar_cache_lookups_total{result}` – Counter. SAR result cache lookups. The cache hit rate is derived from the `result` label, e.g.:

  ```promql
  sum(rate(imageflow_subscription_sar_cache_lookups_total{result="hit"}[5m]))
    /
  sum(rate(imageflow_subscription_sar_cache_lookups_total[5m]))
  ```

- `imageflow_subscription_sar_checks_total{verdict}` – Counter. Target authorization verdicts returned by the gate. The denial rate is alertable directly from the `verdict` label.

Label vocabularies (bounded; documented exception to the reconcile-oriented `result` values in the label convention above):

- `result` – one of `hit` (definitive verdict served from cache, zero API calls), `miss` (cache empty or entry expired at the TTL, a rate-limited or fresh SAR check follows).
- `verdict` – one of `allowed`, `denied` (SAR said the creator lacks `update` — the Forbidden surface follows), `rate_limited` (SAR creation shed by the token-bucket limiter; the check is retried later, the gate never fails open), `unknown` (target kind not installed, SAR API failure, authorizer evaluation error — callers requeue without touching the persisted surface).

Cardinality is bounded by construction: 2 results + 4 verdicts = at most 6 series. There are deliberately **no identity/target labels** — usernames and target names are unbounded user input; per-target and per-creator attribution belongs to the Forbidden condition, per-target `status.targets[].reason`, kube Events (`TargetsForbidden`, `TargetsAuthorized`, `MissingCreatorIdentity`) and structured logs. Reconcile-level health of the periodic re-verification is read from the unchanged controller-runtime metrics (`controller_runtime_reconcile_total{controller="subscription-authz"}`, `workqueue_*{name="subscription-authz"}`).

Registration: the shared collector set is registered once per process against controller-runtime's global metrics registry (`sigs.k8s.io/controller-runtime/pkg/metrics.Registry`) on first gate construction; duplicate registration attempts are tolerated so test managers can coexist.

## T9 delivery metrics

The delivery write engine (ADR-005/ADR-008, the `subscription-delivery` controller in `internal/controller` with the engine in `internal/delivery`) exposes the end-to-end delivery latency required by the T9 ticket plus the apply-result, failure and bounded-history surfaces. One latency sample is recorded per delivered Image Change (from the registration's `created` timestamp on the source tag event to the moment the value-changing SSA apply landed); one apply result per SSA apply round; one failure per target-entry TRANSITION into a failure reason (steady-state retries do not inflate the counter); one truncation per `recentDeliveries` record that leaves the bounded window.

- `imageflow_delivery_latency_seconds` – Histogram. End-to-end delivery latency: Image Change (source tag event `created`) → the value-changing apply landed on the target. Only deliveries that changed a field value are observed — an equal-value reconcile performs no write and records no sample. Buckets span the sub-second fresh-registration path up to requeue-bounded recovery windows: `0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 900` seconds.

  ```promql
  histogram_quantile(0.95,
    sum by (le) (rate(imageflow_delivery_latency_seconds_bucket[15m])))
  ```

- `imageflow_delivery_applies_total{result}` – Counter. SSA apply rounds performed by the engine. `conflict` counts a 409 from the API server (the exclusivity arbiter), which triggers the ADR-008 re-classification: a live rival claimant makes the Conflict terminal, a vanished one leads to a forced takeover that is then counted as `success`. Equal-value reconciles with self-ownership issue NO apply and are deliberately absent from this counter, so its rate is also the write-amplification surface:

  ```promql
  sum(rate(imageflow_delivery_applies_total{result!="success"}[5m]))
    /
  sum(rate(imageflow_delivery_applies_total[5m]))
  ```

- `imageflow_delivery_target_failures_total{reason}` – Counter. Target entries that transitioned into a failure reason. The per-failure breakdown is alertable directly from the `reason` label:

  ```promql
  sum by (reason) (rate(imageflow_delivery_target_failures_total[15m]))
  ```

- `imageflow_deliveries_truncated_total` – Counter. `recentDeliveries` records dropped from the bounded window (`--subscription-delivery-limit`, CRD cap 5, ADR-008 retention). The full history lives in kube Events and the cluster audit log; each truncation batch is additionally audited with a `DeliveryHistoryTruncated` Event on the subscription.

Label vocabularies (bounded; documented exception to the reconcile-oriented `result` values in the label convention above):

- `result` – one of `success` (an uncontested or forced apply landed), `conflict` (the API server answered 409: another manager claims a declared field), `error` (any other apply failure — RBAC, vanished target, API server errors).
- `reason` – the snake_case rendering of the frozen per-target reason vocabulary surfaced in `status.targets[].reason`: `target_not_found`, `container_not_found`, `apply_forbidden`, `apply_error`, `unsupported`, `source_unresolved`, `invalid_image_reference`, `invalid_target`, `invalid_image_field`, `kind_not_installed`, `conflict`.

Cardinality is bounded by construction: 3 results + 11 reasons + 1 bare counter = at most 15 series (plus the histogram buckets). There are deliberately **no workload, namespace or subscription labels** — target and subscription names are unbounded user input; per-delivery attribution belongs to `status.targets[]`, `status.recentDeliveries[]`, kube Events (`ImageApplied` on both the subscription and the workload, `DeliveryConflict`, `UnsupportedTarget`, `DeliveryHistoryTruncated`) and structured logs. Reconcile-level health of the engine is read from the unchanged controller-runtime metrics (`controller_runtime_reconcile_total{controller="subscription-delivery"}`, `workqueue_*{name="subscription-delivery"}`).

Registration: the shared T9 collector set is registered once per process against controller-runtime's global metrics registry (`sigs.k8s.io/controller-runtime/pkg/metrics.Registry`) on first reconciler construction; duplicate registration attempts are tolerated so test managers can coexist.

## T11 observer metrics

The workload observer (ADR-005 §3/ADR-006, the `subscription-observer` controller in `internal/controller/subscription_observer*.go`) exposes the per-stage outcome counters that complete the T1 per-stage failure-rate surface: the ImageResolved/ImageApplied stages are covered by the T9 delivery metrics above, the Rollout stage by `imageflow_observer_rollout_outcomes_total`, and the Health stage by `imageflow_observer_health_outcomes_total`. One rollout sample is recorded per per-target `rolloutStatus` TRANSITION the observer writes (steady-state passes record nothing — the same write gate that suppresses duplicate status writes and Events); one health sample accompanies each such transition, reporting the health result recorded at that moment.

- `imageflow_observer_rollout_outcomes_total{kind,result}` – Counter. Per-target rollout transitions determined from native workload status (built-in first-batch kinds, ADR-006) or flipped to the honest Unknown (every non-observable class, ADR-005 §5). Rollout-stage failure rate:

  ```promql
  sum(rate(imageflow_observer_rollout_outcomes_total{result="failed"}[15m]))
    /
  sum(rate(imageflow_observer_rollout_outcomes_total[15m]))
  ```

  Per-kind breakdown (which built-in adapter's rollouts fail):

  ```promql
  sum by (kind) (rate(imageflow_observer_rollout_outcomes_total{result="failed"}[15m]))
  ```

- `imageflow_observer_health_outcomes_total{result}` – Counter. Health-stage results recorded at rollout transitions. In v1 only `unknown` is ever incremented: the CUE status policy evaluator is deferred to v1.1 (ADR-005 §4/D-016), health is never fabricated and never derived from rollout success. A nonzero rate on `healthy`/`unhealthy` in v1 is therefore a contract-violation alert by construction:

  ```promql
  sum(rate(imageflow_observer_health_outcomes_total{result!="unknown"}[15m]))
  ```

Label vocabularies (bounded; same documented exception style as §T9):

- `kind` – one of `deployment`, `statefulset`, `daemonset` (the ADR-006 first batch, judged from native status) or `other` (every non-observable class: third-party CRDs, imageFields-located targets, excluded kinds — all end in the honest Unknown).
- `result` (rollout) – one of `progressing`, `succeeded`, `failed`, `unknown`: the lower-case rendering of the `rolloutStatus` value the transition wrote (`Superseded`/`Conflict` are engine-owned terminal states and never observer transitions, hence absent).
- `result` (health) – one of `healthy`, `unhealthy`, `unknown` (the `healthStatus` vocabulary; v1 emits `unknown` only).

Cardinality is bounded by construction: 4 kinds × 4 rollout results + 3 health results = at most 19 series. There are deliberately **no workload, namespace or subscription labels** — per-delivery attribution belongs to `status.targets[]`, `status.recentDeliveries[]`, the dual-namespace kube Events (`RolloutSucceeded`, `RolloutFailed`, `RolloutUnknown`, `DeliverySuperseded` alongside T9's `ImageApplied`) and structured logs. Reconcile-level health of the observer is read from the unchanged controller-runtime metrics (`controller_runtime_reconcile_total{controller="subscription-observer"}`, `workqueue_*{name="subscription-observer"}`).

The mirror float-up (delivery summaries surfaced onto `ImageStreamTag`/`ImageTag` `status.events[]`) reuses the §T5 collector `imageflow_mirror_events_truncated_total{kind}` and the `EventSummaryTruncated` Event when summaries leave the bounded window (`--mirror-event-limit`, CRD cap 5, ADR-008 §4): it is the same fact on the same window, so no separate T11 collector exists for it.

Registration: the shared T11 collector set is registered once per process against controller-runtime's global metrics registry (`sigs.k8s.io/controller-runtime/pkg/metrics.Registry`) on first observer reconciler construction; duplicate registration attempts are tolerated so test managers can coexist (same pattern as §T9).

## Stability

- Metric names and label keys are part of the operator contract.
- Changing a metric name or removing a label is a breaking change and requires a version bump + release note.
- Adding new labels to an existing metric is allowed only when the cardinality is bounded.
