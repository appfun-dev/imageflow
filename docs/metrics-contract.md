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

## Stability

- Metric names and label keys are part of the operator contract.
- Changing a metric name or removing a label is a breaking change and requires a version bump + release note.
- Adding new labels to an existing metric is allowed only when the cardinality is bounded.
