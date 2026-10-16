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

## Stability

- Metric names and label keys are part of the operator contract.
- Changing a metric name or removing a label is a breaking change and requires a version bump + release note.
- Adding new labels to an existing metric is allowed only when the cardinality is bounded.
