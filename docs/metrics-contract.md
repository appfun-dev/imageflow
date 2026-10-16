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

- `subsystem` maps to a logical component: `manager`, `import`, `mapping`, `subscription`, `mirror`, `observer`.
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

## Stability

- Metric names and label keys are part of the operator contract.
- Changing a metric name or removing a label is a breaking change and requires a version bump + release note.
- Adding new labels to an existing metric is allowed only when the cardinality is bounded.
