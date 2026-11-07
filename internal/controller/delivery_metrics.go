package controller

import (
	"errors"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// T9 metrics (docs/metrics-contract.md, "## T9 delivery metrics"). Frozen
// names and bounded label vocabularies:
//
//	imageflow_delivery_latency_seconds
//	imageflow_delivery_applies_total{result=success|conflict|error}
//	imageflow_delivery_target_failures_total{reason=...}
//	imageflow_deliveries_truncated_total
const (
	metricNameDeliveryLatency    = "imageflow_delivery_latency_seconds"
	metricNameDeliveryApplies    = "imageflow_delivery_applies_total"
	metricNameDeliveryFailures   = "imageflow_delivery_target_failures_total"
	metricNameDeliveriesTruncate = "imageflow_deliveries_truncated_total"
)

// SSA apply result label values (bounded).
const (
	applyResultSuccess  = "success"
	applyResultConflict = "conflict"
	applyResultError    = "error"
)

// Per-target failure reason label values (bounded; snake_case rendering of
// the stable per-target reason vocabulary).
const (
	failureLabelTargetNotFound        = "target_not_found"
	failureLabelContainerNotFound     = "container_not_found"
	failureLabelApplyForbidden        = "apply_forbidden"
	failureLabelApplyError            = "apply_error"
	failureLabelUnsupported           = "unsupported"
	failureLabelSourceUnresolved      = "source_unresolved"
	failureLabelInvalidImageReference = "invalid_image_reference"
	failureLabelInvalidTarget         = "invalid_target"
	failureLabelInvalidImageField     = "invalid_image_field"
	failureLabelKindNotInstalled      = "kind_not_installed"
	failureLabelConflict              = "conflict"
)

// deliveryLatencyBuckets spans the sub-second fresh-registration path up to
// requeue-bounded recovery windows (seconds).
var deliveryLatencyBuckets = []float64{
	0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 900,
}

// deliveryMetrics carries the T9 collectors. Construct one per reconciler;
// registration against a prometheus.Registerer is separate so tests can use
// unregistered collectors with testutil.
type deliveryMetrics struct {
	// latency is the end-to-end delivery latency: Image Change (the catalog
	// association's created timestamp) → ImageApplied (the value-changing
	// apply landing). The T9 ticket metric.
	latency prometheus.Histogram
	// applies counts SSA apply calls by result. Equal-and-owned fields skip
	// the apply entirely and are NOT counted: the counter is the
	// write-amplification surface (zero on steady-state passes).
	applies *prometheus.CounterVec
	// failures counts per-target failure-state TRANSITIONS by the bounded
	// reason vocabulary (ImageApplied-stage failure rate; rollout/health
	// stage failure rates belong to T11).
	failures *prometheus.CounterVec
	// truncated counts recentDeliveries entries dropped by the bounded
	// window (ADR-008; overflow also lands in kube Events and the audit log).
	truncated prometheus.Counter
}

func newDeliveryMetrics() *deliveryMetrics {
	return &deliveryMetrics{
		latency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    metricNameDeliveryLatency,
			Help:    "End-to-end delivery latency in seconds: Image Change (catalog association created) to ImageApplied (value-changing SSA apply landed).",
			Buckets: deliveryLatencyBuckets,
		}),
		applies: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricNameDeliveryApplies,
			Help: "Total Server-Side Apply calls of the delivery engine by result. success = apply landed; conflict = rejected by a 409 (first-wins re-classification follows); error = any other API failure. Equal-and-owned fields produce no apply and are not counted.",
		}, []string{"result"}),
		failures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricNameDeliveryFailures,
			Help: "Total per-target failure-state transitions of the delivery engine by stable reason (same vocabulary as status.targets[].reason, snake_case).",
		}, []string{"reason"}),
		truncated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: metricNameDeliveriesTruncate,
			Help: "Total recentDeliveries entries dropped by the bounded window (--subscription-delivery-limit, CRD cap 5) when a status write occurred (ADR-008).",
		}),
	}
}

func (m *deliveryMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.latency, m.applies, m.failures, m.truncated}
}

// registerDeliveryMetrics registers the collectors with reg. A nil reg
// disables metrics. Already-registered collectors are tolerated:
// controller-runtime's metrics registry is process-global while
// Registrar.Register may legitimately run more than once per process (e.g.
// integration suites).
func registerDeliveryMetrics(reg prometheus.Registerer, m *deliveryMetrics) {
	if reg == nil {
		return
	}
	for _, c := range m.collectors() {
		var already prometheus.AlreadyRegisteredError
		if err := reg.Register(c); err != nil && !errors.As(err, &already) {
			// Programming error (duplicate metric name inside our own set):
			// surface it loudly instead of silently dropping instrumentation.
			panic(err)
		}
	}
}

var (
	sharedDeliveryMetricsOnce     sync.Once
	sharedDeliveryMetricsInstance *deliveryMetrics
)

// sharedDeliveryMetrics returns the process-wide T9 collector set, registered
// against controller-runtime's global metrics registry on first use.
func sharedDeliveryMetrics() *deliveryMetrics {
	sharedDeliveryMetricsOnce.Do(func() {
		sharedDeliveryMetricsInstance = newDeliveryMetrics()
		registerDeliveryMetrics(ctrlmetrics.Registry, sharedDeliveryMetricsInstance)
	})
	return sharedDeliveryMetricsInstance
}
