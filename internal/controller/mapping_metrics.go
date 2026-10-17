package controller

import (
	"errors"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// T4 metrics (docs/metrics-contract.md, "## T4 mapping metrics"). Frozen names
// and bounded label vocabularies:
//
//	imageflow_mapping_registrations_total{outcome=changed|noop|failed}
//	imageflow_mapping_task_duration_seconds{result=success|failure}
//	imageflow_mapping_gc_deletions_total{kind=mapping|import}
const (
	metricNameRegistrations = "imageflow_mapping_registrations_total"
	metricNameTaskDuration  = "imageflow_mapping_task_duration_seconds"
	metricNameGCDeletions   = "imageflow_mapping_gc_deletions_total"
)

// Registration outcome label values (bounded).
const (
	regOutcomeChanged = "changed"
	regOutcomeNoop    = "noop"
	regOutcomeFailed  = "failed"
)

// Task duration result label values (bounded).
const (
	taskResultSuccess = "success"
	taskResultFailure = "failure"
)

// GC kind label values (bounded).
const (
	gcKindMapping = "mapping"
	gcKindImport  = "import"
)

// taskDurationBuckets spans CI-scale registration latency up to pathological
// requeue backoff windows (seconds).
var taskDurationBuckets = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}

// mappingMetrics carries the T4 collectors. Construct one per reconciler set;
// registration against a prometheus.Registerer is separate so tests can use
// unregistered collectors with testutil.
type mappingMetrics struct {
	registrations *prometheus.CounterVec
	taskDuration  *prometheus.HistogramVec
	gcDeletions   *prometheus.CounterVec
}

func newMappingMetrics() *mappingMetrics {
	return &mappingMetrics{
		registrations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricNameRegistrations,
			Help: "Total ImageStreamMapping registrations by outcome. " +
				"changed = real Image Change recorded; noop = idempotent re-registration of the " +
				"current (stream, tag, digest); failed = terminal task failure.",
		}, []string{"outcome"}),
		taskDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    metricNameTaskDuration,
			Help:    "ImageStreamMapping task duration (completedAt - startedAt) in seconds.",
			Buckets: taskDurationBuckets,
		}, []string{"result"}),
		gcDeletions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricNameGCDeletions,
			Help: "Total completed task objects deleted by TTL garbage collection, by task kind " +
				"(mapping = ImageStreamMapping, import = ImageStreamImport).",
		}, []string{"kind"}),
	}
}

func (m *mappingMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.registrations, m.taskDuration, m.gcDeletions}
}

// registerMappingMetrics registers the collectors with reg. A nil reg disables
// metrics. Already-registered collectors are tolerated: controller-runtime's
// metrics registry is process-global while Registrar.Register may legitimately
// run more than once per process (e.g. integration suites).
func registerMappingMetrics(reg prometheus.Registerer, m *mappingMetrics) {
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
	sharedMetricsOnce     sync.Once
	sharedMetricsInstance *mappingMetrics
)

// sharedMappingMetrics returns the process-wide T4 collector set, registered
// against controller-runtime's global metrics registry on first use.
func sharedMappingMetrics() *mappingMetrics {
	sharedMetricsOnce.Do(func() {
		sharedMetricsInstance = newMappingMetrics()
		registerMappingMetrics(ctrlmetrics.Registry, sharedMetricsInstance)
	})
	return sharedMetricsInstance
}
