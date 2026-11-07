package controller

import (
	"errors"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// T7 metrics (docs/metrics-contract.md, "## T7 import metrics"). Frozen names
// and bounded label vocabularies:
//
//	imageflow_import_tasks_total{result,reason}
//	imageflow_import_duration_seconds{result}
//	imageflow_import_registrations_total{outcome}
//	imageflow_import_scheduled_creations_total{result}
//
// The ticket asks for import duration and failure rate: duration is the
// histogram, the failure rate is derived from the tasks counter
// (sum(rate(...{result="failure"})) / sum(rate(...))), and the `reason` label
// breaks failures down by the same diagnosable code that prefixes the Complete
// condition message.
const (
	metricNameImportTasks            = "imageflow_import_tasks_total"
	metricNameImportDuration         = "imageflow_import_duration_seconds"
	metricNameImportRegistrations    = "imageflow_import_registrations_total"
	metricNameImportScheduledCreates = "imageflow_import_scheduled_creations_total"
)

// Import task result label values (bounded; mirror the terminal Complete
// condition reason Succeeded/Failed).
const (
	importResultSuccess = "success"
	importResultFailure = "failure"
)

// Import registration outcome label values (bounded). On the import path
// "changed" is the drift-detection counter: the external tag moved to a new
// digest (or was associated for the first time).
const (
	importOutcomeChanged = "changed"
	importOutcomeNoop    = "noop"
)

// Scheduled-import creation result label values (bounded).
const (
	scheduledCreateCreated = "created"
	scheduledCreateError   = "error"
)

// importDurationBuckets spans a local registry round trip up to a slow
// cross-network manifest fetch plus the client request timeout (default 30s).
var importDurationBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300}

// importMetrics carries the T7 collectors. Construct one per reconciler set;
// registration against a prometheus.Registerer is separate so unit tests can
// use unregistered collectors with testutil.
type importMetrics struct {
	tasks              *prometheus.CounterVec
	duration           *prometheus.HistogramVec
	registrations      *prometheus.CounterVec
	scheduledCreations *prometheus.CounterVec
}

func newImportMetrics() *importMetrics {
	return &importMetrics{
		tasks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricNameImportTasks,
			Help: "Total ImageStreamImport tasks that reached a terminal state, by result and by the " +
				"diagnosable reason code carried in the Complete condition message. " +
				"result=success always pairs with reason=Succeeded; result=failure pairs with the " +
				"failure taxonomy (NetworkUnreachable, RegistryTimeout, Unauthorized, " +
				"ManifestNotFound, DigestMismatch, RegistryDenied, RegistryError, " +
				"InvalidImageReference, PullSecretNotFound, PullSecretInvalid, StreamNotFound).",
		}, []string{"result", "reason"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    metricNameImportDuration,
			Help:    "ImageStreamImport task duration (completedAt - startedAt, injected clock) in seconds.",
			Buckets: importDurationBuckets,
		}, []string{"result"}),
		registrations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricNameImportRegistrations,
			Help: "Catalog registrations performed by successful imports, by outcome. " +
				"changed = the external tag pointed at a different digest than the catalog " +
				"(drift detected, real Image Change recorded); noop = the tag already referenced " +
				"the resolved digest (no Image Change).",
		}, []string{"outcome"}),
		scheduledCreations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricNameImportScheduledCreates,
			Help: "ImageStreamImport tasks created by the scheduled-import controller, by result " +
				"(error = the task could not be created; the stream Event names the cause).",
		}, []string{"result"}),
	}
}

func (m *importMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.tasks, m.duration, m.registrations, m.scheduledCreations}
}

// registerImportMetrics registers the collectors with reg. A nil reg disables
// metrics. Already-registered collectors are tolerated: controller-runtime's
// metrics registry is process-global while Registrar.Register may legitimately
// run more than once per process (e.g. integration suites).
func registerImportMetrics(reg prometheus.Registerer, m *importMetrics) {
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
	sharedImportMetricsOnce     sync.Once
	sharedImportMetricsInstance *importMetrics
)

// sharedImportMetrics returns the process-wide T7 collector set, registered
// against controller-runtime's global metrics registry on first use.
func sharedImportMetrics() *importMetrics {
	sharedImportMetricsOnce.Do(func() {
		sharedImportMetricsInstance = newImportMetrics()
		registerImportMetrics(ctrlmetrics.Registry, sharedImportMetricsInstance)
	})
	return sharedImportMetricsInstance
}
