package controller

import (
	"errors"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// T11 metrics (docs/metrics-contract.md, "## T11 workload observer metrics").
// Frozen names and bounded label vocabularies, subsystem "observer" per the
// §Naming convention imageflow_<subsystem>_<metric>_<unit>:
//
//	imageflow_observer_rollout_outcomes_total{kind=...,result=...}
//	imageflow_observer_health_outcomes_total{result=...}
//
// These are the Rollout-stage and Health-stage failure-rate surfaces the T9
// contract reserved ("failures = ImageApplied-stage failure rate;
// rollout/health stage failure rates belong to T11"). imageflow_delivery_*
// collectors are NOT touched here.
const (
	metricNameObserverRolloutOutcomes = "imageflow_observer_rollout_outcomes_total"
	metricNameObserverHealthOutcomes  = "imageflow_observer_health_outcomes_total"
)

// Rollout-outcome result label values (bounded; snake_case rendering of the
// RolloutStatus vocabulary the observer can transition an entry into).
const (
	observerResultProgressing = "progressing"
	observerResultSucceeded   = "succeeded"
	observerResultFailed      = "failed"
	observerResultUnknown     = "unknown"
)

// Health-outcome result label values (bounded; snake_case rendering of the
// HealthStatus vocabulary). v1 only ever reports "unknown" honestly (the CUE
// status policy evaluator is deferred to v1.1, ADR-005 §4): the series exists
// so the health-stage failure rate is observable from day one and so the v1.1
// evaluator extends the vocabulary without renaming anything.
const (
	observerHealthHealthy   = "healthy"
	observerHealthUnhealthy = "unhealthy"
	observerHealthUnknown   = "unknown"
)

// Kind label values (bounded): the three ADR-006 first-batch built-ins plus
// "other" for every non-observable target class (third-party CRDs, imageFields
// locators, non-first-batch kinds). Raw kind strings are deliberately NOT
// used as labels — user-controlled values would be unbounded cardinality.
const (
	observerKindDeployment  = "deployment"
	observerKindStatefulSet = "statefulset"
	observerKindDaemonSet   = "daemonset"
	observerKindOther       = "other"
)

// observerMetrics carries the T11 collectors. Construct one per reconciler;
// registration against a prometheus.Registerer is separate so tests can use
// unregistered collectors with testutil.
type observerMetrics struct {
	// rolloutOutcomes counts per-target rolloutStatus TRANSITIONS decided by
	// the workload observer, by bounded kind and result. Steady-state passes
	// (no transition) never increment: the counter is the rollout-stage
	// outcome/failure-rate surface, not a poll counter.
	rolloutOutcomes *prometheus.CounterVec
	// healthOutcomes counts the health result attached to every rollout
	// transition (v1: always "unknown" — honest, never fabricated, ADR-005).
	healthOutcomes *prometheus.CounterVec
}

func newObserverMetrics() *observerMetrics {
	return &observerMetrics{
		rolloutOutcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricNameObserverRolloutOutcomes,
			Help: "Total per-target rolloutStatus transitions written by the workload observer, by bounded kind (deployment|statefulset|daemonset|other) and result (progressing|succeeded|failed|unknown). Rollout-stage failure rate = failed / (succeeded+failed+unknown) per kind.",
		}, []string{"kind", "result"}),
		healthOutcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricNameObserverHealthOutcomes,
			Help: "Total health results attached to rollout transitions by the workload observer (healthy|unhealthy|unknown). v1 has no CUE status policy evaluator (deferred to v1.1, ADR-005 §4), so this only ever reports unknown — success is never fabricated.",
		}, []string{"result"}),
	}
}

func (m *observerMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.rolloutOutcomes, m.healthOutcomes}
}

// registerObserverMetrics registers the collectors with reg. A nil reg
// disables metrics. Already-registered collectors are tolerated:
// controller-runtime's metrics registry is process-global while
// Registrar.Register may legitimately run more than once per process (e.g.
// integration suites).
func registerObserverMetrics(reg prometheus.Registerer, m *observerMetrics) {
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
	sharedObserverMetricsOnce     sync.Once
	sharedObserverMetricsInstance *observerMetrics
)

// sharedObserverMetrics returns the process-wide T11 collector set,
// registered against controller-runtime's global metrics registry on first
// use.
func sharedObserverMetrics() *observerMetrics {
	sharedObserverMetricsOnce.Do(func() {
		sharedObserverMetricsInstance = newObserverMetrics()
		registerObserverMetrics(ctrlmetrics.Registry, sharedObserverMetricsInstance)
	})
	return sharedObserverMetricsInstance
}
