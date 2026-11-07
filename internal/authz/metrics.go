package authz

import (
	"errors"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// T8 metrics (docs/metrics-contract.md, "## T8 authorization metrics").
// Frozen names and bounded label vocabularies:
//
//	imageflow_subscription_sar_cache_lookups_total{result=hit|miss}
//	imageflow_subscription_sar_checks_total{verdict=allowed|denied|rate_limited|unknown}
//
// The SAR cache hit rate — the metric the T8 ticket requires — is derived
// from the lookups counter:
//
//	sum(rate(imageflow_subscription_sar_cache_lookups_total{result="hit"}[5m]))
//	  /
//	sum(rate(imageflow_subscription_sar_cache_lookups_total[5m]))
const (
	metricNameSARCacheLookups = "imageflow_subscription_sar_cache_lookups_total"
	metricNameSARChecks       = "imageflow_subscription_sar_checks_total"
)

// Cache lookup result label values (bounded).
const (
	lookupResultHit  = "hit"
	lookupResultMiss = "miss"
)

// SAR check verdict label values (bounded). rate_limited is broken out of
// unknown so backpressure is directly alertable.
const (
	verdictLabelAllowed     = "allowed"
	verdictLabelDenied      = "denied"
	verdictLabelRateLimited = "rate_limited"
	verdictLabelUnknown     = "unknown"
)

// Metrics carries the T8 collectors. Construct one per gate via NewMetrics
// (unregistered, for tests with testutil); production gates pass nil in
// Options and share the process-wide registered set.
type Metrics struct {
	lookups *prometheus.CounterVec
	checks  *prometheus.CounterVec
}

// NewMetrics builds an unregistered collector set (tests).
func NewMetrics() *Metrics {
	return &Metrics{
		lookups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricNameSARCacheLookups,
			Help: "Total SAR result cache lookups by outcome. The cache hit rate is " +
				"derived from the result label: hit / (hit + miss).",
		}, []string{"result"}),
		checks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricNameSARChecks,
			Help: "Total target authorization verdicts returned (cache hits included), " +
				"by verdict: allowed, denied (SAR said the creator lacks update), " +
				"rate_limited (SAR creation shed by the limiter, retried later) or " +
				"unknown (unmapped kind, SAR API failure).",
		}, []string{"verdict"}),
	}
}

func (m *Metrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.lookups, m.checks}
}

// registerMetrics registers the collectors with reg, tolerating duplicates:
// the controller-runtime metrics registry is process-global while gates may
// legitimately be constructed more than once per process (webhook +
// controller, integration suites).
func registerMetrics(reg prometheus.Registerer, m *Metrics) {
	if reg == nil {
		return
	}
	for _, c := range m.collectors() {
		var already prometheus.AlreadyRegisteredError
		if err := reg.Register(c); err != nil && !errors.As(err, &already) {
			// Programming error (name clash inside our own frozen set):
			// surface it loudly instead of silently dropping instrumentation.
			panic(err)
		}
	}
}

var (
	sharedMetricsOnce     sync.Once
	sharedMetricsInstance *Metrics
)

// sharedMetrics returns the process-wide T8 collector set, registered against
// controller-runtime's global metrics registry on first use, so webhook-side
// and controller-side gates report through one endpoint.
func sharedMetrics() *Metrics {
	sharedMetricsOnce.Do(func() {
		sharedMetricsInstance = NewMetrics()
		registerMetrics(ctrlmetrics.Registry, sharedMetricsInstance)
	})
	return sharedMetricsInstance
}
