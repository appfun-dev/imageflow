package registry

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics names frozen by docs/metrics-contract.md (T6):
//
//	imageflow_registry_requests_total{operation,result}
//	imageflow_registry_request_duration_seconds{operation,result}
const (
	metricsNamespace = "imageflow"
	metricsSubsystem = "registry"
)

// durationBuckets are tuned for registry API calls over the network.
var durationBuckets = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

// metrics holds the T6 registry collectors. A nil *metrics is a valid
// no-op recorder (metrics disabled).
type metrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func newMetrics(registerer prometheus.Registerer) (*metrics, error) {
	if registerer == nil {
		return nil, nil
	}
	m := &metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "requests_total",
			Help: "Total number of registry API calls made through the S2 client. " +
				"Failure rate is derived from the result label.",
		}, []string{"operation", "result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "request_duration_seconds",
			Help:      "Duration of registry API calls made through the S2 client, including token exchange.",
			Buckets:   durationBuckets,
		}, []string{"operation", "result"}),
	}
	if err := registerer.Register(m.requests); err != nil {
		return nil, fmt.Errorf("register imageflow_registry_requests_total: %w", err)
	}
	if err := registerer.Register(m.duration); err != nil {
		return nil, fmt.Errorf("register imageflow_registry_request_duration_seconds: %w", err)
	}
	return m, nil
}

// observe records one logical registry operation.
func (m *metrics) observe(operation, result string, elapsed time.Duration) {
	if m == nil {
		return
	}
	m.requests.WithLabelValues(operation, result).Inc()
	m.duration.WithLabelValues(operation, result).Observe(elapsed.Seconds())
}
