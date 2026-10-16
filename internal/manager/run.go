package manager

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	"github.com/appfun-dev/imageflow/internal/controller"
)

// Options bundles the inputs required to construct and start the manager.
type Options struct {
	Scheme                 *runtime.Scheme
	Config                 *rest.Config
	MetricsBindAddress     string
	MetricsSecure          bool
	MetricsCertPath        string
	MetricsCertName        string
	MetricsCertKey         string
	HealthProbeBindAddress string
	WebhookPort            int
	WebhookCertPath        string
	WebhookCertName        string
	WebhookCertKey         string
	EnableHTTP2            bool
	LeaderElection         bool
	LeaderElectionID       string
	ControllerOptions      controller.Options
}

// Run constructs the controller-manager and blocks until ctx is cancelled.
func Run(ctx context.Context, opts Options) error {
	tlsOpts := []func(*tls.Config){}
	if !opts.EnableHTTP2 {
		tlsOpts = append(tlsOpts, func(c *tls.Config) {
			c.NextProtos = []string{"http/1.1"}
		})
	}

	webhookTLSOpts := tlsOpts
	var webhookCertWatcher *certwatcher.CertWatcher
	if opts.WebhookCertPath != "" {
		var err error
		webhookCertWatcher, err = certwatcher.New(
			filepath.Join(opts.WebhookCertPath, opts.WebhookCertName),
			filepath.Join(opts.WebhookCertPath, opts.WebhookCertKey),
		)
		if err != nil {
			return fmt.Errorf("initialize webhook certificate watcher: %w", err)
		}
		webhookTLSOpts = append(webhookTLSOpts, func(c *tls.Config) {
			c.GetCertificate = webhookCertWatcher.GetCertificate
		})
	}

	webhookServer := webhook.NewServer(webhook.Options{
		Port:    opts.WebhookPort,
		TLSOpts: webhookTLSOpts,
	})

	metricsServerOptions := metricsserver.Options{
		BindAddress:   opts.MetricsBindAddress,
		SecureServing: opts.MetricsSecure,
		TLSOpts:       tlsOpts,
	}
	if opts.MetricsSecure {
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	var metricsCertWatcher *certwatcher.CertWatcher
	if opts.MetricsCertPath != "" {
		var err error
		metricsCertWatcher, err = certwatcher.New(
			filepath.Join(opts.MetricsCertPath, opts.MetricsCertName),
			filepath.Join(opts.MetricsCertPath, opts.MetricsCertKey),
		)
		if err != nil {
			return fmt.Errorf("initialize metrics certificate watcher: %w", err)
		}
		metricsServerOptions.TLSOpts = append(metricsServerOptions.TLSOpts, func(c *tls.Config) {
			c.GetCertificate = metricsCertWatcher.GetCertificate
		})
	}

	mgr, err := ctrl.NewManager(opts.Config, ctrl.Options{
		Scheme:                 opts.Scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: opts.HealthProbeBindAddress,
		LeaderElection:         opts.LeaderElection,
		LeaderElectionID:       opts.LeaderElectionID,
	})
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}

	if err := controller.Setup(mgr, opts.ControllerOptions); err != nil {
		return fmt.Errorf("set up controllers: %w", err)
	}

	if metricsCertWatcher != nil {
		if err := mgr.Add(metricsCertWatcher); err != nil {
			return fmt.Errorf("add metrics cert watcher: %w", err)
		}
	}
	if webhookCertWatcher != nil {
		if err := mgr.Add(webhookCertWatcher); err != nil {
			return fmt.Errorf("add webhook cert watcher: %w", err)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("add health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("add ready check: %w", err)
	}

	return mgr.Start(ctx)
}

// Exit prints the error and exits the process with code 1.
func Exit(err error) {
	fmt.Fprintf(os.Stderr, "manager failed: %v\n", err)
	os.Exit(1)
}
