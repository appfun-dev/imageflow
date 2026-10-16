/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"os"

	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
	"github.com/appfun-dev/imageflow/internal/controller"
	"github.com/appfun-dev/imageflow/internal/manager"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(imageflowv1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr          string
		metricsCertPath      string
		metricsCertName      string
		metricsCertKey       string
		webhookCertPath      string
		webhookCertName      string
		webhookCertKey       string
		probeAddr            string
		secureMetrics        bool
		enableLeaderElection bool
		enableHTTP2          bool
		webhookPort          int
	)

	var cfg config.Config
	cfg.BindFlags(flag.CommandLine)

	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"Serve the metrics endpoint over HTTPS. Use --metrics-secure=false for HTTP.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "", "Directory containing the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "Name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "Name of the metrics server key file.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "Directory containing the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "Name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "Name of the webhook key file.")
	flag.IntVar(&webhookPort, "webhook-port", 9443, "The port the webhook server listens on.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	if err := cfg.Validate(); err != nil {
		setupLog.Error(err, "invalid configuration")
		os.Exit(1)
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	setupLog.Info("starting manager")

	if err := manager.Run(ctrl.SetupSignalHandler(), manager.Options{
		Scheme:                 scheme,
		Config:                 ctrl.GetConfigOrDie(),
		MetricsBindAddress:     metricsAddr,
		MetricsSecure:          secureMetrics,
		MetricsCertPath:        metricsCertPath,
		MetricsCertName:        metricsCertName,
		MetricsCertKey:         metricsCertKey,
		HealthProbeBindAddress: probeAddr,
		WebhookPort:            webhookPort,
		WebhookCertPath:        webhookCertPath,
		WebhookCertName:        webhookCertName,
		WebhookCertKey:         webhookCertKey,
		EnableHTTP2:            enableHTTP2,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "1ff9bb0a.appfun.dev",
		ControllerOptions: controller.Options{
			Clock:  clock.Real{},
			Config: cfg,
		},
	}); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
