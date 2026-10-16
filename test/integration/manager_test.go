package integration

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
	"github.com/appfun-dev/imageflow/internal/controller"
	"github.com/appfun-dev/imageflow/internal/manager"
)

func TestIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Suite")
}

var _ = Describe("Manager", Ordered, func() {
	var (
		env    *envtest.Environment
		ctx    context.Context
		cancel context.CancelFunc
	)

	BeforeAll(func() {
		assets := os.Getenv("KUBEBUILDER_ASSETS")
		if assets == "" {
			Skip("KUBEBUILDER_ASSETS not set")
		}

		ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

		env = &envtest.Environment{
			BinaryAssetsDirectory: assets,
		}
		cfg, err := env.Start()
		Expect(err).NotTo(HaveOccurred())

		scheme := runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())

		ctx, cancel = context.WithCancel(context.Background())
		go func() {
			defer GinkgoRecover()
			err := manager.Run(ctx, manager.Options{
				Scheme:                 scheme,
				Config:                 cfg,
				MetricsBindAddress:     "127.0.0.1:28443",
				HealthProbeBindAddress: "127.0.0.1:28081",
				WebhookPort:            29443,
				MetricsSecure:          false,
				LeaderElection:         false,
				ControllerOptions: controller.Options{
					Clock:  clock.Real{},
					Config: config.Config{},
				},
			})
			Expect(err).NotTo(HaveOccurred())
		}()

		Eventually(func() error {
			resp, err := http.Get("http://127.0.0.1:28081/healthz")
			if err != nil {
				return err
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("healthz status %d", resp.StatusCode)
			}
			return nil
		}, 20*time.Second, 500*time.Millisecond).Should(Succeed())
	})

	AfterAll(func() {
		cancel()
		Expect(env.Stop()).To(Succeed())
	})

	It("serves metrics containing controller-runtime queue depth", func() {
		Eventually(func() string {
			resp, err := http.Get("http://127.0.0.1:28443/metrics")
			if err != nil {
				return ""
			}
			defer func() { _ = resp.Body.Close() }()
			body, _ := io.ReadAll(resp.Body)
			return string(body)
		}, 10*time.Second, 500*time.Millisecond).Should(ContainSubstring("workqueue_depth"))
	})

	It("serves the webhook server over TLS", func() {
		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}
		resp, err := client.Get("https://127.0.0.1:29443/")
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = resp.Body.Close() }()
		// No handlers are registered, so the server returns 404, which proves it is listening.
		Expect(resp.StatusCode).To(BeNumerically(">=", 400))
		body, _ := io.ReadAll(resp.Body)
		Expect(strings.ToLower(string(body))).To(ContainSubstring("not found"))
	})
})
