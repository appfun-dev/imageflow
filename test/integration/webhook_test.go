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

package integration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	crwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	iwebhook "github.com/appfun-dev/imageflow/internal/webhook"
)

// Admission webhook suite (ticket T3). This environment installs the generated
// webhook configurations (config/webhook) against a webhook-only manager that
// registers handlers through the same seam production uses
// (internal/webhook.Register, driven in production by the registrar in
// internal/controller/webhook.go). No controllers are registered here on
// purpose: controller-runtime validates controller names against a
// process-global registry and the Manager suite in this package already runs
// manager.Run with the noop controller.
//
// The webhook server is served with the envtest-generated serving certificate
// (WebhookInstallOptions.LocalServingCertDir) because the API server must trust
// the webhook's CA (LocalServingCAData is injected into the installed webhook
// configurations); the self-signed helper in webhookcert_test.go cannot serve
// admission traffic and stays in use by the Manager suite only.
//
// Division of enforcement under test (spec decision 6):
//   - webhook-owned: created-by injection, importPolicy defaulting, stream
//     name "--" ban, SourceRef shape, locator mutual exclusion, imageFields
//     grammar, path duplicate/overlap;
//   - CEL-owned (must hold even with the webhook down; catalog_crd_test.go
//     pins the no-webhook-installation side): mirror and ImageArtifact write
//     protection.
var _ = Describe("Admission webhooks", Ordered, func() {
	var (
		webhookEnv  *envtest.Environment
		k8sClient   client.Client
		aliceClient client.Client
		saClient    client.Client
		mgr         ctrl.Manager
		ctx         context.Context
		mgrCtx      context.Context
		mgrCancel   context.CancelFunc
		ns          string
	)

	const (
		appName    = "payments-api"
		appTag     = "stable"
		sourceKind = "ImageStreamTag"
		sourceName = appName + ":" + appTag

		aliceUsername = "alice@example.com"
		saUsername    = "system:serviceaccount:team-a:ci-bot"
		saCreatedBy   = "serviceAccount:team-a:ci-bot"
		userCreatedBy = "user:" + aliceUsername
	)

	hexA := strings.Repeat("a", 64)
	digestA := "sha256:" + hexA
	createdByAnnotation := iwebhook.CreatedByAnnotation

	appTargetRef := func() imageflowv1alpha1.TargetReference {
		return imageflowv1alpha1.TargetReference{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
			Namespace:  ns,
			Name:       appName,
		}
	}
	thirdPartyTargetRef := func() imageflowv1alpha1.TargetReference {
		return imageflowv1alpha1.TargetReference{
			APIVersion: "example.io/v1",
			Kind:       "WebService",
			Namespace:  ns,
			Name:       "payments-edge",
		}
	}
	// newSubscription builds a valid subscription (built-in Deployment target
	// located by container name); specs mutate it into the case under test.
	newSubscription := func(name string) *imageflowv1alpha1.ImageSubscription {
		return &imageflowv1alpha1.ImageSubscription{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Spec: imageflowv1alpha1.ImageSubscriptionSpec{
				Source: imageflowv1alpha1.SubscriptionSourceRef{
					Kind:      sourceKind,
					Name:      sourceName,
					Namespace: ns,
				},
				Targets: []imageflowv1alpha1.SubscriptionTarget{{
					TargetRef:      appTargetRef(),
					ContainerNames: []string{appName},
				}},
			},
		}
	}
	withImageFieldsTarget := func(name string, fields ...string) *imageflowv1alpha1.ImageSubscription {
		sub := newSubscription(name)
		sub.Spec.Targets = []imageflowv1alpha1.SubscriptionTarget{{
			TargetRef:   thirdPartyTargetRef(),
			ImageFields: fields,
		}}
		return sub
	}
	expectInvalid := func(err error, wantMessage string) {
		GinkgoHelper()
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(),
			"webhook rejections must surface as Invalid (422), got: %v", err)
		Expect(err.Error()).To(ContainSubstring(wantMessage))
	}

	BeforeAll(func() {
		assets := os.Getenv("KUBEBUILDER_ASSETS")
		if assets == "" {
			Skip("KUBEBUILDER_ASSETS not set")
		}

		ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

		webhookEnv = &envtest.Environment{
			BinaryAssetsDirectory: assets,
			CRDDirectoryPaths: []string{
				filepath.Join("..", "..", "config", "crd", "bases"),
			},
			ErrorIfCRDPathMissing: true,
			WebhookInstallOptions: envtest.WebhookInstallOptions{
				Paths: []string{filepath.Join("..", "..", "config", "webhook")},
			},
		}
		cfg, err := webhookEnv.Start()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(webhookEnv.Stop()).To(Succeed())
		})

		scheme := runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(imageflowv1alpha1.AddToScheme(scheme)).To(Succeed())
		k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
		Expect(err).NotTo(HaveOccurred())

		// Distinct identities for the created-by injection cases: a human-like
		// user and a service account (envtest authenticates client
		// certificates). system:masters short-circuits the apiserver's RBAC for
		// these synthetic identities: authorization is not under test here
		// (real RBAC/SAR paths belong to kind e2e per the spec testing
		// decisions); what must reach the webhook is the distinct USERNAME of
		// each identity.
		aliceCfg, err := webhookEnv.AddUser(envtest.User{
			Name:   aliceUsername,
			Groups: []string{"system:masters", "system:authenticated"},
		}, cfg)
		Expect(err).NotTo(HaveOccurred())
		aliceClient, err = client.New(aliceCfg.Config(), client.Options{Scheme: scheme})
		Expect(err).NotTo(HaveOccurred())
		saCfg, err := webhookEnv.AddUser(envtest.User{
			Name: saUsername,
			Groups: []string{
				"system:masters",
				"system:serviceaccounts",
				"system:serviceaccounts:team-a",
				"system:authenticated",
			},
		}, cfg)
		Expect(err).NotTo(HaveOccurred())
		saClient, err = client.New(saCfg.Config(), client.Options{Scheme: scheme})
		Expect(err).NotTo(HaveOccurred())

		// Webhook-only manager: registers the handlers through the production
		// seam and serves them with the envtest-generated serving certificate.
		// The manager gets its own context so the webhook-unavailability spec
		// can stop the server without cancelling the client context the rest
		// of the suite keeps using.
		ctx = context.Background()
		mgrCtx, mgrCancel = context.WithCancel(context.Background())
		mgr, err = ctrl.NewManager(cfg, ctrl.Options{
			Scheme:  scheme,
			Metrics: metricsserver.Options{BindAddress: "0"},
			WebhookServer: crwebhook.NewServer(crwebhook.Options{
				Port:    webhookEnv.WebhookInstallOptions.LocalServingPort,
				CertDir: webhookEnv.WebhookInstallOptions.LocalServingCertDir,
			}),
		})
		Expect(err).NotTo(HaveOccurred())
		iwebhook.Register(mgr)

		go func() {
			defer GinkgoRecover()
			err := mgr.Start(mgrCtx)
			Expect(err).NotTo(HaveOccurred())
		}()
		webhookStarted := mgr.GetWebhookServer().StartedChecker()
		Eventually(func() error { return webhookStarted(nil) }, 20*time.Second, 250*time.Millisecond).
			Should(Succeed())

		namespace := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "webhook-"},
		}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
		ns = namespace.Name
	})

	AfterAll(func() {
		// Idempotent: the last spec cancels the manager itself.
		if mgrCancel != nil {
			mgrCancel()
		}
	})

	Context("mutating: created-by injection (ADR-003)", func() {
		It("injects created-by for a user identity", func() {
			sub := newSubscription("created-by-user")
			Expect(aliceClient.Create(ctx, sub)).To(Succeed())

			got := &imageflowv1alpha1.ImageSubscription{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sub), got)).To(Succeed())
			Expect(got.GetAnnotations()).To(HaveKeyWithValue(createdByAnnotation, userCreatedBy),
				"the authenticated requester must be recorded for the T8 SAR gate")
		})

		It("injects created-by for a serviceAccount identity", func() {
			sub := newSubscription("created-by-sa")
			Expect(saClient.Create(ctx, sub)).To(Succeed())

			got := &imageflowv1alpha1.ImageSubscription{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sub), got)).To(Succeed())
			Expect(got.GetAnnotations()).To(HaveKeyWithValue(createdByAnnotation, saCreatedBy))
		})

		It("overrides a user-claimed created-by with the authenticated identity", func() {
			sub := newSubscription("created-by-claimed")
			sub.Annotations = map[string]string{createdByAnnotation: "user:evil"}
			Expect(aliceClient.Create(ctx, sub)).To(Succeed())

			got := &imageflowv1alpha1.ImageSubscription{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sub), got)).To(Succeed())
			Expect(got.GetAnnotations()).To(HaveKeyWithValue(createdByAnnotation, userCreatedBy),
				"a self-claimed identity must never survive admission")
		})

		It("keeps created-by immutable across updates by another principal", func() {
			sub := newSubscription("created-by-immutable")
			Expect(aliceClient.Create(ctx, sub)).To(Succeed())

			By("attempting to re-attribute the creator with the suite admin client")
			current := &imageflowv1alpha1.ImageSubscription{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sub), current)).To(Succeed())
			current.Annotations[createdByAnnotation] = "user:mallory"
			Expect(k8sClient.Update(ctx, current)).To(Succeed())

			got := &imageflowv1alpha1.ImageSubscription{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sub), got)).To(Succeed())
			Expect(got.GetAnnotations()).To(HaveKeyWithValue(createdByAnnotation, userCreatedBy))

			By("attempting to drop the annotation entirely")
			delete(got.Annotations, createdByAnnotation)
			Expect(k8sClient.Update(ctx, got)).To(Succeed())

			final := &imageflowv1alpha1.ImageSubscription{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sub), final)).To(Succeed())
			Expect(final.GetAnnotations()).To(HaveKeyWithValue(createdByAnnotation, userCreatedBy),
				"created-by names the creator for the object's whole lifetime")
		})
	})

	Context("mutating: defaulting (ADR-002 R8)", func() {
		It("defaults importPolicy.intervalSeconds to 900 and scheduled to false", func() {
			stream := &imageflowv1alpha1.ImageStream{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "defaults-probe"},
				Spec: imageflowv1alpha1.ImageStreamSpec{
					Tags: []imageflowv1alpha1.TagReference{{Name: appTag}},
				},
			}
			Expect(k8sClient.Create(ctx, stream)).To(Succeed())

			got := &imageflowv1alpha1.ImageStream{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(stream), got)).To(Succeed())
			Expect(got.Spec.Tags).To(HaveLen(1))
			policy := got.Spec.Tags[0].ImportPolicy
			Expect(policy.IntervalSeconds).To(Equal(ptr.To(int32(900))),
				"missing intervalSeconds must default to the ADR-002 R8 value"+
					" (CRD default first, webhook defaulting as second layer)")
			Expect(policy.Scheduled).To(Equal(ptr.To(false)))
		})
	})

	Context("validating: ImageStream name shape (ADR-002)", func() {
		It(`rejects ImageStream names containing "--"`, func() {
			stream := &imageflowv1alpha1.ImageStream{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pay--ments"},
			}
			expectInvalid(k8sClient.Create(ctx, stream), `must not contain "--"`)

			after := &imageflowv1alpha1.ImageStream{}
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(stream), after)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "a rejected create must not persist the object")
		})

		It("accepts stream names with single hyphens and dots", func() {
			stream := &imageflowv1alpha1.ImageStream{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "payments-api.v2"},
			}
			Expect(k8sClient.Create(ctx, stream)).To(Succeed())
		})
	})

	Context("validating: SourceRef logical form (ADR-002)", func() {
		It("rejects source names that are not the logical <stream>:<tag> form", func() {
			invalidNames := []string{
				appName,           // no colon at all
				"a:b:c",           // more than one colon
				":" + appTag,      // empty stream part
				appName + ":",     // empty tag part
				appName + "--x:y", // encoded-form stream with separator
			}
			for i, name := range invalidNames {
				sub := newSubscription("bad-source-form")
				sub.Spec.Source.Name = name
				err := k8sClient.Create(ctx, sub)
				Expect(err).To(HaveOccurred(), "source name %q must be rejected", name)
				Expect(apierrors.IsInvalid(err)).To(BeTrue(), "case %d (%q): %v", i, name, err)
				Expect(err.Error()).To(SatisfyAny(
					ContainSubstring("<stream>:<tag>"),
					ContainSubstring(`must not contain "--"`),
					ContainSubstring("valid ImageStream name"),
					ContainSubstring("OCI tag grammar"),
				), "case %d (%q): %v", i, name, err)
			}
		})

		It("rejects sources whose stream or tag part violates the name grammars", func() {
			sub := newSubscription("bad-source-grammar")
			sub.Spec.Source.Name = "Payments_API:" + appTag
			expectInvalid(k8sClient.Create(ctx, sub), "RFC 1123")

			sub = newSubscription("bad-source-tag")
			sub.Spec.Source.Name = appName + ":st@ble"
			expectInvalid(k8sClient.Create(ctx, sub), "OCI tag grammar")
		})

		It("rejects source kinds other than ImageStreamTag", func() {
			sub := newSubscription("bad-source-kind")
			sub.Spec.Source.Kind = "ImageTag"
			err := k8sClient.Create(ctx, sub)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue())
		})

		It("accepts the logical <stream>:<tag> form with cross-namespace source", func() {
			sub := newSubscription("good-source")
			sub.Spec.Source.Namespace = "imageflow-system"
			Expect(k8sClient.Create(ctx, sub)).To(Succeed())
		})
	})

	Context("validating: target locator shape (spec decision 6, ADR-005/006)", func() {
		It("rejects targets that set both containerNames and imageFields", func() {
			// The exact shape catalog_crd_test.go pins as CRD-allowed: with the
			// webhook installed the whole configuration is rejected.
			sub := newSubscription("both-locators")
			sub.Spec.Targets[0].ImageFields = []string{
				".spec.template.spec.containers[name=edge].image",
			}
			expectInvalid(k8sClient.Create(ctx, sub), "mutually exclusive")
		})

		It("rejects targets that set no locator at all", func() {
			sub := newSubscription("no-locator")
			sub.Spec.Targets[0].ContainerNames = nil
			expectInvalid(k8sClient.Create(ctx, sub), "exactly one locator is required")
		})

		It("rejects built-in Kind targets located by imageFields", func() {
			sub := newSubscription("builtin-imagefields")
			sub.Spec.Targets[0].ContainerNames = nil
			sub.Spec.Targets[0].ImageFields = []string{
				".spec.template.spec.containers[name=app].image",
			}
			expectInvalid(k8sClient.Create(ctx, sub), "must locate images by containerNames")
		})

		It("rejects empty and duplicate container names", func() {
			sub := newSubscription("empty-container-name")
			sub.Spec.Targets[0].ContainerNames = []string{""}
			expectInvalid(k8sClient.Create(ctx, sub), "DNS-1123")

			sub = newSubscription("duplicate-container-names")
			sub.Spec.Targets[0].ContainerNames = []string{"app", "app"}
			expectInvalid(k8sClient.Create(ctx, sub), "Duplicate value")
		})

		It("accepts containerNames targets of non-first-batch Kinds (runtime reports Unsupported, ADR-006)", func() {
			sub := newSubscription("job-target")
			sub.Spec.Targets[0].TargetRef = imageflowv1alpha1.TargetReference{
				APIVersion: "batch/v1",
				Kind:       "Job",
				Namespace:  ns,
				Name:       "nightly",
			}
			Expect(k8sClient.Create(ctx, sub)).To(Succeed())
		})
	})

	Context("validating: imageFields grammar (ADR-005)", func() {
		It("rejects every forbidden syntax family", func() {
			rejected := []struct {
				path    string
				message string
			}{
				{".spec.containers[*].image", "wildcards"},
				{".spec.containers[0].image", "numeric subscript"},
				{".spec.containers[name=a][env=b].image", "chained or nested filters"},
				{".spec.containers[name=a,env=b].image", "multi-condition"},
				{".spec.containers[name!=a].image", "equality filters"},
				{"$.spec.image", "JSONPath"},
				{"spec.image", `must start with "."`},
				{".spec.containers[name=a]", "must end on a plain field"},
				{`.spec.containers[name="a"].image`, "quoted values"},
				{".spec.containers[name=a].image()", `segments are separated by "."`},
				{".spec..image", "empty segment"},
				{".spec.containers[name=a b].image", "filter value"},
			}
			for _, tc := range rejected {
				sub := withImageFieldsTarget("bad-grammar", tc.path)
				err := k8sClient.Create(ctx, sub)
				Expect(err).To(HaveOccurred(), "path %q must be rejected", tc.path)
				Expect(apierrors.IsInvalid(err)).To(BeTrue(), "path %q: %v", tc.path, err)
				Expect(err.Error()).To(ContainSubstring(tc.message), "path %q: %v", tc.path, err)
			}
		})

		It("accepts restricted dot paths with single-layer [name=x] equality filters", func() {
			sub := withImageFieldsTarget("good-grammar",
				".spec.image",
				".spec.runtime.containers[name=edge].image",
				".spec.sidecars[name=proxy].image",
				".spec.template.spec.initContainers[name=init-0].image",
			)
			Expect(k8sClient.Create(ctx, sub)).To(Succeed())
		})

		It("rejects duplicate and overlapping paths but accepts distinct fields and elements", func() {
			duplicate := withImageFieldsTarget("duplicate-paths",
				".spec.runtime.containers[name=edge].image",
				".spec.runtime.containers[name=edge].image",
			)
			expectInvalid(k8sClient.Create(ctx, duplicate), "duplicates imageFields[0]")

			overlap := withImageFieldsTarget("overlapping-paths",
				".spec.runtime",
				".spec.runtime.containers[name=edge].image",
			)
			expectInvalid(k8sClient.Create(ctx, overlap), "overlaps imageFields[0]")

			distinct := withImageFieldsTarget("distinct-paths",
				".spec.containers[name=a].image",
				".spec.containers[name=b].image",
				".spec.containers[name=a].imagePullPolicy",
			)
			Expect(k8sClient.Create(ctx, distinct)).To(Succeed())
		})
	})

	Context("validating: updates", func() {
		It("rejects an update that introduces an invalid shape", func() {
			sub := newSubscription("update-shape")
			Expect(k8sClient.Create(ctx, sub)).To(Succeed())

			current := &imageflowv1alpha1.ImageSubscription{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sub), current)).To(Succeed())
			current.Spec.Targets[0].ImageFields = []string{
				".spec.template.spec.containers[name=edge].image",
			}
			expectInvalid(k8sClient.Update(ctx, current), "mutually exclusive")

			after := &imageflowv1alpha1.ImageSubscription{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sub), after)).To(Succeed())
			Expect(after.Spec.Targets[0].ImageFields).To(BeEmpty(),
				"a rejected update must leave the stored object untouched")
		})
	})

	// Must stay the last spec of this container: it shuts the webhook server
	// down to prove the CEL boundary and the fail-closed posture.
	It("keeps the mirror and ImageArtifact boundary CEL-guarded when the webhook is unavailable", func() {
		By("stopping the webhook server")
		mgrCancel()
		webhookStopped := mgr.GetWebhookServer().StartedChecker()
		Eventually(func() error { return webhookStopped(nil) }, 20*time.Second, 250*time.Millisecond).
			ShouldNot(Succeed())

		// The API server does not depend on the webhook for kinds it does not
		// intercept, so a plain client keeps working.
		By("failing closed on webhook-owned subscription validation")
		sub := newSubscription("fail-closed")
		sub.Spec.Targets[0].ImageFields = []string{
			".spec.template.spec.containers[name=edge].image",
		}
		err := k8sClient.Create(ctx, sub)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeFalse(),
			"the rejection must come from the unreachable webhook (failurePolicy=Fail), not from schema validation")
		Expect(err.Error()).To(ContainSubstring("failed calling webhook"))

		By("proving mirror objects never depended on the webhook")
		mirror := &imageflowv1alpha1.ImageStreamTag{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: appName + "--" + appTag},
			Spec:       imageflowv1alpha1.ImageStreamTagSpec{Stream: appName, Tag: appTag},
		}
		Expect(k8sClient.Create(ctx, mirror)).To(Succeed())
		imageTag := &imageflowv1alpha1.ImageTag{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: appName + "--sha256-" + hexA},
			Spec:       imageflowv1alpha1.ImageTagSpec{Stream: appName, Digest: digestA},
		}
		Expect(k8sClient.Create(ctx, imageTag)).To(Succeed())

		By("rejecting mirror spec hijacking through CEL alone")
		mirror.Spec.Tag = "hijack-attempt"
		err = k8sClient.Update(ctx, mirror)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "CEL rejection must surface as Invalid")
		Expect(err.Error()).To(ContainSubstring("controller-owned mirror object"))

		imageTag.Spec.Digest = "sha256:" + strings.Repeat("b", 64)
		err = k8sClient.Update(ctx, imageTag)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("controller-owned mirror object"))

		By("rejecting ImageArtifact spec writes through CEL alone")
		artifact := &imageflowv1alpha1.ImageArtifact{
			ObjectMeta: metav1.ObjectMeta{Name: "sha256-" + hexA},
			Spec:       imageflowv1alpha1.ImageArtifactSpec{Digest: digestA},
		}
		Expect(k8sClient.Create(ctx, artifact)).To(Succeed())
		artifact.Spec.ImageReference = "quay.io/example/" + appName + "@" + digestA
		err = k8sClient.Update(ctx, artifact)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("controller-owned and immutable"))

		By("keeping annotations as the user-writable mirror surface")
		after := &imageflowv1alpha1.ImageStreamTag{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(mirror), after)).To(Succeed())
		after.Annotations = map[string]string{"imageflow.appfun.dev/note": "still open"}
		Expect(k8sClient.Update(ctx, after)).To(Succeed())
	})
})
