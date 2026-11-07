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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
	"github.com/appfun-dev/imageflow/internal/controller"
	"github.com/appfun-dev/imageflow/internal/registry"
	"github.com/appfun-dev/imageflow/test/registrydouble"
)

// T7 named cases: the Import pull path against a real API server (CRD
// validation and CEL included) and a real OCI Distribution v2 registry double
// (test/registrydouble), driven by the REAL S2 client (internal/registry) and
// the injected clock (seam S3). Zero real network: the double listens on
// loopback and the S2 client is built with a transport that trusts its CA.
//
// Controller names carry the "-t7" suffix: controller-runtime validates
// controller names process-globally, and the other suites in this package boot
// their own managers in the same test process.
const (
	t7NamespacePrefix = "t7-import-"
	t7Repo            = "imageflow/t7-integration"
	t7OnDemandTag     = "v1"
	t7StreamTag       = "stable"
	t7ScheduledTagRef = "sched"
	t7OnDemandStream  = "payments-api-import"
	t7ScheduledStream = "payments-api-scheduled"
	t7CanaryPassword  = "t7-integration-canary-password"
	t7CanaryUser      = "imageflow+t7integration"
	t7SecretName      = "t7-pull-secret"
	t7Deadline        = 60 * time.Second
	t7Poll            = 200 * time.Millisecond
	t7CompleteJSONPth = `{.status.conditions[?(@.type=="Complete")].reason}`
	t7MetricsAddr     = "127.0.0.1:28447"
)

var _ = Describe("T7 Import pull path", Label("t7"), Ordered, func() {
	var (
		t7Env      *envtest.Environment
		restCfg    *rest.Config
		k8s        client.Client
		ctx        context.Context
		cancel     context.CancelFunc
		ns         string
		fakeClk    *clock.Fake
		mgrCancel  context.CancelFunc
		mgrStopped chan struct{}

		anonRegistry *registrydouble.Server // anonymous pulls
		authRegistry *registrydouble.Server // basic-auth pulls (credential specs)
	)

	t7Config := func() config.Config {
		return config.Config{
			MappingTTLSuccess:        time.Hour,
			MappingTTLFailure:        24 * time.Hour,
			StreamTagHistoryLimit:    imageflowv1alpha1.StreamTagHistoryMaxItems,
			ScheduledDefaultInterval: 10 * time.Minute,
			ImportDefaultInterval:    10 * time.Minute,
		}
	}

	BeforeAll(func() {
		assets := os.Getenv("KUBEBUILDER_ASSETS")
		if assets == "" {
			Skip("KUBEBUILDER_ASSETS not set")
		}
		t7Env = &envtest.Environment{
			BinaryAssetsDirectory: assets,
			CRDDirectoryPaths: []string{
				filepath.Join("..", "..", "config", "crd", "bases"),
			},
			ErrorIfCRDPathMissing: true,
		}
		var err error
		restCfg, err = t7Env.Start()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(t7Env.Stop()).To(Succeed())
		})

		scheme := runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(imageflowv1alpha1.AddToScheme(scheme)).To(Succeed())
		k8s, err = client.New(restCfg, client.Options{Scheme: scheme})
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel = context.WithCancel(context.Background())
		DeferCleanup(cancel)

		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: t7NamespacePrefix}}
		Expect(k8s.Create(ctx, namespace)).To(Succeed())
		ns = namespace.Name

		fakeClk = clock.NewFake(time.Date(2026, 4, 4, 0, 0, 0, 0, time.UTC))

		By("starting the registry doubles (anonymous + basic auth)")
		anonRegistry, err = registrydouble.New(registrydouble.Options{})
		Expect(err).NotTo(HaveOccurred())
		authRegistry, err = registrydouble.New(registrydouble.Options{
			Username: t7CanaryUser, Password: t7CanaryPassword,
		})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(anonRegistry.Close)
		DeferCleanup(authRegistry.Close)

		By("building the real S2 client with a transport trusting both doubles")
		pool := x509.NewCertPool()
		Expect(pool.AppendCertsFromPEM(anonRegistry.CACertPEM())).To(BeTrue())
		Expect(pool.AppendCertsFromPEM(authRegistry.CACertPEM())).To(BeTrue())
		seam, err := registry.NewClient(registry.Options{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{
				RootCAs: pool, MinVersion: tls.VersionTLS12,
			}},
			RequestTimeout: 5 * time.Second,
		})
		Expect(err).NotTo(HaveOccurred())

		By("starting the T7 controllers plus the T4 TTL GC in one manager")
		mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
			Scheme: scheme,
			// Suite-unique metrics port: the shared manager suite binds 28443.
			Metrics: metricsserver.Options{BindAddress: t7MetricsAddr},
		})
		Expect(err).NotTo(HaveOccurred())
		opts := controller.Options{Clock: fakeClk, Config: t7Config()}
		Expect(controller.SetupImportControllers(mgr, opts, controller.ImportControllerNames{
			Import:    "import-t7",
			Scheduled: "scheduled-import-t7",
		}, controller.ImportDeps{Registry: seam})).To(Succeed())
		// TTL GC is T4 machinery, reused untouched: the Import kind is already
		// covered there, this suite only RE-VERIFIES it (ticket AC).
		Expect(controller.SetupTaskGCControllers(mgr, opts, controller.MappingControllerNames{
			Mapping:   "mapping-t7",
			GCMapping: "task-gc-mapping-t7",
			GCImport:  "task-gc-import-t7",
		})).To(Succeed())

		var mgrCtx context.Context
		mgrCtx, mgrCancel = context.WithCancel(ctx)
		mgrStopped = make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(mgrStopped)
			Expect(mgr.Start(mgrCtx)).To(Succeed())
		}()
		Expect(mgr.GetCache().WaitForCacheSync(mgrCtx)).To(BeTrue())
	})

	AfterAll(func() {
		if mgrCancel != nil {
			mgrCancel()
		}
		if mgrStopped != nil {
			Eventually(mgrStopped, 30*time.Second, 100*time.Millisecond).Should(BeClosed())
		}
	})

	// --- helpers ---------------------------------------------------------

	importKey := func(name string) types.NamespacedName {
		return types.NamespacedName{Namespace: ns, Name: name}
	}

	createStream := func(name string, tags ...imageflowv1alpha1.TagReference) {
		Expect(k8s.Create(ctx, &imageflowv1alpha1.ImageStream{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Spec:       imageflowv1alpha1.ImageStreamSpec{Tags: tags},
		})).To(Succeed())
	}

	createImport := func(name, stream, tag, from string, pullSecret string) {
		task := &imageflowv1alpha1.ImageStreamImport{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Spec: imageflowv1alpha1.ImageStreamImportSpec{
				Stream: stream,
				Tag:    tag,
				From:   imageflowv1alpha1.ImageSourceReference{Name: from},
			},
		}
		if pullSecret != "" {
			task.Spec.PullSecretRef = &imageflowv1alpha1.LocalSecretReference{Name: pullSecret}
		}
		Expect(k8s.Create(ctx, task)).To(Succeed())
	}

	waitImportComplete := func(name string) *imageflowv1alpha1.ImageStreamImport {
		var done *imageflowv1alpha1.ImageStreamImport
		Eventually(func(g Gomega) {
			got := &imageflowv1alpha1.ImageStreamImport{}
			g.Expect(k8s.Get(ctx, importKey(name), got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions,
				imageflowv1alpha1.ConditionTypeComplete)
			g.Expect(cond).NotTo(BeNil(), "task %s has no Complete condition yet", name)
			g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			done = got
		}, t7Deadline, t7Poll).Should(Succeed())
		return done
	}

	completeMessage := func(task *imageflowv1alpha1.ImageStreamImport) string {
		cond := meta.FindStatusCondition(task.Status.Conditions,
			imageflowv1alpha1.ConditionTypeComplete)
		Expect(cond).NotTo(BeNil())
		return cond.Message
	}

	streamItems := func(stream, tag string) []imageflowv1alpha1.TagEvent {
		var got imageflowv1alpha1.ImageStream
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: stream}, &got)).To(Succeed())
		for _, entry := range got.Status.Tags {
			if entry.Tag == tag {
				return entry.Items
			}
		}
		return nil
	}

	digestsOf := func(items []imageflowv1alpha1.TagEvent) []string {
		out := make([]string, 0, len(items))
		for _, item := range items {
			out = append(out, item.Digest)
		}
		return out
	}

	eventsFor := func(reason, involvedName string) []corev1.Event {
		var list corev1.EventList
		Expect(k8s.List(ctx, &list, client.InNamespace(ns))).To(Succeed())
		var out []corev1.Event
		for _, event := range list.Items {
			if event.Reason == reason && event.InvolvedObject.Name == involvedName {
				out = append(out, event)
			}
		}
		return out
	}

	completeMessagesOf := func(tasks []imageflowv1alpha1.ImageStreamImport) []string {
		out := make([]string, 0, len(tasks))
		for i := range tasks {
			if cond := meta.FindStatusCondition(tasks[i].Status.Conditions,
				imageflowv1alpha1.ConditionTypeComplete); cond != nil {
				out = append(out, cond.Message)
			}
		}
		return out
	}

	allEventMessages := func() []string {
		var list corev1.EventList
		Expect(k8s.List(ctx, &list, client.InNamespace(ns))).To(Succeed())
		out := make([]string, 0, len(list.Items))
		for _, event := range list.Items {
			out = append(out, event.Message)
		}
		return out
	}

	// touch re-triggers reconciliation without changing the spec: the schedule
	// and TTL decisions are recomputed from persisted state plus the injected
	// clock, so a metadata patch is exactly the "wake up" a real timer would be.
	touch := func(obj client.Object) {
		patch := client.MergeFrom(obj.DeepCopyObject().(client.Object))
		if obj.GetAnnotations() == nil {
			obj.SetAnnotations(map[string]string{})
		}
		obj.GetAnnotations()["t7.imageflow.appfun.dev/probe"] = fmt.Sprintf("%d", time.Now().UnixNano())
		Expect(k8s.Patch(ctx, obj, patch)).To(Succeed())
	}

	scheduledTasks := func(stream, tag string) []imageflowv1alpha1.ImageStreamImport {
		var list imageflowv1alpha1.ImageStreamImportList
		Expect(k8s.List(ctx, &list,
			client.InNamespace(ns),
			client.MatchingLabels{controller.LabelImportSource: controller.LabelValueScheduledImport},
		)).To(Succeed())
		var out []imageflowv1alpha1.ImageStreamImport
		for _, task := range list.Items {
			if task.Spec.Stream == stream && task.Spec.Tag == tag {
				out = append(out, task)
			}
		}
		return out
	}

	createPullSecret := func(host, user, password string) {
		Expect(k8s.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: t7SecretName},
			Type:       corev1.SecretTypeDockerConfigJson,
			Data: map[string][]byte{
				corev1.DockerConfigJsonKey: registrydouble.DockerConfigJSON(host, user, password),
			},
		})).To(Succeed())
	}

	assertNoCanary := func(objs ...client.Object) {
		for _, obj := range objs {
			raw, err := json.Marshal(obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(raw)).NotTo(ContainSubstring(t7CanaryPassword),
				"credential material leaked into %T", obj)
			Expect(string(raw)).NotTo(ContainSubstring(t7CanaryUser),
				"credential material leaked into %T", obj)
		}
		for _, message := range allEventMessages() {
			Expect(message).NotTo(ContainSubstring(t7CanaryPassword),
				"credential material leaked into an Event")
			Expect(message).NotTo(ContainSubstring(t7CanaryUser),
				"credential material leaked into an Event")
		}
	}

	// --- specs ------------------------------------------------------------

	It("t7-imports-an-external-tag-on-demand-into-the-catalog", func() {
		digest := anonRegistry.MustPutTag(t7Repo, t7OnDemandTag, "on-demand-seed")
		createStream(t7OnDemandStream, imageflowv1alpha1.TagReference{Name: t7StreamTag})

		createImport("t7-on-demand", t7OnDemandStream, t7StreamTag,
			anonRegistry.Reference(t7Repo, t7OnDemandTag), "")
		done := waitImportComplete("t7-on-demand")

		By("reporting a successful, fully diagnosed task status")
		cond := meta.FindStatusCondition(done.Status.Conditions, imageflowv1alpha1.ConditionTypeComplete)
		Expect(cond.Reason).To(Equal(imageflowv1alpha1.ReasonSucceeded))
		Expect(done.Status.Digest).To(Equal(digest))
		Expect(done.Status.ImageReference).
			To(Equal(anonRegistry.DigestReferenceAt(anonRegistry.Host(), t7Repo, digest)))
		Expect(done.Status.ArtifactRef).To(Equal("sha256-" + strings.TrimPrefix(digest, "sha256:")))
		Expect(done.Status.StartedAt).NotTo(BeNil())
		Expect(done.Status.CompletedAt).NotTo(BeNil())

		By("registering the association in the catalog through the shared machinery")
		items := streamItems(t7OnDemandStream, t7StreamTag)
		Expect(items).To(HaveLen(1))
		Expect(items[0].Digest).To(Equal(digest))
		Expect(items[0].Created.Time.Equal(fakeClk.Now())).To(BeTrue(),
			"association timestamps must come from the injected clock")

		artifact := &imageflowv1alpha1.ImageArtifact{}
		Expect(k8s.Get(ctx, types.NamespacedName{Name: done.Status.ArtifactRef}, artifact)).To(Succeed())
		Expect(artifact.Spec.Digest).To(Equal(digest))

		By("attributing the Image Change to the import source")
		Eventually(func(g Gomega) {
			changed := eventsFor(controller.EventReasonImageChanged, t7OnDemandStream)
			g.Expect(changed).To(HaveLen(1))
			g.Expect(changed[0].Message).To(ContainSubstring("ImageStreamImport"))
			g.Expect(changed[0].Message).NotTo(ContainSubstring("ImageStreamMapping"))
			g.Expect(changed[0].Message).To(ContainSubstring(digest))
		}, t7Deadline, t7Poll).Should(Succeed())
		Eventually(func(g Gomega) {
			g.Expect(eventsFor(controller.EventReasonImportSucceeded, "t7-on-demand")).To(HaveLen(1))
		}, t7Deadline, t7Poll).Should(Succeed())

		By("not claiming drift for a first association")
		Expect(eventsFor(controller.EventReasonTagDriftDetected, t7OnDemandStream)).To(BeEmpty())
	})

	It("t7-imports-a-digest-reference-after-verifying-the-manifest", func() {
		digest := anonRegistry.MustPutTag(t7Repo, "by-digest", "digest-seed")
		createStream("t7-digest-stream")
		createImport("t7-by-digest", "t7-digest-stream", t7StreamTag,
			anonRegistry.DigestReferenceAt(anonRegistry.Host(), t7Repo, digest), "")

		done := waitImportComplete("t7-by-digest")
		Expect(done.Status.Digest).To(Equal(digest))
		Expect(completeMessage(done)).To(ContainSubstring("first association recorded"))

		By("a digest that the registry does not serve is a diagnosable manifest failure")
		missing := "sha256:" + strings.Repeat("0f", 32)
		createImport("t7-by-digest-missing", "t7-digest-stream", t7StreamTag,
			anonRegistry.DigestReferenceAt(anonRegistry.Host(), t7Repo, missing), "")
		failed := waitImportComplete("t7-by-digest-missing")
		Expect(completeMessage(failed)).To(HavePrefix("ManifestNotFound:"))
	})

	It("t7-failure-paths-report-diagnosable-reasons-for-network-credential-and-manifest-problems", func() {
		createStream("t7-failure-stream")
		authRegistry.MustPutTag(t7Repo, "auth-only", "auth-seed")

		cases := []struct {
			name   string
			from   string
			secret string
			prefix string
		}{
			{"t7-fail-network", "127.0.0.1:1/" + t7Repo + ":" + t7OnDemandTag, "",
				"NetworkUnreachable:"},
			{"t7-fail-unauthorized", authRegistry.Reference(t7Repo, "auth-only"), "",
				"Unauthorized:"},
			{"t7-fail-manifest", anonRegistry.Reference(t7Repo, "no-such-tag-here"), "",
				"ManifestNotFound:"},
			{"t7-fail-secret", anonRegistry.Reference(t7Repo, t7OnDemandTag), "t7-no-such-secret",
				"PullSecretNotFound:"},
			{"t7-fail-reference", "payments-api-without-registry:v1", "",
				"InvalidImageReference:"},
		}
		for _, tc := range cases {
			createImport(tc.name, "t7-failure-stream", t7StreamTag, tc.from, tc.secret)
		}

		for _, tc := range cases {
			done := waitImportComplete(tc.name)
			cond := meta.FindStatusCondition(done.Status.Conditions,
				imageflowv1alpha1.ConditionTypeComplete)
			Expect(cond.Reason).To(Equal(imageflowv1alpha1.ReasonFailed), tc.name)
			Expect(cond.Message).To(HavePrefix(tc.prefix), tc.name)
			Expect(done.Status.Digest).To(BeEmpty(), tc.name)
			Expect(done.Status.ArtifactRef).To(BeEmpty(), tc.name)

			By("every failure is audited with a Warning event and never retried silently")
			Eventually(func(g Gomega) {
				failed := eventsFor(controller.EventReasonImportFailed, tc.name)
				g.Expect(failed).To(HaveLen(1))
				g.Expect(failed[0].Type).To(Equal(corev1.EventTypeWarning))
			}, t7Deadline, t7Poll).Should(Succeed())

			Consistently(func(g Gomega) {
				got := &imageflowv1alpha1.ImageStreamImport{}
				g.Expect(k8s.Get(ctx, importKey(tc.name), got)).To(Succeed())
				g.Expect(completeMessage(got)).To(HavePrefix(tc.prefix),
					"a terminal failure must stay terminal")
			}, 3*time.Second, 500*time.Millisecond).Should(Succeed())
		}

		By("the catalog stays untouched by failed imports")
		Expect(streamItems("t7-failure-stream", t7StreamTag)).To(BeEmpty())
	})

	It("t7-credentials-from-a-pull-secret-never-surface-in-status-or-events", func() {
		digest := authRegistry.MustPutTag(t7Repo, "credentialed", "credential-seed")
		createStream("t7-credential-stream")
		createPullSecret(authRegistry.Host(), t7CanaryUser, t7CanaryPassword)

		createImport("t7-with-secret", "t7-credential-stream", t7StreamTag,
			authRegistry.Reference(t7Repo, "credentialed"), t7SecretName)
		done := waitImportComplete("t7-with-secret")
		Expect(done.Status.Digest).To(Equal(digest),
			"the authenticated import must succeed: %s", completeMessage(done))
		assertNoCanary(done)

		By("a rejected credential is diagnosable and equally leak-free")
		wrong := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "t7-wrong-secret"},
			Type:       corev1.SecretTypeDockerConfigJson,
			Data: map[string][]byte{corev1.DockerConfigJsonKey: registrydouble.DockerConfigJSON(
				authRegistry.Host(), t7CanaryUser, "not-the-real-password")},
		}
		Expect(k8s.Create(ctx, wrong)).To(Succeed())
		createImport("t7-wrong-secret-task", "t7-credential-stream", t7StreamTag,
			authRegistry.Reference(t7Repo, "credentialed"), "t7-wrong-secret")
		rejected := waitImportComplete("t7-wrong-secret-task")
		Expect(completeMessage(rejected)).To(HavePrefix("Unauthorized:"))
		Expect(completeMessage(rejected)).NotTo(ContainSubstring("not-the-real-password"))
		assertNoCanary(rejected, wrong)

		var stream imageflowv1alpha1.ImageStream
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "t7-credential-stream"},
			&stream)).To(Succeed())
		assertNoCanary(&stream)
	})

	It("t7-scheduled-import-re-imports-the-tag-at-intervalseconds-under-the-injected-clock", func() {
		anonRegistry.MustPutTag(t7Repo, t7ScheduledTagRef, "scheduled-seed-one")
		interval := int32(600)
		createStream(t7ScheduledStream, imageflowv1alpha1.TagReference{
			Name: t7StreamTag,
			From: &imageflowv1alpha1.ImageSourceReference{
				Name: anonRegistry.Reference(t7Repo, t7ScheduledTagRef),
			},
			ImportPolicy: imageflowv1alpha1.TagImportPolicy{
				Scheduled:       ptrBoolT7(true),
				IntervalSeconds: &interval,
			},
		})

		By("the first scheduled task is created as soon as the policy is seen")
		var first []imageflowv1alpha1.ImageStreamImport
		Eventually(func(g Gomega) {
			first = scheduledTasks(t7ScheduledStream, t7StreamTag)
			g.Expect(first).To(HaveLen(1))
		}, t7Deadline, t7Poll).Should(Succeed())
		Expect(first[0].Labels).To(HaveKeyWithValue(
			controller.LabelImportSource, controller.LabelValueScheduledImport))
		Expect(first[0].Spec.PullSecretRef).To(BeNil(),
			"the v1 ImageStream API carries no per-tag pull secret: scheduled imports are anonymous")
		Expect(first[0].OwnerReferences).To(HaveLen(1))
		Expect(first[0].OwnerReferences[0].Kind).To(Equal("ImageStream"))
		Expect(first[0].OwnerReferences[0].Controller).To(Equal(ptrBoolT7(true)))

		var stream imageflowv1alpha1.ImageStream
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: t7ScheduledStream},
			&stream)).To(Succeed())
		Expect(first[0].OwnerReferences[0].UID).To(Equal(stream.UID),
			"the ownerRef must point at the polling stream (cascade GC contract)")

		By("it completes and registers the external digest")
		firstDigest := anonRegistry.DigestOf(t7Repo, t7ScheduledTagRef)
		Eventually(func(g Gomega) {
			got := &imageflowv1alpha1.ImageStreamImport{}
			g.Expect(k8s.Get(ctx, importKey(first[0].Name), got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions,
				imageflowv1alpha1.ConditionTypeComplete)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Reason).To(Equal(imageflowv1alpha1.ReasonSucceeded))
			g.Expect(got.Status.Digest).To(Equal(firstDigest))
		}, t7Deadline, t7Poll).Should(Succeed())
		Eventually(func(g Gomega) {
			g.Expect(streamItems(t7ScheduledStream, t7StreamTag)).To(HaveLen(1))
		}, t7Deadline, t7Poll).Should(Succeed())

		By("inside the interval no second task is created")
		fakeClk.Advance(5 * time.Minute)
		touch(&stream)
		Consistently(func(g Gomega) {
			g.Expect(scheduledTasks(t7ScheduledStream, t7StreamTag)).To(HaveLen(1))
		}, 3*time.Second, 500*time.Millisecond).Should(Succeed())

		By("past the interval the tag is imported again")
		fakeClk.Advance(6 * time.Minute) // 11m since the first completion
		touch(&stream)
		Eventually(func(g Gomega) {
			g.Expect(scheduledTasks(t7ScheduledStream, t7StreamTag)).To(HaveLen(2))
		}, t7Deadline, t7Poll).Should(Succeed())
	})

	It("t7-detects-external-tag-drift-and-attributes-the-image-change-to-the-import-source", func() {
		changesBefore := len(eventsFor(controller.EventReasonImageChanged, t7ScheduledStream))
		driftsBefore := len(eventsFor(controller.EventReasonTagDriftDetected, t7ScheduledStream))
		oldDigest := anonRegistry.DigestOf(t7Repo, t7ScheduledTagRef)

		By("the external tag keeps its name but points at new content")
		newDigest := anonRegistry.MustPutTag(t7Repo, t7ScheduledTagRef, "scheduled-seed-two")
		Expect(newDigest).NotTo(Equal(oldDigest))

		var stream imageflowv1alpha1.ImageStream
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: t7ScheduledStream},
			&stream)).To(Succeed())
		fakeClk.Advance(11 * time.Minute)
		touch(&stream)

		By("a real Image Change lands in the bounded catalog history")
		Eventually(func(g Gomega) {
			g.Expect(digestsOf(streamItems(t7ScheduledStream, t7StreamTag))).
				To(Equal([]string{newDigest, oldDigest}))
		}, t7Deadline, t7Poll).Should(Succeed())

		By("the drift is attributable to the import path by reason and by message")
		Eventually(func(g Gomega) {
			drifts := eventsFor(controller.EventReasonTagDriftDetected, t7ScheduledStream)
			g.Expect(drifts).To(HaveLen(driftsBefore + 1))
			newest := drifts[len(drifts)-1]
			g.Expect(newest.Message).To(ContainSubstring(oldDigest))
			g.Expect(newest.Message).To(ContainSubstring(newDigest))
			g.Expect(newest.Message).To(ContainSubstring("trigger: scheduled"))

			changed := eventsFor(controller.EventReasonImageChanged, t7ScheduledStream)
			g.Expect(changed).To(HaveLen(changesBefore + 1))
			message := changed[len(changed)-1].Message
			g.Expect(message).To(ContainSubstring("ImageStreamImport"))
			g.Expect(message).NotTo(ContainSubstring("ImageStreamMapping"))
			g.Expect(message).To(ContainSubstring("previous: " + oldDigest))
		}, t7Deadline, t7Poll).Should(Succeed())

		By("the task that performed the drifted import says so")
		Eventually(func(g Gomega) {
			g.Expect(completeMessagesOf(scheduledTasks(t7ScheduledStream, t7StreamTag))).
				To(ContainElement(ContainSubstring("tag drift detected")))
		}, t7Deadline, t7Poll).Should(Succeed())

		By("without drift the next scheduled import records no change at all")
		known := map[string]struct{}{}
		for _, task := range scheduledTasks(t7ScheduledStream, t7StreamTag) {
			known[task.Name] = struct{}{}
		}
		changesAfterDrift := len(eventsFor(controller.EventReasonImageChanged, t7ScheduledStream))
		driftsAfterDrift := len(eventsFor(controller.EventReasonTagDriftDetected, t7ScheduledStream))
		fakeClk.Advance(11 * time.Minute)
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: t7ScheduledStream},
			&stream)).To(Succeed())
		touch(&stream)

		var quietName string
		Eventually(func(g Gomega) {
			for _, task := range scheduledTasks(t7ScheduledStream, t7StreamTag) {
				if _, seen := known[task.Name]; !seen {
					quietName = task.Name
				}
			}
			g.Expect(quietName).NotTo(BeEmpty(), "the next scheduled tick must create a task")
		}, t7Deadline, t7Poll).Should(Succeed())
		Expect(completeMessage(waitImportComplete(quietName))).To(ContainSubstring("No drift"))

		Expect(digestsOf(streamItems(t7ScheduledStream, t7StreamTag))).
			To(Equal([]string{newDigest, oldDigest}), "no drift must not touch the history")
		Expect(eventsFor(controller.EventReasonImageChanged, t7ScheduledStream)).
			To(HaveLen(changesAfterDrift))
		Expect(eventsFor(controller.EventReasonTagDriftDetected, t7ScheduledStream)).
			To(HaveLen(driftsAfterDrift))
	})

	It("t7-scheduled-import-failures-stay-visible-on-the-stream-and-retry-next-tick", func() {
		historyBefore := digestsOf(streamItems(t7ScheduledStream, t7StreamTag))
		Expect(historyBefore).NotTo(BeEmpty(), "the drift spec must have populated the catalog")

		var stream imageflowv1alpha1.ImageStream
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: t7ScheduledStream},
			&stream)).To(Succeed())
		known := map[string]struct{}{}
		for _, task := range scheduledTasks(t7ScheduledStream, t7StreamTag) {
			known[task.Name] = struct{}{}
		}

		By("the external registry starts answering 404 for the polled tag")
		anonRegistry.SetFault(registrydouble.FaultNotFound)
		DeferCleanup(anonRegistry.SetFault, registrydouble.FaultNone)

		fakeClk.Advance(11 * time.Minute)
		touch(&stream)

		var failedName string
		Eventually(func(g Gomega) {
			for _, task := range scheduledTasks(t7ScheduledStream, t7StreamTag) {
				if _, seen := known[task.Name]; !seen {
					failedName = task.Name
				}
			}
			g.Expect(failedName).NotTo(BeEmpty(), "the failing tick must still create a task")
		}, t7Deadline, t7Poll).Should(Succeed())
		failed := waitImportComplete(failedName)
		Expect(completeMessage(failed)).To(HavePrefix("ManifestNotFound:"),
			"the failure must be diagnosable from the task alone")

		By("the failing poll is reported on the stream, not only on the ephemeral task")
		Eventually(func(g Gomega) {
			reported := eventsFor(controller.EventReasonScheduledImportFailed, t7ScheduledStream)
			g.Expect(reported).NotTo(BeEmpty())
			newest := reported[len(reported)-1]
			g.Expect(newest.Type).To(Equal(corev1.EventTypeWarning))
			g.Expect(newest.Message).To(ContainSubstring("ManifestNotFound"))
			g.Expect(newest.Message).To(ContainSubstring(t7StreamTag))
		}, t7Deadline, t7Poll).Should(Succeed())

		By("the catalog keeps the last known good association")
		Expect(digestsOf(streamItems(t7ScheduledStream, t7StreamTag))).To(Equal(historyBefore))

		By("once the registry answers again the next tick succeeds")
		anonRegistry.SetFault(registrydouble.FaultNone)
		known[failedName] = struct{}{}
		fakeClk.Advance(11 * time.Minute)
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: t7ScheduledStream},
			&stream)).To(Succeed())
		touch(&stream)

		var recoveredName string
		Eventually(func(g Gomega) {
			for _, task := range scheduledTasks(t7ScheduledStream, t7StreamTag) {
				if _, seen := known[task.Name]; !seen {
					recoveredName = task.Name
				}
			}
			g.Expect(recoveredName).NotTo(BeEmpty(), "the retry tick must create a task")
		}, t7Deadline, t7Poll).Should(Succeed())
		recovered := waitImportComplete(recoveredName)
		Expect(completeMessage(recovered)).To(ContainSubstring("No drift"),
			"the tag content did not change while the registry was failing")
		Expect(digestsOf(streamItems(t7ScheduledStream, t7StreamTag))).To(Equal(historyBefore))
	})

	It("t7-exposes-import-duration-and-failure-rate-metrics", func() {
		body := func() string {
			resp, err := http.Get("http://" + t7MetricsAddr + "/metrics")
			if err != nil {
				return ""
			}
			defer func() { _ = resp.Body.Close() }()
			payload, _ := io.ReadAll(resp.Body)
			return string(payload)
		}
		Eventually(func(g Gomega) {
			metrics := body()
			// Import duration (histogram) and failure rate (tasks counter with
			// the diagnosable reason label) are the ticket's two metrics.
			g.Expect(metrics).To(ContainSubstring("imageflow_import_duration_seconds"))
			g.Expect(metrics).To(ContainSubstring(`imageflow_import_tasks_total{reason="Succeeded",result="success"}`))
			g.Expect(metrics).To(ContainSubstring(`imageflow_import_tasks_total{reason="ManifestNotFound",result="failure"}`))
			g.Expect(metrics).To(ContainSubstring(`imageflow_import_tasks_total{reason="NetworkUnreachable",result="failure"}`))
			g.Expect(metrics).To(ContainSubstring(`imageflow_import_tasks_total{reason="Unauthorized",result="failure"}`))
			g.Expect(metrics).To(ContainSubstring(`imageflow_import_registrations_total{outcome="changed"}`))
			g.Expect(metrics).To(ContainSubstring(`imageflow_import_registrations_total{outcome="noop"}`))
			g.Expect(metrics).To(ContainSubstring(`imageflow_import_scheduled_creations_total{result="created"}`))
		}, t7Deadline, t7Poll).Should(Succeed())
	})

	// Runs last: it advances the injected clock by more than the success TTL,
	// which would make every earlier schedule immediately due.
	It("t7-ttl-gc-collects-completed-import-tasks-with-the-t4-machinery", func() {
		anonRegistry.MustPutTag(t7Repo, "gc-target", "gc-seed")
		createStream("t7-gc-stream", imageflowv1alpha1.TagReference{Name: t7StreamTag})

		createImport("t7-gc-success", "t7-gc-stream", t7StreamTag,
			anonRegistry.Reference(t7Repo, "gc-target"), "")
		victim := waitImportComplete("t7-gc-success")
		Expect(victim.Status.CompletedAt).NotTo(BeNil())

		assertPresentT7 := func(obj client.Object) {
			Consistently(func(g Gomega) {
				g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
			}, 3*time.Second, 250*time.Millisecond).Should(Succeed())
		}

		By("59 minutes after completion the successful import is retained")
		fakeClk.Advance(59 * time.Minute)
		touch(victim)
		assertPresentT7(victim)

		By("past the 1h success TTL the T4 GC deletes it and audits the kind")
		fakeClk.Advance(2 * time.Minute)
		touch(victim)
		Eventually(func(g Gomega) {
			err := k8s.Get(ctx, importKey("t7-gc-success"), victim)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "want the task collected, got %v", err)
		}, t7Deadline, t7Poll).Should(Succeed())
		Eventually(func(g Gomega) {
			collected := eventsFor(controller.EventReasonTaskGarbageCollected, "t7-gc-success")
			g.Expect(collected).To(HaveLen(1))
			g.Expect(collected[0].Message).To(ContainSubstring("import"))
			g.Expect(collected[0].Message).To(ContainSubstring(imageflowv1alpha1.ReasonSucceeded))
		}, t7Deadline, t7Poll).Should(Succeed())

		By("a failed import survives the success TTL and is collected after the failure TTL")
		createImport("t7-gc-failed", "t7-no-such-stream", t7StreamTag,
			anonRegistry.Reference(t7Repo, "gc-target"), "")
		failed := waitImportComplete("t7-gc-failed")
		Expect(completeMessage(failed)).To(HavePrefix("StreamNotFound:"))

		fakeClk.Advance(2 * time.Hour) // past 1h success TTL, inside the 24h failure TTL
		touch(failed)
		assertPresentT7(failed)

		fakeClk.Advance(23 * time.Hour)
		touch(failed)
		Eventually(func(g Gomega) {
			err := k8s.Get(ctx, importKey("t7-gc-failed"), failed)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "want the task collected, got %v", err)
		}, t7Deadline, t7Poll).Should(Succeed())
	})
})

func ptrBoolT7(v bool) *bool { return &v }
