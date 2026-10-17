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
	"fmt"
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
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
	"github.com/appfun-dev/imageflow/internal/controller"
)

// T4 named cases: Mapping push-path registration against a real API server
// (CRD validation and CEL included), driven by the injected clock (seam S3).
//
// The whole ticket shares ONE envtest environment to keep the suite light:
// the manager-driven specs run inside an inner Ordered container; the manager
// is fully stopped before the crash-recovery spec runs, so that spec drives
// hand-built reconcilers against the API server as the only shared state
// (exactly what a controller restart sees).
//
// Controller names carry the "-it" suffix: controller-runtime validates
// controller names process-globally, and the pre-existing Manager suite in
// this package boots another manager in the same test process.
var _ = Describe("T4 Mapping push path", Label("t4"), Ordered, func() {
	var (
		t4Env      *envtest.Environment
		restCfg    *rest.Config
		k8s        client.Client
		watchCl    client.WithWatch
		ctx        context.Context
		cancel     context.CancelFunc
		ns         string
		fakeClk    *clock.Fake
		mgrCancel  context.CancelFunc
		mgrStopped chan struct{}
		hexA       = strings.Repeat("a", 64)
		hexB       = strings.Repeat("b", 64)
		hexC       = strings.Repeat("c", 64)
		hexD       = strings.Repeat("d", 64)
		repo       = "quay.io/example/payments-api"
		stream     = "payments-api"
		tag        = "stable"
		deadline   = 60 * time.Second
		poll       = 200 * time.Millisecond
	)

	ref := func(hex string) string { return repo + "@sha256:" + hex }
	digest := func(hex string) string { return "sha256:" + hex }

	t4Config := func() config.Config {
		return config.Config{
			MappingTTLSuccess:     time.Hour,
			MappingTTLFailure:     24 * time.Hour,
			StreamTagHistoryLimit: 3,
		}
	}

	BeforeAll(func() {
		assets := os.Getenv("KUBEBUILDER_ASSETS")
		if assets == "" {
			Skip("KUBEBUILDER_ASSETS not set")
		}
		t4Env = &envtest.Environment{
			BinaryAssetsDirectory: assets,
			CRDDirectoryPaths: []string{
				filepath.Join("..", "..", "config", "crd", "bases"),
			},
			ErrorIfCRDPathMissing: true,
		}
		var err error
		restCfg, err = t4Env.Start()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(t4Env.Stop()).To(Succeed())
		})

		scheme := runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(imageflowv1alpha1.AddToScheme(scheme)).To(Succeed())
		k8s, err = client.New(restCfg, client.Options{Scheme: scheme})
		Expect(err).NotTo(HaveOccurred())
		watchCl, err = client.NewWithWatch(restCfg, client.Options{Scheme: scheme})
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel = context.WithCancel(context.Background())
		DeferCleanup(cancel)

		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "t4-mapping-"}}
		Expect(k8s.Create(ctx, namespace)).To(Succeed())
		ns = namespace.Name

		fakeClk = clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	})

	// --- helpers ---------------------------------------------------------

	mappingKey := func(name string) types.NamespacedName {
		return types.NamespacedName{Namespace: ns, Name: name}
	}
	streamKey := func() types.NamespacedName {
		return types.NamespacedName{Namespace: ns, Name: stream}
	}

	createMapping := func(name, targetStream, targetTag, image string) {
		Expect(k8s.Create(ctx, &imageflowv1alpha1.ImageStreamMapping{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Spec: imageflowv1alpha1.ImageStreamMappingSpec{
				Stream: targetStream,
				Tag:    targetTag,
				Image:  image,
			},
		})).To(Succeed())
	}

	waitMappingComplete := func(name string) *imageflowv1alpha1.ImageStreamMapping {
		var done *imageflowv1alpha1.ImageStreamMapping
		Eventually(func(g Gomega) {
			got := &imageflowv1alpha1.ImageStreamMapping{}
			g.Expect(k8s.Get(ctx, mappingKey(name), got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions,
				imageflowv1alpha1.ConditionTypeComplete)
			g.Expect(cond).NotTo(BeNil(), "task %s has no Complete condition yet", name)
			g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			done = got
		}, deadline, poll).Should(Succeed())
		return done
	}

	currentItems := func(targetTag string) []imageflowv1alpha1.TagEvent {
		var got imageflowv1alpha1.ImageStream
		Expect(k8s.Get(ctx, streamKey(), &got)).To(Succeed())
		for _, entry := range got.Status.Tags {
			if entry.Tag == targetTag {
				return entry.Items
			}
		}
		return nil
	}

	digestsOf := func(items []imageflowv1alpha1.TagEvent) []string {
		out := make([]string, 0, len(items))
		for _, it := range items {
			out = append(out, it.Digest)
		}
		return out
	}

	eventsFor := func(reason, involvedName string) []corev1.Event {
		var list corev1.EventList
		Expect(k8s.List(ctx, &list, client.InNamespace(ns))).To(Succeed())
		var out []corev1.Event
		for _, e := range list.Items {
			if e.Reason == reason && e.InvolvedObject.Name == involvedName {
				out = append(out, e)
			}
		}
		return out
	}

	// touch re-triggers reconciliation of a terminal task (the GC requeue timer
	// runs on real time; the injected clock decides the TTL verdict).
	touch := func(obj client.Object) {
		patch := client.MergeFrom(obj.DeepCopyObject().(client.Object))
		if obj.GetAnnotations() == nil {
			obj.SetAnnotations(map[string]string{})
		}
		obj.GetAnnotations()["t4.imageflow.appfun.dev/gc-probe"] =
			fmt.Sprintf("%d", time.Now().UnixNano())
		Expect(k8s.Patch(ctx, obj, patch)).To(Succeed())
	}

	assertGone := func(obj client.Object) {
		Eventually(func(g Gomega) {
			err := k8s.Get(ctx, client.ObjectKeyFromObject(obj), obj)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"object should be deleted, got %v", err)
		}, deadline, poll).Should(Succeed())
	}

	assertPresent := func(obj client.Object) {
		Consistently(func(g Gomega) {
			g.Expect(k8s.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		}, 3*time.Second, 250*time.Millisecond).Should(Succeed())
	}

	// --- manager-driven specs ---------------------------------------------

	Context("with the T4 controllers running in a manager", Ordered, func() {
		BeforeAll(func() {
			scheme := runtime.NewScheme()
			Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
			Expect(imageflowv1alpha1.AddToScheme(scheme)).To(Succeed())
			mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
				Scheme: scheme,
				Metrics: metricsserver.Options{
					BindAddress: "0", // disabled: no port conflicts with the other suites
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(controller.SetupMappingControllers(mgr, controller.Options{
				Clock:  fakeClk,
				Config: t4Config(),
			}, controller.MappingControllerNames{
				Mapping:   "mapping-it",
				GCMapping: "task-gc-mapping-it",
				GCImport:  "task-gc-import-it",
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

			// The stream all registration specs write to (spec-declared tag;
			// the mapping path also accepts undeclared tags, covered by the
			// retag step of the first spec).
			Expect(k8s.Create(ctx, &imageflowv1alpha1.ImageStream{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: stream},
				Spec: imageflowv1alpha1.ImageStreamSpec{
					Tags: []imageflowv1alpha1.TagReference{{Name: tag}},
				},
			})).To(Succeed())
		})

		AfterAll(func() {
			mgrCancel()
			Eventually(mgrStopped, 30*time.Second, 100*time.Millisecond).Should(BeClosed())
		})

		It("t4-registers-pushed-digest-with-artifact-dedup-and-bounded-history", func() {
			By("registering digest A onto (stream, tag)")
			createMapping("push-a", stream, tag, ref(hexA))
			done := waitMappingComplete("push-a")
			cond := meta.FindStatusCondition(done.Status.Conditions,
				imageflowv1alpha1.ConditionTypeComplete)
			Expect(cond.Reason).To(Equal(imageflowv1alpha1.ReasonSucceeded))
			Expect(done.Status.Digest).To(Equal(digest(hexA)))
			Expect(done.Status.ArtifactRef).To(Equal("sha256-" + hexA))
			Expect(done.Status.StartedAt).NotTo(BeNil())
			Expect(done.Status.CompletedAt).NotTo(BeNil())

			By("verifying the stream status association (clock seam + generation)")
			items := currentItems(tag)
			Expect(items).To(HaveLen(1))
			Expect(items[0].Digest).To(Equal(digest(hexA)))
			Expect(items[0].ImageReference).To(Equal(ref(hexA)))
			Expect(items[0].Created.Time.Equal(fakeClk.Now())).To(BeTrue(),
				"association timestamp must come from the injected clock")
			Expect(items[0].Generation).To(BeNumerically(">=", int64(1)))

			By("verifying the content-addressed ImageArtifact")
			artifact := &imageflowv1alpha1.ImageArtifact{}
			Expect(k8s.Get(ctx, types.NamespacedName{Name: "sha256-" + hexA}, artifact)).
				To(Succeed())
			Expect(artifact.Spec.Digest).To(Equal(digest(hexA)))
			Expect(artifact.Spec.ImageReference).To(Equal(ref(hexA)))

			By("verifying the Image Change audit event attributes the Mapping source")
			Eventually(func(g Gomega) {
				changed := eventsFor(controller.EventReasonImageChanged, stream)
				g.Expect(changed).To(HaveLen(1))
				g.Expect(changed[0].Message).To(ContainSubstring("ImageStreamMapping"))
				g.Expect(changed[0].Message).To(ContainSubstring("push-a"))
				g.Expect(changed[0].Message).To(ContainSubstring(digest(hexA)))
			}, deadline, poll).Should(Succeed())
			Eventually(func(g Gomega) {
				g.Expect(eventsFor(controller.EventReasonMappingRegistered, "push-a")).
					To(HaveLen(1))
			}, deadline, poll).Should(Succeed())

			By("proving artifact dedup: a second task with the same digest adds no artifact effects")
			userAnnotated := "notes.example.com/team"
			artifact.Annotations = map[string]string{userAnnotated: "keep-me"}
			Expect(k8s.Update(ctx, artifact)).To(Succeed())
			rvBefore := artifact.ResourceVersion

			fakeClk.Advance(time.Minute)
			createMapping("push-a-retag", stream, "canary", ref(hexA)) // retag = new Mapping
			retag := waitMappingComplete("push-a-retag")
			Expect(retag.Status.ArtifactRef).To(Equal("sha256-" + hexA))
			Expect(currentItems("canary")).To(HaveLen(1))

			reused := &imageflowv1alpha1.ImageArtifact{}
			Expect(k8s.Get(ctx, types.NamespacedName{Name: "sha256-" + hexA}, reused)).
				To(Succeed())
			Expect(reused.ResourceVersion).To(Equal(rvBefore),
				"existing artifact must never be rewritten")
			Expect(reused.Annotations).To(HaveKeyWithValue(userAnnotated, "keep-me"),
				"user annotations on the artifact must survive re-registration")

			var artifacts imageflowv1alpha1.ImageArtifactList
			Expect(k8s.List(ctx, &artifacts)).To(Succeed())
			Expect(artifacts.Items).To(HaveLen(1),
				"same digest must dedup to one cluster-scoped artifact")
		})

		It("t4-repeated-same-tuple-registration-is-noop-success-without-duplicate-image-change", func() {
			var before imageflowv1alpha1.ImageStream
			Expect(k8s.Get(ctx, streamKey(), &before)).To(Succeed())
			rvBefore := before.ResourceVersion
			itemsBefore := currentItems(tag)
			changesBefore := len(eventsFor(controller.EventReasonImageChanged, stream))

			fakeClk.Advance(time.Minute)
			createMapping("push-a-retry", stream, tag, ref(hexA)) // pipeline retry semantics
			done := waitMappingComplete("push-a-retry")

			cond := meta.FindStatusCondition(done.Status.Conditions,
				imageflowv1alpha1.ConditionTypeComplete)
			Expect(cond.Reason).To(Equal(imageflowv1alpha1.ReasonSucceeded),
				"retry must still succeed")
			Expect(cond.Message).To(ContainSubstring("No-op success"))
			Expect(done.Status.Digest).To(Equal(digest(hexA)))

			By("the stream object is not written at all")
			var after imageflowv1alpha1.ImageStream
			Expect(k8s.Get(ctx, streamKey(), &after)).To(Succeed())
			Expect(after.ResourceVersion).To(Equal(rvBefore))
			Expect(currentItems(tag)).To(Equal(itemsBefore))

			By("no duplicate Image Change event is emitted")
			Consistently(func(g Gomega) {
				g.Expect(eventsFor(controller.EventReasonImageChanged, stream)).
					To(HaveLen(changesBefore))
			}, 3*time.Second, 250*time.Millisecond).Should(Succeed())
		})

		It("t4-detects-real-image-change-when-tag-stays-and-digest-changes", func() {
			changesBefore := len(eventsFor(controller.EventReasonImageChanged, stream))

			fakeClk.Advance(time.Minute)
			createMapping("push-b", stream, tag, ref(hexB))
			waitMappingComplete("push-b")

			items := currentItems(tag)
			Expect(digestsOf(items)).To(Equal([]string{digest(hexB), digest(hexA)}),
				"most recent association first")

			Eventually(func(g Gomega) {
				changed := eventsFor(controller.EventReasonImageChanged, stream)
				g.Expect(changed).To(HaveLen(changesBefore + 1))
				newest := changed[len(changed)-1]
				g.Expect(newest.Message).To(ContainSubstring(digest(hexB)))
				g.Expect(newest.Message).To(ContainSubstring("previous: " + digest(hexA)))
				g.Expect(newest.Message).To(ContainSubstring("ImageStreamMapping"))
				g.Expect(newest.Type).To(Equal(corev1.EventTypeNormal))
			}, deadline, poll).Should(Succeed())
		})

		It("t4-bounds-tag-history-to-depth-3-and-spills-overflow-entries-to-events", func() {
			fakeClk.Advance(time.Minute)
			createMapping("push-c", stream, tag, ref(hexC))
			waitMappingComplete("push-c")
			Expect(currentItems(tag)).To(HaveLen(3)) // [C B A] — at the bound, nothing dropped
			Expect(eventsFor(controller.EventReasonTagHistoryOverflow, stream)).To(BeEmpty())

			fakeClk.Advance(time.Minute)
			createMapping("push-d", stream, tag, ref(hexD))
			waitMappingComplete("push-d")

			Expect(digestsOf(currentItems(tag))).To(Equal(
				[]string{digest(hexD), digest(hexC), digest(hexB)}),
				"history stays bounded at depth 3, most recent first")

			Eventually(func(g Gomega) {
				overflow := eventsFor(controller.EventReasonTagHistoryOverflow, stream)
				g.Expect(overflow).To(HaveLen(1))
				g.Expect(overflow[0].Message).To(ContainSubstring(digest(hexA)),
					"the dropped entry must land in Events (ADR-008)")
			}, deadline, poll).Should(Succeed())
		})

		It("t4-ttl-gc-deletes-successful-mapping-after-injected-clock-passes-success-ttl", func() {
			// A fresh victim created at the current injected time keeps the TTL
			// arithmetic relative to its own completedAt, independent of how far
			// previous specs advanced the clock.
			fakeClk.Advance(time.Minute)
			hexF := strings.Repeat("f", 64)
			createMapping("gc-victim-success", stream, tag, ref(hexF))
			victim := waitMappingComplete("gc-victim-success")
			Expect(victim.Status.CompletedAt).NotTo(BeNil())

			By("59 minutes after completion — still retained")
			fakeClk.Advance(59 * time.Minute)
			touch(victim)
			assertPresent(victim)

			By("advancing past the 1h success TTL — collected")
			fakeClk.Advance(2 * time.Minute)
			touch(victim)
			assertGone(victim)

			Eventually(func(g Gomega) {
				gcEvents := eventsFor(controller.EventReasonTaskGarbageCollected,
					"gc-victim-success")
				g.Expect(gcEvents).To(HaveLen(1))
				g.Expect(gcEvents[0].Message).
					To(ContainSubstring(imageflowv1alpha1.ReasonSucceeded))
			}, deadline, poll).Should(Succeed())
		})

		It("t4-ttl-gc-retains-failed-mapping-for-24h-with-diagnosable-reason", func() {
			createMapping("push-missing-stream", "no-such-stream", tag, ref(hexB))
			done := waitMappingComplete("push-missing-stream")
			cond := meta.FindStatusCondition(done.Status.Conditions,
				imageflowv1alpha1.ConditionTypeComplete)
			Expect(cond.Reason).To(Equal(imageflowv1alpha1.ReasonFailed))
			Expect(cond.Message).To(HavePrefix("StreamNotFound:"),
				"diagnosable failure taxonomy")

			Eventually(func(g Gomega) {
				failed := eventsFor(controller.EventReasonMappingFailed, "push-missing-stream")
				g.Expect(failed).To(HaveLen(1))
				g.Expect(failed[0].Type).To(Equal(corev1.EventTypeWarning))
			}, deadline, poll).Should(Succeed())

			By("past the success TTL but inside the failure TTL: retained for diagnosis")
			fakeClk.Advance(2 * time.Hour)
			victim := &imageflowv1alpha1.ImageStreamMapping{}
			Expect(k8s.Get(ctx, mappingKey("push-missing-stream"), victim)).To(Succeed())
			touch(victim)
			assertPresent(victim)

			By("past the 24h failure TTL: collected")
			fakeClk.Advance(23 * time.Hour)
			touch(victim)
			assertGone(victim)
		})

		It("t4-ttl-gc-applies-uniformly-to-the-import-task-kind", func() {
			// The test writes the terminal TaskObjectStatus directly — exactly
			// the surface T7's Import controller will produce. GC must collect
			// it with the same flags and clock seam, without Import-specific
			// code.
			importTask := &imageflowv1alpha1.ImageStreamImport{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "import-gc"},
				Spec: imageflowv1alpha1.ImageStreamImportSpec{
					Stream: stream,
					Tag:    tag,
					From: imageflowv1alpha1.ImageSourceReference{
						Kind: "DockerImage",
						Name: repo + ":stable",
					},
				},
			}
			Expect(k8s.Create(ctx, importTask)).To(Succeed())

			now := fakeClk.Now()
			started := metav1.NewTime(now.Add(-time.Minute))
			completed := metav1.NewTime(now)
			importTask.Status.TaskObjectStatus = imageflowv1alpha1.TaskObjectStatus{
				StartedAt:   &started,
				CompletedAt: &completed,
				Conditions: []metav1.Condition{{
					Type:               imageflowv1alpha1.ConditionTypeComplete,
					Status:             metav1.ConditionTrue,
					Reason:             imageflowv1alpha1.ReasonSucceeded,
					Message:            "imported by test",
					LastTransitionTime: completed,
				}},
			}
			Expect(k8s.Status().Update(ctx, importTask)).To(Succeed())

			assertPresent(importTask) // inside the success TTL: retained

			fakeClk.Advance(time.Hour + time.Minute)
			touch(importTask)
			assertGone(importTask)

			Eventually(func(g Gomega) {
				gcEvents := eventsFor(controller.EventReasonTaskGarbageCollected, "import-gc")
				g.Expect(gcEvents).To(HaveLen(1))
				g.Expect(gcEvents[0].Message).To(ContainSubstring("import"))
			}, deadline, poll).Should(Succeed())
		})
	})

	// --- crash recovery (manager fully stopped; reconcilers built by hand) ---

	It("t4-rebuilds-registration-state-from-persisted-objects-after-crash-without-duplicate-image-change", func() {
		restartStream := "restart-app"
		restartTag := "stable"
		hexR := strings.Repeat("e", 64)
		imageRef := "quay.io/example/restart-app@sha256:" + hexR

		Expect(k8s.Create(ctx, &imageflowv1alpha1.ImageStream{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: restartStream},
		})).To(Succeed())
		mapping := &imageflowv1alpha1.ImageStreamMapping{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "crash-task"},
			Spec: imageflowv1alpha1.ImageStreamMappingSpec{
				Stream: restartStream, Tag: restartTag, Image: imageRef,
			},
		}
		Expect(k8s.Create(ctx, mapping)).To(Succeed())
		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "crash-task"}}
		recoveryClk := clock.NewFake(time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC))

		By("first reconcile: catalog writes land, then the terminal status write crashes")
		crashRecorder := record.NewFakeRecorder(64)
		crashing := interceptor.NewClient(watchCl, interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string,
				obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if m, ok := obj.(*imageflowv1alpha1.ImageStreamMapping); ok && sub == "status" &&
					meta.FindStatusCondition(m.Status.Conditions,
						imageflowv1alpha1.ConditionTypeComplete) != nil {
					// Crash only the terminal write; startedAt must persist
					// first so the recovered task reports a full lifecycle.
					return apierrors.NewInternalError(
						fmt.Errorf("simulated crash before completion"))
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		})
		crashReconciler := controller.NewMappingReconciler(crashing, crashRecorder,
			recoveryClk, t4Config(), nil)
		_, err := crashReconciler.Reconcile(ctx, req)
		Expect(err).To(HaveOccurred(), "the simulated crash must surface as reconcile error")

		By("the catalog side effects are persisted despite the crash")
		var crashedStream imageflowv1alpha1.ImageStream
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: restartStream},
			&crashedStream)).To(Succeed())
		Expect(crashedStream.Status.Tags).To(HaveLen(1))
		Expect(crashedStream.Status.Tags[0].Items).To(HaveLen(1))
		Expect(crashedStream.Status.Tags[0].Items[0].Digest).To(Equal("sha256:" + hexR))
		artifact := &imageflowv1alpha1.ImageArtifact{}
		Expect(k8s.Get(ctx, types.NamespacedName{Name: "sha256-" + hexR}, artifact)).To(Succeed())
		partial := &imageflowv1alpha1.ImageStreamMapping{}
		Expect(k8s.Get(ctx, req.NamespacedName, partial)).To(Succeed())
		Expect(partial.Status.StartedAt).NotTo(BeNil())
		Expect(partial.Status.CompletedAt).To(BeNil(),
			"task must still be non-terminal after the crash")

		By("restart: a fresh reconciler (zero shared memory) converges from persisted state")
		restartRecorder := record.NewFakeRecorder(64)
		restarted := controller.NewMappingReconciler(k8s, restartRecorder,
			recoveryClk, t4Config(), nil)
		res, err := restarted.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(ctrl.Result{}))

		By("the task completes as an idempotent success")
		var done imageflowv1alpha1.ImageStreamMapping
		Expect(k8s.Get(ctx, req.NamespacedName, &done)).To(Succeed())
		cond := meta.FindStatusCondition(done.Status.Conditions,
			imageflowv1alpha1.ConditionTypeComplete)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal(imageflowv1alpha1.ReasonSucceeded))
		Expect(cond.Message).To(ContainSubstring("No-op success"))

		By("exactly one Image Change exists across the crash boundary")
		var afterRestart imageflowv1alpha1.ImageStream
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: restartStream},
			&afterRestart)).To(Succeed())
		Expect(afterRestart.Status.Tags[0].Items).To(HaveLen(1), "no duplicate history entry")
		firstEvents := drainT4FakeEvents(crashRecorder)
		restartEvents := drainT4FakeEvents(restartRecorder)
		Expect(countT4Events(firstEvents, controller.EventReasonImageChanged)).To(Equal(1),
			"the crashed attempt recorded the single Image Change")
		Expect(countT4Events(restartEvents, controller.EventReasonImageChanged)).To(Equal(0),
			"the restarted attempt must not emit a duplicate Image Change")
		Expect(countT4Events(restartEvents, controller.EventReasonMappingRegistered)).To(Equal(1))
	})
})

// drainT4FakeEvents empties a FakeRecorder channel.
func drainT4FakeEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func countT4Events(events []string, reason string) int {
	n := 0
	for _, e := range events {
		if strings.Contains(e, " "+reason+" ") {
			n++
		}
	}
	return n
}
