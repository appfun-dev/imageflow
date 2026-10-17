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
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
	"github.com/appfun-dev/imageflow/internal/controller"
)

// Mirror synthesis controller suite (ticket T5). The controller under test is
// registered under a dedicated name ("mirror-t5") on a minimal manager:
// controller-runtime validates controller names against a process-global set
// and the shared "Manager" container in manager_test.go already builds the
// production "mirror" controller in this same test process (against an
// environment without CRDs, which is harmless: informer setup retries until
// cancellation).
//
// Named cases map to the T5 acceptance criteria: synthesis agreement with the
// source of truth, ownerRef cascade GC, CEL re-verification on
// controller-created mirrors, idempotent synthesis (no write amplification),
// consumer synthesis, and the bounded delivery-event write entry point that
// T11 floats delivery summaries through.
var _ = Describe("Mirror synthesis controller", Ordered, func() {
	var (
		mirrorEnv *envtest.Environment
		k8sClient client.Client
		mgrCancel context.CancelFunc
		ctx       context.Context
		cancel    context.CancelFunc
		ns        string // catalog namespace: streams + mirrors live here
		teamNS    string // consumer namespace: subscriptions live here
	)

	const (
		mirrorTestController = "mirror-t5"
		tagStable            = "stable"
		tagLatest            = "latest"
		tagEdge              = "edge"
		repoT5               = "quay.io/example/t5"
		noteAnnoT5           = "imageflow.appfun.dev/note"
	)

	// digestT5 builds distinct sha256 digests per letter so cluster-scoped
	// ImageArtifacts never collide across specs.
	digestT5 := func(letter string) string {
		return "sha256:" + strings.Repeat(letter, 64)
	}
	refT5 := func(letter string) string {
		return repoT5 + "@" + digestT5(letter)
	}
	artifactNameT5 := func(letter string) string {
		return "sha256-" + strings.Repeat(letter, 64)
	}
	istNameT5 := func(stream string) string {
		return stream + "--" + tagStable
	}
	istLatestNameT5 := func(stream string) string {
		return stream + "--" + tagLatest
	}
	itNameT5 := func(stream, letter string) string {
		return stream + "--sha256-" + strings.Repeat(letter, 64)
	}
	ago := func(d time.Duration) metav1.Time {
		return metav1.NewTime(time.Now().Add(-d))
	}
	history := func(letter string, created metav1.Time) imageflowv1alpha1.TagEvent {
		return imageflowv1alpha1.TagEvent{
			Digest:         digestT5(letter),
			ImageReference: refT5(letter),
			Created:        created,
		}
	}

	createStream := func(name string, tags ...string) *imageflowv1alpha1.ImageStream {
		stream := &imageflowv1alpha1.ImageStream{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		}
		for _, tag := range tags {
			stream.Spec.Tags = append(stream.Spec.Tags, imageflowv1alpha1.TagReference{Name: tag})
		}
		Expect(k8sClient.Create(ctx, stream)).To(Succeed())
		return stream
	}

	// writeStreamStatus simulates the registration path (T4's lane): the
	// mirror controller treats stream status strictly as read-only input.
	writeStreamStatus := func(name string, tags []imageflowv1alpha1.NamedTagEventList) {
		fresh := &imageflowv1alpha1.ImageStream{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, fresh)).To(Succeed())
		fresh.Status.Tags = tags
		Expect(k8sClient.Status().Update(ctx, fresh)).To(Succeed())
	}

	touchStream := func(name string) {
		fresh := &imageflowv1alpha1.ImageStream{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, fresh)).To(Succeed())
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations["imageflow.appfun.dev/t5-touch"] = fmt.Sprint(time.Now().UnixNano())
		Expect(k8sClient.Update(ctx, fresh)).To(Succeed())
	}

	getIST := func(name string) *imageflowv1alpha1.ImageStreamTag {
		ist := &imageflowv1alpha1.ImageStreamTag{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, ist)).To(Succeed())
		return ist
	}

	listMirrorNamesOf := func(streamName string) []string {
		names := []string{}
		ists := &imageflowv1alpha1.ImageStreamTagList{}
		Expect(k8sClient.List(ctx, ists, client.InNamespace(ns))).To(Succeed())
		for i := range ists.Items {
			if ists.Items[i].Spec.Stream == streamName {
				names = append(names, "ImageStreamTag/"+ists.Items[i].Name)
			}
		}
		its := &imageflowv1alpha1.ImageTagList{}
		Expect(k8sClient.List(ctx, its, client.InNamespace(ns))).To(Succeed())
		for i := range its.Items {
			if its.Items[i].Spec.Stream == streamName {
				names = append(names, "ImageTag/"+its.Items[i].Name)
			}
		}
		return names
	}

	assertOwnerRefT5 := func(refs []metav1.OwnerReference, streamName string) {
		ExpectWithOffset(1, refs).To(HaveLen(1))
		ref := refs[0]
		ExpectWithOffset(1, ref.APIVersion).To(Equal(imageflowv1alpha1.GroupVersion.String()))
		ExpectWithOffset(1, ref.Kind).To(Equal("ImageStream"))
		ExpectWithOffset(1, ref.Name).To(Equal(streamName))
		ExpectWithOffset(1, ref.Controller).NotTo(BeNil())
		ExpectWithOffset(1, *ref.Controller).To(BeTrue())
		ExpectWithOffset(1, ref.BlockOwnerDeletion).NotTo(BeNil())
		ExpectWithOffset(1, *ref.BlockOwnerDeletion).To(BeTrue())
	}

	BeforeAll(func() {
		assets := os.Getenv("KUBEBUILDER_ASSETS")
		if assets == "" {
			Skip("KUBEBUILDER_ASSETS not set")
		}

		mirrorEnv = &envtest.Environment{
			BinaryAssetsDirectory: assets,
			CRDDirectoryPaths: []string{
				filepath.Join("..", "..", "config", "crd", "bases"),
			},
			ErrorIfCRDPathMissing: true,
		}
		cfg, err := mirrorEnv.Start()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(mirrorEnv.Stop()).To(Succeed())
		})

		scheme := runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(imageflowv1alpha1.AddToScheme(scheme)).To(Succeed())
		k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
		Expect(err).NotTo(HaveOccurred())

		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:                 scheme,
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(controller.RegisterMirror(mgr, mirrorTestController, controller.Options{
			Clock:  clock.Real{},
			Config: config.Config{MirrorEventLimit: imageflowv1alpha1.MirrorEventMaxItems},
		})).To(Succeed())

		var mgrCtx context.Context
		mgrCtx, mgrCancel = context.WithCancel(context.Background())
		go func() {
			defer GinkgoRecover()
			Expect(mgr.Start(mgrCtx)).To(Succeed())
		}()
		DeferCleanup(mgrCancel)

		ctx, cancel = context.WithCancel(context.Background())
		DeferCleanup(cancel)

		catalog := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "t5-catalog-"}}
		Expect(k8sClient.Create(ctx, catalog)).To(Succeed())
		ns = catalog.Name
		team := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "t5-team-"}}
		Expect(k8sClient.Create(ctx, team)).To(Succeed())
		teamNS = team.Name
	})

	// AC: 登记数据存在时，对应 ImageStreamTag/ImageTag 自动出现，spec/status 与事实源一致。
	It("synthesizes ImageStreamTag and ImageTag mirrors agreeing with the source of truth", func() {
		const streamName = "t5-synth"
		createStream(streamName, tagStable, tagLatest, "declared-only")

		artifact := &imageflowv1alpha1.ImageArtifact{
			ObjectMeta: metav1.ObjectMeta{Name: artifactNameT5("a")},
			Spec: imageflowv1alpha1.ImageArtifactSpec{
				Digest:         digestT5("a"),
				ImageReference: refT5("a"),
			},
		}
		Expect(k8sClient.Create(ctx, artifact)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, artifact))).To(Succeed())
		})

		createdA := ago(2 * time.Hour)
		createdB := ago(4 * time.Hour)
		createdC := ago(time.Hour)
		writeStreamStatus(streamName, []imageflowv1alpha1.NamedTagEventList{
			{Tag: tagStable, Items: []imageflowv1alpha1.TagEvent{history("a", createdA), history("b", createdB)}},
			{Tag: tagLatest, Items: []imageflowv1alpha1.TagEvent{history("c", createdC)}},
			{Tag: "declared-only"}, // declared without association: nothing to mirror yet
		})

		stream := &imageflowv1alpha1.ImageStream{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: streamName}, stream)).To(Succeed())

		By("materializing the ImageStreamTag mirror of the current association")
		var ist *imageflowv1alpha1.ImageStreamTag
		Eventually(func(g Gomega) {
			ist = &imageflowv1alpha1.ImageStreamTag{}
			g.Expect(k8sClient.Get(ctx,
				client.ObjectKey{Namespace: ns, Name: istNameT5(streamName)}, ist)).To(Succeed())
			g.Expect(ist.Status.Image).NotTo(BeNil())
			g.Expect(ist.Status.Image.Digest).To(Equal(digestT5("a")))
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		Expect(ist.Spec.Stream).To(Equal(streamName))
		Expect(ist.Spec.Tag).To(Equal(tagStable))
		assertOwnerRefT5(ist.OwnerReferences, streamName)
		Expect(ist.OwnerReferences[0].UID).To(Equal(stream.UID))
		Expect(ist.Status.ObservedGeneration).To(Equal(stream.Generation))
		Expect(ist.Status.Image.ImageReference).To(Equal(refT5("a")))
		Expect(ist.Status.Image.ArtifactRef).To(Equal(artifactNameT5("a")))
		Expect(ist.Status.Image.Created.Unix()).To(Equal(createdA.Unix()))
		Expect(ist.Status.Events).To(HaveLen(2))
		Expect(ist.Status.Events[0].Type).To(Equal(imageflowv1alpha1.MirrorEventImageChanged))
		Expect(ist.Status.Events[0].NewDigest).To(Equal(digestT5("a")))
		Expect(ist.Status.Events[0].OldDigest).To(Equal(digestT5("b")))
		Expect(ist.Status.Events[1].NewDigest).To(Equal(digestT5("b")))
		Expect(meta.IsStatusConditionTrue(ist.Status.Conditions, "Ready")).To(BeTrue())
		linked := meta.FindStatusCondition(ist.Status.Conditions, "ArtifactLinked")
		Expect(linked).NotTo(BeNil())
		Expect(linked.Status).To(Equal(metav1.ConditionTrue))

		By("tolerating the absent artifact on the latest mirror")
		Eventually(func(g Gomega) {
			latest := &imageflowv1alpha1.ImageStreamTag{}
			g.Expect(k8sClient.Get(ctx,
				client.ObjectKey{Namespace: ns, Name: istLatestNameT5(streamName)}, latest)).To(Succeed())
			g.Expect(latest.Status.Image).NotTo(BeNil())
			g.Expect(latest.Status.Image.Digest).To(Equal(digestT5("c")))
			g.Expect(latest.Status.Image.ArtifactRef).To(BeEmpty())
			absent := meta.FindStatusCondition(latest.Status.Conditions, "ArtifactLinked")
			g.Expect(absent).NotTo(BeNil())
			g.Expect(absent.Status).To(Equal(metav1.ConditionFalse))
			g.Expect(absent.Reason).To(Equal("ArtifactNotFound"))
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		By("materializing one ImageTag mirror per digest in the bounded histories")
		Eventually(func(g Gomega) {
			g.Expect(listMirrorNamesOf(streamName)).To(ConsistOf(
				"ImageStreamTag/"+istNameT5(streamName),
				"ImageStreamTag/"+istLatestNameT5(streamName),
				"ImageTag/"+itNameT5(streamName, "a"),
				"ImageTag/"+itNameT5(streamName, "b"),
				"ImageTag/"+itNameT5(streamName, "c"),
			))
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		it := &imageflowv1alpha1.ImageTag{}
		Expect(k8sClient.Get(ctx,
			client.ObjectKey{Namespace: ns, Name: itNameT5(streamName, "a")}, it)).To(Succeed())
		Expect(it.Spec.Stream).To(Equal(streamName))
		Expect(it.Spec.Digest).To(Equal(digestT5("a")))
		assertOwnerRefT5(it.OwnerReferences, streamName)
		Expect(it.Status.Image.ArtifactRef).To(Equal(artifactNameT5("a")))

		By("never mirroring a declared tag without an association")
		Consistently(func(g Gomega) {
			g.Expect(listMirrorNamesOf(streamName)).To(HaveLen(5))
		}).WithTimeout(2 * time.Second).WithPolling(200 * time.Millisecond).Should(Succeed())
	})

	// AC: 删除 ImageStream → 全部 mirror 对象级联 GC。envtest runs no
	// kube-controller-manager, so the Kubernetes GC itself cannot fire here;
	// this case pins (a) the controlling ownerRef contract that makes the
	// real-cluster cascade work and (b) the controller-side sweep that makes
	// cleanup deterministic everywhere. The kind-e2e named case (real GC
	// cascade) is specified for T12's e2e suite in issue #6.
	It("cascade-deletes every mirror when the ImageStream is deleted", func() {
		const streamName = "t5-gc"
		stream := createStream(streamName, tagStable)
		writeStreamStatus(streamName, []imageflowv1alpha1.NamedTagEventList{
			{Tag: tagStable, Items: []imageflowv1alpha1.TagEvent{history("d", ago(time.Hour))}},
		})

		Eventually(func(g Gomega) {
			g.Expect(listMirrorNamesOf(streamName)).To(HaveLen(2))
			ist := &imageflowv1alpha1.ImageStreamTag{}
			g.Expect(k8sClient.Get(ctx,
				client.ObjectKey{Namespace: ns, Name: istNameT5(streamName)}, ist)).To(Succeed())
			g.Expect(ist.OwnerReferences).To(HaveLen(1))
			ref := ist.OwnerReferences[0]
			g.Expect(ref.Controller).NotTo(BeNil())
			g.Expect(*ref.Controller).To(BeTrue())
			g.Expect(ref.BlockOwnerDeletion).NotTo(BeNil())
			g.Expect(*ref.BlockOwnerDeletion).To(BeTrue())
			g.Expect(ref.Kind).To(Equal("ImageStream"))
			g.Expect(ref.Name).To(Equal(streamName))
			g.Expect(ref.UID).To(Equal(stream.UID))
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		Expect(k8sClient.Delete(ctx, stream)).To(Succeed())

		Eventually(func(g Gomega) {
			g.Expect(listMirrorNamesOf(streamName)).To(BeEmpty())
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	})

	// AC: mirror spec/status 用户写仍被拒（与 T2 CEL 联动复验）—— on a
	// controller-synthesized mirror, with annotations as the sole user surface.
	It("keeps rejecting user spec writes on controller-synthesized mirrors (T2 CEL re-verification)", func() {
		const streamName = "t5-cel"
		createStream(streamName, tagStable)
		writeStreamStatus(streamName, []imageflowv1alpha1.NamedTagEventList{
			{Tag: tagStable, Items: []imageflowv1alpha1.TagEvent{history("e", ago(time.Hour))}},
		})
		Eventually(func(g Gomega) {
			ist := &imageflowv1alpha1.ImageStreamTag{}
			g.Expect(k8sClient.Get(ctx,
				client.ObjectKey{Namespace: ns, Name: istNameT5(streamName)}, ist)).To(Succeed())
			g.Expect(ist.Status.Image).NotTo(BeNil())
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		ist := getIST(istNameT5(streamName))
		ist.Spec.Tag = "hijacked"
		err := k8sClient.Update(ctx, ist)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "CEL rejection must surface as Invalid")
		Expect(err.Error()).To(ContainSubstring("controller-owned mirror object"))

		after := getIST(istNameT5(streamName))
		Expect(after.Spec.Tag).To(Equal(tagStable))

		after.Annotations = map[string]string{noteAnnoT5: "user bookkeeping"}
		Expect(k8sClient.Update(ctx, after)).To(Succeed())
		final := getIST(istNameT5(streamName))
		Expect(final.Annotations).To(HaveKeyWithValue(noteAnnoT5, "user bookkeeping"))
		Expect(final.Spec.Tag).To(Equal(tagStable))
	})

	// AC: 合成回路幂等：重复 reconcile 不产生更新风暴（无写放大）。
	It("synthesizes idempotently without write amplification on repeated reconciles", func() {
		const streamName = "t5-idem"
		createStream(streamName, tagStable)
		writeStreamStatus(streamName, []imageflowv1alpha1.NamedTagEventList{
			{Tag: tagStable, Items: []imageflowv1alpha1.TagEvent{
				history("f", ago(2*time.Hour)), history("9", ago(3*time.Hour)),
			}},
		})
		Eventually(func(g Gomega) {
			g.Expect(listMirrorNamesOf(streamName)).To(HaveLen(3))
			ist := &imageflowv1alpha1.ImageStreamTag{}
			g.Expect(k8sClient.Get(ctx,
				client.ObjectKey{Namespace: ns, Name: istNameT5(streamName)}, ist)).To(Succeed())
			g.Expect(ist.Status.Image).NotTo(BeNil())
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		// Let every mirror settle, then snapshot resourceVersions.
		snapshot := func() map[string]string {
			rvs := map[string]string{}
			ists := &imageflowv1alpha1.ImageStreamTagList{}
			Expect(k8sClient.List(ctx, ists, client.InNamespace(ns))).To(Succeed())
			for i := range ists.Items {
				rvs["ist/"+ists.Items[i].Name] = ists.Items[i].ResourceVersion
			}
			its := &imageflowv1alpha1.ImageTagList{}
			Expect(k8sClient.List(ctx, its, client.InNamespace(ns))).To(Succeed())
			for i := range its.Items {
				rvs["it/"+its.Items[i].Name] = its.Items[i].ResourceVersion
			}
			return rvs
		}
		var previous map[string]string
		Eventually(func(g Gomega) {
			current := snapshot()
			// The deferred store also runs when the g assertion panics, so
			// every poll advances the baseline; two consecutive equal
			// snapshots mean the synthesis loop has quiesced.
			defer func() { previous = current }()
			g.Expect(previous).NotTo(BeNil())
			g.Expect(current).To(Equal(previous))
		}).WithTimeout(20 * time.Second).WithPolling(250 * time.Millisecond).Should(Succeed())
		settled := previous

		// Trigger a fresh reconcile pass on the stream; nothing may be rewritten.
		touchStream(streamName)
		Consistently(func(g Gomega) {
			g.Expect(snapshot()).To(Equal(settled))
		}).WithTimeout(3 * time.Second).WithPolling(250 * time.Millisecond).Should(Succeed())
	})

	// AC: consumers[] 字段形态可用（read-only synthesis from subscriptions）。
	It("floats subscription consumers into mirror status read-only", func() {
		const streamName = "t5-cons"
		createStream(streamName, tagStable)
		writeStreamStatus(streamName, []imageflowv1alpha1.NamedTagEventList{
			{Tag: tagStable, Items: []imageflowv1alpha1.TagEvent{history("8", ago(time.Hour))}},
		})
		Eventually(func(g Gomega) {
			ist := &imageflowv1alpha1.ImageStreamTag{}
			g.Expect(k8sClient.Get(ctx,
				client.ObjectKey{Namespace: ns, Name: istNameT5(streamName)}, ist)).To(Succeed())
			g.Expect(ist.Status.Image).NotTo(BeNil())
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		sub := &imageflowv1alpha1.ImageSubscription{
			ObjectMeta: metav1.ObjectMeta{Namespace: teamNS, Name: "t5-consumer"},
			Spec: imageflowv1alpha1.ImageSubscriptionSpec{
				Source: imageflowv1alpha1.SubscriptionSourceRef{
					Kind:      "ImageStreamTag",
					Name:      streamName + ":" + tagStable,
					Namespace: ns,
				},
				Targets: []imageflowv1alpha1.SubscriptionTarget{{
					TargetRef: imageflowv1alpha1.TargetReference{
						APIVersion: "apps/v1",
						Kind:       "Deployment",
						Namespace:  teamNS,
						Name:       "t5-workload",
					},
					ContainerNames: []string{"app"},
				}},
			},
		}
		Expect(k8sClient.Create(ctx, sub)).To(Succeed())

		want := []imageflowv1alpha1.ConsumerReference{{Namespace: teamNS, Name: "t5-consumer"}}
		Eventually(func(g Gomega) {
			ist := &imageflowv1alpha1.ImageStreamTag{}
			g.Expect(k8sClient.Get(ctx,
				client.ObjectKey{Namespace: ns, Name: istNameT5(streamName)}, ist)).To(Succeed())
			g.Expect(ist.Status.Consumers).To(Equal(want))
			it := &imageflowv1alpha1.ImageTag{}
			g.Expect(k8sClient.Get(ctx,
				client.ObjectKey{Namespace: ns, Name: itNameT5(streamName, "8")}, it)).To(Succeed())
			g.Expect(it.Status.Consumers).To(Equal(want))
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		Expect(k8sClient.Delete(ctx, sub)).To(Succeed())
		Eventually(func(g Gomega) {
			ist := &imageflowv1alpha1.ImageStreamTag{}
			g.Expect(k8sClient.Get(ctx,
				client.ObjectKey{Namespace: ns, Name: istNameT5(streamName)}, ist)).To(Succeed())
			g.Expect(ist.Status.Consumers).To(BeEmpty())
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	})

	// AC: 变化历史、投递事件摘要（有界 5）字段形态可用；合成器为 T11 填充
	// 留好写入口 —— Delivery entries written directly to the mirror status
	// (T11's path) survive re-synthesis, newest-first, bounded.
	It("preserves delivery events written by the subscription controller and bounds the summary", func() {
		const streamName = "t5-deliv"
		createStream(streamName, tagStable)
		writeStreamStatus(streamName, []imageflowv1alpha1.NamedTagEventList{
			{Tag: tagStable, Items: []imageflowv1alpha1.TagEvent{
				history("7", ago(time.Hour)), history("6", ago(2*time.Hour)), history("5", ago(3*time.Hour)),
			}},
		})
		Eventually(func(g Gomega) {
			ist := &imageflowv1alpha1.ImageStreamTag{}
			g.Expect(k8sClient.Get(ctx,
				client.ObjectKey{Namespace: ns, Name: istNameT5(streamName)}, ist)).To(Succeed())
			g.Expect(ist.Status.Events).To(HaveLen(imageflowv1alpha1.MirrorEventMaxItems - 2))
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		By("simulating T11: delivery summaries appended through the status subresource")
		ist := getIST(istNameT5(streamName))
		delivery := func(offset time.Duration, name string) imageflowv1alpha1.MirrorStatusEvent {
			return imageflowv1alpha1.MirrorStatusEvent{
				Type:       imageflowv1alpha1.MirrorEventDelivery,
				Reason:     "Applied",
				Message:    "delivery floated up by the subscription controller",
				ObservedAt: metav1.NewTime(time.Now().Add(offset)),
				NewDigest:  digestT5("7"),
				Result:     imageflowv1alpha1.RolloutSucceeded,
				Subscription: &imageflowv1alpha1.SubscriptionObjectRef{
					Namespace: teamNS, Name: name,
				},
			}
		}
		// Stored window (cap 5): 4 deliveries + the newest ImageChanged entry.
		ist.Status.Events = []imageflowv1alpha1.MirrorStatusEvent{
			delivery(4*time.Minute, "d4"), delivery(3*time.Minute, "d3"),
			delivery(2*time.Minute, "d2"), delivery(time.Minute, "d1"),
			ist.Status.Events[0],
		}
		Expect(k8sClient.Status().Update(ctx, ist)).To(Succeed())

		By("re-synthesizing on a stream touch: deliveries survive, window stays bounded")
		touchStream(streamName)
		Eventually(func(g Gomega) {
			merged := &imageflowv1alpha1.ImageStreamTag{}
			g.Expect(k8sClient.Get(ctx,
				client.ObjectKey{Namespace: ns, Name: istNameT5(streamName)}, merged)).To(Succeed())
			g.Expect(merged.Status.Events).To(HaveLen(imageflowv1alpha1.MirrorEventMaxItems))
			for i, wantSub := range []string{"d4", "d3", "d2", "d1"} {
				g.Expect(merged.Status.Events[i].Type).To(Equal(imageflowv1alpha1.MirrorEventDelivery))
				g.Expect(merged.Status.Events[i].Subscription.Name).To(Equal(wantSub))
			}
			// The newest recomputed ImageChanged entry keeps the 5th slot;
			// the two older ones left the bounded window.
			g.Expect(merged.Status.Events[4].Type).To(Equal(imageflowv1alpha1.MirrorEventImageChanged))
			g.Expect(merged.Status.Events[4].NewDigest).To(Equal(digestT5("7")))
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	})

	// Artifact linkage reacts to the registration path (T4's lane) through the
	// ImageArtifact watch, without any stream-side event.
	It("links artifactRef when the ImageArtifact appears and mirrors status pruning", func() {
		const streamName = "t5-art"
		createStream(streamName, tagStable, tagEdge)
		writeStreamStatus(streamName, []imageflowv1alpha1.NamedTagEventList{
			{Tag: tagStable, Items: []imageflowv1alpha1.TagEvent{history("4", ago(time.Hour))}},
			{Tag: tagEdge, Items: []imageflowv1alpha1.TagEvent{history("3", ago(time.Hour))}},
		})
		Eventually(func(g Gomega) {
			ist := &imageflowv1alpha1.ImageStreamTag{}
			g.Expect(k8sClient.Get(ctx,
				client.ObjectKey{Namespace: ns, Name: istNameT5(streamName)}, ist)).To(Succeed())
			g.Expect(ist.Status.Image).NotTo(BeNil())
			g.Expect(ist.Status.Image.ArtifactRef).To(BeEmpty())
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())

		artifact := &imageflowv1alpha1.ImageArtifact{
			ObjectMeta: metav1.ObjectMeta{Name: artifactNameT5("4")},
			Spec:       imageflowv1alpha1.ImageArtifactSpec{Digest: digestT5("4")},
		}
		Expect(k8sClient.Create(ctx, artifact)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, artifact))).To(Succeed())
		})

		Eventually(func(g Gomega) {
			ist := &imageflowv1alpha1.ImageStreamTag{}
			g.Expect(k8sClient.Get(ctx,
				client.ObjectKey{Namespace: ns, Name: istNameT5(streamName)}, ist)).To(Succeed())
			g.Expect(ist.Status.Image.ArtifactRef).To(Equal(artifactNameT5("4")))
			linked := meta.FindStatusCondition(ist.Status.Conditions, "ArtifactLinked")
			g.Expect(linked).NotTo(BeNil())
			g.Expect(linked.Status).To(Equal(metav1.ConditionTrue))
		}).WithTimeout(20*time.Second).WithPolling(100*time.Millisecond).
			Should(Succeed(), "the ImageArtifact watch must re-trigger synthesis")

		By("pruning mirrors whose association left the stream status")
		writeStreamStatus(streamName, []imageflowv1alpha1.NamedTagEventList{
			{Tag: tagStable, Items: []imageflowv1alpha1.TagEvent{history("4", ago(time.Hour))}},
		})
		Eventually(func(g Gomega) {
			g.Expect(listMirrorNamesOf(streamName)).To(ConsistOf(
				"ImageStreamTag/"+istNameT5(streamName),
				"ImageTag/"+itNameT5(streamName, "4"),
			))
		}).WithTimeout(20 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	})
})
