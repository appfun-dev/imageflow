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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
)

// Catalog CRD validation suite (ticket T2). No webhook is installed in this
// environment on purpose: every write-protection and shape assertion below must
// hold on CRD-level CEL / structural schema alone (spec decision 6: mirror and
// ImageArtifact protection must not depend on webhook availability).
var _ = Describe("Catalog CRD model", Ordered, func() {
	var (
		catalogEnv *envtest.Environment
		k8sClient  client.Client
		ctx        context.Context
		cancel     context.CancelFunc
		ns         string
	)

	const (
		appName     = "payments-api"
		appTag      = "stable"
		registryRef = "quay.io/example/payments-api"
		sourceKind  = "ImageStreamTag"
		sourceName  = appName + ":" + appTag
		noteAnno    = "imageflow.appfun.dev/note"
	)

	hexA := strings.Repeat("a", 64)
	digestA := "sha256:" + hexA
	digestB := "sha256:" + strings.Repeat("b", 64)
	digestC := "sha256:" + strings.Repeat("c", 64)
	refA := registryRef + "@" + digestA
	refB := registryRef + "@" + digestB
	refC := registryRef + "@" + digestC
	artifactName := "sha256-" + hexA
	istMirrorName := appName + "--" + appTag
	itMirrorName := appName + "--sha256-" + hexA

	istMirrorKey := func() client.ObjectKey {
		return client.ObjectKey{Namespace: ns, Name: istMirrorName}
	}
	artifactKey := func() client.ObjectKey {
		return client.ObjectKey{Name: artifactName}
	}
	streamKey := func() client.ObjectKey {
		return client.ObjectKey{Namespace: ns, Name: appName}
	}
	appTargetRef := func() imageflowv1alpha1.TargetReference {
		return imageflowv1alpha1.TargetReference{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
			Namespace:  ns,
			Name:       appName,
		}
	}
	tagEvent := func(digest, ref string) imageflowv1alpha1.TagEvent {
		return imageflowv1alpha1.TagEvent{
			Digest:         digest,
			ImageReference: ref,
			Created:        metav1.Now(),
		}
	}
	mirrorEvent := func() imageflowv1alpha1.MirrorStatusEvent {
		return imageflowv1alpha1.MirrorStatusEvent{
			Type:       imageflowv1alpha1.MirrorEventImageChanged,
			OldDigest:  digestA,
			NewDigest:  digestB,
			ObservedAt: metav1.Now(),
		}
	}
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
	delivery := func(ref string) imageflowv1alpha1.DeliveryResult {
		return imageflowv1alpha1.DeliveryResult{
			Source: imageflowv1alpha1.SubscriptionSourceRef{
				Kind:      sourceKind,
				Name:      sourceName,
				Namespace: ns,
			},
			NewImage:   ref,
			TargetRef:  appTargetRef(),
			ObservedAt: metav1.Now(),
		}
	}

	BeforeAll(func() {
		assets := os.Getenv("KUBEBUILDER_ASSETS")
		if assets == "" {
			Skip("KUBEBUILDER_ASSETS not set")
		}

		catalogEnv = &envtest.Environment{
			BinaryAssetsDirectory: assets,
			CRDDirectoryPaths: []string{
				filepath.Join("..", "..", "config", "crd", "bases"),
			},
			ErrorIfCRDPathMissing: true,
		}
		cfg, err := catalogEnv.Start()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(catalogEnv.Stop()).To(Succeed())
		})

		scheme := runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(imageflowv1alpha1.AddToScheme(scheme)).To(Succeed())
		k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel = context.WithCancel(context.Background())
		DeferCleanup(cancel)

		namespace := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "catalog-"},
		}
		Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
		ns = namespace.Name
	})

	It("creates and reads back all seven catalog kinds through the API", func() {
		By("creating the ImageStream single source of truth")
		stream := &imageflowv1alpha1.ImageStream{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: appName},
			Spec: imageflowv1alpha1.ImageStreamSpec{
				Tags: []imageflowv1alpha1.TagReference{
					{
						Name: appTag,
						From: &imageflowv1alpha1.ImageSourceReference{
							Name: registryRef + ":" + appTag,
						},
						ImportPolicy: imageflowv1alpha1.TagImportPolicy{
							Scheduled:       ptr.To(true),
							IntervalSeconds: ptr.To(int32(900)),
						},
					},
					{
						Name:         "latest",
						ImportPolicy: imageflowv1alpha1.TagImportPolicy{},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, stream)).To(Succeed())

		gotStream := &imageflowv1alpha1.ImageStream{}
		Expect(k8sClient.Get(ctx, streamKey(), gotStream)).To(Succeed())
		Expect(gotStream.Spec.Tags).To(HaveLen(2))
		stable := gotStream.Spec.Tags[0]
		Expect(stable.From).NotTo(BeNil())
		Expect(stable.From.Kind).To(Equal("DockerImage"), "from.kind must default to DockerImage")
		Expect(stable.ImportPolicy.Scheduled).To(Equal(ptr.To(true)))
		Expect(stable.ImportPolicy.IntervalSeconds).To(Equal(ptr.To(int32(900))))
		latest := gotStream.Spec.Tags[1]
		Expect(latest.ImportPolicy.Scheduled).To(Equal(ptr.To(false)),
			"importPolicy.scheduled must default to false")
		Expect(latest.ImportPolicy.IntervalSeconds).To(Equal(ptr.To(int32(900))),
			"importPolicy.intervalSeconds must default to 900 (ADR-004)")

		By("creating the cluster-scoped ImageArtifact")
		artifact := &imageflowv1alpha1.ImageArtifact{
			ObjectMeta: metav1.ObjectMeta{Name: artifactName},
			Spec: imageflowv1alpha1.ImageArtifactSpec{
				Digest:         digestA,
				ImageReference: refA,
				SizeBytes:      ptr.To(int64(2048)),
			},
		}
		Expect(k8sClient.Create(ctx, artifact)).To(Succeed())
		gotArtifact := &imageflowv1alpha1.ImageArtifact{}
		Expect(k8sClient.Get(ctx, artifactKey(), gotArtifact)).To(Succeed())
		Expect(gotArtifact.Spec.Digest).To(Equal(digestA))

		By("creating the ImageSubscription")
		sub := newSubscription("payments-stable")
		Expect(k8sClient.Create(ctx, sub)).To(Succeed())
		gotSub := &imageflowv1alpha1.ImageSubscription{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sub), gotSub)).To(Succeed())
		Expect(gotSub.Spec.Source.Name).To(Equal(sourceName))
		Expect(gotSub.Spec.Targets).To(HaveLen(1))

		By("creating both mirror views")
		istMirror := &imageflowv1alpha1.ImageStreamTag{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: istMirrorName},
			Spec: imageflowv1alpha1.ImageStreamTagSpec{
				Stream: appName,
				Tag:    appTag,
			},
		}
		Expect(k8sClient.Create(ctx, istMirror)).To(Succeed())
		itMirror := &imageflowv1alpha1.ImageTag{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: itMirrorName},
			Spec: imageflowv1alpha1.ImageTagSpec{
				Stream: appName,
				Digest: digestA,
			},
		}
		Expect(k8sClient.Create(ctx, itMirror)).To(Succeed())

		By("creating both registration task kinds")
		imp := &imageflowv1alpha1.ImageStreamImport{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "import-stable-1"},
			Spec: imageflowv1alpha1.ImageStreamImportSpec{
				Stream: appName,
				Tag:    appTag,
				From:   imageflowv1alpha1.ImageSourceReference{Name: registryRef + ":" + appTag},
			},
		}
		Expect(k8sClient.Create(ctx, imp)).To(Succeed())
		gotImp := &imageflowv1alpha1.ImageStreamImport{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(imp), gotImp)).To(Succeed())
		Expect(gotImp.Spec.From.Kind).To(Equal("DockerImage"))

		mapping := &imageflowv1alpha1.ImageStreamMapping{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "mapping-stable-1"},
			Spec: imageflowv1alpha1.ImageStreamMappingSpec{
				Stream: appName,
				Tag:    appTag,
				Image:  refA,
			},
		}
		Expect(k8sClient.Create(ctx, mapping)).To(Succeed())
		gotMapping := &imageflowv1alpha1.ImageStreamMapping{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(mapping), gotMapping)).To(Succeed())
		Expect(gotMapping.Spec.Image).To(Equal(refA))

		By("listing the mirror kinds for kubectl get/watch parity")
		istList := &imageflowv1alpha1.ImageStreamTagList{}
		Expect(k8sClient.List(ctx, istList, client.InNamespace(ns))).To(Succeed())
		Expect(istList.Items).To(HaveLen(1))
		itList := &imageflowv1alpha1.ImageTagList{}
		Expect(k8sClient.List(ctx, itList, client.InNamespace(ns))).To(Succeed())
		Expect(itList.Items).To(HaveLen(1))
	})

	It("rejects user writes to ImageStreamTag spec via CEL without any webhook", func() {
		mirror := &imageflowv1alpha1.ImageStreamTag{}
		Expect(k8sClient.Get(ctx, istMirrorKey(), mirror)).To(Succeed())
		mirror.Spec.Tag = "hijacked"

		err := k8sClient.Update(ctx, mirror)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "CEL rejection must surface as Invalid")
		Expect(err.Error()).To(ContainSubstring("controller-owned mirror object"))

		after := &imageflowv1alpha1.ImageStreamTag{}
		Expect(k8sClient.Get(ctx, istMirrorKey(), after)).To(Succeed())
		Expect(after.Spec.Tag).To(Equal(appTag))
	})

	It("rejects user writes to ImageTag spec via CEL without any webhook", func() {
		key := client.ObjectKey{Namespace: ns, Name: itMirrorName}
		mirror := &imageflowv1alpha1.ImageTag{}
		Expect(k8sClient.Get(ctx, key, mirror)).To(Succeed())
		mirror.Spec.Digest = digestB

		err := k8sClient.Update(ctx, mirror)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "CEL rejection must surface as Invalid")
		Expect(err.Error()).To(ContainSubstring("controller-owned mirror object"))

		after := &imageflowv1alpha1.ImageTag{}
		Expect(k8sClient.Get(ctx, key, after)).To(Succeed())
		Expect(after.Spec.Digest).To(Equal(digestA))
	})

	It("makes mirror status unwritable through the main resource endpoint", func() {
		mirror := &imageflowv1alpha1.ImageStreamTag{}
		Expect(k8sClient.Get(ctx, istMirrorKey(), mirror)).To(Succeed())
		mirror.Status.Image = &imageflowv1alpha1.MirrorImage{Digest: digestB}
		mirror.Status.Events = []imageflowv1alpha1.MirrorStatusEvent{mirrorEvent()}

		// The main endpoint accepts the write but strips status (subresource split):
		// a user cannot smuggle mirror status in through a whole-object update.
		Expect(k8sClient.Update(ctx, mirror)).To(Succeed())

		after := &imageflowv1alpha1.ImageStreamTag{}
		Expect(k8sClient.Get(ctx, istMirrorKey(), after)).To(Succeed())
		Expect(after.Status.Image).To(BeNil())
		Expect(after.Status.Events).To(BeEmpty())
	})

	It("allows annotation-only updates on mirror objects as the sole user surface", func() {
		mirror := &imageflowv1alpha1.ImageStreamTag{}
		Expect(k8sClient.Get(ctx, istMirrorKey(), mirror)).To(Succeed())
		mirror.Annotations = map[string]string{noteAnno: "kept for humans"}
		Expect(k8sClient.Update(ctx, mirror)).To(Succeed())

		after := &imageflowv1alpha1.ImageStreamTag{}
		Expect(k8sClient.Get(ctx, istMirrorKey(), after)).To(Succeed())
		Expect(after.Annotations).To(HaveKeyWithValue(noteAnno, "kept for humans"))
		Expect(after.Spec.Tag).To(Equal(appTag))
	})

	It("rejects ImageArtifact user writes except metadata.annotations via CEL", func() {
		By("rejecting a spec write")
		artifact := &imageflowv1alpha1.ImageArtifact{}
		Expect(k8sClient.Get(ctx, artifactKey(), artifact)).To(Succeed())
		artifact.Spec.SizeBytes = ptr.To(int64(1))
		err := k8sClient.Update(ctx, artifact)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "CEL rejection must surface as Invalid")
		Expect(err.Error()).To(ContainSubstring("controller-owned and immutable"))

		By("ignoring a status write through the main endpoint")
		Expect(k8sClient.Get(ctx, artifactKey(), artifact)).To(Succeed())
		artifact.Status.Conditions = []metav1.Condition{{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			Reason:             "UserSmuggled",
			LastTransitionTime: metav1.Now(),
		}}
		Expect(k8sClient.Update(ctx, artifact)).To(Succeed())
		after := &imageflowv1alpha1.ImageArtifact{}
		Expect(k8sClient.Get(ctx, artifactKey(), after)).To(Succeed())
		Expect(after.Status.Conditions).To(BeEmpty())

		By("accepting an annotation-only write")
		after.Annotations = map[string]string{noteAnno: "org bookkeeping"}
		Expect(k8sClient.Update(ctx, after)).To(Succeed())
		final := &imageflowv1alpha1.ImageArtifact{}
		Expect(k8sClient.Get(ctx, artifactKey(), final)).To(Succeed())
		Expect(final.Annotations).To(HaveKeyWithValue(noteAnno, "org bookkeeping"))
		Expect(final.Spec.Digest).To(Equal(digestA))
		Expect(final.Spec.SizeBytes).To(Equal(ptr.To(int64(2048))))
	})

	It("enforces the bounded stream tag history shape of 3 items per tag", func() {
		stream := &imageflowv1alpha1.ImageStream{}
		Expect(k8sClient.Get(ctx, streamKey(), stream)).To(Succeed())
		stream.Status.Tags = []imageflowv1alpha1.NamedTagEventList{{
			Tag: appTag,
			Items: []imageflowv1alpha1.TagEvent{
				tagEvent(digestA, refA), tagEvent(digestB, refB), tagEvent(digestC, refC),
			},
		}}
		Expect(k8sClient.Status().Update(ctx, stream)).To(Succeed())

		updated := &imageflowv1alpha1.ImageStream{}
		Expect(k8sClient.Get(ctx, streamKey(), updated)).To(Succeed())
		Expect(updated.Status.Tags).To(HaveLen(1))
		Expect(updated.Status.Tags[0].Items).To(HaveLen(imageflowv1alpha1.StreamTagHistoryMaxItems))

		updated.Status.Tags[0].Items = append(updated.Status.Tags[0].Items, tagEvent(digestA, refA))
		err := k8sClient.Status().Update(ctx, updated)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("at most 3"))
	})

	It("rejects malformed digests in stream tag history items", func() {
		stream := &imageflowv1alpha1.ImageStream{}
		Expect(k8sClient.Get(ctx, streamKey(), stream)).To(Succeed())
		stream.Status.Tags = []imageflowv1alpha1.NamedTagEventList{{
			Tag: appTag,
			Items: []imageflowv1alpha1.TagEvent{{Digest: "sha256:zzz", ImageReference: refA,
				Created: metav1.Now()}},
		}}
		err := k8sClient.Status().Update(ctx, stream)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("digest"))
	})

	It("enforces the bounded subscription delivery shape of 5 recent results", func() {
		sub := newSubscription("bounded-deliveries")
		Expect(k8sClient.Create(ctx, sub)).To(Succeed())

		sub.Status.RecentDeliveries = make([]imageflowv1alpha1.DeliveryResult, 0, 6)
		for range imageflowv1alpha1.SubscriptionDeliveryMaxItems {
			sub.Status.RecentDeliveries = append(sub.Status.RecentDeliveries, delivery(refA))
		}
		Expect(k8sClient.Status().Update(ctx, sub)).To(Succeed())

		sub.Status.RecentDeliveries = append(sub.Status.RecentDeliveries, delivery(refB))
		err := k8sClient.Status().Update(ctx, sub)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("at most 5"))
	})

	It("enforces the bounded mirror event shape of 5 summaries", func() {
		mirror := &imageflowv1alpha1.ImageStreamTag{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: appName + "--bounded"},
			Spec:       imageflowv1alpha1.ImageStreamTagSpec{Stream: appName, Tag: "bounded"},
		}
		Expect(k8sClient.Create(ctx, mirror)).To(Succeed())

		mirror.Status.Events = make([]imageflowv1alpha1.MirrorStatusEvent, 0, 6)
		for range imageflowv1alpha1.MirrorEventMaxItems {
			mirror.Status.Events = append(mirror.Status.Events, mirrorEvent())
		}
		Expect(k8sClient.Status().Update(ctx, mirror)).To(Succeed())

		mirror.Status.Events = append(mirror.Status.Events, mirrorEvent())
		err := k8sClient.Status().Update(ctx, mirror)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("at most 5"))
	})

	It("rejects subscription sources other than ImageStreamTag", func() {
		sub := newSubscription("bad-source-kind")
		sub.Spec.Source.Kind = "ImageTag"
		err := k8sClient.Create(ctx, sub)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue())
	})

	It("rejects ImageStreamMapping registrations without a digest reference", func() {
		mapping := &imageflowv1alpha1.ImageStreamMapping{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "mapping-tag-only"},
			Spec: imageflowv1alpha1.ImageStreamMappingSpec{
				Stream: appName,
				Tag:    appTag,
				Image:  registryRef + ":" + appTag,
			},
		}
		err := k8sClient.Create(ctx, mapping)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(),
			"tag-only registrations cannot produce an Image Change (ADR-004)")
	})

	It("keeps stream spec user-writable but rejects scheduled imports without a source", func() {
		stream := &imageflowv1alpha1.ImageStream{}
		Expect(k8sClient.Get(ctx, streamKey(), stream)).To(Succeed())
		before := len(stream.Spec.Tags)
		stream.Spec.Tags = append(stream.Spec.Tags, imageflowv1alpha1.TagReference{
			Name:         "drift",
			ImportPolicy: imageflowv1alpha1.TagImportPolicy{Scheduled: ptr.To(true)},
		})
		err := k8sClient.Update(ctx, stream)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("from is required"))

		// Users keep full spec ownership on streams: with a source it is accepted.
		Expect(k8sClient.Get(ctx, streamKey(), stream)).To(Succeed())
		stream.Spec.Tags = append(stream.Spec.Tags, imageflowv1alpha1.TagReference{
			Name: "drift",
			From: &imageflowv1alpha1.ImageSourceReference{Name: registryRef + ":drift"},
			ImportPolicy: imageflowv1alpha1.TagImportPolicy{
				Scheduled: ptr.To(true), IntervalSeconds: ptr.To(int32(60)),
			},
		})
		Expect(k8sClient.Update(ctx, stream)).To(Succeed())
		after := &imageflowv1alpha1.ImageStream{}
		Expect(k8sClient.Get(ctx, streamKey(), after)).To(Succeed())
		Expect(after.Spec.Tags).To(HaveLen(before + 1))
	})

	It("documents the webhook boundary: the CRD shape allows both locator fields", func() {
		// Spec decision 6 assigns containerNames/imageFields mutual exclusion, the
		// imageFields grammar and path-overlap detection to the T3 validating
		// webhook. T2 deliberately adds no CEL XOR here so those webhook-owned
		// named cases stay testable; this case pins the division of enforcement.
		sub := newSubscription("both-locators")
		sub.Spec.Targets[0].ImageFields = []string{
			".spec.template.spec.containers[name=edge].image",
		}
		Expect(k8sClient.Create(ctx, sub)).To(Succeed())
	})

	It("rejects rollout and health values outside the frozen vocabulary", func() {
		sub := newSubscription("vocabulary")
		Expect(k8sClient.Create(ctx, sub)).To(Succeed())

		sub.Status.Targets = []imageflowv1alpha1.TargetDeliveryStatus{{
			TargetRef:     appTargetRef(),
			RolloutStatus: "Bogus",
		}}
		err := k8sClient.Status().Update(ctx, sub)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue())

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sub), sub)).To(Succeed())
		sub.Status.Targets = []imageflowv1alpha1.TargetDeliveryStatus{{
			TargetRef:    appTargetRef(),
			HealthStatus: "Bogus",
		}}
		err = k8sClient.Status().Update(ctx, sub)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue())
	})
})
