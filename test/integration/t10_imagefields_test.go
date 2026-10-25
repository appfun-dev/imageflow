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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
	"github.com/appfun-dev/imageflow/internal/controller"
	"github.com/appfun-dev/imageflow/internal/delivery"
	"github.com/appfun-dev/imageflow/internal/imagefields"
	iwebhook "github.com/appfun-dev/imageflow/internal/webhook"
)

// T10 named cases: the generic image field resolver driving third-party
// CRD targets (EdgeService, apps.thirdparty.test/v1alpha1) through the
// SAME delivery engine as the built-in kinds — real Server-Side Apply,
// real managedFields exclusivity (ADR-008), real status subresources.
//
// Layering mirrors the T9 lane: hand-built reconcilers with an
// interceptor-audited client prove the write contract (multi-path
// delivery, per-violation-class explicit status with ZERO writes,
// first-wins Conflict, disjoint-path coexistence at element granularity,
// minimal-field updates on the real managedFields trie); the manager
// context boots the production registrars plus the T10 resolver and
// proves the live wiring end to end.
//
// Third-party kinds have no watch surface by design (T9 seam): the live
// lane therefore triggers deliveries through stream events and
// subscription creates, exactly like production.
var _ = Describe("T10 third-party imageFields delivery", Label("t10"), Ordered, func() {
	var (
		t10iEnv   *envtest.Environment
		restCfg   *rest.Config
		k8s       client.Client
		watchCl   client.WithWatch
		ctx       context.Context
		cancel    context.CancelFunc
		ns        string
		t10iClock *clock.Fake
	)

	const (
		t10iAuthzName    = "subscription-authz-t10"
		t10iDeliveryName = "subscription-delivery-t10"
		t10iEdgeAPI      = "apps.thirdparty.test/v1alpha1"
		t10iEdgeKind     = "EdgeService"
		t10iEdgePlural   = "edgeservices"
		t10iEdgeGroup    = "apps.thirdparty.test"
		t10iSourceKind   = "ImageStreamTag"
		t10iTag          = "current"
		t10iRepo         = "quay.io/example/edge@sha256:"
		t10iStatusSub    = "status"
		t10iGateMessage  = "t10i lane: gate surface written by the test"
		t10iCreatorUser  = "t10i-creator@example.org"
		t10iCreatorIdent = "user:t10i-creator@example.org"
		t10iSARPeriod    = 2 * time.Second

		// Field-name literals reused by helpers and body builders; the
		// package linter fails on literals repeated three times.
		t10iFieldSpec       = "spec"
		t10iFieldRuntime    = "runtime"
		t10iFieldContainers = "containers"
		t10iFieldName       = "name"
		t10iFieldImage      = "image"
		t10iFieldLegacy     = "legacy"
		t10iFieldImages     = "images"

		// imageFields paths under test (canonical ADR-005 grammar).
		t10iPathEdgeImage   = ".spec.edgeImage"
		t10iPathEdgeCtn     = ".spec.runtime.containers[name=edge].image"
		t10iPathCoreCtn     = ".spec.runtime.containers[name=core].image"
		t10iPathEdgeMode    = ".spec.runtime.containers[name=edge].mode"
		t10iPathMissing     = ".spec.runtime.initImage"
		t10iPathNonString   = ".spec.runtime.size"
		t10iPathGhostFilter = ".spec.runtime.containers[name=ghost].image"
		t10iPathLegacyOld   = ".spec.legacy.images[name=old].image"
		t10iPathStatusRoot  = ".status.phase"
		t10iPathMetaRoot    = ".metadata.labels.app"

		// Pre-images of the default live object (reused by assertions).
		t10iPrePlain  = "quay.io/example/edge:pre-plain"
		t10iPreCore   = "quay.io/example/edge:pre-core"
		t10iPreLegacy = "quay.io/example/edge:pre-old"

		// As in the T9 lane: the first delivery onto a foreign-created
		// object costs one honest 409 plus one forced takeover apply.
		t10iTakeoverApplies = 2
	)

	var (
		t10iDigestA = t10iRepo + strings.Repeat("a1", 32)
		t10iDigestB = t10iRepo + strings.Repeat("b2", 32)
		t10iDigestC = t10iRepo + strings.Repeat("c3", 32)
		t10iEdgeGVK = schema.GroupVersionKind{
			Group: t10iEdgeGroup, Version: "v1alpha1", Kind: t10iEdgeKind,
		}
	)

	BeforeAll(func() {
		assets := os.Getenv("KUBEBUILDER_ASSETS")
		if assets == "" {
			Skip("KUBEBUILDER_ASSETS not set")
		}
		t10iEnv = &envtest.Environment{
			BinaryAssetsDirectory: assets,
			CRDDirectoryPaths: []string{
				"../../config/crd/bases",
				"../testdata/thirdparty",
			},
			ErrorIfCRDPathMissing: true,
		}
		var err error
		restCfg, err = t10iEnv.Start()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(t10iEnv.Stop()).To(Succeed())
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

		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "t10i-fields-"}}
		Expect(k8s.Create(ctx, namespace)).To(Succeed())
		ns = namespace.Name
		t10iClock = clock.NewFake(time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))

		// The production registration path (the same call the T10
		// Registrar makes through controller.Setup). This binary is the
		// integration suite — the pinned honest-Unsupported unit test
		// lives in the internal/controller package and never sees this.
		controller.RegisterGenericPathAdapter()
		Expect(delivery.LookupPathAdapter()).NotTo(BeNil())
	})

	// --- helpers ------------------------------------------------------------

	t10iAudit := struct {
		applies      int
		statusWrites int
		otherWrites  int
	}{}

	t10iResetAudit := func() {
		t10iAudit.applies, t10iAudit.statusWrites, t10iAudit.otherWrites = 0, 0, 0
	}

	t10iHandBuilt := func(cfg config.Config) (*controller.SubscriptionDeliveryReconciler, *record.FakeRecorder) {
		intercepted := interceptor.NewClient(watchCl, interceptor.Funcs{
			Create: func(c context.Context, cl client.WithWatch, obj client.Object,
				_ ...client.CreateOption) error {
				t10iAudit.otherWrites++
				return cl.Create(c, obj)
			},
			Update: func(c context.Context, cl client.WithWatch, obj client.Object,
				_ ...client.UpdateOption) error {
				t10iAudit.otherWrites++
				return cl.Update(c, obj)
			},
			Patch: func(c context.Context, cl client.WithWatch, obj client.Object,
				p client.Patch, opts ...client.PatchOption) error {
				if p.Type() == types.ApplyPatchType {
					t10iAudit.applies++
				} else {
					t10iAudit.otherWrites++
				}
				// Options MUST be forwarded: FieldOwner/ForceOwnership
				// carry the whole SSA semantics of this lane.
				return cl.Patch(c, obj, p, opts...)
			},
			Delete: func(c context.Context, cl client.WithWatch, obj client.Object,
				_ ...client.DeleteOption) error {
				t10iAudit.otherWrites++
				return cl.Delete(c, obj)
			},
			SubResourceUpdate: func(c context.Context, cl client.Client, subResource string,
				obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if _, ok := obj.(*imageflowv1alpha1.ImageSubscription); ok && subResource == t10iStatusSub {
					t10iAudit.statusWrites++
				} else {
					t10iAudit.otherWrites++
				}
				return cl.SubResource(subResource).Update(c, obj, opts...)
			},
			SubResourcePatch: func(c context.Context, cl client.Client, subResource string,
				obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
				t10iAudit.otherWrites++
				return cl.SubResource(subResource).Patch(c, obj, p, opts...)
			},
		})
		recorder := record.NewFakeRecorder(64)
		return controller.NewSubscriptionDeliveryReconciler(
			intercepted, nil, recorder, t10iClock, cfg, nil), recorder
	}

	t10iRequest := func(name string) ctrl.Request {
		return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}
	}

	t10iGetSub := func(name string) *imageflowv1alpha1.ImageSubscription {
		sub := &imageflowv1alpha1.ImageSubscription{}
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, sub)).To(Succeed())
		return sub
	}

	t10iEntry := func(sub *imageflowv1alpha1.ImageSubscription,
		name string) *imageflowv1alpha1.TargetDeliveryStatus {
		for i := range sub.Status.Targets {
			if sub.Status.Targets[i].TargetRef.Name == name {
				return &sub.Status.Targets[i]
			}
		}
		return nil
	}

	t10iCondition := func(sub *imageflowv1alpha1.ImageSubscription, condType string) *metav1.Condition {
		return meta.FindStatusCondition(sub.Status.Conditions, condType)
	}

	t10iDrainEvents := func(rec *record.FakeRecorder) []string {
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

	t10iEventReasonsOn := func(objName string) []string {
		var list corev1.EventList
		Expect(k8s.List(ctx, &list, client.InNamespace(ns))).To(Succeed())
		var reasons []string
		for _, event := range list.Items {
			if event.InvolvedObject.Name == objName {
				reasons = append(reasons, event.Reason)
			}
		}
		return reasons
	}

	t10iNewSub := func(name, stream, createdBy string,
		targets ...imageflowv1alpha1.SubscriptionTarget) *imageflowv1alpha1.ImageSubscription {
		sub := &imageflowv1alpha1.ImageSubscription{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Spec: imageflowv1alpha1.ImageSubscriptionSpec{
				Source: imageflowv1alpha1.SubscriptionSourceRef{
					Kind:      t10iSourceKind,
					Name:      stream + ":" + t10iTag,
					Namespace: ns,
				},
				Targets: targets,
			},
		}
		if createdBy != "" {
			sub.Annotations = map[string]string{iwebhook.CreatedByAnnotation: createdBy}
		}
		return sub
	}

	t10iPathTarget := func(name string, fields ...string) imageflowv1alpha1.SubscriptionTarget {
		return imageflowv1alpha1.SubscriptionTarget{
			TargetRef: imageflowv1alpha1.TargetReference{
				APIVersion: t10iEdgeAPI,
				Kind:       t10iEdgeKind,
				Namespace:  ns,
				Name:       name,
			},
			ImageFields: fields,
		}
	}

	t10iSetGate := func(subName string, verdict metav1.ConditionStatus, reason string) {
		sub := t10iGetSub(subName)
		meta.SetStatusCondition(&sub.Status.Conditions, metav1.Condition{
			Type:               imageflowv1alpha1.ConditionTypeForbidden,
			Status:             verdict,
			Reason:             reason,
			Message:            t10iGateMessage,
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: sub.Generation,
		})
		Expect(k8s.Status().Update(ctx, sub)).To(Succeed())
	}

	t10iAuthorize := func(subName string) {
		t10iSetGate(subName, metav1.ConditionFalse, controller.ReasonAuthorized)
	}

	t10iRetagStream := func(name, digest string) {
		stream := &imageflowv1alpha1.ImageStream{}
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, stream)).To(Succeed())
		parts := strings.SplitN(digest, "@", 2)
		stream.Status.Tags = []imageflowv1alpha1.NamedTagEventList{{
			Tag: t10iTag,
			Items: []imageflowv1alpha1.TagEvent{{
				ImageReference: digest,
				Digest:         parts[1],
				Created:        metav1.NewTime(time.Now().Add(-time.Minute)),
			}},
		}}
		Expect(k8s.Status().Update(ctx, stream)).To(Succeed())
	}

	t10iCreateStream := func(name, digest string) {
		stream := &imageflowv1alpha1.ImageStream{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		}
		Expect(k8s.Create(ctx, stream)).To(Succeed())
		t10iRetagStream(name, digest)
	}

	// t10iEdgeElement builds one stable-key array element.
	t10iEdgeElement := func(name, image, mode string) map[string]interface{} {
		element := map[string]interface{}{t10iFieldName: name, t10iFieldImage: image}
		if mode != "" {
			element["mode"] = mode
		}
		return element
	}

	// t10iNewEdge builds the representative third-party live object: a
	// plain string field, a list-type=map container array (stable key
	// "name"), a non-string leaf, a declared-but-unset leaf and an
	// ATOMIC legacy array (the guard shape).
	t10iNewEdge := func(name string) *unstructured.Unstructured {
		obj := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": t10iEdgeAPI,
			"kind":       t10iEdgeKind,
			"metadata":   map[string]interface{}{"namespace": ns, "name": name},
			t10iFieldSpec: map[string]interface{}{
				"edgeImage": t10iPrePlain,
				t10iFieldRuntime: map[string]interface{}{
					"size": int64(3),
					t10iFieldContainers: []interface{}{
						t10iEdgeElement("edge", "quay.io/example/edge:pre-edge", "canary"),
						t10iEdgeElement("core", t10iPreCore, ""),
					},
				},
				t10iFieldLegacy: map[string]interface{}{
					t10iFieldImages: []interface{}{
						t10iEdgeElement("old", t10iPreLegacy, ""),
					},
				},
			},
		}}
		return obj
	}

	t10iCreateEdge := func(name string) *unstructured.Unstructured {
		obj := t10iNewEdge(name)
		Expect(k8s.Create(ctx, obj)).To(Succeed())
		return obj
	}

	t10iGetEdge := func(name string) *unstructured.Unstructured {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(t10iEdgeGVK)
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, obj)).To(Succeed())
		return obj
	}

	t10iEdgeField := func(obj *unstructured.Unstructured, fields ...string) string {
		value, found, err := unstructured.NestedString(obj.Object, fields...)
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue(), "field %v must exist", fields)
		return value
	}

	t10iContainerImage := func(obj *unstructured.Unstructured, name string) string {
		entries, found, err := unstructured.NestedSlice(obj.Object,
			t10iFieldSpec, t10iFieldRuntime, t10iFieldContainers)
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		for _, raw := range entries {
			element, ok := raw.(map[string]interface{})
			Expect(ok).To(BeTrue())
			if element[t10iFieldName] == name {
				image, _ := element[t10iFieldImage].(string)
				return image
			}
		}
		Fail("container " + name + " not found in the live object")
		return ""
	}

	// t10iOwnsPath probes the REAL managedFields trie: does the given
	// apply-manager own this exact canonical path? Entry presence alone
	// proves nothing (the audit annotation is shared), so every AC4
	// assertion goes through delivery.FieldsV1OwnsPath.
	t10iOwnsPath := func(obj metav1.Object, manager, rawPath string) bool {
		path, err := imagefields.Parse(rawPath)
		Expect(err).NotTo(HaveOccurred())
		for _, entry := range obj.GetManagedFields() {
			if entry.Manager == manager &&
				entry.Operation == metav1.ManagedFieldsOperationApply &&
				entry.FieldsV1 != nil &&
				delivery.FieldsV1OwnsPath(entry.FieldsV1.Raw, path) {
				return true
			}
		}
		return false
	}

	t10iManagerNames := func(obj metav1.Object) []string {
		var names []string
		for _, entry := range obj.GetManagedFields() {
			names = append(names, entry.Manager)
		}
		return names
	}

	t10iGrantEdgeUpdate := func(roleName, bindingName, userName string) {
		role := &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: roleName},
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{t10iEdgeGroup},
				Resources: []string{t10iEdgePlural},
				Verbs:     []string{"get", "update"},
			}},
		}
		Expect(k8s.Create(ctx, role)).To(Succeed())
		Expect(k8s.Create(ctx, &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: bindingName},
			RoleRef: rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "Role",
				Name:     roleName,
			},
			Subjects: []rbacv1.Subject{{Kind: rbacv1.UserKind, Name: userName}},
		})).To(Succeed())
	}

	t10iCounterValue := func(metricName, labelName, labelValue string) float64 {
		families, err := ctrlmetrics.Registry.Gather()
		Expect(err).NotTo(HaveOccurred())
		var total float64
		for _, family := range families {
			if family.GetName() != metricName {
				continue
			}
			for _, m := range family.GetMetric() {
				for _, label := range m.GetLabel() {
					if label.GetName() == labelName && label.GetValue() == labelValue {
						total += m.GetCounter().GetValue()
					}
				}
			}
		}
		return total
	}

	// --- hand-built reconciler specs (real SSA, injected clock) -------------

	Context("with hand-built reconcilers against the API server", func() {
		It("t10i-envtest-multiple-imagefields-paths-including-array-filter-become-digests", func() {
			const stream, edgeName, subName = "t10i-ac1-stream", "t10i-ac1-edge", "t10i-ac1-sub"
			t10iCreateStream(stream, t10iDigestA)
			t10iCreateEdge(edgeName)
			sub := t10iNewSub(subName, stream, "",
				t10iPathTarget(edgeName, t10iPathEdgeCtn, t10iPathEdgeImage, t10iPathCoreCtn))
			Expect(k8s.Create(ctx, sub)).To(Succeed())
			t10iAuthorize(subName)

			t10iResetAudit()
			reconciler, recorder := t10iHandBuilt(config.Config{})
			result, err := reconciler.Reconcile(ctx, t10iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())
			Expect(t10iAudit.applies).To(Equal(t10iTakeoverApplies), "409 + forced takeover")
			Expect(t10iAudit.statusWrites).To(Equal(1))
			Expect(t10iAudit.otherWrites).To(BeZero())

			By("every declared path holds ONLY the digest reference")
			stored := t10iGetEdge(edgeName)
			Expect(t10iContainerImage(stored, "edge")).To(Equal(t10iDigestA))
			Expect(t10iContainerImage(stored, "core")).To(Equal(t10iDigestA))
			Expect(t10iEdgeField(stored, t10iFieldSpec, "edgeImage")).To(Equal(t10iDigestA))
			Expect(t10iManagerNames(stored)).To(ContainElement(delivery.FieldManagerFor(ns, subName)))
			Expect(stored.GetAnnotations()).To(HaveKeyWithValue(
				delivery.ManagedByAnnotation, delivery.ManagedByAnnotationValue))

			By("the delivery status follows the frozen per-target shape")
			got := t10iGetSub(subName)
			entry := t10iEntry(got, edgeName)
			Expect(entry).NotTo(BeNil())
			Expect(entry.Reason).To(Equal(controller.TargetReasonApplied))
			Expect(entry.ImageApplied).To(BeTrue())
			Expect(entry.DesiredImage).To(Equal(t10iDigestA))
			Expect(entry.ObservedImage).To(Equal(t10iDigestA))
			Expect(entry.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutProgressing),
				"the engine seeds Progressing on a value-changing apply; T11 owns observation")
			Expect(entry.HealthStatus).To(Equal(imageflowv1alpha1.HealthUnknown))
			Expect(entry.ObservedGeneration).To(Equal(got.Generation))
			Expect(got.Status.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutProgressing))
			Expect(got.Status.RecentDeliveries).To(HaveLen(1))
			delivery0 := got.Status.RecentDeliveries[0]
			Expect(delivery0.NewImage).To(Equal(t10iDigestA))
			Expect(delivery0.RolloutResult).To(Equal(imageflowv1alpha1.RolloutProgressing))
			Expect(delivery0.Reason).To(Equal(controller.TargetReasonApplied))
			Expect(t10iCondition(got, imageflowv1alpha1.ConditionTypeConflict).Status).
				To(Equal(metav1.ConditionFalse))
			Expect(t10iCondition(got, imageflowv1alpha1.ConditionTypeUnsupported).Status).
				To(Equal(metav1.ConditionFalse), "with the resolver registered the target is supported")

			events := t10iDrainEvents(recorder)
			Expect(events).To(HaveLen(2), "dual-namespace ImageApplied events")
			for _, e := range events {
				Expect(e).To(ContainSubstring(controller.EventReasonImageApplied))
			}
		})

		It("t10i-envtest-minimal-field-update-leaves-undeclared-fields-and-ownership-untouched", func() {
			const stream, edgeName, subName = "t10i-ac4-stream", "t10i-ac4-edge", "t10i-ac4-sub"
			t10iCreateStream(stream, t10iDigestA)
			t10iCreateEdge(edgeName)
			sub := t10iNewSub(subName, stream, "", t10iPathTarget(edgeName, t10iPathEdgeCtn))
			Expect(k8s.Create(ctx, sub)).To(Succeed())
			t10iAuthorize(subName)

			t10iResetAudit()
			reconciler, _ := t10iHandBuilt(config.Config{})
			_, err := reconciler.Reconcile(ctx, t10iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			Expect(t10iAudit.applies).To(Equal(t10iTakeoverApplies))

			stored := t10iGetEdge(edgeName)
			Expect(t10iContainerImage(stored, "edge")).To(Equal(t10iDigestA))

			By("unrelated VALUES survive the delivery write")
			Expect(t10iEdgeField(stored, t10iFieldSpec, "edgeImage")).To(Equal(t10iPrePlain))
			Expect(t10iContainerImage(stored, "core")).To(Equal(t10iPreCore))
			size, found, err := unstructured.NestedInt64(stored.Object,
				t10iFieldSpec, t10iFieldRuntime, "size")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(size).To(Equal(int64(3)))
			legacy, legacyFound, legacyErr := unstructured.NestedSlice(stored.Object,
				t10iFieldSpec, t10iFieldLegacy, t10iFieldImages)
			Expect(legacyErr).NotTo(HaveOccurred())
			Expect(legacyFound).To(BeTrue())
			Expect(legacy).To(HaveLen(1))
			legacyElement, ok := legacy[0].(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(legacyElement[t10iFieldImage]).To(Equal(t10iPreLegacy))
			Expect(legacyElement["mode"]).To(BeNil(), "the element keeps exactly its created keys")

			By("unrelated FIELDS are not owned: the managedFields trie carries " +
				"exactly the declared path")
			manager := delivery.FieldManagerFor(ns, subName)
			Expect(t10iOwnsPath(stored, manager, t10iPathEdgeCtn)).
				To(BeTrue(), "the declared path must be owned via real SSA")
			Expect(t10iOwnsPath(stored, manager, t10iPathEdgeMode)).
				To(BeFalse(), "the sibling field of the written element must stay unowned")
			Expect(t10iOwnsPath(stored, manager, t10iPathCoreCtn)).
				To(BeFalse(), "the undeclared array element must stay unowned")
			Expect(t10iOwnsPath(stored, manager, t10iPathEdgeImage)).
				To(BeFalse(), "the undeclared plain field must stay unowned")
			Expect(t10iOwnsPath(stored, manager, t10iPathLegacyOld)).
				To(BeFalse(), "the atomic legacy array must stay unowned")
		})

		It("t10i-envtest-equal-value-reconcile-of-imagefields-target-produces-no-patch", func() {
			const stream, edgeName, subName = "t10i-ac1b-stream", "t10i-ac1b-edge", "t10i-ac1b-sub"
			t10iCreateStream(stream, t10iDigestA)
			t10iCreateEdge(edgeName)
			sub := t10iNewSub(subName, stream, "",
				t10iPathTarget(edgeName, t10iPathEdgeCtn, t10iPathEdgeImage))
			Expect(k8s.Create(ctx, sub)).To(Succeed())
			t10iAuthorize(subName)

			reconciler, recorder := t10iHandBuilt(config.Config{})
			t10iResetAudit()
			_, err := reconciler.Reconcile(ctx, t10iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			Expect(t10iAudit.applies).To(Equal(t10iTakeoverApplies))
			rvAfterDelivery := t10iGetEdge(edgeName).GetResourceVersion()
			t10iDrainEvents(recorder)

			By("re-reconciling the converged state must not touch anything")
			t10iResetAudit()
			result, err := reconciler.Reconcile(ctx, t10iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())
			Expect(t10iAudit.applies).To(BeZero(), "equal value + self ownership = ZERO patches")
			Expect(t10iAudit.statusWrites).To(BeZero())
			Expect(t10iAudit.otherWrites).To(BeZero())
			Expect(t10iDrainEvents(recorder)).To(BeEmpty())
			Expect(t10iGetEdge(edgeName).GetResourceVersion()).To(Equal(rvAfterDelivery))
		})

		It("t10i-envtest-every-invalid-field-class-gets-an-explicit-status-and-zero-writes", func() {
			const stream, subName = "t10i-ac2-stream", "t10i-ac2-sub"
			t10iCreateStream(stream, t10iDigestA)

			// One object per violation class (the engine dedups targets by
			// TargetRef). The ambiguous-key object carries a legacy array
			// with two elements sharing one identity key.
			const dupName = "t10i-ac2-ambiguous"
			classes := []struct{ name, path, want string }{
				{"t10i-ac2-missing", t10iPathMissing, "does not exist"},
				{"t10i-ac2-nonstring", t10iPathNonString, "not a string"},
				{"t10i-ac2-ghost", t10iPathGhostFilter, "selects no element"},
				{"t10i-ac2-atomic", t10iPathLegacyOld, "atomically"},
				{"t10i-ac2-status", t10iPathStatusRoot, "not writable"},
				{"t10i-ac2-meta", t10iPathMetaRoot, "system-owned"},
				{dupName, t10iPathLegacyOld, "not unique"},
			}
			rvBefore := map[string]string{}
			for _, class := range classes {
				if class.name == dupName {
					dup := t10iNewEdge(dupName)
					Expect(unstructured.SetNestedSlice(dup.Object, []interface{}{
						t10iEdgeElement("old", "quay.io/example/edge:pre-dup1", ""),
						t10iEdgeElement("old", "quay.io/example/edge:pre-dup2", ""),
					}, t10iFieldSpec, t10iFieldLegacy, t10iFieldImages)).To(Succeed())
					Expect(k8s.Create(ctx, dup)).To(Succeed())
				} else {
					t10iCreateEdge(class.name)
				}
				rvBefore[class.name] = t10iGetEdge(class.name).GetResourceVersion()
			}

			var targets []imageflowv1alpha1.SubscriptionTarget
			for _, class := range classes {
				targets = append(targets, t10iPathTarget(class.name, class.path))
			}
			sub := t10iNewSub(subName, stream, "", targets...)
			Expect(k8s.Create(ctx, sub)).To(Succeed())
			t10iAuthorize(subName)

			t10iResetAudit()
			reconciler, recorder := t10iHandBuilt(config.Config{})
			result, err := reconciler.Reconcile(ctx, t10iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero(),
				"InvalidImageField is deterministic: no requeue churn, no guessing")
			Expect(t10iAudit.applies).To(BeZero(), "ZERO writes across every violation class")
			Expect(t10iAudit.otherWrites).To(BeZero())
			Expect(t10iAudit.statusWrites).To(Equal(1))

			By("each class carries its own explicit discriminator in the entry message")
			got := t10iGetSub(subName)
			Expect(got.Status.Targets).To(HaveLen(len(classes)))
			for _, class := range classes {
				entry := t10iEntry(got, class.name)
				Expect(entry).NotTo(BeNil(), "entry for "+class.name)
				Expect(entry.Reason).To(Equal(controller.TargetReasonInvalidImageField), class.name)
				Expect(entry.Message).To(ContainSubstring(class.want), class.name)
				Expect(entry.Message).To(ContainSubstring(class.path),
					class.name+": the message must name the offending path")
				Expect(entry.ImageApplied).To(BeFalse(), class.name)
				Expect(entry.ObservedImage).To(BeEmpty(), class.name)
				Expect(entry.DesiredImage).To(Equal(t10iDigestA), class.name)
				Expect(entry.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutUnknown), class.name)
				Expect(entry.HealthStatus).To(Equal(imageflowv1alpha1.HealthUnknown), class.name)
			}
			Expect(got.Status.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutUnknown),
				"a delivery that wrote nothing must never look successful")
			Expect(t10iCondition(got, imageflowv1alpha1.ConditionTypeConflict).Status).
				To(Equal(metav1.ConditionFalse))
			Expect(t10iCondition(got, imageflowv1alpha1.ConditionTypeUnsupported).Status).
				To(Equal(metav1.ConditionFalse))

			By("every live object stays byte-identical (zero writes)")
			for name, rv := range rvBefore {
				Expect(t10iGetEdge(name).GetResourceVersion()).To(Equal(rv), name)
			}

			warnings := 0
			for _, e := range t10iDrainEvents(recorder) {
				if strings.Contains(e, "Warning "+controller.TargetReasonInvalidImageField) {
					warnings++
				}
			}
			Expect(warnings).To(Equal(len(classes)),
				"one Warning event per failed target, reason InvalidImageField")
		})

		It("t10i-envtest-second-subscription-same-path-terminal-conflict-zero-writes", func() {
			const (
				stream      = "t10i-ac3-stream"
				rivalStream = "t10i-ac3-rival-stream"
				edgeName    = "t10i-ac3-edge"
				firstSub    = "t10i-ac3-first"
				secondSub   = "t10i-ac3-second"
			)
			t10iCreateStream(stream, t10iDigestA)
			t10iCreateStream(rivalStream, t10iDigestB)
			t10iCreateEdge(edgeName)
			first := t10iNewSub(firstSub, stream, "", t10iPathTarget(edgeName, t10iPathEdgeCtn))
			Expect(k8s.Create(ctx, first)).To(Succeed())
			t10iAuthorize(firstSub)
			owner, _ := t10iHandBuilt(config.Config{})
			_, err := owner.Reconcile(ctx, t10iRequest(firstSub))
			Expect(err).NotTo(HaveOccurred())
			Expect(t10iContainerImage(t10iGetEdge(edgeName), "edge")).To(Equal(t10iDigestA))

			By("the second subscription on the same path loses (first-wins)")
			second := t10iNewSub(secondSub, rivalStream, "", t10iPathTarget(edgeName, t10iPathEdgeCtn))
			Expect(k8s.Create(ctx, second)).To(Succeed())
			t10iAuthorize(secondSub)
			late, recorder := t10iHandBuilt(config.Config{})
			t10iResetAudit()
			result, err := late.Reconcile(ctx, t10iRequest(secondSub))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero(), "terminal Conflict settles without requeue churn")
			Expect(t10iAudit.applies).To(BeZero(), "the loser must perform ZERO writes")
			Expect(t10iAudit.otherWrites).To(BeZero())

			stored := t10iGetEdge(edgeName)
			Expect(t10iContainerImage(stored, "edge")).To(Equal(t10iDigestA),
				"the occupant's value must survive the conflict")
			Expect(t10iManagerNames(stored)).
				NotTo(ContainElement(delivery.FieldManagerFor(ns, secondSub)))

			got := t10iGetSub(secondSub)
			entry := t10iEntry(got, edgeName)
			Expect(entry).NotTo(BeNil())
			Expect(entry.Reason).To(Equal(controller.TargetReasonConflict))
			Expect(entry.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutConflict))
			Expect(entry.Message).To(ContainSubstring(ns + "/" + firstSub))
			Expect(got.Status.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutConflict))
			cond := t10iCondition(got, imageflowv1alpha1.ConditionTypeConflict)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(controller.ReasonFieldOwnershipConflict))
			Expect(got.Status.RecentDeliveries).To(HaveLen(1))
			Expect(got.Status.RecentDeliveries[0].RolloutResult).
				To(Equal(imageflowv1alpha1.RolloutConflict))
			Expect(t10iDrainEvents(recorder)).To(ContainElement(ContainSubstring(
				"Warning " + controller.EventReasonDeliveryConflict)))
		})

		It("t10i-envtest-disjoint-paths-three-subscriptions-coexist-at-element-granularity", func() {
			const (
				stream   = "t10i-ac3b-stream"
				streamB  = "t10i-ac3b-stream-b"
				streamC  = "t10i-ac3b-stream-c"
				edgeName = "t10i-ac3b-edge"
				subA     = "t10i-ac3b-sub-a"
				subB     = "t10i-ac3b-sub-b"
				subC     = "t10i-ac3b-sub-c"
			)
			t10iCreateStream(stream, t10iDigestA)
			t10iCreateStream(streamB, t10iDigestB)
			t10iCreateStream(streamC, t10iDigestC)
			t10iCreateEdge(edgeName)

			specs := []struct{ sub, stream, path string }{
				{subA, stream, t10iPathEdgeCtn},
				{subB, streamB, t10iPathEdgeImage},
				{subC, streamC, t10iPathCoreCtn},
			}
			for _, s := range specs {
				sub := t10iNewSub(s.sub, s.stream, "", t10iPathTarget(edgeName, s.path))
				Expect(k8s.Create(ctx, sub)).To(Succeed())
				t10iAuthorize(s.sub)
			}

			reconciler, _ := t10iHandBuilt(config.Config{})
			t10iResetAudit()
			for _, s := range specs {
				_, err := reconciler.Reconcile(ctx, t10iRequest(s.sub))
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(t10iAudit.applies).To(Equal(3*t10iTakeoverApplies),
				"each subscription delivers its own path")

			stored := t10iGetEdge(edgeName)
			Expect(t10iContainerImage(stored, "edge")).To(Equal(t10iDigestA))
			Expect(t10iEdgeField(stored, t10iFieldSpec, "edgeImage")).To(Equal(t10iDigestB))
			Expect(t10iContainerImage(stored, "core")).To(Equal(t10iDigestC))
			Expect(t10iManagerNames(stored)).To(ContainElements(
				delivery.FieldManagerFor(ns, subA),
				delivery.FieldManagerFor(ns, subB),
				delivery.FieldManagerFor(ns, subC)))

			By("element-granular ownership: two managers share one array")
			Expect(t10iOwnsPath(stored, delivery.FieldManagerFor(ns, subA), t10iPathEdgeCtn)).To(BeTrue())
			Expect(t10iOwnsPath(stored, delivery.FieldManagerFor(ns, subC), t10iPathCoreCtn)).To(BeTrue())
			Expect(t10iOwnsPath(stored, delivery.FieldManagerFor(ns, subA), t10iPathCoreCtn)).To(BeFalse())
			Expect(t10iOwnsPath(stored, delivery.FieldManagerFor(ns, subB), t10iPathEdgeImage)).To(BeTrue())

			for _, s := range specs {
				got := t10iGetSub(s.sub)
				entry := t10iEntry(got, edgeName)
				Expect(entry).NotTo(BeNil())
				Expect(entry.Reason).To(Equal(controller.TargetReasonApplied), s.sub)
				Expect(entry.ImageApplied).To(BeTrue(), s.sub)
				Expect(t10iCondition(got, imageflowv1alpha1.ConditionTypeConflict).Status).
					To(Equal(metav1.ConditionFalse), s.sub)
			}
		})
	})

	// --- manager-driven specs (production registrars + resolver) ------------

	Context("with the production registrars and resolver in a manager", Ordered, func() {
		var (
			mgrCancel context.CancelFunc
			mgrDone   chan struct{}
		)

		const (
			liveStream  = "t10i-live-stream"
			liveEdge    = "t10i-live-edge"
			liveSub     = "t10i-live-sub"
			badEdge     = "t10i-live-invalid"
			badSub      = "t10i-live-invalid-sub"
			failMetric  = "imageflow_delivery_target_failures_total"
			failReason  = "invalid_image_field"
			labelReason = "reason"
		)

		BeforeAll(func() {
			scheme := runtime.NewScheme()
			Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
			Expect(imageflowv1alpha1.AddToScheme(scheme)).To(Succeed())
			mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
				Scheme:  scheme,
				Metrics: metricsserver.Options{BindAddress: "0"},
			})
			Expect(err).NotTo(HaveOccurred())
			cfg := config.Config{SARRecheckPeriod: t10iSARPeriod}
			Expect(controller.RegisterSubscriptionAuthz(mgr, t10iAuthzName,
				controller.Options{Clock: clock.Real{}, Config: cfg})).To(Succeed())
			Expect(controller.RegisterSubscriptionDelivery(mgr, t10iDeliveryName,
				controller.Options{Clock: clock.Real{}, Config: cfg})).To(Succeed())
			var mgrCtx context.Context
			mgrCtx, mgrCancel = context.WithCancel(ctx)
			mgrDone = make(chan struct{})
			go func() {
				defer GinkgoRecover()
				defer close(mgrDone)
				Expect(mgr.Start(mgrCtx)).To(Succeed())
			}()
			Expect(mgr.GetCache().WaitForCacheSync(mgrCtx)).To(BeTrue())

			t10iGrantEdgeUpdate("t10i-creator-role", "t10i-creator-binding", t10iCreatorUser)
		})

		AfterAll(func() {
			if mgrCancel != nil {
				mgrCancel()
				Eventually(mgrDone, 30*time.Second, 100*time.Millisecond).Should(BeClosed())
			}
		})

		It("t10i-envtest-live-manager-delivers-and-retags-a-third-party-target", func() {
			t10iCreateEdge(liveEdge)
			t10iCreateStream(liveStream, t10iDigestA)
			sub := t10iNewSub(liveSub, liveStream, t10iCreatorIdent,
				t10iPathTarget(liveEdge, t10iPathEdgeCtn, t10iPathEdgeImage))
			Expect(k8s.Create(ctx, sub)).To(Succeed())

			By("the T8 gate authorizes the granted creator and the engine delivers")
			Eventually(func(g Gomega) {
				edge := t10iGetEdge(liveEdge)
				g.Expect(t10iContainerImage(edge, "edge")).To(Equal(t10iDigestA))
				g.Expect(t10iEdgeField(edge, t10iFieldSpec, "edgeImage")).To(Equal(t10iDigestA))
				got := t10iGetSub(liveSub)
				entry := t10iEntry(got, liveEdge)
				g.Expect(entry).NotTo(BeNil())
				g.Expect(entry.Reason).To(Equal(controller.TargetReasonApplied))
				g.Expect(entry.ImageApplied).To(BeTrue())
				g.Expect(entry.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutProgressing))
				cond := t10iCondition(got, imageflowv1alpha1.ConditionTypeForbidden)
				g.Expect(cond).NotTo(BeNil())
				g.Expect(cond.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(cond.Reason).To(Equal(controller.ReasonAuthorized))
			}, 90*time.Second, 250*time.Millisecond).Should(Succeed())

			By("dual-namespace ImageApplied events reach the subscription and the CRD object")
			Eventually(func(g Gomega) {
				g.Expect(t10iEventReasonsOn(liveSub)).
					To(ContainElement(controller.EventReasonImageApplied))
				g.Expect(t10iEventReasonsOn(liveEdge)).
					To(ContainElement(controller.EventReasonImageApplied))
			}, 30*time.Second, 250*time.Millisecond).Should(Succeed())

			By("a fresh registration (Image Change) redelivers through the stream watch")
			t10iRetagStream(liveStream, t10iDigestB)
			Eventually(func(g Gomega) {
				edge := t10iGetEdge(liveEdge)
				g.Expect(t10iContainerImage(edge, "edge")).To(Equal(t10iDigestB))
				got := t10iGetSub(liveSub)
				window := got.Status.RecentDeliveries
				g.Expect(window).To(HaveLen(2))
				g.Expect(window[0].NewImage).To(Equal(t10iDigestB))
				g.Expect(window[1].RolloutResult).To(Equal(imageflowv1alpha1.RolloutSuperseded))
			}, 90*time.Second, 250*time.Millisecond).Should(Succeed())
		})

		It("t10i-envtest-live-manager-reports-invalid-fields-honestly-and-counts-them", func() {
			baseline := t10iCounterValue(failMetric, labelReason, failReason)
			t10iCreateEdge(badEdge)
			sub := t10iNewSub(badSub, liveStream, t10iCreatorIdent,
				t10iPathTarget(badEdge, t10iPathMissing))
			Expect(k8s.Create(ctx, sub)).To(Succeed())

			Eventually(func(g Gomega) {
				got := t10iGetSub(badSub)
				entry := t10iEntry(got, badEdge)
				g.Expect(entry).NotTo(BeNil())
				g.Expect(entry.Reason).To(Equal(controller.TargetReasonInvalidImageField))
				g.Expect(entry.Message).To(ContainSubstring("does not exist"))
				g.Expect(entry.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutUnknown))
				g.Expect(t10iCounterValue(failMetric, labelReason, failReason)).
					To(BeNumerically(">", baseline))
			}, 90*time.Second, 250*time.Millisecond).Should(Succeed())

			Consistently(func(g Gomega) {
				edge := t10iGetEdge(badEdge)
				g.Expect(t10iEdgeField(edge, t10iFieldSpec, "edgeImage")).
					To(Equal(t10iPrePlain), "zero writes, no guessing")
			}, 5*time.Second, 500*time.Millisecond).Should(Succeed())
		})
	})
})
