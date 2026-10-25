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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
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

// T9 named cases: the delivery write engine against a REAL API server —
// real Server-Side Apply, real managedFields (the ADR-008 exclusivity
// registry in its production on-the-wire shape), real status subresources
// and real CRD validation.
//
// Layering: the hand-built context drives the production reconciler with an
// interceptor-audited client (every mutating call counted — the zero-write
// guarantees of the T8 gate and of terminal Conflict are asserted on those
// counters) and the injected clock (seam S3). The manager context boots the
// production registrars (T8 gate + T9 engine) and proves the watch wiring
// end to end: digest registration (Image Change) -> authorization -> SSA
// apply -> bounded status, plus manager-restart convergence from persisted
// state and the delivery metrics on the process-wide registry.
//
// Controller names carry the "-t9" suffixes: controller-runtime validates
// controller names process-globally and other lanes in this package boot
// their own managers.
var _ = Describe("T9 delivery write engine", Label("t9"), Ordered, func() {
	var (
		t9iEnv   *envtest.Environment
		restCfg  *rest.Config
		k8s      client.Client
		watchCl  client.WithWatch
		ctx      context.Context
		cancel   context.CancelFunc
		ns       string
		t9iClock *clock.Fake
	)

	const (
		t9iAuthzName       = "subscription-authz-t9"
		t9iDeliveryName    = "subscription-delivery-t9"
		t9iRestartAuthz    = "subscription-authz-t9-rst"
		t9iRestartDelivery = "subscription-delivery-t9-rst"
		t9iKindDeploy      = "Deployment"
		t9iKindSts         = "StatefulSet"
		t9iKindDs          = "DaemonSet"
		t9iAPIApps         = "apps/v1"
		t9iSourceKind      = "ImageStreamTag"
		t9iTag             = "current"
		t9iContainer       = "app"
		t9iSidecar         = "log-shipper"
		t9iPreImage        = "quay.io/example/checkout:pre-t9"
		t9iRepo            = "quay.io/example/checkout@sha256:"
		t9iLabelApp        = "t9i-app"
		t9iStatusSub       = "status"
		t9iGateMessage     = "t9i lane: gate surface written by the test"
		t9iCreatorUser     = "t9i-creator@example.org"
		t9iCreatorIdent    = "user:t9i-creator@example.org"
		t9iLiveStream      = "t9i-live-stream"
		t9iLiveDeploy      = "t9i-ac1-live"
		t9iLiveSub         = "t9i-ac1-live-sub"
		t9iWaitingRequeue  = 30 * time.Second
		t9iGatePending     = 5 * time.Second
		t9iSARPeriod       = 2 * time.Second
		// First delivery onto a workload created by a foreign client
		// (kubectl/typed Create owns every field) takes the honest
		// ADR-008 re-classification path against the real API server:
		// one conflicting apply (409 — the creator manager owns the
		// image field), then InspectOwnership + findLiveClaimant prove
		// no live subscription claims it, and one forced apply takes
		// the field over. Hence two Apply calls per first delivery.
		t9iTakeoverApplies = 2
	)

	var (
		t9iDigestA = t9iRepo + strings.Repeat("a1", 32)
		t9iDigestB = t9iRepo + strings.Repeat("b2", 32)
		t9iDigestC = t9iRepo + strings.Repeat("c3", 32)
		t9iDigestD = t9iRepo + strings.Repeat("d4", 32)
	)

	BeforeAll(func() {
		assets := os.Getenv("KUBEBUILDER_ASSETS")
		if assets == "" {
			Skip("KUBEBUILDER_ASSETS not set")
		}
		t9iEnv = &envtest.Environment{
			BinaryAssetsDirectory: assets,
			CRDDirectoryPaths: []string{
				"../../config/crd/bases",
			},
			ErrorIfCRDPathMissing: true,
		}
		var err error
		restCfg, err = t9iEnv.Start()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(t9iEnv.Stop()).To(Succeed())
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

		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "t9i-delivery-"}}
		Expect(k8s.Create(ctx, namespace)).To(Succeed())
		ns = namespace.Name
		t9iClock = clock.NewFake(time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC))
	})

	// --- helpers ------------------------------------------------------------

	// t9iAudit counts every mutating call of the reconciler under test. The
	// zero-write guarantees (T8 gate, terminal Conflict, steady state) are
	// assertions on these counters; blockStatus simulates a crash between
	// the workload write and the status write (restart-recovery lane).
	t9iAudit := struct {
		applies      int
		statusWrites int
		otherWrites  int
		blockStatus  error
	}{}

	t9iResetAudit := func() {
		t9iAudit.applies, t9iAudit.statusWrites, t9iAudit.otherWrites = 0, 0, 0
		t9iAudit.blockStatus = nil
	}

	t9iHandBuilt := func(cfg config.Config) (*controller.SubscriptionDeliveryReconciler, *record.FakeRecorder) {
		intercepted := interceptor.NewClient(watchCl, interceptor.Funcs{
			Create: func(c context.Context, cl client.WithWatch, obj client.Object,
				_ ...client.CreateOption) error {
				t9iAudit.otherWrites++
				return cl.Create(c, obj)
			},
			Update: func(c context.Context, cl client.WithWatch, obj client.Object,
				_ ...client.UpdateOption) error {
				t9iAudit.otherWrites++
				return cl.Update(c, obj)
			},
			Patch: func(c context.Context, cl client.WithWatch, obj client.Object,
				p client.Patch, opts ...client.PatchOption) error {
				if p.Type() == types.ApplyPatchType {
					t9iAudit.applies++
				} else {
					t9iAudit.otherWrites++
				}
				// Options MUST be forwarded: FieldOwner/ForceOwnership
				// carry the whole SSA semantics of this lane.
				return cl.Patch(c, obj, p, opts...)
			},
			Delete: func(c context.Context, cl client.WithWatch, obj client.Object,
				_ ...client.DeleteOption) error {
				t9iAudit.otherWrites++
				return cl.Delete(c, obj)
			},
			SubResourceUpdate: func(c context.Context, cl client.Client, subResource string,
				obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if sub, ok := obj.(*imageflowv1alpha1.ImageSubscription); ok && subResource == t9iStatusSub {
					_ = sub
					t9iAudit.statusWrites++
					if t9iAudit.blockStatus != nil {
						return t9iAudit.blockStatus
					}
				} else {
					t9iAudit.otherWrites++
				}
				return cl.SubResource(subResource).Update(c, obj, opts...)
			},
			SubResourcePatch: func(c context.Context, cl client.Client, subResource string,
				obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
				t9iAudit.otherWrites++
				return cl.SubResource(subResource).Patch(c, obj, p, opts...)
			},
		})
		recorder := record.NewFakeRecorder(64)
		return controller.NewSubscriptionDeliveryReconciler(
			intercepted, nil, recorder, t9iClock, cfg, nil), recorder
	}

	t9iRequest := func(name string) ctrl.Request {
		return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}
	}

	t9iGetSub := func(name string) *imageflowv1alpha1.ImageSubscription {
		sub := &imageflowv1alpha1.ImageSubscription{}
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, sub)).To(Succeed())
		return sub
	}

	t9iGetDeploy := func(name string) *appsv1.Deployment {
		deploy := &appsv1.Deployment{}
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, deploy)).To(Succeed())
		return deploy
	}

	t9iEntry := func(sub *imageflowv1alpha1.ImageSubscription,
		name string) *imageflowv1alpha1.TargetDeliveryStatus {
		for i := range sub.Status.Targets {
			if sub.Status.Targets[i].TargetRef.Name == name {
				return &sub.Status.Targets[i]
			}
		}
		return nil
	}

	t9iCondition := func(sub *imageflowv1alpha1.ImageSubscription, condType string) *metav1.Condition {
		return meta.FindStatusCondition(sub.Status.Conditions, condType)
	}

	t9iDrainEvents := func(rec *record.FakeRecorder) []string {
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

	t9iEventReasonsOn := func(objName string) []string {
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

	t9iNewSub := func(name, stream, createdBy string,
		targets ...imageflowv1alpha1.SubscriptionTarget) *imageflowv1alpha1.ImageSubscription {
		sub := &imageflowv1alpha1.ImageSubscription{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Spec: imageflowv1alpha1.ImageSubscriptionSpec{
				Source: imageflowv1alpha1.SubscriptionSourceRef{
					Kind:      t9iSourceKind,
					Name:      stream + ":" + t9iTag,
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

	t9iContainerTarget := func(kind, name string,
		containers ...string) imageflowv1alpha1.SubscriptionTarget {
		return imageflowv1alpha1.SubscriptionTarget{
			TargetRef: imageflowv1alpha1.TargetReference{
				APIVersion: t9iAPIApps,
				Kind:       kind,
				Namespace:  ns,
				Name:       name,
			},
			ContainerNames: containers,
		}
	}

	// t9iSetGate writes the T8 Forbidden surface directly — the hand-built
	// lane drives the T9 engine in isolation, the manager lane exercises the
	// real T8 controller.
	t9iSetGate := func(subName string, verdict metav1.ConditionStatus, reason string) {
		sub := t9iGetSub(subName)
		meta.SetStatusCondition(&sub.Status.Conditions, metav1.Condition{
			Type:               imageflowv1alpha1.ConditionTypeForbidden,
			Status:             verdict,
			Reason:             reason,
			Message:            t9iGateMessage,
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: sub.Generation,
		})
		Expect(k8s.Status().Update(ctx, sub)).To(Succeed())
	}

	t9iAuthorize := func(subName string) {
		t9iSetGate(subName, metav1.ConditionFalse, controller.ReasonAuthorized)
	}

	// t9iRetagStream re-points the tag at a new digest — a registration
	// (Mapping/Import) landing, i.e. the Image Change trigger source.
	t9iRetagStream := func(name, digest string) {
		stream := &imageflowv1alpha1.ImageStream{}
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, stream)).To(Succeed())
		parts := strings.SplitN(digest, "@", 2)
		stream.Status.Tags = []imageflowv1alpha1.NamedTagEventList{{
			Tag: t9iTag,
			Items: []imageflowv1alpha1.TagEvent{{
				ImageReference: digest,
				Digest:         parts[1],
				Created:        metav1.NewTime(time.Now().Add(-time.Minute)),
			}},
		}}
		Expect(k8s.Status().Update(ctx, stream)).To(Succeed())
	}

	t9iCreateStream := func(name, digest string) {
		stream := &imageflowv1alpha1.ImageStream{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		}
		Expect(k8s.Create(ctx, stream)).To(Succeed())
		t9iRetagStream(name, digest)
	}

	t9iPodSpec := func(containers ...[2]string) corev1.PodSpec {
		spec := corev1.PodSpec{}
		for _, pair := range containers {
			spec.Containers = append(spec.Containers, corev1.Container{Name: pair[0], Image: pair[1]})
		}
		return spec
	}

	t9iSelector := func(name string) (map[string]string, *metav1.LabelSelector) {
		labels := map[string]string{t9iLabelApp: name}
		return labels, &metav1.LabelSelector{MatchLabels: labels}
	}

	t9iCreateDeployment := func(name string, containers ...[2]string) {
		labels, selector := t9iSelector(name)
		Expect(k8s.Create(ctx, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To(int32(0)),
				Selector: selector,
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       t9iPodSpec(containers...),
				},
			},
		})).To(Succeed())
	}

	t9iCreateStatefulSet := func(name string, containers ...[2]string) {
		labels, selector := t9iSelector(name)
		Expect(k8s.Create(ctx, &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Spec: appsv1.StatefulSetSpec{
				Replicas:    ptr.To(int32(0)),
				ServiceName: name + "-svc",
				Selector:    selector,
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       t9iPodSpec(containers...),
				},
			},
		})).To(Succeed())
	}

	t9iCreateDaemonSet := func(name string, containers ...[2]string) {
		labels, selector := t9iSelector(name)
		Expect(k8s.Create(ctx, &appsv1.DaemonSet{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Spec: appsv1.DaemonSetSpec{
				Selector: selector,
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       t9iPodSpec(containers...),
				},
			},
		})).To(Succeed())
	}

	// t9iOwnsContainerImage probes the REAL managedFields trie: does the
	// given apply-manager currently own the container image field? This is
	// the production exclusivity question (ADR-008) — manager entries may
	// legitimately survive for other, shared fields (e.g. the audit
	// annotation), so entry presence alone proves nothing.
	t9iOwnsContainerImage := func(obj metav1.Object, manager, container string) bool {
		path, err := imagefields.Parse(fmt.Sprintf(
			".spec.template.spec.containers[name=%s].image", container))
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

	t9iManagerNames := func(obj metav1.Object) []string {
		var names []string
		for _, entry := range obj.GetManagedFields() {
			names = append(names, entry.Manager)
		}
		return names
	}

	t9iGrantWorkloadUpdate := func(roleName, bindingName, userName string) {
		role := &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: roleName},
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{"apps"},
				Resources: []string{"deployments", "statefulsets", "daemonsets"},
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

	// t9iHistogramCount sums the sample count of every series of one
	// histogram family on the process-wide controller-runtime registry.
	t9iHistogramCount := func(metricName string) uint64 {
		families, err := ctrlmetrics.Registry.Gather()
		Expect(err).NotTo(HaveOccurred())
		var total uint64
		for _, family := range families {
			if family.GetName() != metricName {
				continue
			}
			for _, m := range family.GetMetric() {
				total += m.GetHistogram().GetSampleCount()
			}
		}
		return total
	}

	// t9iCounterValue sums the counters of one label value across a family.
	t9iCounterValue := func(metricName, labelName, labelValue string) float64 {
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
		It("t9i-envtest-deployment-container-image-becomes-the-digest-reference", func() {
			const stream, deployName, subName = "t9i-ac1-stream", "t9i-ac1-deploy", "t9i-ac1-sub"
			t9iCreateStream(stream, t9iDigestA)
			t9iCreateDeployment(deployName, [2]string{t9iContainer, t9iPreImage})
			sub := t9iNewSub(subName, stream, "", t9iContainerTarget(t9iKindDeploy, deployName, t9iContainer))
			Expect(k8s.Create(ctx, sub)).To(Succeed())
			t9iAuthorize(subName)

			t9iResetAudit()
			reconciler, recorder := t9iHandBuilt(config.Config{})
			result, err := reconciler.Reconcile(ctx, t9iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())
			Expect(t9iAudit.applies).To(Equal(t9iTakeoverApplies), "409 + forced takeover")
			Expect(t9iAudit.statusWrites).To(Equal(1))
			Expect(t9iAudit.otherWrites).To(BeZero())

			By("the workload holds ONLY the digest reference, owned via SSA")
			stored := t9iGetDeploy(deployName)
			Expect(stored.Spec.Template.Spec.Containers[0].Image).To(Equal(t9iDigestA))
			Expect(t9iManagerNames(stored)).To(ContainElement(delivery.FieldManagerFor(ns, subName)),
				"the per-subscription field manager must own the write in real managedFields")
			Expect(stored.Annotations).To(HaveKeyWithValue(
				delivery.ManagedByAnnotation, delivery.ManagedByAnnotationValue))

			By("the delivery status follows the frozen shape")
			got := t9iGetSub(subName)
			entry := t9iEntry(got, deployName)
			Expect(entry).NotTo(BeNil())
			Expect(entry.Reason).To(Equal(controller.TargetReasonApplied))
			Expect(entry.ImageApplied).To(BeTrue())
			Expect(entry.DesiredImage).To(Equal(t9iDigestA))
			Expect(entry.ObservedImage).To(Equal(t9iDigestA))
			Expect(entry.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutProgressing))
			Expect(entry.HealthStatus).To(Equal(imageflowv1alpha1.HealthUnknown))
			Expect(entry.ObservedGeneration).To(Equal(got.Generation))
			Expect(got.Status.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutProgressing))
			Expect(got.Status.RecentDeliveries).To(HaveLen(1))
			record0 := got.Status.RecentDeliveries[0]
			Expect(record0.NewImage).To(Equal(t9iDigestA))
			Expect(record0.RolloutResult).To(Equal(imageflowv1alpha1.RolloutProgressing))
			Expect(record0.Reason).To(Equal(controller.TargetReasonApplied))
			Expect(t9iCondition(got, imageflowv1alpha1.ConditionTypeConflict).Status).
				To(Equal(metav1.ConditionFalse))
			Expect(t9iCondition(got, imageflowv1alpha1.ConditionTypeUnsupported).Status).
				To(Equal(metav1.ConditionFalse))

			events := t9iDrainEvents(recorder)
			Expect(events).To(HaveLen(2), "dual-namespace ImageApplied events")
			for _, e := range events {
				Expect(e).To(ContainSubstring(controller.EventReasonImageApplied))
			}
		})

		It("t9i-envtest-statefulset-container-image-becomes-the-digest-reference", func() {
			const stream, stsName, subName = "t9i-ac1b-stream", "t9i-ac1b-sts", "t9i-ac1b-sub"
			t9iCreateStream(stream, t9iDigestB)
			t9iCreateStatefulSet(stsName, [2]string{t9iContainer, t9iPreImage})
			sub := t9iNewSub(subName, stream, "", t9iContainerTarget(t9iKindSts, stsName, t9iContainer))
			Expect(k8s.Create(ctx, sub)).To(Succeed())
			t9iAuthorize(subName)

			t9iResetAudit()
			reconciler, _ := t9iHandBuilt(config.Config{})
			_, err := reconciler.Reconcile(ctx, t9iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			Expect(t9iAudit.applies).To(Equal(t9iTakeoverApplies))

			sts := &appsv1.StatefulSet{}
			Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: stsName}, sts)).To(Succeed())
			Expect(sts.Spec.Template.Spec.Containers[0].Image).To(Equal(t9iDigestB))
			Expect(t9iManagerNames(sts)).To(ContainElement(delivery.FieldManagerFor(ns, subName)))
			entry := t9iEntry(t9iGetSub(subName), stsName)
			Expect(entry).NotTo(BeNil())
			Expect(entry.Reason).To(Equal(controller.TargetReasonApplied))
			Expect(entry.ImageApplied).To(BeTrue())
		})

		It("t9i-envtest-daemonset-container-image-becomes-the-digest-reference", func() {
			const stream, dsName, subName = "t9i-ac1c-stream", "t9i-ac1c-ds", "t9i-ac1c-sub"
			t9iCreateStream(stream, t9iDigestC)
			t9iCreateDaemonSet(dsName, [2]string{t9iContainer, t9iPreImage})
			sub := t9iNewSub(subName, stream, "", t9iContainerTarget(t9iKindDs, dsName, t9iContainer))
			Expect(k8s.Create(ctx, sub)).To(Succeed())
			t9iAuthorize(subName)

			t9iResetAudit()
			reconciler, _ := t9iHandBuilt(config.Config{})
			_, err := reconciler.Reconcile(ctx, t9iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			Expect(t9iAudit.applies).To(Equal(t9iTakeoverApplies))

			ds := &appsv1.DaemonSet{}
			Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: dsName}, ds)).To(Succeed())
			Expect(ds.Spec.Template.Spec.Containers[0].Image).To(Equal(t9iDigestC))
			Expect(t9iManagerNames(ds)).To(ContainElement(delivery.FieldManagerFor(ns, subName)))
			entry := t9iEntry(t9iGetSub(subName), dsName)
			Expect(entry).NotTo(BeNil())
			Expect(entry.Reason).To(Equal(controller.TargetReasonApplied))
			Expect(entry.ImageApplied).To(BeTrue())
		})

		It("t9i-envtest-equal-value-reconcile-produces-no-patch", func() {
			const stream, deployName, subName = "t9i-ac2-stream", "t9i-ac2-deploy", "t9i-ac2-sub"
			t9iCreateStream(stream, t9iDigestA)
			t9iCreateDeployment(deployName, [2]string{t9iContainer, t9iPreImage})
			sub := t9iNewSub(subName, stream, "", t9iContainerTarget(t9iKindDeploy, deployName, t9iContainer))
			Expect(k8s.Create(ctx, sub)).To(Succeed())
			t9iAuthorize(subName)

			reconciler, recorder := t9iHandBuilt(config.Config{})
			t9iResetAudit()
			_, err := reconciler.Reconcile(ctx, t9iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			Expect(t9iAudit.applies).To(Equal(t9iTakeoverApplies))
			rvAfterDelivery := t9iGetDeploy(deployName).ResourceVersion
			t9iDrainEvents(recorder)

			By("re-reconciling the converged state must not touch anything")
			t9iResetAudit()
			result, err := reconciler.Reconcile(ctx, t9iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())
			Expect(t9iAudit.applies).To(BeZero(), "equal value + self ownership = ZERO patches")
			Expect(t9iAudit.statusWrites).To(BeZero(), "the equality gate suppresses the status write")
			Expect(t9iAudit.otherWrites).To(BeZero())
			Expect(t9iDrainEvents(recorder)).To(BeEmpty())
			Expect(t9iGetDeploy(deployName).ResourceVersion).To(Equal(rvAfterDelivery))
		})

		It("t9i-envtest-second-subscription-same-field-terminal-conflict-zero-writes", func() {
			const (
				stream      = "t9i-ac3-stream"
				deployName  = "t9i-ac3-deploy"
				firstSub    = "t9i-ac3-first"
				secondSub   = "t9i-ac3-second"
				rivalStream = "t9i-ac3-rival-stream"
			)
			t9iCreateStream(stream, t9iDigestA)
			t9iCreateStream(rivalStream, t9iDigestB)
			t9iCreateDeployment(deployName, [2]string{t9iContainer, t9iPreImage})
			first := t9iNewSub(firstSub, stream, "", t9iContainerTarget(t9iKindDeploy, deployName, t9iContainer))
			Expect(k8s.Create(ctx, first)).To(Succeed())
			t9iAuthorize(firstSub)
			owner, _ := t9iHandBuilt(config.Config{})
			_, err := owner.Reconcile(ctx, t9iRequest(firstSub))
			Expect(err).NotTo(HaveOccurred())
			Expect(t9iGetDeploy(deployName).Spec.Template.Spec.Containers[0].Image).To(Equal(t9iDigestA))

			By("the second subscription on the same field loses (first-wins)")
			second := t9iNewSub(secondSub, rivalStream, "",
				t9iContainerTarget(t9iKindDeploy, deployName, t9iContainer))
			Expect(k8s.Create(ctx, second)).To(Succeed())
			t9iAuthorize(secondSub)
			late, recorder := t9iHandBuilt(config.Config{})
			t9iResetAudit()
			result, err := late.Reconcile(ctx, t9iRequest(secondSub))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero(), "terminal Conflict settles without requeue churn")
			Expect(t9iAudit.applies).To(BeZero(), "the loser must perform ZERO writes")
			Expect(t9iAudit.otherWrites).To(BeZero())

			stored := t9iGetDeploy(deployName)
			Expect(stored.Spec.Template.Spec.Containers[0].Image).To(Equal(t9iDigestA),
				"the occupant's value must survive the conflict")
			Expect(t9iManagerNames(stored)).NotTo(ContainElement(delivery.FieldManagerFor(ns, secondSub)))

			got := t9iGetSub(secondSub)
			entry := t9iEntry(got, deployName)
			Expect(entry).NotTo(BeNil())
			Expect(entry.Reason).To(Equal(controller.TargetReasonConflict))
			Expect(entry.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutConflict))
			Expect(entry.Message).To(ContainSubstring(ns + "/" + firstSub))
			Expect(got.Status.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutConflict))
			cond := t9iCondition(got, imageflowv1alpha1.ConditionTypeConflict)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(controller.ReasonFieldOwnershipConflict))
			Expect(got.Status.RecentDeliveries).To(HaveLen(1))
			Expect(got.Status.RecentDeliveries[0].RolloutResult).
				To(Equal(imageflowv1alpha1.RolloutConflict))
			Expect(t9iDrainEvents(recorder)).To(ContainElement(ContainSubstring(
				"Warning " + controller.EventReasonDeliveryConflict)))
		})

		It("t9i-envtest-different-containers-two-subscriptions-coexist", func() {
			const (
				stream     = "t9i-ac3b-stream"
				sideStream = "t9i-ac3b-side-stream"
				deployName = "t9i-ac3b-deploy"
				appSub     = "t9i-ac3b-app"
				sideSub    = "t9i-ac3b-side"
			)
			t9iCreateStream(stream, t9iDigestA)
			t9iCreateStream(sideStream, t9iDigestB)
			t9iCreateDeployment(deployName,
				[2]string{t9iContainer, t9iPreImage},
				[2]string{t9iSidecar, t9iPreImage})
			appTarget := t9iNewSub(appSub, stream, "",
				t9iContainerTarget(t9iKindDeploy, deployName, t9iContainer))
			Expect(k8s.Create(ctx, appTarget)).To(Succeed())
			sideTarget := t9iNewSub(sideSub, sideStream, "",
				t9iContainerTarget(t9iKindDeploy, deployName, t9iSidecar))
			Expect(k8s.Create(ctx, sideTarget)).To(Succeed())
			t9iAuthorize(appSub)
			t9iAuthorize(sideSub)

			reconciler, _ := t9iHandBuilt(config.Config{})
			t9iResetAudit()
			_, err := reconciler.Reconcile(ctx, t9iRequest(appSub))
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(ctx, t9iRequest(sideSub))
			Expect(err).NotTo(HaveOccurred())
			Expect(t9iAudit.applies).To(Equal(2*t9iTakeoverApplies), "each subscription delivers its own container")

			stored := t9iGetDeploy(deployName)
			Expect(stored.Spec.Template.Spec.Containers).To(HaveLen(2))
			for _, c := range stored.Spec.Template.Spec.Containers {
				switch c.Name {
				case t9iContainer:
					Expect(c.Image).To(Equal(t9iDigestA))
				case t9iSidecar:
					Expect(c.Image).To(Equal(t9iDigestB))
				}
			}
			Expect(t9iManagerNames(stored)).To(ContainElements(
				delivery.FieldManagerFor(ns, appSub), delivery.FieldManagerFor(ns, sideSub)))
			for _, name := range []string{appSub, sideSub} {
				got := t9iGetSub(name)
				entry := t9iEntry(got, deployName)
				Expect(entry).NotTo(BeNil())
				Expect(entry.Reason).To(Equal(controller.TargetReasonApplied))
				Expect(t9iCondition(got, imageflowv1alpha1.ConditionTypeConflict).Status).
					To(Equal(metav1.ConditionFalse))
			}
		})

		It("t9i-envtest-occupant-deletion-reclaims-ownership-via-forced-replay", func() {
			const (
				stream      = "t9i-ac4-stream"
				heirStream  = "t9i-ac4-heir-stream"
				deployName  = "t9i-ac4-deploy"
				occupantSub = "t9i-ac4-occupant"
				heirSub     = "t9i-ac4-heir"
			)
			t9iCreateStream(stream, t9iDigestA)
			t9iCreateStream(heirStream, t9iDigestD)
			t9iCreateDeployment(deployName, [2]string{t9iContainer, t9iPreImage})
			occupant := t9iNewSub(occupantSub, stream, "",
				t9iContainerTarget(t9iKindDeploy, deployName, t9iContainer))
			Expect(k8s.Create(ctx, occupant)).To(Succeed())
			t9iAuthorize(occupantSub)
			first, _ := t9iHandBuilt(config.Config{})
			_, err := first.Reconcile(ctx, t9iRequest(occupantSub))
			Expect(err).NotTo(HaveOccurred())

			heir := t9iNewSub(heirSub, heirStream, "",
				t9iContainerTarget(t9iKindDeploy, deployName, t9iContainer))
			Expect(k8s.Create(ctx, heir)).To(Succeed())
			t9iAuthorize(heirSub)

			By("while the occupant lives, the heir is in terminal Conflict")
			blocked, _ := t9iHandBuilt(config.Config{})
			t9iResetAudit()
			_, err = blocked.Reconcile(ctx, t9iRequest(heirSub))
			Expect(err).NotTo(HaveOccurred())
			Expect(t9iAudit.applies).To(BeZero())
			Expect(t9iEntry(t9iGetSub(heirSub), deployName).Reason).
				To(Equal(controller.TargetReasonConflict))

			By("deleting the occupant frees the field; the next replay reclaims it")
			Expect(k8s.Delete(ctx, t9iGetSub(occupantSub))).To(Succeed())
			reclaim, _ := t9iHandBuilt(config.Config{})
			t9iResetAudit()
			_, err = reclaim.Reconcile(ctx, t9iRequest(heirSub))
			Expect(err).NotTo(HaveOccurred())
			Expect(t9iAudit.applies).To(Equal(1), "the orphaned field must be taken over by force")

			stored := t9iGetDeploy(deployName)
			Expect(stored.Spec.Template.Spec.Containers[0].Image).To(Equal(t9iDigestD))
			Expect(t9iOwnsContainerImage(stored, delivery.FieldManagerFor(ns, heirSub), t9iContainer)).
				To(BeTrue(), "the heir must own the image field after the reclaim")
			Expect(t9iOwnsContainerImage(stored, delivery.FieldManagerFor(ns, occupantSub), t9iContainer)).
				To(BeFalse(), "the vanished occupant must lose the image field; under real SSA it may "+
					"keep shared ownership of the unchanged audit annotation, which is harmless")
			got := t9iGetSub(heirSub)
			entry := t9iEntry(got, deployName)
			Expect(entry.Reason).To(Equal(controller.TargetReasonApplied))
			Expect(entry.ImageApplied).To(BeTrue())
			Expect(got.Status.RecentDeliveries).To(HaveLen(2),
				"the conflict record is followed by the successful takeover record")
			Expect(got.Status.RecentDeliveries[0].Reason).To(Equal(controller.TargetReasonApplied))
			Expect(got.Status.RecentDeliveries[1].Reason).To(Equal(controller.TargetReasonConflict))
		})

		It("t9i-envtest-target-created-after-subscription-delivers-on-replay", func() {
			const stream, deployName, subName = "t9i-ac4b-stream", "t9i-ac4b-deploy", "t9i-ac4b-sub"
			t9iCreateStream(stream, t9iDigestA)
			sub := t9iNewSub(subName, stream, "", t9iContainerTarget(t9iKindDeploy, deployName, t9iContainer))
			Expect(k8s.Create(ctx, sub)).To(Succeed())
			t9iAuthorize(subName)

			reconciler, _ := t9iHandBuilt(config.Config{})
			t9iResetAudit()
			result, err := reconciler.Reconcile(ctx, t9iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(t9iWaitingRequeue),
				"a missing target is a waiting state with a bounded requeue")
			Expect(t9iAudit.applies).To(BeZero())
			Expect(t9iEntry(t9iGetSub(subName), deployName).Reason).
				To(Equal(controller.TargetReasonTargetNotFound))

			By("the target appears later; the replayed reconcile delivers deterministically")
			t9iCreateDeployment(deployName, [2]string{t9iContainer, t9iPreImage})
			t9iResetAudit()
			result, err = reconciler.Reconcile(ctx, t9iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())
			Expect(t9iAudit.applies).To(Equal(t9iTakeoverApplies))
			Expect(t9iGetDeploy(deployName).Spec.Template.Spec.Containers[0].Image).To(Equal(t9iDigestA))
			Expect(t9iEntry(t9iGetSub(subName), deployName).Reason).
				To(Equal(controller.TargetReasonApplied))
		})

		It("t9i-envtest-t8-gate-blocks-and-releases-delivery-on-real-conditions", func() {
			const stream, deployName, subName = "t9i-ac5-stream", "t9i-ac5-deploy", "t9i-ac5-sub"
			t9iCreateStream(stream, t9iDigestA)
			t9iCreateDeployment(deployName, [2]string{t9iContainer, t9iPreImage})
			sub := t9iNewSub(subName, stream, "", t9iContainerTarget(t9iKindDeploy, deployName, t9iContainer))
			Expect(k8s.Create(ctx, sub)).To(Succeed())
			rvBefore := t9iGetDeploy(deployName).ResourceVersion

			reconciler, recorder := t9iHandBuilt(config.Config{})

			By("no Forbidden surface yet: zero writes, bounded requeue")
			t9iResetAudit()
			result, err := reconciler.Reconcile(ctx, t9iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(t9iGatePending))
			Expect(t9iAudit.applies).To(BeZero())
			Expect(t9iAudit.statusWrites).To(BeZero())
			Expect(t9iDrainEvents(recorder)).To(BeEmpty())

			By("Forbidden=True: zero writes, zero status, no requeue churn")
			t9iSetGate(subName, metav1.ConditionTrue, controller.ReasonTargetsUnauthorized)
			t9iResetAudit()
			result, err = reconciler.Reconcile(ctx, t9iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())
			Expect(t9iAudit.applies).To(BeZero())
			Expect(t9iAudit.statusWrites).To(BeZero())
			Expect(t9iGetSub(subName).Status.Targets).To(BeEmpty())
			Expect(t9iGetDeploy(deployName).ResourceVersion).To(Equal(rvBefore))

			By("re-authorized: the very same pass delivers")
			t9iAuthorize(subName)
			t9iResetAudit()
			result, err = reconciler.Reconcile(ctx, t9iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())
			Expect(t9iAudit.applies).To(Equal(t9iTakeoverApplies))
			Expect(t9iGetDeploy(deployName).Spec.Template.Spec.Containers[0].Image).To(Equal(t9iDigestA))
			Expect(t9iEntry(t9iGetSub(subName), deployName).Reason).
				To(Equal(controller.TargetReasonApplied))
		})

		It("t9i-envtest-crash-before-status-write-recovers-from-persisted-ownership", func() {
			const stream, deployName, subName = "t9i-ac7-stream", "t9i-ac7-deploy", "t9i-ac7-sub"
			t9iCreateStream(stream, t9iDigestA)
			t9iCreateDeployment(deployName, [2]string{t9iContainer, t9iPreImage})
			sub := t9iNewSub(subName, stream, "", t9iContainerTarget(t9iKindDeploy, deployName, t9iContainer))
			Expect(k8s.Create(ctx, sub)).To(Succeed())
			t9iAuthorize(subName)

			By("the process dies between the workload write and the status write")
			t9iAudit.blockStatus = apierrors.NewServiceUnavailable("simulated crash")
			crashed, recorder := t9iHandBuilt(config.Config{})
			t9iResetAudit()
			t9iAudit.blockStatus = apierrors.NewServiceUnavailable("simulated crash")
			_, err := crashed.Reconcile(ctx, t9iRequest(subName))
			Expect(err).To(HaveOccurred())
			Expect(t9iAudit.applies).To(Equal(t9iTakeoverApplies), "the SSA apply must have landed before the crash")
			Expect(t9iGetDeploy(deployName).Spec.Template.Spec.Containers[0].Image).To(Equal(t9iDigestA))
			Expect(t9iGetSub(subName).Status.Targets).To(BeEmpty())
			Expect(t9iDrainEvents(recorder)).To(BeEmpty(),
				"no effects may be emitted when the status write was lost")

			By("the restarted controller converges from persisted state WITHOUT re-applying")
			t9iResetAudit()
			restarted, freshRecorder := t9iHandBuilt(config.Config{})
			_, err = restarted.Reconcile(ctx, t9iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			Expect(t9iAudit.applies).To(BeZero(),
				"real managedFields must prove self-ownership — no write amplification after restart")
			Expect(t9iAudit.statusWrites).To(Equal(1))
			got := t9iGetSub(subName)
			entry := t9iEntry(got, deployName)
			Expect(entry).NotTo(BeNil())
			Expect(entry.Reason).To(Equal(controller.TargetReasonApplied))
			Expect(entry.ImageApplied).To(BeTrue())
			Expect(entry.DesiredImage).To(Equal(t9iDigestA))
			Expect(entry.ObservedImage).To(Equal(t9iDigestA))
			Expect(got.Status.RecentDeliveries).To(HaveLen(1))
			Expect(t9iDrainEvents(freshRecorder)).To(ContainElement(ContainSubstring(
				controller.EventReasonImageApplied)))
		})

		It("t9i-envtest-recent-deliveries-bounded-and-superseded-latest-wins", func() {
			const stream, deployName, subName = "t9i-ac8-stream", "t9i-ac8-deploy", "t9i-ac8-sub"
			t9iCreateStream(stream, t9iDigestA)
			t9iCreateDeployment(deployName, [2]string{t9iContainer, t9iPreImage})
			sub := t9iNewSub(subName, stream, "", t9iContainerTarget(t9iKindDeploy, deployName, t9iContainer))
			Expect(k8s.Create(ctx, sub)).To(Succeed())
			t9iAuthorize(subName)

			reconciler, recorder := t9iHandBuilt(config.Config{SubscriptionDeliveryLimit: 2})
			_, err := reconciler.Reconcile(ctx, t9iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			t9iDrainEvents(recorder)

			t9iRetagStream(stream, t9iDigestB)
			_, err = reconciler.Reconcile(ctx, t9iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			t9iDrainEvents(recorder)
			window := t9iGetSub(subName).Status.RecentDeliveries
			Expect(window).To(HaveLen(2))
			Expect(window[0].NewImage).To(Equal(t9iDigestB))
			Expect(window[0].RolloutResult).To(Equal(imageflowv1alpha1.RolloutProgressing))
			Expect(window[1].NewImage).To(Equal(t9iDigestA))
			Expect(window[1].RolloutResult).To(Equal(imageflowv1alpha1.RolloutSuperseded),
				"the in-flight older delivery is terminal Superseded (latest-wins)")

			t9iRetagStream(stream, t9iDigestC)
			_, err = reconciler.Reconcile(ctx, t9iRequest(subName))
			Expect(err).NotTo(HaveOccurred())
			window = t9iGetSub(subName).Status.RecentDeliveries
			Expect(window).To(HaveLen(2), "the window is clamped to the configured limit")
			Expect(window[0].NewImage).To(Equal(t9iDigestC))
			Expect(window[1].NewImage).To(Equal(t9iDigestB))
			Expect(window[1].RolloutResult).To(Equal(imageflowv1alpha1.RolloutSuperseded))
			Expect(t9iDrainEvents(recorder)).To(ContainElement(ContainSubstring(
				controller.EventReasonDeliveryHistoryTruncated)))
			Expect(t9iGetDeploy(deployName).Spec.Template.Spec.Containers[0].Image).To(Equal(t9iDigestC))
		})
	})

	// --- manager-driven specs (production registrars, watch wiring) ---------

	Context("with the T8 gate and T9 engine running in a manager", Ordered, func() {
		var (
			mgrCancel context.CancelFunc
			mgrDone   chan struct{}
		)

		startManagers := func(authzName, deliveryName string) {
			scheme := runtime.NewScheme()
			Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
			Expect(imageflowv1alpha1.AddToScheme(scheme)).To(Succeed())
			mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
				Scheme: scheme,
				Metrics: metricsserver.Options{
					BindAddress: "0",
				},
			})
			Expect(err).NotTo(HaveOccurred())
			cfg := config.Config{SARRecheckPeriod: t9iSARPeriod}
			Expect(controller.RegisterSubscriptionAuthz(mgr, authzName,
				controller.Options{Clock: clock.Real{}, Config: cfg})).To(Succeed())
			Expect(controller.RegisterSubscriptionDelivery(mgr, deliveryName,
				controller.Options{Clock: clock.Real{}, Config: cfg})).To(Succeed())
			var mgrCtx context.Context
			mgrCtx, mgrCancel = context.WithCancel(ctx)
			mgrDone = make(chan struct{})
			go func() {
				defer GinkgoRecover()
				defer close(mgrDone)
				Expect(mgr.Start(mgrCtx)).To(Succeed())
			}()
			Expect(mgr.GetCache().WaitForCacheSync(mgrCtx)).To(BeTrue(),
				"the informer caches must settle before specs assert on watches")
		}

		BeforeAll(func() {
			t9iGrantWorkloadUpdate("t9i-creator-role", "t9i-creator-binding", t9iCreatorUser)
			startManagers(t9iAuthzName, t9iDeliveryName)
		})

		AfterAll(func() {
			if mgrCancel != nil {
				mgrCancel()
				Eventually(mgrDone, 30*time.Second, 100*time.Millisecond).Should(BeClosed())
			}
		})

		It("t9i-envtest-image-change-flows-through-the-authz-gate-into-a-real-ssa-apply", func() {
			t9iCreateStream(t9iLiveStream, t9iDigestA)
			t9iCreateDeployment(t9iLiveDeploy, [2]string{t9iContainer, t9iPreImage})
			sub := t9iNewSub(t9iLiveSub, t9iLiveStream, t9iCreatorIdent,
				t9iContainerTarget(t9iKindDeploy, t9iLiveDeploy, t9iContainer))
			Expect(k8s.Create(ctx, sub)).To(Succeed())

			By("the T8 gate authorizes the granted creator and T9 delivers the digest")
			Eventually(func(g Gomega) {
				deploy := t9iGetDeploy(t9iLiveDeploy)
				g.Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal(t9iDigestA))
				got := t9iGetSub(t9iLiveSub)
				entry := t9iEntry(got, t9iLiveDeploy)
				g.Expect(entry).NotTo(BeNil())
				g.Expect(entry.Reason).To(Equal(controller.TargetReasonApplied))
				g.Expect(entry.ImageApplied).To(BeTrue())
				g.Expect(entry.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutProgressing))
				g.Expect(got.Status.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutProgressing))
				cond := t9iCondition(got, imageflowv1alpha1.ConditionTypeForbidden)
				g.Expect(cond).NotTo(BeNil())
				g.Expect(cond.Status).To(Equal(metav1.ConditionFalse),
					"the real T8 controller must authorize the granted creator via real SAR")
				g.Expect(cond.Reason).To(Equal(controller.ReasonAuthorized))
			}, 90*time.Second, 250*time.Millisecond).Should(Succeed())

			By("dual-namespace ImageApplied events reach both involved objects")
			Eventually(func(g Gomega) {
				g.Expect(t9iEventReasonsOn(t9iLiveSub)).
					To(ContainElement(controller.EventReasonImageApplied))
				g.Expect(t9iEventReasonsOn(t9iLiveDeploy)).
					To(ContainElement(controller.EventReasonImageApplied))
			}, 30*time.Second, 250*time.Millisecond).Should(Succeed())

			By("the delivery metrics land on the process-wide registry")
			Eventually(func(g Gomega) {
				g.Expect(t9iHistogramCount("imageflow_delivery_latency_seconds")).
					To(BeNumerically(">=", uint64(1)))
				g.Expect(t9iCounterValue("imageflow_delivery_applies_total", "result", "success")).
					To(BeNumerically(">=", float64(1)))
			}, 30*time.Second, 250*time.Millisecond).Should(Succeed())

			By("a fresh registration (Image Change) is picked up through the watch wiring")
			t9iRetagStream(t9iLiveStream, t9iDigestB)
			Eventually(func(g Gomega) {
				deploy := t9iGetDeploy(t9iLiveDeploy)
				g.Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal(t9iDigestB))
				got := t9iGetSub(t9iLiveSub)
				window := got.Status.RecentDeliveries
				g.Expect(window).To(HaveLen(2))
				g.Expect(window[0].NewImage).To(Equal(t9iDigestB))
				g.Expect(window[0].RolloutResult).To(Equal(imageflowv1alpha1.RolloutProgressing))
				g.Expect(window[1].NewImage).To(Equal(t9iDigestA))
				g.Expect(window[1].RolloutResult).To(Equal(imageflowv1alpha1.RolloutSuperseded))
				g.Expect(t9iHistogramCount("imageflow_delivery_latency_seconds")).
					To(BeNumerically(">=", uint64(2)))
			}, 90*time.Second, 250*time.Millisecond).Should(Succeed())
		})

		It("t9i-envtest-manager-restart-converges-from-persisted-state-without-write-amplification", func() {
			rvBefore := t9iGetDeploy(t9iLiveDeploy).ResourceVersion
			recordsBefore := len(t9iGetSub(t9iLiveSub).Status.RecentDeliveries)

			By("the manager process dies")
			mgrCancel()
			Eventually(mgrDone, 30*time.Second, 100*time.Millisecond).Should(BeClosed())

			By("a fresh manager (controller restart) takes over from persisted state")
			startManagers(t9iRestartAuthz, t9iRestartDelivery)

			Consistently(func(g Gomega) {
				deploy := t9iGetDeploy(t9iLiveDeploy)
				g.Expect(deploy.ResourceVersion).To(Equal(rvBefore),
					"the converged workload must not be rewritten after a restart")
				got := t9iGetSub(t9iLiveSub)
				g.Expect(got.Status.RecentDeliveries).To(HaveLen(recordsBefore),
					"the restart must not duplicate delivery records")
				entry := t9iEntry(got, t9iLiveDeploy)
				g.Expect(entry).NotTo(BeNil())
				g.Expect(entry.Reason).To(Equal(controller.TargetReasonApplied))
			}, 6*time.Second, 500*time.Millisecond).Should(Succeed())

			By("the restarted engine still reacts to new Image Changes")
			t9iRetagStream(t9iLiveStream, t9iDigestC)
			Eventually(func(g Gomega) {
				deploy := t9iGetDeploy(t9iLiveDeploy)
				g.Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal(t9iDigestC))
				got := t9iGetSub(t9iLiveSub)
				entry := t9iEntry(got, t9iLiveDeploy)
				g.Expect(entry).NotTo(BeNil())
				g.Expect(entry.ObservedImage).To(Equal(t9iDigestC))
			}, 90*time.Second, 250*time.Millisecond).Should(Succeed())
		})
	})
})
