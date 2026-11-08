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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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
	"github.com/appfun-dev/imageflow/internal/controller/cueeval"
)

// T11 named cases: the workload observer against a REAL API server.
//
// envtest runs etcd + kube-apiserver only — there is NO kube-controller-
// manager, hence no native rollout machinery. Exactly as the ticket
// prescribes, the rollout determination specs DRIVE the observer by writing
// the workload status subresources directly (observedGeneration, conditions,
// replica/revision/scheduled-number counters) and asserting the honest
// per-kind determination: progressing / succeeded / failed / unknown for
// Deployment, StatefulSet and DaemonSet. The kind e2e lane
// (test/e2e/observer_e2e_test.go) covers the same matrix with real rollouts.
//
// Layering mirrors the T9 lane: the hand-built context drives the production
// observer reconciler with an interceptor-audited client and the injected
// fake clock; one spec chains the REAL T9 delivery engine into the observer
// (latest-wins / Superseded handoff). The manager context boots the
// production registrar and proves the status-driven watch wiring, real
// dual-namespace Events on the API server, the process-registry metrics and
// watch quiescence (no self-triggered hot loop).
//
// Controller names carry the "-t11" suffix: controller-runtime validates
// controller names process-globally and other lanes in this package boot
// their own managers.
var _ = Describe("T11 workload observer", Label("t11"), Ordered, func() {
	var (
		t11iEnv   *envtest.Environment
		restCfg   *rest.Config
		k8s       client.Client
		watchCl   client.WithWatch
		ctx       context.Context
		cancel    context.CancelFunc
		ns        string
		nsWL      string
		t11iClock *clock.Fake
	)

	const (
		t11iObserverName = "subscription-observer-t11"
		t11iKindDeploy   = "Deployment"
		t11iKindSts      = "StatefulSet"
		t11iKindDs       = "DaemonSet"
		t11iAPIApps      = "apps/v1"
		t11iAPIEdge      = "example.com/v1"
		t11iKindEdge     = "EdgeService"
		t11iSourceKind   = "ImageStreamTag"
		t11iTag          = "current"
		t11iContainer    = "app"
		t11iPreImage     = "quay.io/example/checkout:pre-t11"
		t11iRepo         = "quay.io/example/checkout@sha256:"
		t11iLabelApp     = "t11i-app"
		t11iStatusSub    = "status"
		t11iGateMessage  = "t11i lane: gate surface written by the test"
		t11iEdgeField    = ".spec.template.image"
		t11iRevisionOld  = "t11i-rev-1"
		t11iRevisionNew  = "t11i-rev-2"
		t11iWaiting      = 30 * time.Second
		// Native-condition reasons written into the simulated workload
		// status (values the real kube controllers would produce).
		t11iReasonAvailable      = "MinimumReplicasAvailable"
		t11iReasonUpdating       = "ReplicaSetUpdated"
		t11iReasonDeadline       = "ProgressDeadlineExceeded"
		t11iMsgNeverFabricated   = "never fabricated"
		t11iMsgDeliverySummary   = "Delivery summary"
		t11iMsgAuditStages       = "stages — imageResolved"
		t11iMirrorTruncFam       = "imageflow_mirror_events_truncated_total"
		t11iObserverRolloutFam   = "imageflow_observer_rollout_outcomes_total"
		t11iObserverHealthFam    = "imageflow_observer_health_outcomes_total"
		t11iMirrorKindIST        = "ImageStreamTag"
		t11iObserverStageHealth  = "health: Unknown"
		t11iObserverStageApplied = "imageApplied: true"
	)

	var (
		t11iDigestA = t11iRepo + strings.Repeat("a1", 32)
		t11iDigestB = t11iRepo + strings.Repeat("b2", 32)
		t11iDigestC = t11iRepo + strings.Repeat("c3", 32)
		t11iBareA   = "sha256:" + strings.Repeat("a1", 32)
		t11iBareB   = "sha256:" + strings.Repeat("b2", 32)
	)

	BeforeAll(func() {
		assets := os.Getenv("KUBEBUILDER_ASSETS")
		if assets == "" {
			Skip("KUBEBUILDER_ASSETS not set")
		}
		t11iEnv = &envtest.Environment{
			BinaryAssetsDirectory: assets,
			CRDDirectoryPaths: []string{
				"../../config/crd/bases",
			},
			ErrorIfCRDPathMissing: true,
		}
		var err error
		restCfg, err = t11iEnv.Start()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(t11iEnv.Stop()).To(Succeed())
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

		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "t11i-observer-"}}
		Expect(k8s.Create(ctx, namespace)).To(Succeed())
		ns = namespace.Name
		// Second namespace: the manager lane proves the workload-side Event
		// really lands in the TARGET namespace (cross-ns delivery audit).
		wlNamespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "t11i-workload-"}}
		Expect(k8s.Create(ctx, wlNamespace)).To(Succeed())
		nsWL = wlNamespace.Name
		t11iClock = clock.NewFake(time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC))
	})

	// --- helpers ------------------------------------------------------------

	// t11iAudit counts every mutating call of the reconcilers under test
	// (same discipline as the T9 lane; Patch options MUST be forwarded —
	// FieldOwner/ForceOwnership carry the SSA semantics of the engine lane).
	t11iAudit := struct {
		applies      int
		statusWrites int
		otherWrites  int
	}{}

	t11iResetAudit := func() {
		t11iAudit.applies, t11iAudit.statusWrites, t11iAudit.otherWrites = 0, 0, 0
	}

	t11iIntercepted := func() client.WithWatch {
		return interceptor.NewClient(watchCl, interceptor.Funcs{
			Create: func(c context.Context, cl client.WithWatch, obj client.Object,
				_ ...client.CreateOption) error {
				t11iAudit.otherWrites++
				return cl.Create(c, obj)
			},
			Update: func(c context.Context, cl client.WithWatch, obj client.Object,
				opts ...client.UpdateOption) error {
				t11iAudit.otherWrites++
				return cl.Update(c, obj, opts...)
			},
			Patch: func(c context.Context, cl client.WithWatch, obj client.Object,
				p client.Patch, opts ...client.PatchOption) error {
				if p.Type() == types.ApplyPatchType {
					t11iAudit.applies++
				} else {
					t11iAudit.otherWrites++
				}
				return cl.Patch(c, obj, p, opts...)
			},
			Delete: func(c context.Context, cl client.WithWatch, obj client.Object,
				_ ...client.DeleteOption) error {
				t11iAudit.otherWrites++
				return cl.Delete(c, obj)
			},
			SubResourceUpdate: func(c context.Context, cl client.Client, subResource string,
				obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if _, ok := obj.(*imageflowv1alpha1.ImageSubscription); ok && subResource == t11iStatusSub {
					t11iAudit.statusWrites++
				} else {
					t11iAudit.otherWrites++
				}
				return cl.SubResource(subResource).Update(c, obj, opts...)
			},
			SubResourcePatch: func(c context.Context, cl client.Client, subResource string,
				obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
				t11iAudit.otherWrites++
				return cl.SubResource(subResource).Patch(c, obj, p, opts...)
			},
		})
	}

	t11iObserver := func(cfg config.Config) (*controller.SubscriptionObserverReconciler, *record.FakeRecorder) {
		recorder := record.NewFakeRecorder(256)
		return controller.NewSubscriptionObserverReconciler(
			t11iIntercepted(), nil, recorder, t11iClock, cfg, nil, cueeval.NewEvaluator()), recorder
	}

	t11iEngine := func(cfg config.Config) (*controller.SubscriptionDeliveryReconciler, *record.FakeRecorder) {
		recorder := record.NewFakeRecorder(256)
		return controller.NewSubscriptionDeliveryReconciler(
			t11iIntercepted(), nil, recorder, t11iClock, cfg, nil), recorder
	}

	t11iRequest := func(name string) ctrl.Request {
		return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}
	}

	t11iGetSub := func(name string) *imageflowv1alpha1.ImageSubscription {
		sub := &imageflowv1alpha1.ImageSubscription{}
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, sub)).To(Succeed())
		return sub
	}

	t11iEntry := func(sub *imageflowv1alpha1.ImageSubscription,
		name string) *imageflowv1alpha1.TargetDeliveryStatus {
		for i := range sub.Status.Targets {
			if sub.Status.Targets[i].TargetRef.Name == name {
				return &sub.Status.Targets[i]
			}
		}
		return nil
	}

	t11iDrainEvents := func(rec *record.FakeRecorder) []string {
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

	t11iCountContaining := func(events []string, fragment string) int {
		n := 0
		for _, e := range events {
			if strings.Contains(e, fragment) {
				n++
			}
		}
		return n
	}

	t11iNewSub := func(name, stream string,
		targets ...imageflowv1alpha1.SubscriptionTarget) *imageflowv1alpha1.ImageSubscription {
		return &imageflowv1alpha1.ImageSubscription{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Spec: imageflowv1alpha1.ImageSubscriptionSpec{
				Source: imageflowv1alpha1.SubscriptionSourceRef{
					Kind:      t11iSourceKind,
					Name:      stream + ":" + t11iTag,
					Namespace: ns,
				},
				Targets: targets,
			},
		}
	}

	t11iContainerTarget := func(kind, targetNs, name string,
		containers ...string) imageflowv1alpha1.SubscriptionTarget {
		return imageflowv1alpha1.SubscriptionTarget{
			TargetRef: imageflowv1alpha1.TargetReference{
				APIVersion: t11iAPIApps,
				Kind:       kind,
				Namespace:  targetNs,
				Name:       name,
			},
			ContainerNames: containers,
		}
	}

	// t11iAuthorize writes the T8 gate surface directly — the hand-built
	// lane drives the observer in isolation (the manager lane of THIS file
	// registers the observer only; the real T8 controller is pinned by the
	// T9 lane).
	t11iAuthorize := func(subName string) {
		sub := t11iGetSub(subName)
		meta.SetStatusCondition(&sub.Status.Conditions, metav1.Condition{
			Type:               imageflowv1alpha1.ConditionTypeForbidden,
			Status:             metav1.ConditionFalse,
			Reason:             controller.ReasonAuthorized,
			Message:            t11iGateMessage,
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: sub.Generation,
		})
		Expect(k8s.Status().Update(ctx, sub)).To(Succeed())
	}

	// t11iAppliedEntry seeds a per-target entry exactly as the T9 engine
	// leaves it after a value-changing apply: fields written, rollout
	// Progressing, health Unknown, reason Applied.
	t11iAppliedEntry := func(kind, targetNs, name,
		digestRef string) imageflowv1alpha1.TargetDeliveryStatus {
		return imageflowv1alpha1.TargetDeliveryStatus{
			TargetRef: imageflowv1alpha1.TargetReference{
				APIVersion: t11iAPIApps,
				Kind:       kind,
				Namespace:  targetNs,
				Name:       name,
			},
			DesiredImage:  digestRef,
			ObservedImage: digestRef,
			ImageApplied:  true,
			RolloutStatus: imageflowv1alpha1.RolloutProgressing,
			HealthStatus:  imageflowv1alpha1.HealthUnknown,
			Reason:        controller.TargetReasonApplied,
			// Seeded generation of a freshly created workload.
			ObservedGeneration: 1,
			LastTransitionTime: ptr.To(metav1.NewTime(t11iClock.Now().Add(-time.Minute))),
		}
	}

	t11iEdgeEntry := func(name, digestRef string) imageflowv1alpha1.TargetDeliveryStatus {
		entry := t11iAppliedEntry(t11iKindEdge, ns, name, digestRef)
		entry.TargetRef.APIVersion = t11iAPIEdge
		return entry
	}

	t11iRecord := func(stream, oldImage, newImage, kind, targetNs, name string,
		at time.Time) imageflowv1alpha1.DeliveryResult {
		return imageflowv1alpha1.DeliveryResult{
			Source: imageflowv1alpha1.SubscriptionSourceRef{
				Kind:      t11iSourceKind,
				Name:      stream + ":" + t11iTag,
				Namespace: ns,
			},
			OldImage:      oldImage,
			NewImage:      newImage,
			TargetRef:     t11iAppliedEntry(kind, targetNs, name, newImage).TargetRef,
			ImageApplied:  true,
			RolloutResult: imageflowv1alpha1.RolloutProgressing,
			HealthResult:  imageflowv1alpha1.HealthUnknown,
			Reason:        controller.TargetReasonApplied,
			ObservedAt:    metav1.NewTime(at),
		}
	}

	t11iSeedStatus := func(subName string, entries []imageflowv1alpha1.TargetDeliveryStatus,
		records []imageflowv1alpha1.DeliveryResult) {
		sub := t11iGetSub(subName)
		sub.Status.Targets = entries
		sub.Status.RecentDeliveries = records
		Expect(k8s.Status().Update(ctx, sub)).To(Succeed())
	}

	t11iPodSpec := func(image string) corev1.PodSpec {
		return corev1.PodSpec{
			Containers: []corev1.Container{{Name: t11iContainer, Image: image}},
		}
	}

	t11iSelector := func(name string) (map[string]string, *metav1.LabelSelector) {
		labels := map[string]string{t11iLabelApp: name}
		return labels, &metav1.LabelSelector{MatchLabels: labels}
	}

	t11iCreateDeployment := func(namespace, name, image string, replicas int32) {
		labels, selector := t11iSelector(name)
		Expect(k8s.Create(ctx, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To(replicas),
				Selector: selector,
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       t11iPodSpec(image),
				},
			},
		})).To(Succeed())
	}

	t11iCreateStatefulSet := func(namespace, name, image string, replicas int32) {
		labels, selector := t11iSelector(name)
		Expect(k8s.Create(ctx, &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: appsv1.StatefulSetSpec{
				Replicas:    ptr.To(replicas),
				ServiceName: name + "-svc",
				Selector:    selector,
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       t11iPodSpec(image),
				},
			},
		})).To(Succeed())
	}

	t11iCreateDaemonSet := func(namespace, name, image string) {
		labels, selector := t11iSelector(name)
		Expect(k8s.Create(ctx, &appsv1.DaemonSet{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: appsv1.DaemonSetSpec{
				Selector: selector,
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       t11iPodSpec(image),
				},
			},
		})).To(Succeed())
	}

	t11iGetDeploy := func(namespace, name string) *appsv1.Deployment {
		deploy := &appsv1.Deployment{}
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, deploy)).To(Succeed())
		return deploy
	}

	// --- direct workload status writes (the envtest rollout simulator) -------

	t11iSetDeploymentStatus := func(namespace, name string,
		mutate func(status *appsv1.DeploymentStatus, generation int64)) {
		deploy := t11iGetDeploy(namespace, name)
		mutate(&deploy.Status, deploy.Generation)
		Expect(k8s.Status().Update(ctx, deploy)).To(Succeed())
	}

	t11iDeployCond := func(t appsv1.DeploymentConditionType,
		status corev1.ConditionStatus, reason string) appsv1.DeploymentCondition {
		return appsv1.DeploymentCondition{
			Type:               t,
			Status:             status,
			Reason:             reason,
			LastTransitionTime: metav1.Now(),
			LastUpdateTime:     metav1.Now(),
		}
	}

	// t11iDeployProgressing simulates a mid-rollout Deployment: the new
	// ReplicaSet is partially updated, the deployment stays Available.
	t11iDeployProgressing := func(namespace, name string, replicas, updated int32) {
		t11iSetDeploymentStatus(namespace, name, func(s *appsv1.DeploymentStatus, gen int64) {
			s.ObservedGeneration = gen
			s.Replicas = replicas
			s.UpdatedReplicas = updated
			s.AvailableReplicas = updated
			// The API server validates deployment status invariants: an
			// available replica is always a ready replica.
			s.ReadyReplicas = updated
			s.Conditions = []appsv1.DeploymentCondition{
				t11iDeployCond(appsv1.DeploymentAvailable, corev1.ConditionTrue, t11iReasonAvailable),
				t11iDeployCond(appsv1.DeploymentProgressing, corev1.ConditionTrue, t11iReasonUpdating),
			}
		})
	}

	t11iDeploySucceeded := func(namespace, name string, replicas int32) {
		t11iSetDeploymentStatus(namespace, name, func(s *appsv1.DeploymentStatus, gen int64) {
			s.ObservedGeneration = gen
			s.Replicas = replicas
			s.UpdatedReplicas = replicas
			s.AvailableReplicas = replicas
			s.ReadyReplicas = replicas
			s.Conditions = []appsv1.DeploymentCondition{
				t11iDeployCond(appsv1.DeploymentAvailable, corev1.ConditionTrue, t11iReasonAvailable),
				t11iDeployCond(appsv1.DeploymentProgressing, corev1.ConditionTrue, t11iReasonUpdating),
			}
		})
	}

	t11iDeployDeadlineFailed := func(namespace, name string, replicas, updated int32) {
		t11iSetDeploymentStatus(namespace, name, func(s *appsv1.DeploymentStatus, gen int64) {
			s.ObservedGeneration = gen
			s.Replicas = replicas
			s.UpdatedReplicas = updated
			s.AvailableReplicas = updated
			s.ReadyReplicas = updated
			s.Conditions = []appsv1.DeploymentCondition{
				t11iDeployCond(appsv1.DeploymentAvailable, corev1.ConditionTrue, t11iReasonAvailable),
				t11iDeployCond(appsv1.DeploymentProgressing, corev1.ConditionFalse, t11iReasonDeadline),
			}
		})
	}

	t11iSetStsStatus := func(namespace, name, updateRev, currentRev string,
		replicas, updated, ready int32) {
		sts := &appsv1.StatefulSet{}
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, sts)).To(Succeed())
		sts.Status.ObservedGeneration = sts.Generation
		sts.Status.Replicas = replicas
		sts.Status.UpdatedReplicas = updated
		sts.Status.ReadyReplicas = ready
		sts.Status.CurrentRevision = currentRev
		sts.Status.UpdateRevision = updateRev
		Expect(k8s.Status().Update(ctx, sts)).To(Succeed())
	}

	t11iSetDsStatus := func(namespace, name string,
		desired, updated, available, ready, misscheduled int32) {
		ds := &appsv1.DaemonSet{}
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, ds)).To(Succeed())
		ds.Status.ObservedGeneration = ds.Generation
		ds.Status.DesiredNumberScheduled = desired
		ds.Status.UpdatedNumberScheduled = updated
		ds.Status.NumberAvailable = available
		ds.Status.NumberReady = ready
		ds.Status.NumberMisscheduled = misscheduled
		Expect(k8s.Status().Update(ctx, ds)).To(Succeed())
	}

	// --- stream helpers (engine handoff lane) --------------------------------

	t11iRetagStream := func(name, digest string) {
		stream := &imageflowv1alpha1.ImageStream{}
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, stream)).To(Succeed())
		parts := strings.SplitN(digest, "@", 2)
		stream.Status.Tags = []imageflowv1alpha1.NamedTagEventList{{
			Tag: t11iTag,
			Items: []imageflowv1alpha1.TagEvent{{
				ImageReference: digest,
				Digest:         parts[1],
				Created:        metav1.NewTime(time.Now().Add(-time.Minute)),
			}},
		}}
		Expect(k8s.Status().Update(ctx, stream)).To(Succeed())
	}

	t11iCreateStream := func(name, digest string) {
		stream := &imageflowv1alpha1.ImageStream{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		}
		Expect(k8s.Create(ctx, stream)).To(Succeed())
		t11iRetagStream(name, digest)
	}

	// --- mirror helpers (float-up lane) --------------------------------------

	t11iImageChangedEvents := func(count int, base time.Time) []imageflowv1alpha1.MirrorStatusEvent {
		events := make([]imageflowv1alpha1.MirrorStatusEvent, 0, count)
		for i := range count {
			events = append(events, imageflowv1alpha1.MirrorStatusEvent{
				Type:       imageflowv1alpha1.MirrorEventImageChanged,
				Reason:     "Associated",
				Message:    "seeded ImageChanged summary",
				ObservedAt: metav1.NewTime(base.Add(-time.Duration(i+1) * time.Minute)),
			})
		}
		return events
	}

	t11iCreateMirrorIST := func(stream, tag string,
		events []imageflowv1alpha1.MirrorStatusEvent,
		consumers []imageflowv1alpha1.ConsumerReference) *imageflowv1alpha1.ImageStreamTag {
		ist := &imageflowv1alpha1.ImageStreamTag{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: controller.EncodeImageStreamTagName(stream, tag)},
			Spec: imageflowv1alpha1.ImageStreamTagSpec{
				Stream: stream,
				Tag:    tag,
			},
		}
		Expect(k8s.Create(ctx, ist)).To(Succeed())
		ist.Status.Events = events
		ist.Status.Consumers = consumers
		Expect(k8s.Status().Update(ctx, ist)).To(Succeed())
		return ist
	}

	t11iCreateMirrorIT := func(stream, digest string,
		events []imageflowv1alpha1.MirrorStatusEvent) *imageflowv1alpha1.ImageTag {
		it := &imageflowv1alpha1.ImageTag{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: controller.EncodeImageTagName(stream, digest)},
			Spec: imageflowv1alpha1.ImageTagSpec{
				Stream: stream,
				Digest: digest,
			},
		}
		Expect(k8s.Create(ctx, it)).To(Succeed())
		it.Status.Events = events
		Expect(k8s.Status().Update(ctx, it)).To(Succeed())
		return it
	}

	t11iGetMirrorIST := func(name string) *imageflowv1alpha1.ImageStreamTag {
		ist := &imageflowv1alpha1.ImageStreamTag{}
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, ist)).To(Succeed())
		return ist
	}

	t11iGetMirrorIT := func(name string) *imageflowv1alpha1.ImageTag {
		it := &imageflowv1alpha1.ImageTag{}
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, it)).To(Succeed())
		return it
	}

	// --- real-cluster readers (manager lane) ---------------------------------

	t11iRealEventReasons := func(namespace, involvedName string) []string {
		var list corev1.EventList
		Expect(k8s.List(ctx, &list, client.InNamespace(namespace))).To(Succeed())
		var reasons []string
		for _, event := range list.Items {
			if event.InvolvedObject.Name == involvedName {
				reasons = append(reasons, event.Reason)
			}
		}
		return reasons
	}

	t11iRealEventMessages := func(namespace, involvedName, reason string) []string {
		var list corev1.EventList
		Expect(k8s.List(ctx, &list, client.InNamespace(namespace))).To(Succeed())
		var messages []string
		for _, event := range list.Items {
			if event.InvolvedObject.Name == involvedName && event.Reason == reason {
				messages = append(messages, event.Message)
			}
		}
		return messages
	}

	t11iGatherCounter := func(family string, want map[string]string) float64 {
		mfs, err := ctrlmetrics.Registry.Gather()
		Expect(err).NotTo(HaveOccurred())
		var total float64
		for _, mf := range mfs {
			if mf.GetName() != family {
				continue
			}
			for _, m := range mf.GetMetric() {
				matched := true
				for _, lp := range m.GetLabel() {
					if wantValue, ok := want[lp.GetName()]; ok && wantValue != lp.GetValue() {
						matched = false
					}
				}
				if matched && m.GetCounter() != nil {
					total += m.GetCounter().GetValue()
				}
			}
		}
		return total
	}

	// --- hand-built lane ------------------------------------------------------

	It("t11i-envtest-deployment-native-rollout-progression-observed-stage-by-stage", func() {
		const (
			deployName = "t11i-ac2-prog"
			subName    = "t11i-ac2-prog-sub"
			streamName = "t11i-ac2-prog-stream"
		)
		t11iCreateDeployment(ns, deployName, t11iDigestA, 2)
		Expect(k8s.Create(ctx, t11iNewSub(subName, streamName,
			t11iContainerTarget(t11iKindDeploy, ns, deployName, t11iContainer)))).To(Succeed())
		t11iAuthorize(subName)
		t11iSeedStatus(subName,
			[]imageflowv1alpha1.TargetDeliveryStatus{
				t11iAppliedEntry(t11iKindDeploy, ns, deployName, t11iDigestA),
			},
			[]imageflowv1alpha1.DeliveryResult{
				t11iRecord(streamName, t11iDigestB, t11iDigestA, t11iKindDeploy, ns, deployName,
					t11iClock.Now().Add(-time.Minute)),
			})

		obs, rec := t11iObserver(config.Config{})

		By("a partially updated, still-available Deployment is honestly Progressing")
		t11iDeployProgressing(ns, deployName, 2, 1)
		t11iResetAudit()
		res, err := obs.Reconcile(ctx, t11iRequest(subName))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(Equal(t11iWaiting), "an in-flight rollout keeps the poll alive")
		sub := t11iGetSub(subName)
		entry := t11iEntry(sub, deployName)
		Expect(entry).NotTo(BeNil())
		Expect(entry.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutProgressing))
		Expect(entry.Reason).To(Equal(controller.TargetReasonApplied),
			"no rollout transition ⇒ the engine's Applied reason is untouched")
		Expect(entry.ImageApplied).To(BeTrue())
		Expect(entry.HealthStatus).To(Equal(imageflowv1alpha1.HealthUnknown))
		Expect(t11iDrainEvents(rec)).To(BeEmpty(), "Progressing→Progressing is not an observable transition")
		cond := meta.FindStatusCondition(sub.Status.Conditions, controller.ConditionTypeStatusPolicyEvaluated)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(controller.ReasonStatusPolicyNotConfigured))

		By("the fully rolled-out Deployment is Succeeded — imageApplied stays true, health stays Unknown")
		t11iDeploySucceeded(ns, deployName, 2)
		t11iResetAudit()
		res, err = obs.Reconcile(ctx, t11iRequest(subName))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeZero(), "a terminal rollout needs no poll")
		Expect(t11iAudit.statusWrites).To(Equal(1), "exactly one idempotent status write")
		sub = t11iGetSub(subName)
		entry = t11iEntry(sub, deployName)
		Expect(entry.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutSucceeded))
		Expect(entry.Reason).To(Equal(controller.TargetReasonApplied),
			"R3: per-target Reason/Message remain the engine's delivery-stage surface")
		Expect(entry.ImageApplied).To(BeTrue(), "AC1: imageApplied is independent of rolloutStatus")
		Expect(entry.HealthStatus).To(Equal(imageflowv1alpha1.HealthUnknown),
			"AC1/AC3: health is never derived from rollout success (ADR-005 §3)")
		Expect(entry.LastTransitionTime).NotTo(BeNil())
		Expect(entry.LastTransitionTime.Time).To(BeTemporally("==", t11iClock.Now()))
		Expect(sub.Status.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutSucceeded))
		Expect(sub.Status.RecentDeliveries).To(HaveLen(1))
		Expect(sub.Status.RecentDeliveries[0].RolloutResult).To(Equal(imageflowv1alpha1.RolloutSucceeded))
		Expect(sub.Status.RecentDeliveries[0].Reason).To(Equal(controller.TargetReasonRolloutSucceeded))

		By("both namespaces receive the rollout-outcome Event with the full audit trail")
		events := t11iDrainEvents(rec)
		Expect(t11iCountContaining(events, "RolloutSucceeded")).To(Equal(2),
			"subscription side + workload side (ADR-008 dual-ns audit)")
		for _, e := range events {
			Expect(e).To(ContainSubstring(t11iKindDeploy))
			Expect(e).To(ContainSubstring(deployName))
			Expect(e).To(ContainSubstring(t11iSourceKind))
			Expect(e).To(ContainSubstring(streamName + ":" + t11iTag))
			Expect(e).To(ContainSubstring(t11iDigestB), "old image")
			Expect(e).To(ContainSubstring(t11iDigestA), "new image")
			Expect(e).To(ContainSubstring(t11iMsgAuditStages))
			Expect(e).To(ContainSubstring(t11iObserverStageApplied))
			Expect(e).To(ContainSubstring("rollout: Succeeded"))
			Expect(e).To(ContainSubstring(t11iObserverStageHealth))
			Expect(e).To(ContainSubstring("succeeded per native deployment status"))
			Expect(e).To(ContainSubstring("ADR-005"))
		}

		By("steady state: the next pass writes nothing and emits nothing")
		t11iResetAudit()
		res, err = obs.Reconcile(ctx, t11iRequest(subName))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeZero())
		Expect(t11iAudit.statusWrites).To(BeZero())
		Expect(t11iDrainEvents(rec)).To(BeEmpty())
	})

	It("t11i-envtest-deployment-deadline-failure-and-uninitialized-unknown", func() {
		const (
			failDeploy   = "t11i-ac2-fail"
			failSub      = "t11i-ac2-fail-sub"
			failStream   = "t11i-ac2-fail-stream"
			uninitDeploy = "t11i-ac2-uninit"
			uninitSub    = "t11i-ac2-uninit-sub"
			uninitStream = "t11i-ac2-uninit-stream"
		)
		t11iCreateDeployment(ns, failDeploy, t11iDigestA, 2)
		Expect(k8s.Create(ctx, t11iNewSub(failSub, failStream,
			t11iContainerTarget(t11iKindDeploy, ns, failDeploy, t11iContainer)))).To(Succeed())
		t11iAuthorize(failSub)
		t11iSeedStatus(failSub,
			[]imageflowv1alpha1.TargetDeliveryStatus{
				t11iAppliedEntry(t11iKindDeploy, ns, failDeploy, t11iDigestA),
			},
			[]imageflowv1alpha1.DeliveryResult{
				t11iRecord(failStream, t11iDigestB, t11iDigestA, t11iKindDeploy, ns, failDeploy,
					t11iClock.Now().Add(-time.Minute)),
			})

		obs, rec := t11iObserver(config.Config{})

		By("Progressing=False with reason ProgressDeadlineExceeded ⇒ rollout Failed")
		t11iDeployDeadlineFailed(ns, failDeploy, 2, 1)
		_, err := obs.Reconcile(ctx, t11iRequest(failSub))
		Expect(err).NotTo(HaveOccurred())
		sub := t11iGetSub(failSub)
		entry := t11iEntry(sub, failDeploy)
		Expect(entry.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutFailed))
		Expect(entry.Reason).To(Equal(controller.TargetReasonApplied),
			"R3: per-target Reason/Message remain the engine's delivery-stage surface")
		Expect(entry.ImageApplied).To(BeTrue(),
			"AC1: the field WAS written — imageApplied=true must never mask a failed rollout")
		Expect(entry.HealthStatus).To(Equal(imageflowv1alpha1.HealthUnknown))
		Expect(sub.Status.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutFailed),
			"AC4: worst-state aggregate — Failed beats everything")
		Expect(sub.Status.RecentDeliveries[0].RolloutResult).To(Equal(imageflowv1alpha1.RolloutFailed))
		events := t11iDrainEvents(rec)
		Expect(t11iCountContaining(events, "Warning RolloutFailed")).To(Equal(2),
			"a failed rollout is a Warning on both sides")
		Expect(t11iCountContaining(events, "rollout: Failed")).To(Equal(2))
		Expect(t11iCountContaining(events, t11iReasonDeadline)).To(Equal(2),
			"the deadline determination detail rides on the transition Events")

		By("a workload whose native controller never reported stays honestly Unknown")
		t11iCreateDeployment(ns, uninitDeploy, t11iDigestA, 2)
		Expect(k8s.Create(ctx, t11iNewSub(uninitSub, uninitStream,
			t11iContainerTarget(t11iKindDeploy, ns, uninitDeploy, t11iContainer)))).To(Succeed())
		t11iAuthorize(uninitSub)
		t11iSeedStatus(uninitSub,
			[]imageflowv1alpha1.TargetDeliveryStatus{
				t11iAppliedEntry(t11iKindDeploy, ns, uninitDeploy, t11iDigestA),
			}, nil)
		res, err := obs.Reconcile(ctx, t11iRequest(uninitSub))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeZero())
		sub = t11iGetSub(uninitSub)
		entry = t11iEntry(sub, uninitDeploy)
		Expect(entry.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutUnknown))
		Expect(entry.Reason).To(Equal(controller.TargetReasonApplied),
			"R3: per-target Reason/Message remain the engine's delivery-stage surface")
		Expect(sub.Status.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutUnknown))
		uninitEvents := t11iDrainEvents(rec)
		Expect(t11iCountContaining(uninitEvents, "RolloutUnknown")).To(Equal(2))
		Expect(t11iCountContaining(uninitEvents, "has not reported any status")).To(Equal(2),
			"the uninitialized determination detail rides on the transition Events")
	})

	It("t11i-envtest-statefulset-daemonset-independent-targets-worst-state-aggregate", func() {
		const (
			stsName    = "t11i-ac2-sts"
			dsName     = "t11i-ac2-ds"
			subName    = "t11i-ac4-mixed-sub"
			streamName = "t11i-ac4-mixed-stream"
		)
		t11iCreateStatefulSet(ns, stsName, t11iDigestA, 2)
		t11iCreateDaemonSet(ns, dsName, t11iDigestA)
		Expect(k8s.Create(ctx, t11iNewSub(subName, streamName,
			t11iContainerTarget(t11iKindSts, ns, stsName, t11iContainer),
			t11iContainerTarget(t11iKindDs, ns, dsName, t11iContainer)))).To(Succeed())
		t11iAuthorize(subName)
		t11iSeedStatus(subName,
			[]imageflowv1alpha1.TargetDeliveryStatus{
				t11iAppliedEntry(t11iKindSts, ns, stsName, t11iDigestA),
				t11iAppliedEntry(t11iKindDs, ns, dsName, t11iDigestA),
			},
			[]imageflowv1alpha1.DeliveryResult{
				t11iRecord(streamName, t11iDigestB, t11iDigestA, t11iKindSts, ns, stsName,
					t11iClock.Now().Add(-time.Minute)),
				t11iRecord(streamName, t11iDigestB, t11iDigestA, t11iKindDs, ns, dsName,
					t11iClock.Now().Add(-time.Minute)),
			})

		obs, rec := t11iObserver(config.Config{})

		By("StatefulSet: converged revisions + all replicas updated/ready ⇒ Succeeded")
		t11iSetStsStatus(ns, stsName, t11iRevisionNew, t11iRevisionNew, 2, 2, 2)
		By("DaemonSet: only part of the fleet updated ⇒ Progressing")
		t11iSetDsStatus(ns, dsName, 3, 1, 1, 1, 0)
		res, err := obs.Reconcile(ctx, t11iRequest(subName))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(Equal(t11iWaiting), "the DaemonSet is still in flight")
		sub := t11iGetSub(subName)
		Expect(t11iEntry(sub, stsName).RolloutStatus).To(Equal(imageflowv1alpha1.RolloutSucceeded))
		Expect(t11iEntry(sub, stsName).Reason).To(Equal(controller.TargetReasonApplied),
			"R3: per-target Reason/Message remain the engine's delivery-stage surface")
		Expect(t11iEntry(sub, dsName).RolloutStatus).To(Equal(imageflowv1alpha1.RolloutProgressing))
		Expect(t11iEntry(sub, dsName).Reason).To(Equal(controller.TargetReasonApplied),
			"no DaemonSet transition yet ⇒ the engine reason survives")
		Expect(sub.Status.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutProgressing),
			"AC4: worst-state — Progressing beats Succeeded across targets")
		Expect(sub.Status.RecentDeliveries).To(HaveLen(2))
		// The window is newest-first by ObservedAt; both records share the
		// seeded minute, so locate by target instead of position.
		healed := map[string]imageflowv1alpha1.RolloutStatus{}
		for _, r := range sub.Status.RecentDeliveries {
			healed[r.TargetRef.Name] = r.RolloutResult
		}
		Expect(healed[stsName]).To(Equal(imageflowv1alpha1.RolloutSucceeded))
		Expect(healed[dsName]).To(Equal(imageflowv1alpha1.RolloutProgressing),
			"in-flight records are only healed to terminal states")

		By("the DaemonSet finishes ⇒ every target Succeeded ⇒ aggregate Succeeded")
		t11iSetDsStatus(ns, dsName, 3, 3, 3, 3, 0)
		res, err = obs.Reconcile(ctx, t11iRequest(subName))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeZero())
		sub = t11iGetSub(subName)
		Expect(t11iEntry(sub, dsName).RolloutStatus).To(Equal(imageflowv1alpha1.RolloutSucceeded))
		Expect(t11iEntry(sub, dsName).Reason).To(Equal(controller.TargetReasonApplied),
			"R3: per-target Reason/Message remain the engine's delivery-stage surface")
		Expect(sub.Status.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutSucceeded))
		events := t11iDrainEvents(rec)
		Expect(t11iCountContaining(events, "RolloutSucceeded")).To(Equal(4),
			"two transitions × two namespaces (the STS pass drained nothing yet)")
		Expect(t11iCountContaining(events, "updateRevision")).To(Equal(2),
			"the STS determination detail rides on the transition Events")
		Expect(t11iCountContaining(events, "misscheduled")).To(Equal(2),
			"the DS determination detail rides on the transition Events")
	})

	It("t11i-envtest-no-status-policy-and-policy-ref-targets-never-fabricate-success", func() {
		const (
			policyDeploy = "t11i-ac3-policy"
			policySub    = "t11i-ac3-policy-sub"
			policyStream = "t11i-ac3-policy-stream"
			edgeName     = "t11i-ac3-edge"
			plainSub     = "t11i-ac3-plain-sub"
			plainStream  = "t11i-ac3-plain-stream"
			plainEdge    = "t11i-ac3-plain-edge"
		)
		edgeTarget := imageflowv1alpha1.SubscriptionTarget{
			TargetRef: imageflowv1alpha1.TargetReference{
				APIVersion: t11iAPIEdge,
				Kind:       t11iKindEdge,
				Namespace:  ns,
				Name:       edgeName,
			},
			ImageFields: []string{t11iEdgeField},
		}
		policyTarget := t11iContainerTarget(t11iKindDeploy, ns, policyDeploy, t11iContainer)
		policyTarget.StatusPolicyRef = &imageflowv1alpha1.StatusPolicyRef{
			Name:      "t11i-policy",
			Namespace: ns,
			Key:       "evaluate",
		}

		t11iCreateDeployment(ns, policyDeploy, t11iDigestA, 1)
		Expect(k8s.Create(ctx, t11iNewSub(policySub, policyStream,
			edgeTarget, policyTarget))).To(Succeed())
		t11iAuthorize(policySub)
		t11iSeedStatus(policySub,
			[]imageflowv1alpha1.TargetDeliveryStatus{
				t11iEdgeEntry(edgeName, t11iDigestA),
				t11iAppliedEntry(t11iKindDeploy, ns, policyDeploy, t11iDigestA),
			}, nil)

		obs, rec := t11iObserver(config.Config{})

		By("the built-in target with a statusPolicyRef is STILL observed natively (ADR-005 §5)")
		t11iDeploySucceeded(ns, policyDeploy, 1)
		_, err := obs.Reconcile(ctx, t11iRequest(policySub))
		Expect(err).NotTo(HaveOccurred())
		sub := t11iGetSub(policySub)
		Expect(t11iEntry(sub, policyDeploy).RolloutStatus).To(Equal(imageflowv1alpha1.RolloutSucceeded))
		Expect(t11iEntry(sub, policyDeploy).HealthStatus).To(Equal(imageflowv1alpha1.HealthUnknown))

		By("the third-party imageFields target stays honestly Unknown — success is never fabricated")
		edge := t11iEntry(sub, edgeName)
		Expect(edge.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutUnknown))
		Expect(edge.Reason).To(Equal(controller.TargetReasonApplied),
			"R3: per-target Reason/Message remain the engine's delivery-stage surface")
		Expect(edge.HealthStatus).To(Equal(imageflowv1alpha1.HealthUnknown))
		Expect(sub.Status.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutUnknown),
			"AC4: worst-state — one honest Unknown outweighs a Succeeded target")

		By("the StatusPolicyEvaluated Condition is explicit: evaluator deferred, nothing fabricated")
		cond := meta.FindStatusCondition(sub.Status.Conditions, controller.ConditionTypeStatusPolicyEvaluated)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(controller.ReasonStatusPolicyEvaluatorDeferred))
		Expect(cond.Message).To(ContainSubstring("the CUE evaluator is available (v1.3)"))
		events := t11iDrainEvents(rec)
		Expect(t11iCountContaining(events, "rollout: Unknown")).To(Equal(1),
			"the third-party transition has no target-side object in the cluster — sub-side only")
		Expect(t11iCountContaining(events, "rollout: Succeeded")).To(Equal(2))
		Expect(t11iCountContaining(events, "cannot be attributed to arbitrary fields")).To(Equal(1),
			"the honest-Unknown WHY rides on the transition Event")

		By("no statusPolicyRef anywhere ⇒ the Condition says NotConfigured, still honest Unknown")
		plainEdgeTarget := edgeTarget
		plainEdgeTarget.TargetRef.Name = plainEdge
		Expect(k8s.Create(ctx, t11iNewSub(plainSub, plainStream, plainEdgeTarget))).To(Succeed())
		t11iAuthorize(plainSub)
		t11iSeedStatus(plainSub,
			[]imageflowv1alpha1.TargetDeliveryStatus{t11iEdgeEntry(plainEdge, t11iDigestA)}, nil)
		_, err = obs.Reconcile(ctx, t11iRequest(plainSub))
		Expect(err).NotTo(HaveOccurred())
		sub = t11iGetSub(plainSub)
		Expect(t11iEntry(sub, plainEdge).RolloutStatus).To(Equal(imageflowv1alpha1.RolloutUnknown))
		Expect(t11iEntry(sub, plainEdge).Reason).To(Equal(controller.TargetReasonApplied),
			"R3: per-target Reason/Message remain the engine's delivery-stage surface")
		Expect(t11iCountContaining(t11iDrainEvents(rec), "cannot be attributed to arbitrary fields")).To(Equal(1))
		cond = meta.FindStatusCondition(sub.Status.Conditions, controller.ConditionTypeStatusPolicyEvaluated)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(controller.ReasonStatusPolicyNotConfigured))
		Expect(cond.Message).To(ContainSubstring("never fabricated"))
	})

	It("t11i-envtest-latest-wins-superseded-terminal-across-the-engine-handoff", func() {
		const (
			streamName = "t11i-ac5-stream"
			deployName = "t11i-ac5-deploy"
			subName    = "t11i-ac5-sub"
		)
		cfg := config.Config{SubscriptionDeliveryLimit: 2}
		engine, engineRec := t11iEngine(cfg)
		obs, obsRec := t11iObserver(cfg)

		t11iCreateStream(streamName, t11iDigestA)
		t11iCreateDeployment(ns, deployName, t11iPreImage, 1)
		Expect(k8s.Create(ctx, t11iNewSub(subName, streamName,
			t11iContainerTarget(t11iKindDeploy, ns, deployName, t11iContainer)))).To(Succeed())
		t11iAuthorize(subName)

		By("delivery A lands — the engine seeds the in-flight record")
		_, err := engine.Reconcile(ctx, t11iRequest(subName))
		Expect(err).NotTo(HaveOccurred())
		sub := t11iGetSub(subName)
		Expect(sub.Status.RecentDeliveries).To(HaveLen(1))
		Expect(sub.Status.RecentDeliveries[0].NewImage).To(Equal(t11iDigestA))
		Expect(sub.Status.RecentDeliveries[0].RolloutResult).To(Equal(imageflowv1alpha1.RolloutProgressing))
		t11iDrainEvents(engineRec)

		By("retag B mid-rollout ⇒ latest-wins: B re-targets immediately, A becomes Superseded")
		t11iRetagStream(streamName, t11iDigestB)
		_, err = engine.Reconcile(ctx, t11iRequest(subName))
		Expect(err).NotTo(HaveOccurred())
		sub = t11iGetSub(subName)
		Expect(sub.Status.RecentDeliveries).To(HaveLen(2))
		Expect(sub.Status.RecentDeliveries[0].NewImage).To(Equal(t11iDigestB))
		Expect(sub.Status.RecentDeliveries[0].RolloutResult).To(Equal(imageflowv1alpha1.RolloutProgressing))
		Expect(sub.Status.RecentDeliveries[1].NewImage).To(Equal(t11iDigestA))
		Expect(sub.Status.RecentDeliveries[1].RolloutResult).To(Equal(imageflowv1alpha1.RolloutSuperseded))
		events := t11iDrainEvents(engineRec)
		// The same pass also emits the ImageApplied events of delivery B —
		// filter down to the supersede audit itself.
		var superseded []string
		for _, e := range events {
			if strings.Contains(e, "DeliverySuperseded") {
				superseded = append(superseded, e)
			}
		}
		Expect(superseded).To(HaveLen(2),
			"AC6: the supersede audit lands in BOTH namespaces (subscription + workload)")
		for _, e := range superseded {
			Expect(e).To(ContainSubstring("Delivery superseded"))
			Expect(e).To(ContainSubstring("latest-wins"))
			Expect(e).To(ContainSubstring("ADR-008"))
			Expect(e).To(ContainSubstring(t11iDigestA), "the superseded image")
			Expect(e).To(ContainSubstring(t11iDigestB), "the winning image")
			Expect(e).To(ContainSubstring(streamName + ":" + t11iTag))
			Expect(e).To(ContainSubstring(deployName))
			Expect(e).To(ContainSubstring("no retry"))
			Expect(e).To(ContainSubstring("no cross-target rollback"))
		}

		By("retag C ⇒ B is superseded too and A leaves the bounded window (flag limit 2)")
		t11iRetagStream(streamName, t11iDigestC)
		_, err = engine.Reconcile(ctx, t11iRequest(subName))
		Expect(err).NotTo(HaveOccurred())
		sub = t11iGetSub(subName)
		Expect(sub.Status.RecentDeliveries).To(HaveLen(2),
			"AC8: --subscription-delivery-limit bounds the window")
		Expect(sub.Status.RecentDeliveries[0].NewImage).To(Equal(t11iDigestC))
		Expect(sub.Status.RecentDeliveries[1].NewImage).To(Equal(t11iDigestB))
		Expect(sub.Status.RecentDeliveries[1].RolloutResult).To(Equal(imageflowv1alpha1.RolloutSuperseded))
		events = t11iDrainEvents(engineRec)
		Expect(t11iCountContaining(events, "DeliveryHistoryTruncated")).To(Equal(1))
		Expect(t11iCountContaining(events, "DeliverySuperseded")).To(Equal(2))

		By("the observer heals ONLY the live record; Superseded stays terminal")
		t11iDeploySucceeded(ns, deployName, 1)
		_, err = obs.Reconcile(ctx, t11iRequest(subName))
		Expect(err).NotTo(HaveOccurred())
		sub = t11iGetSub(subName)
		entry := t11iEntry(sub, deployName)
		Expect(entry.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutSucceeded))
		Expect(entry.Reason).To(Equal(controller.TargetReasonApplied),
			"R3: per-target Reason/Message remain the engine's delivery-stage surface")
		Expect(sub.Status.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutSucceeded))
		Expect(sub.Status.RecentDeliveries).To(HaveLen(2))
		Expect(sub.Status.RecentDeliveries[0].NewImage).To(Equal(t11iDigestC))
		Expect(sub.Status.RecentDeliveries[0].RolloutResult).To(Equal(imageflowv1alpha1.RolloutSucceeded))
		Expect(sub.Status.RecentDeliveries[0].Reason).To(Equal(controller.TargetReasonRolloutSucceeded))
		Expect(sub.Status.RecentDeliveries[1].NewImage).To(Equal(t11iDigestB))
		Expect(sub.Status.RecentDeliveries[1].RolloutResult).To(Equal(imageflowv1alpha1.RolloutSuperseded),
			"AC5: Superseded is terminal — the observer never resurrects or retries it")
		Expect(sub.Status.RecentDeliveries[1].Reason).To(Equal(controller.TargetReasonApplied),
			"the terminal record keeps its historical reason byte-identical")
		events = t11iDrainEvents(obsRec)
		Expect(t11iCountContaining(events, "RolloutSucceeded")).To(Equal(2))
		Expect(t11iCountContaining(events, "DeliverySuperseded")).To(BeZero(),
			"supersede audit is the engine's; the observer emits rollout outcomes only")
	})

	It("t11i-envtest-mirror-floatup-bounded-delivery-summaries-and-consumers", func() {
		const (
			streamName = "t11i-ac7-stream"
			deployName = "t11i-ac7-deploy"
			subName    = "t11i-ac7-sub"
		)
		cfg := config.Config{MirrorEventLimit: 3}
		obs, rec := t11iObserver(cfg)

		istName := controller.EncodeImageStreamTagName(streamName, t11iTag)
		itName := controller.EncodeImageTagName(streamName, t11iBareA)
		consumers := []imageflowv1alpha1.ConsumerReference{{Namespace: ns, Name: subName}}
		t11iCreateMirrorIST(streamName, t11iTag,
			t11iImageChangedEvents(5, t11iClock.Now()), consumers)
		t11iCreateMirrorIT(streamName, t11iBareA,
			t11iImageChangedEvents(1, t11iClock.Now()))

		t11iCreateDeployment(ns, deployName, t11iDigestA, 1)
		Expect(k8s.Create(ctx, t11iNewSub(subName, streamName,
			t11iContainerTarget(t11iKindDeploy, ns, deployName, t11iContainer)))).To(Succeed())
		t11iAuthorize(subName)
		t11iSeedStatus(subName,
			[]imageflowv1alpha1.TargetDeliveryStatus{
				t11iAppliedEntry(t11iKindDeploy, ns, deployName, t11iDigestA),
			},
			[]imageflowv1alpha1.DeliveryResult{
				t11iRecord(streamName, t11iDigestB, t11iDigestA, t11iKindDeploy, ns, deployName,
					t11iClock.Now().Add(-30*time.Second)),
			})

		truncBefore := t11iGatherCounter(t11iMirrorTruncFam, map[string]string{"kind": t11iMirrorKindIST})

		By("the rollout outcome floats up onto BOTH mirror kinds as a Delivery summary")
		t11iDeploySucceeded(ns, deployName, 1)
		_, err := obs.Reconcile(ctx, t11iRequest(subName))
		Expect(err).NotTo(HaveOccurred())

		ist := t11iGetMirrorIST(istName)
		Expect(ist.Status.Events).To(HaveLen(3),
			"AC7/AC8: bounded window (--mirror-event-limit=3), newest first")
		delivery := ist.Status.Events[0]
		Expect(delivery.Type).To(Equal(imageflowv1alpha1.MirrorEventDelivery))
		Expect(delivery.Result).To(Equal(imageflowv1alpha1.RolloutSucceeded))
		Expect(delivery.Reason).To(Equal(controller.TargetReasonRolloutSucceeded))
		Expect(delivery.NewDigest).To(Equal(t11iBareA))
		Expect(delivery.OldDigest).To(Equal(t11iBareB))
		Expect(delivery.Subscription).NotTo(BeNil())
		Expect(delivery.Subscription.Namespace).To(Equal(ns))
		Expect(delivery.Subscription.Name).To(Equal(subName))
		Expect(delivery.Target).NotTo(BeNil())
		Expect(delivery.Target.Name).To(Equal(deployName))
		Expect(delivery.Message).To(ContainSubstring(t11iMsgDeliverySummary))
		Expect(delivery.Message).To(ContainSubstring("imageApplied never implies rollout success"))
		Expect(delivery.Message).To(ContainSubstring("ADR-005"))
		Expect(ist.Status.Events[1].Type).To(Equal(imageflowv1alpha1.MirrorEventImageChanged))
		Expect(ist.Status.Events[2].Type).To(Equal(imageflowv1alpha1.MirrorEventImageChanged))
		Expect(ist.Status.Consumers).To(Equal(consumers),
			"AC7: consumer references are preserved, never clobbered by the float-up")

		it := t11iGetMirrorIT(itName)
		Expect(it.Status.Events).To(HaveLen(2), "the digest-scoped mirror keeps its own history")
		Expect(it.Status.Events[0].Type).To(Equal(imageflowv1alpha1.MirrorEventDelivery))
		Expect(it.Status.Events[0].Result).To(Equal(imageflowv1alpha1.RolloutSucceeded))
		Expect(it.Status.Events[0].NewDigest).To(Equal(t11iBareA))
		Expect(it.Status.Events[1].Type).To(Equal(imageflowv1alpha1.MirrorEventImageChanged))

		By("overflow leaves the window with an Event, a log and the shared truncation metric")
		events := t11iDrainEvents(rec)
		Expect(t11iCountContaining(events, "EventSummaryTruncated")).To(Equal(1))
		Expect(t11iCountContaining(events, "bounded to 3 entries")).To(Equal(1))
		Expect(t11iCountContaining(events, "3 older entries left the window")).To(Equal(1))
		truncAfter := t11iGatherCounter(t11iMirrorTruncFam, map[string]string{"kind": t11iMirrorKindIST})
		Expect(truncAfter-truncBefore).To(Equal(float64(3)),
			"5 seeded ImageChanged + 1 Delivery ⇒ 3 dropped at limit 3")

		By("steady state: re-running neither rewrites the mirrors nor re-emits")
		t11iResetAudit()
		_, err = obs.Reconcile(ctx, t11iRequest(subName))
		Expect(err).NotTo(HaveOccurred())
		Expect(t11iDrainEvents(rec)).To(BeEmpty())
		Expect(t11iGetMirrorIST(istName).Status.Events).To(HaveLen(3))
		Expect(t11iGatherCounter(t11iMirrorTruncFam,
			map[string]string{"kind": t11iMirrorKindIST})).To(Equal(truncAfter))
	})

	// --- manager lane ---------------------------------------------------------

	Context("manager lane", func() {
		const (
			deployName = "t11i-acm-deploy"
			subName    = "t11i-acm-sub"
			streamName = "t11i-acm-stream"
		)
		var (
			mgrCancel context.CancelFunc
			mgrDone   chan struct{}
		)

		BeforeAll(func() {
			// The workload starts with NO status: the observer's first pass
			// (triggered by the initial list) must honestly report Unknown
			// for the uninitialized native status; the Succeeded transition
			// afterwards can then ONLY come from the status-driven watch —
			// the observer does not requeue Unknown rollouts.
			t11iCreateDeployment(nsWL, deployName, t11iDigestA, 2)
			Expect(k8s.Create(ctx, t11iNewSub(subName, streamName,
				t11iContainerTarget(t11iKindDeploy, nsWL, deployName, t11iContainer)))).To(Succeed())
			t11iAuthorize(subName)
			t11iSeedStatus(subName,
				[]imageflowv1alpha1.TargetDeliveryStatus{
					t11iAppliedEntry(t11iKindDeploy, nsWL, deployName, t11iDigestA),
				},
				[]imageflowv1alpha1.DeliveryResult{
					t11iRecord(streamName, t11iDigestB, t11iDigestA, t11iKindDeploy, nsWL, deployName,
						time.Now().Add(-time.Minute)),
				})

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
			Expect(controller.RegisterSubscriptionObserver(mgr, t11iObserverName,
				controller.Options{Clock: clock.Real{}, Config: config.Config{}})).To(Succeed())
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
		})

		AfterAll(func() {
			if mgrCancel != nil {
				mgrCancel()
				Eventually(mgrDone, 30*time.Second, 100*time.Millisecond).Should(BeClosed())
			}
		})

		It("t11i-envtest-manager-status-watch-dual-ns-events-and-metrics", func() {
			By("the uninitialized workload is honestly Unknown without any manual reconcile")
			Eventually(func(g Gomega) {
				entry := t11iEntry(t11iGetSub(subName), deployName)
				g.Expect(entry).NotTo(BeNil())
				g.Expect(entry.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutUnknown))
				g.Expect(entry.Reason).To(Equal(controller.TargetReasonApplied),
					"R3: per-target Reason/Message remain the engine's delivery-stage surface")
			}, 20*time.Second, 100*time.Millisecond).Should(Succeed())
			Eventually(func(g Gomega) {
				g.Expect(t11iRealEventReasons(ns, subName)).To(ContainElement("RolloutUnknown"))
				g.Expect(t11iRealEventReasons(nsWL, deployName)).To(ContainElement("RolloutUnknown"),
					"AC6: the workload-side Event lands in the TARGET namespace")
			}, 20*time.Second, 250*time.Millisecond).Should(Succeed())

			By("writing the native status ⇒ the STATUS watch alone drives Succeeded")
			t11iDeploySucceeded(nsWL, deployName, 2)
			Eventually(func(g Gomega) {
				sub := t11iGetSub(subName)
				entry := t11iEntry(sub, deployName)
				g.Expect(entry).NotTo(BeNil())
				g.Expect(entry.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutSucceeded))
				g.Expect(entry.Reason).To(Equal(controller.TargetReasonApplied))
				g.Expect(entry.ImageApplied).To(BeTrue())
				g.Expect(entry.HealthStatus).To(Equal(imageflowv1alpha1.HealthUnknown))
				g.Expect(sub.Status.RolloutStatus).To(Equal(imageflowv1alpha1.RolloutSucceeded))
				g.Expect(sub.Status.RecentDeliveries).To(HaveLen(1))
				g.Expect(sub.Status.RecentDeliveries[0].RolloutResult).
					To(Equal(imageflowv1alpha1.RolloutSucceeded))
			}, 30*time.Second, 100*time.Millisecond).Should(Succeed())

			By("real dual-namespace Events carry the complete audit trail")
			Eventually(func(g Gomega) {
				g.Expect(t11iRealEventReasons(ns, subName)).To(ContainElement("RolloutSucceeded"))
				g.Expect(t11iRealEventReasons(nsWL, deployName)).To(ContainElement("RolloutSucceeded"))
			}, 20*time.Second, 250*time.Millisecond).Should(Succeed())
			messages := t11iRealEventMessages(ns, subName, "RolloutSucceeded")
			Expect(messages).NotTo(BeEmpty())
			for _, fragment := range []string{
				t11iKindDeploy, deployName, t11iSourceKind, streamName + ":" + t11iTag,
				t11iDigestB, t11iDigestA, t11iMsgAuditStages, t11iObserverStageApplied,
				"rollout: Succeeded", t11iObserverStageHealth, "ADR-005",
			} {
				Expect(messages[0]).To(ContainSubstring(fragment))
			}

			By("AC9: per-stage outcome metrics are live on the process registry")
			Expect(t11iGatherCounter(t11iObserverRolloutFam, map[string]string{
				"kind": "deployment", "result": "succeeded",
			})).To(BeNumerically(">=", 1))
			Expect(t11iGatherCounter(t11iObserverRolloutFam, map[string]string{
				"kind": "deployment", "result": "unknown",
			})).To(BeNumerically(">=", 1))
			Expect(t11iGatherCounter(t11iObserverHealthFam, map[string]string{
				"result": "unknown",
			})).To(BeNumerically(">=", 1))

			By("no hot loop: the observer quiesces after its own status write")
			settled := t11iGetSub(subName)
			Consistently(func(g Gomega) {
				g.Expect(t11iGetSub(subName).ResourceVersion).To(Equal(settled.ResourceVersion))
			}, 4*time.Second, 250*time.Millisecond).Should(Succeed())
		})
	})
})
