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

package controller

// T11 controller-layer unit tests: the workload observer against a fake
// client and the injected clock. This lane owns the observation DECISIONS
// (per-kind rollout determination incl. the pod-image-failure scan, the
// honest-Unknown contract for non-observable classes, worst-state
// aggregation combinations, in-flight record healing, the dual-namespace
// rollout-outcome Events with their audit payload, the mirror delivery
// float-up merge/bounding, and the engine-side observer-reason preservation
// + latest-wins Superseded event). The REAL apiserver behavior — status
// subresource writes against directly-written workload status, controller
// registration and Event delivery — is proven one layer up in
// test/integration/t11_observer_test.go (envtest).

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	testutil "github.com/prometheus/client_golang/prometheus/testutil"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
	"github.com/appfun-dev/imageflow/internal/controller/cueeval"
)

const (
	t11uNamespace  = "t11u-ns"
	t11uSubName    = "t11u-sub"
	t11uStreamName = "t11u-stream"
	t11uTagName    = "v1"
	t11uDeployName = "t11u-deploy"
	t11uSTSName    = "t11u-sts"
	t11uDSName     = "t11u-ds"
	t11uEdgeName   = "t11u-edge"
	t11uRSName     = "t11u-rs"
	t11uPodName    = "t11u-pod"
	t11uContainer  = "app"
	t11uRepo       = "registry.example.com/t11u/app"
	t11uAppsAPI    = "apps/v1"

	t11uWaitingPullBackOff = "ImagePullBackOff"
	t11uLocator            = "containers [app]"
)

var (
	t11uStart = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

	t11uDigestA = "sha256:" + strings.Repeat("a", 64)
	t11uDigestB = "sha256:" + strings.Repeat("b", 64)
	t11uDigestC = "sha256:" + strings.Repeat("c", 64)
	t11uRefA    = t11uRepo + "@" + t11uDigestA
	t11uRefB    = t11uRepo + "@" + t11uDigestB
	t11uRefC    = t11uRepo + "@" + t11uDigestC

	t11uSourceLogical = t11uStreamName + ":" + t11uTagName

	t11uDeployUID = types.UID("t11u-deploy-uid")
	t11uSTSUID    = types.UID("t11u-sts-uid")
	t11uDSUID     = types.UID("t11u-ds-uid")
	t11uRSUID     = types.UID("t11u-rs-uid")

	t11uEdgeGVK = schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "EdgeService"}
)

// ---------------------------------------------------------------------------
// fixture
// ---------------------------------------------------------------------------

type t11uFixture struct {
	t          *testing.T
	client     client.Client
	recorder   *record.FakeRecorder
	clock      *clock.Fake
	metrics    *observerMetrics
	reconciler *SubscriptionObserverReconciler
	cfg        config.Config
}

func newT11uFixture(t *testing.T, cfg config.Config, objs ...client.Object) *t11uFixture {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := imageflowv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add imageflow scheme: %v", err)
	}
	f := &t11uFixture{
		t:        t,
		cfg:      cfg,
		recorder: record.NewFakeRecorder(256),
		clock:    clock.NewFake(t11uStart),
	}
	f.client = fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(
			&imageflowv1alpha1.ImageSubscription{},
			&imageflowv1alpha1.ImageStreamTag{},
			&imageflowv1alpha1.ImageTag{},
		).
		Build()
	f.metrics = newObserverMetrics()
	f.reconciler = NewSubscriptionObserverReconciler(f.client, nil, f.recorder, f.clock, cfg, f.metrics, nil) // nil evaluator: T3 tests don't exercise CUE evaluation
	return f
}

var t11uCtx = context.Background()

// applyStatus persists status through the subresource (the fake client strips
// status from Create for subresource-registered kinds).
func (f *t11uFixture) applyStatus(obj client.Object, set func()) {
	f.t.Helper()
	set()
	if err := f.client.Status().Update(t11uCtx, obj); err != nil {
		f.t.Fatalf("status update %T %s: %v", obj, client.ObjectKeyFromObject(obj), err)
	}
}

func (f *t11uFixture) getSub() *imageflowv1alpha1.ImageSubscription {
	f.t.Helper()
	sub := &imageflowv1alpha1.ImageSubscription{}
	key := types.NamespacedName{Namespace: t11uNamespace, Name: t11uSubName}
	if err := f.client.Get(t11uCtx, key, sub); err != nil {
		f.t.Fatalf("get sub: %v", err)
	}
	return sub
}

func (f *t11uFixture) reconcileSub() (ctrl.Result, error) {
	f.t.Helper()
	return f.reconciler.Reconcile(t11uCtx, ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: t11uNamespace, Name: t11uSubName,
	}})
}

func (f *t11uFixture) drainEvents() []string {
	var out []string
	for {
		select {
		case e := <-f.recorder.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// ---------------------------------------------------------------------------
// builders
// ---------------------------------------------------------------------------

func t11uSourceRef() imageflowv1alpha1.SubscriptionSourceRef {
	return imageflowv1alpha1.SubscriptionSourceRef{
		Kind:      mirrorSourceKindImageStreamTag,
		Namespace: t11uNamespace,
		Name:      t11uSourceLogical,
	}
}

func t11uDeployRef() imageflowv1alpha1.TargetReference {
	return imageflowv1alpha1.TargetReference{
		APIVersion: t11uAppsAPI, Kind: kindDeployment,
		Namespace: t11uNamespace, Name: t11uDeployName,
	}
}

func t11uSTSRef() imageflowv1alpha1.TargetReference {
	return imageflowv1alpha1.TargetReference{
		APIVersion: t11uAppsAPI, Kind: kindStatefulSet,
		Namespace: t11uNamespace, Name: t11uSTSName,
	}
}

func t11uDSRef() imageflowv1alpha1.TargetReference {
	return imageflowv1alpha1.TargetReference{
		APIVersion: t11uAppsAPI, Kind: kindDaemonSet,
		Namespace: t11uNamespace, Name: t11uDSName,
	}
}

func t11uEdgeRef() imageflowv1alpha1.TargetReference {
	return imageflowv1alpha1.TargetReference{
		APIVersion: t11uEdgeGVK.GroupVersion().String(), Kind: t11uEdgeGVK.Kind,
		Namespace: t11uNamespace, Name: t11uEdgeName,
	}
}

func t11uContainerTarget(ref imageflowv1alpha1.TargetReference) imageflowv1alpha1.SubscriptionTarget {
	return imageflowv1alpha1.SubscriptionTarget{
		TargetRef:      ref,
		ContainerNames: []string{t11uContainer},
	}
}

// t11uAuthorizedConditions renders the T8 gate precondition: Forbidden=False.
func t11uAuthorizedConditions() []metav1.Condition {
	return []metav1.Condition{{
		Type:               imageflowv1alpha1.ConditionTypeForbidden,
		Status:             metav1.ConditionFalse,
		Reason:             "Authorized",
		ObservedGeneration: 1,
		LastTransitionTime: metav1.NewTime(t11uStart.Add(-time.Hour)),
	}}
}

// t11uSeededEntry renders the entry shape the T9 engine leaves behind after
// a value-changing apply: fields written, rollout Progressing, health Unknown.
// t11uEngineSteadyMessage is the delivery engine's steady-state per-target
// Message (R3: the observer never rewrites entry Reason/Message).
const t11uEngineSteadyMessage = "all declared image fields hold the digest"

func t11uSeededEntry(ref imageflowv1alpha1.TargetReference, desired string) imageflowv1alpha1.TargetDeliveryStatus {
	return imageflowv1alpha1.TargetDeliveryStatus{
		TargetRef:          ref,
		DesiredImage:       desired,
		ObservedImage:      desired,
		ImageApplied:       true,
		RolloutStatus:      imageflowv1alpha1.RolloutProgressing,
		HealthStatus:       imageflowv1alpha1.HealthUnknown,
		Reason:             TargetReasonApplied,
		Message:            t11uEngineSteadyMessage,
		ObservedGeneration: 1,
		LastTransitionTime: ptr.To(metav1.NewTime(t11uStart.Add(-time.Minute))),
	}
}

func t11uSeededRecord(ref imageflowv1alpha1.TargetReference, oldImage, newImage string, at time.Time) imageflowv1alpha1.DeliveryResult {
	return imageflowv1alpha1.DeliveryResult{
		Source:       t11uSourceRef(),
		OldImage:     oldImage,
		NewImage:     newImage,
		TargetRef:    ref,
		ImageApplied: true,
		RolloutResult: imageflowv1alpha1.
			RolloutProgressing,
		HealthResult: imageflowv1alpha1.HealthUnknown,
		Reason:       TargetReasonApplied,
		ObservedAt:   metav1.NewTime(at),
	}
}

func t11uSub(targets []imageflowv1alpha1.SubscriptionTarget, status imageflowv1alpha1.ImageSubscriptionStatus) *imageflowv1alpha1.ImageSubscription {
	return &imageflowv1alpha1.ImageSubscription{
		ObjectMeta: metav1.ObjectMeta{Namespace: t11uNamespace, Name: t11uSubName},
		Spec: imageflowv1alpha1.ImageSubscriptionSpec{
			Source:  t11uSourceRef(),
			Targets: targets,
		},
		Status: status,
	}
}

func t11uPodTemplate(image string) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: t11uContainer, Image: image}},
		},
	}
}

func t11uDeployment(image string, st appsv1.DeploymentStatus) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: t11uNamespace, Name: t11uDeployName,
			UID: t11uDeployUID, Generation: 3,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(2)),
			Template: t11uPodTemplate(image),
		},
		Status: st,
	}
}

func t11uDeploySucceeded() *appsv1.Deployment {
	return t11uDeployment(t11uRefA, appsv1.DeploymentStatus{
		ObservedGeneration: 3,
		Replicas:           2,
		UpdatedReplicas:    2,
		AvailableReplicas:  2,
		ReadyReplicas:      2,
		Conditions: []appsv1.DeploymentCondition{{
			Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue,
			Reason: "MinimumReplicasAvailable",
		}},
	})
}

func t11uDeployProgressing() *appsv1.Deployment {
	return t11uDeployment(t11uRefA, appsv1.DeploymentStatus{
		ObservedGeneration: 3,
		Replicas:           3,
		UpdatedReplicas:    1,
		AvailableReplicas:  2,
		Conditions: []appsv1.DeploymentCondition{{
			Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue,
			Reason: "MinimumReplicasAvailable",
		}, {
			Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue,
			Reason: "ReplicaSetUpdated",
		}},
	})
}

func t11uStatefulSet(image string, st appsv1.StatefulSetStatus) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: t11uNamespace, Name: t11uSTSName,
			UID: t11uSTSUID, Generation: 2,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:            ptr.To(int32(2)),
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Template:            t11uPodTemplate(image),
		},
		Status: st,
	}
}

func t11uDaemonSet(image string, st appsv1.DaemonSetStatus) *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: t11uNamespace, Name: t11uDSName,
			UID: t11uDSUID, Generation: 5,
		},
		Spec:   appsv1.DaemonSetSpec{Template: t11uPodTemplate(image)},
		Status: st,
	}
}

func t11uOwnerRef(kind, name string, uid types.UID) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: t11uAppsAPI, Kind: kind, Name: name,
		UID: uid, Controller: ptr.To(true),
	}
}

func t11uWaitingStatus(image, reason, message string) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:  t11uContainer,
		Image: image,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
			Reason: reason, Message: message,
		}},
	}
}

func t11uPod(owner metav1.OwnerReference, statuses ...corev1.ContainerStatus) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: t11uNamespace, Name: t11uPodName, OwnerReferences: []metav1.OwnerReference{owner},
		},
		Status: corev1.PodStatus{ContainerStatuses: statuses},
	}
}

// t11uHealthStatus builds a container status for the health-stage tests:
// readiness + restart count + an optional waiting reason (the crash-loop
// signal scanPodsForHealth judges Unhealthy).
func t11uHealthStatus(image string, ready bool, restarts int32, waitingReason string) corev1.ContainerStatus {
	cs := corev1.ContainerStatus{
		Name:         t11uContainer,
		Image:        image,
		Ready:        ready,
		RestartCount: restarts,
	}
	if waitingReason != "" {
		cs.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: waitingReason}}
	}
	return cs
}

// t11uHealthPod is t11uPod with an explicit name: health fixtures seed
// several pods per workload, so the shared t11uPodName constant cannot
// distinguish them (worst-case aggregation picks the first by name order).
func t11uHealthPod(name string, owner metav1.OwnerReference, statuses ...corev1.ContainerStatus) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: t11uNamespace, Name: name, OwnerReferences: []metav1.OwnerReference{owner},
		},
		Status: corev1.PodStatus{ContainerStatuses: statuses},
	}
}

func t11uReplicaSet() *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: t11uNamespace, Name: t11uRSName, UID: t11uRSUID,
			OwnerReferences: []metav1.OwnerReference{
				t11uOwnerRef(kindDeployment, t11uDeployName, t11uDeployUID),
			},
		},
	}
}

func t11uImageChangedEv(at time.Time, oldDigest, newDigest string) imageflowv1alpha1.MirrorStatusEvent {
	return imageflowv1alpha1.MirrorStatusEvent{
		Type:       imageflowv1alpha1.MirrorEventImageChanged,
		Reason:     mirrorReasonAssociated,
		Message:    "tag moved to a new digest",
		ObservedAt: metav1.NewTime(at),
		OldDigest:  oldDigest,
		NewDigest:  newDigest,
	}
}

func t11uDeliveryEv(at time.Time, newDigest string, result imageflowv1alpha1.RolloutStatus) imageflowv1alpha1.MirrorStatusEvent {
	ref := t11uDeployRef()
	return imageflowv1alpha1.MirrorStatusEvent{
		Type:       imageflowv1alpha1.MirrorEventDelivery,
		Reason:     TargetReasonRolloutSucceeded,
		Message:    "Delivery summary",
		ObservedAt: metav1.NewTime(at),
		OldDigest:  t11uDigestA,
		NewDigest:  newDigest,
		Subscription: &imageflowv1alpha1.SubscriptionObjectRef{
			Namespace: t11uNamespace, Name: t11uSubName,
		},
		Target: &ref,
		Result: result,
	}
}

func t11uIST(events []imageflowv1alpha1.MirrorStatusEvent, consumers []imageflowv1alpha1.ConsumerReference) *imageflowv1alpha1.ImageStreamTag {
	return &imageflowv1alpha1.ImageStreamTag{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: t11uNamespace,
			Name:      EncodeImageStreamTagName(t11uStreamName, t11uTagName),
		},
		Spec: imageflowv1alpha1.ImageStreamTagSpec{Stream: t11uStreamName, Tag: t11uTagName},
		Status: imageflowv1alpha1.ImageStreamTagStatus{MirrorObjectStatus: imageflowv1alpha1.MirrorObjectStatus{
			Events:    events,
			Consumers: consumers,
		}},
	}
}

func t11uIT(digest string, events []imageflowv1alpha1.MirrorStatusEvent) *imageflowv1alpha1.ImageTag {
	return &imageflowv1alpha1.ImageTag{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: t11uNamespace,
			Name:      EncodeImageTagName(t11uStreamName, digest),
		},
		Spec: imageflowv1alpha1.ImageTagSpec{Stream: t11uStreamName, Digest: digest},
		Status: imageflowv1alpha1.ImageTagStatus{MirrorObjectStatus: imageflowv1alpha1.MirrorObjectStatus{
			Events: events,
		}},
	}
}

func t11uEventsWith(events []string, reason string) []string {
	var out []string
	for _, e := range events {
		if strings.Contains(e, " "+reason+" ") {
			out = append(out, e)
		}
	}
	return out
}

func t11uPolicyRef() *imageflowv1alpha1.StatusPolicyRef {
	return &imageflowv1alpha1.StatusPolicyRef{
		Namespace: t11uNamespace, Name: "t11u-policy", Key: "policy.cue",
	}
}

// ---------------------------------------------------------------------------
// AC4: worst-state aggregation combinations (pure unit)
// ---------------------------------------------------------------------------

func TestObserverAggregateRolloutStatusWorstStateCombinations(t *testing.T) {
	cases := []struct {
		name string
		in   []imageflowv1alpha1.RolloutStatus
		want imageflowv1alpha1.RolloutStatus
	}{
		{"no targets aggregate to empty", nil, ""},
		{"all succeeded stays succeeded", []imageflowv1alpha1.RolloutStatus{
			imageflowv1alpha1.RolloutSucceeded, imageflowv1alpha1.RolloutSucceeded,
		}, imageflowv1alpha1.RolloutSucceeded},
		{"succeeded plus progressing is progressing", []imageflowv1alpha1.RolloutStatus{
			imageflowv1alpha1.RolloutSucceeded, imageflowv1alpha1.RolloutProgressing,
		}, imageflowv1alpha1.RolloutProgressing},
		{"progressing plus unknown is unknown", []imageflowv1alpha1.RolloutStatus{
			imageflowv1alpha1.RolloutProgressing, imageflowv1alpha1.RolloutUnknown,
		}, imageflowv1alpha1.RolloutUnknown},
		{"unknown plus failed is failed", []imageflowv1alpha1.RolloutStatus{
			imageflowv1alpha1.RolloutUnknown, imageflowv1alpha1.RolloutFailed,
		}, imageflowv1alpha1.RolloutFailed},
		{"failed beats conflict", []imageflowv1alpha1.RolloutStatus{
			imageflowv1alpha1.RolloutFailed, imageflowv1alpha1.RolloutConflict,
		}, imageflowv1alpha1.RolloutFailed},
		{"conflict beats unknown", []imageflowv1alpha1.RolloutStatus{
			imageflowv1alpha1.RolloutConflict, imageflowv1alpha1.RolloutUnknown,
		}, imageflowv1alpha1.RolloutConflict},
		{"succeeded plus superseded is superseded", []imageflowv1alpha1.RolloutStatus{
			imageflowv1alpha1.RolloutSucceeded, imageflowv1alpha1.RolloutSuperseded,
		}, imageflowv1alpha1.RolloutSuperseded},
		{"progressing beats superseded", []imageflowv1alpha1.RolloutStatus{
			imageflowv1alpha1.RolloutProgressing, imageflowv1alpha1.RolloutSuperseded,
		}, imageflowv1alpha1.RolloutProgressing},
		{"unrecognized status falls back to unknown", []imageflowv1alpha1.RolloutStatus{
			imageflowv1alpha1.RolloutSucceeded, "Bogus",
		}, imageflowv1alpha1.RolloutUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			targets := make([]imageflowv1alpha1.TargetDeliveryStatus, 0, len(tc.in))
			for i, rs := range tc.in {
				targets = append(targets, imageflowv1alpha1.TargetDeliveryStatus{
					TargetRef: imageflowv1alpha1.TargetReference{
						APIVersion: t11uAppsAPI, Kind: kindDeployment,
						Namespace: t11uNamespace, Name: fmt.Sprintf("t%d", i),
					},
					RolloutStatus: rs,
				})
			}
			if got := AggregateRolloutStatus(targets); got != tc.want {
				t.Fatalf("AggregateRolloutStatus(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// AC2: per-kind determination tables (progressing/succeeded/failed/unknown)
// ---------------------------------------------------------------------------

func TestObserverDetermineDeploymentRolloutStates(t *testing.T) {
	deadlineFailed := appsv1.DeploymentStatus{
		ObservedGeneration: 3,
		Replicas:           2,
		UpdatedReplicas:    2,
		Conditions: []appsv1.DeploymentCondition{{
			Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse,
			Reason:  progressDeadlineExceededReason,
			Message: "ReplicaSet t11u-rs has timed out progressing",
		}},
	}
	cases := []struct {
		name    string
		deploy  *appsv1.Deployment
		target  imageflowv1alpha1.SubscriptionTarget
		entry   imageflowv1alpha1.TargetDeliveryStatus
		want    imageflowv1alpha1.RolloutStatus
		wantSub string
	}{
		{
			name:   "succeeded when generation caught up and all replicas updated and available",
			deploy: t11uDeploySucceeded(),
			target: t11uContainerTarget(t11uDeployRef()),
			entry:  t11uSeededEntry(t11uDeployRef(), t11uRefA),
			want:   imageflowv1alpha1.RolloutSucceeded,
		},
		{
			name:   "progressing while updated replicas lag the desired count",
			deploy: t11uDeployProgressing(),
			target: t11uContainerTarget(t11uDeployRef()),
			entry:  t11uSeededEntry(t11uDeployRef(), t11uRefA),
			want:   imageflowv1alpha1.RolloutProgressing,
		},
		{
			name:    "failed on ProgressDeadlineExceeded",
			deploy:  t11uDeployment(t11uRefA, deadlineFailed),
			target:  t11uContainerTarget(t11uDeployRef()),
			entry:   t11uSeededEntry(t11uDeployRef(), t11uRefA),
			want:    imageflowv1alpha1.RolloutFailed,
			wantSub: progressDeadlineExceededReason,
		},
		{
			name:    "unknown while the native controller reported nothing yet",
			deploy:  t11uDeployment(t11uRefA, appsv1.DeploymentStatus{}),
			target:  t11uContainerTarget(t11uDeployRef()),
			entry:   t11uSeededEntry(t11uDeployRef(), t11uRefA),
			want:    imageflowv1alpha1.RolloutUnknown,
			wantSub: "has not reported any status",
		},
		{
			name:    "unknown when the spec no longer holds the desired image",
			deploy:  t11uDeploySucceeded(),
			target:  t11uContainerTarget(t11uDeployRef()),
			entry:   t11uSeededEntry(t11uDeployRef(), t11uRefB),
			want:    imageflowv1alpha1.RolloutUnknown,
			wantSub: "currently references",
		},
		{
			name:    "unknown when the declared container is missing from the template",
			deploy:  t11uDeploySucceeded(),
			target:  imageflowv1alpha1.SubscriptionTarget{TargetRef: t11uDeployRef(), ContainerNames: []string{"ghost"}},
			entry:   t11uSeededEntry(t11uDeployRef(), t11uRefA),
			want:    imageflowv1alpha1.RolloutUnknown,
			wantSub: "not present in the pod template",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newT11uFixture(t, config.Config{}, tc.deploy)
			gvk := appsv1.SchemeGroupVersion.WithKind(kindDeployment)
			facts, found, err := f.reconciler.getWorkloadFacts(t11uCtx, gvk, tc.target.TargetRef)
			if err != nil || !found {
				t.Fatalf("getWorkloadFacts: found=%v err=%v", found, err)
			}
			pods, err := f.reconciler.listWorkloadPods(t11uCtx, facts)
			if err != nil {
				t.Fatalf("listWorkloadPods: %v", err)
			}
			det := determineRollout(facts, tc.target, tc.entry, pods)
			if det.status != tc.want {
				t.Fatalf("determineRollout = %q (%s), want %q", det.status, det.message, tc.want)
			}
			if tc.wantSub != "" && !strings.Contains(det.message, tc.wantSub) {
				t.Fatalf("message %q does not contain %q", det.message, tc.wantSub)
			}
		})
	}
}

func TestObserverDetermineStatefulSetRolloutStates(t *testing.T) {
	cases := []struct {
		name string
		sts  *appsv1.StatefulSet
		want imageflowv1alpha1.RolloutStatus
	}{
		{
			name: "succeeded when revisions converged and replicas are updated and ready",
			sts: t11uStatefulSet(t11uRefA, appsv1.StatefulSetStatus{
				ObservedGeneration: 2, Replicas: 2, ReadyReplicas: 2, UpdatedReplicas: 2,
				CurrentRevision: "r2", UpdateRevision: "r2",
			}),
			want: imageflowv1alpha1.RolloutSucceeded,
		},
		{
			name: "progressing while currentRevision lags updateRevision",
			sts: t11uStatefulSet(t11uRefA, appsv1.StatefulSetStatus{
				ObservedGeneration: 2, Replicas: 2, ReadyReplicas: 2, UpdatedReplicas: 1,
				CurrentRevision: "r1", UpdateRevision: "r2",
			}),
			want: imageflowv1alpha1.RolloutProgressing,
		},
		{
			name: "progressing while ready replicas lag the desired count",
			sts: t11uStatefulSet(t11uRefA, appsv1.StatefulSetStatus{
				ObservedGeneration: 2, Replicas: 2, ReadyReplicas: 1, UpdatedReplicas: 2,
				CurrentRevision: "r2", UpdateRevision: "r2",
			}),
			want: imageflowv1alpha1.RolloutProgressing,
		},
		{
			name: "unknown while the native controller reported nothing yet",
			sts:  t11uStatefulSet(t11uRefA, appsv1.StatefulSetStatus{}),
			want: imageflowv1alpha1.RolloutUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newT11uFixture(t, config.Config{}, tc.sts)
			gvk := appsv1.SchemeGroupVersion.WithKind(kindStatefulSet)
			target := t11uContainerTarget(t11uSTSRef())
			facts, found, err := f.reconciler.getWorkloadFacts(t11uCtx, gvk, target.TargetRef)
			if err != nil || !found {
				t.Fatalf("getWorkloadFacts: found=%v err=%v", found, err)
			}
			pods, err := f.reconciler.listWorkloadPods(t11uCtx, facts)
			if err != nil {
				t.Fatalf("listWorkloadPods: %v", err)
			}
			det := determineRollout(facts, target, t11uSeededEntry(target.TargetRef, t11uRefA), pods)
			if det.status != tc.want {
				t.Fatalf("determineRollout = %q (%s), want %q", det.status, det.message, tc.want)
			}
		})
	}
}

func TestObserverDetermineDaemonSetRolloutStates(t *testing.T) {
	cases := []struct {
		name string
		ds   *appsv1.DaemonSet
		want imageflowv1alpha1.RolloutStatus
	}{
		{
			name: "succeeded when every desired pod is updated available and ready",
			ds: t11uDaemonSet(t11uRefA, appsv1.DaemonSetStatus{
				ObservedGeneration: 5, DesiredNumberScheduled: 3, CurrentNumberScheduled: 3,
				UpdatedNumberScheduled: 3, NumberAvailable: 3, NumberReady: 3, NumberMisscheduled: 0,
			}),
			want: imageflowv1alpha1.RolloutSucceeded,
		},
		{
			name: "progressing while updated pods lag the desired schedule",
			ds: t11uDaemonSet(t11uRefA, appsv1.DaemonSetStatus{
				ObservedGeneration: 5, DesiredNumberScheduled: 3, CurrentNumberScheduled: 3,
				UpdatedNumberScheduled: 1, NumberAvailable: 3, NumberReady: 3,
			}),
			want: imageflowv1alpha1.RolloutProgressing,
		},
		{
			name: "progressing while misscheduled pods remain",
			ds: t11uDaemonSet(t11uRefA, appsv1.DaemonSetStatus{
				ObservedGeneration: 5, DesiredNumberScheduled: 3, CurrentNumberScheduled: 3,
				UpdatedNumberScheduled: 3, NumberAvailable: 3, NumberReady: 3, NumberMisscheduled: 1,
			}),
			want: imageflowv1alpha1.RolloutProgressing,
		},
		{
			name: "unknown while the native controller reported nothing yet",
			ds:   t11uDaemonSet(t11uRefA, appsv1.DaemonSetStatus{}),
			want: imageflowv1alpha1.RolloutUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newT11uFixture(t, config.Config{}, tc.ds)
			gvk := appsv1.SchemeGroupVersion.WithKind(kindDaemonSet)
			target := t11uContainerTarget(t11uDSRef())
			facts, found, err := f.reconciler.getWorkloadFacts(t11uCtx, gvk, target.TargetRef)
			if err != nil || !found {
				t.Fatalf("getWorkloadFacts: found=%v err=%v", found, err)
			}
			pods, err := f.reconciler.listWorkloadPods(t11uCtx, facts)
			if err != nil {
				t.Fatalf("listWorkloadPods: %v", err)
			}
			det := determineRollout(facts, target, t11uSeededEntry(target.TargetRef, t11uRefA), pods)
			if det.status != tc.want {
				t.Fatalf("determineRollout = %q (%s), want %q", det.status, det.message, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// AC2 (failed): pod-level image-failure scan for all three kinds
// ---------------------------------------------------------------------------

func TestObserverPodImageFailureMarksRolloutFailed(t *testing.T) {
	deployNotAvailable := appsv1.DeploymentStatus{
		ObservedGeneration: 3, Replicas: 2, UpdatedReplicas: 2, AvailableReplicas: 0,
		Conditions: []appsv1.DeploymentCondition{{
			Type: appsv1.DeploymentAvailable, Status: corev1.ConditionFalse,
			Reason: "MinimumReplicasUnavailable",
		}},
	}
	cases := []struct {
		name       string
		objs       []client.Object
		gvk        schema.GroupVersionKind
		target     imageflowv1alpha1.SubscriptionTarget
		want       imageflowv1alpha1.RolloutStatus
		wantSubs   []string
		wantNotSub string
	}{
		{
			name: "deployment fails through the ReplicaSet two-hop on ImagePullBackOff",
			objs: []client.Object{
				t11uDeployment(t11uRefA, deployNotAvailable),
				t11uReplicaSet(),
				t11uPod(
					t11uOwnerRef("ReplicaSet", t11uRSName, t11uRSUID),
					t11uWaitingStatus(t11uRefA, t11uWaitingPullBackOff, `Back-off pulling image "`+t11uRefA+`"`)),
			},
			gvk:      appsv1.SchemeGroupVersion.WithKind(kindDeployment),
			target:   t11uContainerTarget(t11uDeployRef()),
			want:     imageflowv1alpha1.RolloutFailed,
			wantSubs: []string{t11uPodName, t11uWaitingPullBackOff, t11uContainer},
		},
		{
			name: "a pod stuck on a FOREIGN image does not fail this delivery",
			objs: []client.Object{
				t11uDeployment(t11uRefA, deployNotAvailable),
				t11uReplicaSet(),
				t11uPod(
					t11uOwnerRef("ReplicaSet", t11uRSName, t11uRSUID),
					t11uWaitingStatus(t11uRefB, "ErrImagePull", "old replica still terminating")),
			},
			gvk:    appsv1.SchemeGroupVersion.WithKind(kindDeployment),
			target: t11uContainerTarget(t11uDeployRef()),
			want:   imageflowv1alpha1.RolloutProgressing,
		},
		{
			name: "CrashLoopBackOff is health-domain and never fails the rollout stage",
			objs: []client.Object{
				t11uDeployment(t11uRefA, deployNotAvailable),
				t11uReplicaSet(),
				t11uPod(
					t11uOwnerRef("ReplicaSet", t11uRSName, t11uRSUID),
					t11uWaitingStatus(t11uRefA, "CrashLoopBackOff", "back-off restarting failed container")),
			},
			gvk:        appsv1.SchemeGroupVersion.WithKind(kindDeployment),
			target:     t11uContainerTarget(t11uDeployRef()),
			want:       imageflowv1alpha1.RolloutProgressing,
			wantNotSub: "failed",
		},
		{
			name: "statefulset fails on direct pod ownership with ErrImagePull",
			objs: []client.Object{
				t11uStatefulSet(t11uRefA, appsv1.StatefulSetStatus{
					ObservedGeneration: 2, Replicas: 2, ReadyReplicas: 0, UpdatedReplicas: 0,
					CurrentRevision: "r1", UpdateRevision: "r2",
				}),
				t11uPod(
					t11uOwnerRef(kindStatefulSet, t11uSTSName, t11uSTSUID),
					t11uWaitingStatus(t11uRefA, "ErrImagePull", "manifest unknown")),
			},
			gvk:      appsv1.SchemeGroupVersion.WithKind(kindStatefulSet),
			target:   t11uContainerTarget(t11uSTSRef()),
			want:     imageflowv1alpha1.RolloutFailed,
			wantSubs: []string{"ErrImagePull", "manifest unknown"},
		},
		{
			name: "daemonset fails on an init container image the kubelet rejects",
			objs: []client.Object{
				t11uDaemonSet(t11uRefA, appsv1.DaemonSetStatus{
					ObservedGeneration: 5, DesiredNumberScheduled: 1, CurrentNumberScheduled: 1,
					UpdatedNumberScheduled: 1, NumberAvailable: 0, NumberReady: 0,
				}),
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: t11uNamespace, Name: t11uPodName,
						OwnerReferences: []metav1.OwnerReference{
							t11uOwnerRef(kindDaemonSet, t11uDSName, t11uDSUID),
						},
					},
					Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{
						t11uWaitingStatus(t11uRefA, "CreateContainerConfigError", "configmap not found"),
					}},
				},
			},
			gvk:      appsv1.SchemeGroupVersion.WithKind(kindDaemonSet),
			target:   t11uContainerTarget(t11uDSRef()),
			want:     imageflowv1alpha1.RolloutFailed,
			wantSubs: []string{"CreateContainerConfigError"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newT11uFixture(t, config.Config{}, tc.objs...)
			facts, found, err := f.reconciler.getWorkloadFacts(t11uCtx, tc.gvk, tc.target.TargetRef)
			if err != nil || !found {
				t.Fatalf("getWorkloadFacts: found=%v err=%v", found, err)
			}
			pods, err := f.reconciler.listWorkloadPods(t11uCtx, facts)
			if err != nil {
				t.Fatalf("listWorkloadPods: %v", err)
			}
			det := determineRollout(facts, tc.target, t11uSeededEntry(tc.target.TargetRef, t11uRefA), pods)
			if det.status != tc.want {
				t.Fatalf("determineRollout = %q (%s), want %q", det.status, det.message, tc.want)
			}
			for _, sub := range tc.wantSubs {
				if !strings.Contains(det.message, sub) {
					t.Fatalf("message %q does not contain %q", det.message, sub)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// T4 (spec v1.5): native health determination — per-kind verdict matrix,
// the rollout gate, and CUE priority over the native scan.
// ---------------------------------------------------------------------------

func TestObserverDetermineDeploymentHealthStates(t *testing.T) {
	rsOwner := t11uOwnerRef("ReplicaSet", t11uRSName, t11uRSUID)
	cases := []struct {
		name string
		objs []client.Object
		want imageflowv1alpha1.HealthStatus
	}{
		{
			name: "healthy when every container is ready with zero restarts",
			objs: []client.Object{
				t11uReplicaSet(),
				t11uHealthPod("t11u-pod-a", rsOwner, t11uHealthStatus(t11uRefA, true, 0, "")),
			},
			want: imageflowv1alpha1.HealthHealthy,
		},
		{
			name: "healthy when ready containers recovered after restarts",
			objs: []client.Object{
				t11uReplicaSet(),
				t11uHealthPod("t11u-pod-a", rsOwner, t11uHealthStatus(t11uRefA, true, 3, "")),
			},
			want: imageflowv1alpha1.HealthHealthy,
		},
		{
			name: "unhealthy when a container is not ready after restarts",
			objs: []client.Object{
				t11uReplicaSet(),
				t11uHealthPod("t11u-pod-a", rsOwner, t11uHealthStatus(t11uRefA, false, 5, "CrashLoopBackOff")),
			},
			want: imageflowv1alpha1.HealthUnhealthy,
		},
		{
			// Spec v1.5 D3 row 4: a not-ready container with zero restarts
			// behind a Succeeded rollout is Unhealthy — the rollout verdict
			// asserts startup already finished, so "awaiting initial
			// startup" has no honest window and the deployment failed to
			// come up (scanPodsForHealth rule 4).
			name: "unhealthy when a not-ready container has no restarts after a succeeded rollout",
			objs: []client.Object{
				t11uReplicaSet(),
				t11uHealthPod("t11u-pod-a", rsOwner, t11uHealthStatus(t11uRefA, false, 0, "")),
			},
			want: imageflowv1alpha1.HealthUnhealthy,
		},
		{
			name: "worst case wins across pods of the same workload",
			objs: []client.Object{
				t11uReplicaSet(),
				t11uHealthPod("t11u-pod-a", rsOwner, t11uHealthStatus(t11uRefA, true, 0, "")),
				t11uHealthPod("t11u-pod-b", rsOwner, t11uHealthStatus(t11uRefA, false, 2, "CrashLoopBackOff")),
			},
			want: imageflowv1alpha1.HealthUnhealthy,
		},
		{
			name: "unknown when no pods are found for the workload",
			objs: []client.Object{t11uReplicaSet()},
			want: imageflowv1alpha1.HealthUnknown,
		},
		{
			name: "a pod running a foreign image says nothing about this delivery",
			objs: []client.Object{
				t11uReplicaSet(),
				t11uHealthPod("t11u-pod-a", rsOwner, t11uHealthStatus(t11uRefB, true, 0, "")),
			},
			want: imageflowv1alpha1.HealthUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			objs := append([]client.Object{t11uDeploySucceeded()}, tc.objs...)
			f := newT11uFixture(t, config.Config{}, objs...)
			target := t11uContainerTarget(t11uDeployRef())
			gvk := appsv1.SchemeGroupVersion.WithKind(kindDeployment)
			facts, found, err := f.reconciler.getWorkloadFacts(t11uCtx, gvk, target.TargetRef)
			if err != nil || !found {
				t.Fatalf("getWorkloadFacts: found=%v err=%v", found, err)
			}
			pods, err := f.reconciler.listWorkloadPods(t11uCtx, facts)
			if err != nil {
				t.Fatalf("listWorkloadPods: %v", err)
			}
			if got := determineHealth(imageflowv1alpha1.RolloutSucceeded, pods, t11uRefA, observerKindDeployment); got != tc.want {
				t.Fatalf("determineHealth = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestObserverDetermineStatefulSetHealthStates(t *testing.T) {
	stsOwner := t11uOwnerRef(kindStatefulSet, t11uSTSName, t11uSTSUID)
	stsSucceeded := func() *appsv1.StatefulSet {
		return t11uStatefulSet(t11uRefA, appsv1.StatefulSetStatus{
			ObservedGeneration: 2, Replicas: 2, ReadyReplicas: 2, UpdatedReplicas: 2,
			CurrentRevision: "r2", UpdateRevision: "r2",
		})
	}
	cases := []struct {
		name string
		pods []*corev1.Pod
		want imageflowv1alpha1.HealthStatus
	}{
		{
			name: "healthy when every container is ready with zero restarts",
			pods: []*corev1.Pod{t11uHealthPod("t11u-pod-a", stsOwner, t11uHealthStatus(t11uRefA, true, 0, ""))},
			want: imageflowv1alpha1.HealthHealthy,
		},
		{
			name: "healthy when ready containers recovered after restarts",
			pods: []*corev1.Pod{t11uHealthPod("t11u-pod-a", stsOwner, t11uHealthStatus(t11uRefA, true, 4, ""))},
			want: imageflowv1alpha1.HealthHealthy,
		},
		{
			name: "unhealthy when a container is not ready after restarts",
			pods: []*corev1.Pod{t11uHealthPod("t11u-pod-a", stsOwner, t11uHealthStatus(t11uRefA, false, 7, "CrashLoopBackOff"))},
			want: imageflowv1alpha1.HealthUnhealthy,
		},
		{
			// Spec v1.5 D3 row 4, same as the Deployment table: Unhealthy,
			// not a startup transient, behind a Succeeded rollout.
			name: "unhealthy when a not-ready container has no restarts after a succeeded rollout",
			pods: []*corev1.Pod{t11uHealthPod("t11u-pod-a", stsOwner, t11uHealthStatus(t11uRefA, false, 0, ""))},
			want: imageflowv1alpha1.HealthUnhealthy,
		},
		{
			name: "unknown when no pods are found for the workload",
			pods: nil,
			want: imageflowv1alpha1.HealthUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			objs := []client.Object{stsSucceeded()}
			for _, pod := range tc.pods {
				objs = append(objs, pod)
			}
			f := newT11uFixture(t, config.Config{}, objs...)
			target := t11uContainerTarget(t11uSTSRef())
			gvk := appsv1.SchemeGroupVersion.WithKind(kindStatefulSet)
			facts, found, err := f.reconciler.getWorkloadFacts(t11uCtx, gvk, target.TargetRef)
			if err != nil || !found {
				t.Fatalf("getWorkloadFacts: found=%v err=%v", found, err)
			}
			pods, err := f.reconciler.listWorkloadPods(t11uCtx, facts)
			if err != nil {
				t.Fatalf("listWorkloadPods: %v", err)
			}
			if got := determineHealth(imageflowv1alpha1.RolloutSucceeded, pods, t11uRefA, observerKindStatefulSet); got != tc.want {
				t.Fatalf("determineHealth = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestObserverDetermineDaemonSetHealthStates(t *testing.T) {
	dsOwner := t11uOwnerRef(kindDaemonSet, t11uDSName, t11uDSUID)
	dsSucceeded := func() *appsv1.DaemonSet {
		return t11uDaemonSet(t11uRefA, appsv1.DaemonSetStatus{
			ObservedGeneration: 5, DesiredNumberScheduled: 2, CurrentNumberScheduled: 2,
			UpdatedNumberScheduled: 2, NumberAvailable: 2, NumberReady: 2,
		})
	}
	cases := []struct {
		name string
		pods []*corev1.Pod
		want imageflowv1alpha1.HealthStatus
	}{
		{
			name: "healthy when every container is ready with zero restarts",
			pods: []*corev1.Pod{t11uHealthPod("t11u-pod-a", dsOwner, t11uHealthStatus(t11uRefA, true, 0, ""))},
			want: imageflowv1alpha1.HealthHealthy,
		},
		{
			name: "healthy when ready containers recovered after restarts",
			pods: []*corev1.Pod{t11uHealthPod("t11u-pod-a", dsOwner, t11uHealthStatus(t11uRefA, true, 2, ""))},
			want: imageflowv1alpha1.HealthHealthy,
		},
		{
			name: "unhealthy when a container is not ready after restarts",
			pods: []*corev1.Pod{t11uHealthPod("t11u-pod-a", dsOwner, t11uHealthStatus(t11uRefA, false, 9, "CrashLoopBackOff"))},
			want: imageflowv1alpha1.HealthUnhealthy,
		},
		{
			// Spec v1.5 D3 row 4, same as the Deployment table: Unhealthy,
			// not a startup transient, behind a Succeeded rollout.
			name: "unhealthy when a not-ready container has no restarts after a succeeded rollout",
			pods: []*corev1.Pod{t11uHealthPod("t11u-pod-a", dsOwner, t11uHealthStatus(t11uRefA, false, 0, ""))},
			want: imageflowv1alpha1.HealthUnhealthy,
		},
		{
			name: "unknown when no pods are found for the workload",
			pods: nil,
			want: imageflowv1alpha1.HealthUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			objs := []client.Object{dsSucceeded()}
			for _, pod := range tc.pods {
				objs = append(objs, pod)
			}
			f := newT11uFixture(t, config.Config{}, objs...)
			target := t11uContainerTarget(t11uDSRef())
			gvk := appsv1.SchemeGroupVersion.WithKind(kindDaemonSet)
			facts, found, err := f.reconciler.getWorkloadFacts(t11uCtx, gvk, target.TargetRef)
			if err != nil || !found {
				t.Fatalf("getWorkloadFacts: found=%v err=%v", found, err)
			}
			pods, err := f.reconciler.listWorkloadPods(t11uCtx, facts)
			if err != nil {
				t.Fatalf("listWorkloadPods: %v", err)
			}
			if got := determineHealth(imageflowv1alpha1.RolloutSucceeded, pods, t11uRefA, observerKindDaemonSet); got != tc.want {
				t.Fatalf("determineHealth = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestObserverHealthRolloutGateBlocksEvaluation(t *testing.T) {
	cases := []struct {
		name    string
		rollout imageflowv1alpha1.RolloutStatus
	}{
		{"progressing", imageflowv1alpha1.RolloutProgressing},
		{"failed", imageflowv1alpha1.RolloutFailed},
		{"unknown", imageflowv1alpha1.RolloutUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Seed a pod that WOULD be Healthy if scanned: the gate verdict
			// proves scanPodsForHealth never ran (spec v1.5 D2/D7).
			f := newT11uFixture(t, config.Config{},
				t11uDeploySucceeded(),
				t11uReplicaSet(),
				t11uHealthPod("t11u-pod-a", t11uOwnerRef("ReplicaSet", t11uRSName, t11uRSUID),
					t11uHealthStatus(t11uRefA, true, 0, "")))
			target := t11uContainerTarget(t11uDeployRef())
			gvk := appsv1.SchemeGroupVersion.WithKind(kindDeployment)
			facts, found, err := f.reconciler.getWorkloadFacts(t11uCtx, gvk, target.TargetRef)
			if err != nil || !found {
				t.Fatalf("getWorkloadFacts: found=%v err=%v", found, err)
			}
			pods, err := f.reconciler.listWorkloadPods(t11uCtx, facts)
			if err != nil {
				t.Fatalf("listWorkloadPods: %v", err)
			}
			if got := determineHealth(tc.rollout, pods, t11uRefA, observerKindDeployment); got != imageflowv1alpha1.HealthUnknown {
				t.Fatalf("determineHealth = %q, want Unknown behind the rollout gate (the seeded pod would be Healthy if scanned)", got)
			}
		})
	}
}

func TestObserverHealthCUEPriorityOverNative(t *testing.T) {
	// The seeded pod would make the NATIVE verdict Healthy ("all containers
	// ready"); the CUE policy reports Unhealthy. Spec v1.5 D4: a configured
	// statusPolicyRef.healthKey keeps the v1.3 CUE evaluator authoritative.
	policyRef := func() *imageflowv1alpha1.StatusPolicyRef {
		return &imageflowv1alpha1.StatusPolicyRef{
			Namespace: t11uNamespace, Name: "t11u-policy", HealthKey: "health.cue",
		}
	}
	policyCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: t11uNamespace, Name: "t11u-policy"},
		Data:       map[string]string{"health.cue": `status: "Unhealthy"`},
	}
	healthyPod := t11uHealthPod("t11u-pod-a", t11uOwnerRef("ReplicaSet", t11uRSName, t11uRSUID),
		t11uHealthStatus(t11uRefA, true, 0, ""))
	mkTarget := func() imageflowv1alpha1.SubscriptionTarget {
		target := t11uContainerTarget(t11uDeployRef())
		target.StatusPolicyRef = policyRef()
		return target
	}
	mkSub := func(target imageflowv1alpha1.SubscriptionTarget) *imageflowv1alpha1.ImageSubscription {
		return t11uSub([]imageflowv1alpha1.SubscriptionTarget{target}, imageflowv1alpha1.ImageSubscriptionStatus{
			Conditions:         t11uAuthorizedConditions(),
			Targets:            []imageflowv1alpha1.TargetDeliveryStatus{t11uSeededEntry(t11uDeployRef(), t11uRefA)},
			RolloutStatus:      imageflowv1alpha1.RolloutProgressing,
			ObservedGeneration: 1,
		})
	}

	t.Run("healthKey routes health to the CUE evaluator and skips the native scan", func(t *testing.T) {
		f := newT11uFixture(t, config.Config{}, t11uDeploySucceeded(), t11uReplicaSet(), healthyPod, policyCM)
		f.reconciler = NewSubscriptionObserverReconciler(f.client, nil, f.recorder, f.clock, f.cfg, f.metrics, cueeval.NewEvaluator())
		target := mkTarget()
		out := f.reconciler.observeTarget(t11uCtx, mkSub(target), target)
		if out == nil {
			t.Fatal("observeTarget returned nil, want a planned outcome")
		}
		if out.evalErrClass != "" {
			t.Fatalf("CUE evaluation failed: %s/%s", out.evalErrClass, out.evalErrReason)
		}
		if out.health != imageflowv1alpha1.HealthUnhealthy {
			t.Fatalf("health = %q, want Unhealthy from CUE (native pods would say Healthy)", out.health)
		}
		if out.healthTransition == nil ||
			out.healthTransition.from != imageflowv1alpha1.HealthUnknown ||
			out.healthTransition.to != imageflowv1alpha1.HealthUnhealthy {
			t.Fatalf("healthTransition = %+v, want Unknown -> Unhealthy", out.healthTransition)
		}
		// No rolloutKey is configured: rollout still comes from the native
		// ADR-006 observation — CUE supplements health, it does not replace.
		if out.rollout != imageflowv1alpha1.RolloutSucceeded {
			t.Fatalf("rollout = %q, want Succeeded from native observation", out.rollout)
		}
	})

	t.Run("healthKey skips the native scan even when the evaluator is deferred", func(t *testing.T) {
		// nil evaluator (fixture default): CUE cannot run, yet the native
		// health scan is still skipped — the healthKey gate in observeTarget
		// does not depend on evaluator availability.
		f := newT11uFixture(t, config.Config{}, t11uDeploySucceeded(), t11uReplicaSet(), healthyPod)
		target := mkTarget()
		out := f.reconciler.observeTarget(t11uCtx, mkSub(target), target)
		if out == nil {
			t.Fatal("observeTarget returned nil, want a planned outcome")
		}
		if out.health != "" || out.healthTransition != nil {
			t.Fatalf("health = %q transition %+v: the native scan ran despite healthKey", out.health, out.healthTransition)
		}
		if out.rollout != imageflowv1alpha1.RolloutSucceeded {
			t.Fatalf("rollout = %q, want Succeeded from native observation", out.rollout)
		}
	})
}

// ---------------------------------------------------------------------------
// AC3: observability classification + honest Unknown (never fabricated)
// ---------------------------------------------------------------------------

func TestObserverObservableBuiltinKindClassification(t *testing.T) {
	deployGVK := appsv1.SchemeGroupVersion.WithKind(kindDeployment)
	edgeTarget := imageflowv1alpha1.SubscriptionTarget{
		TargetRef:      t11uEdgeRef(),
		ContainerNames: []string{t11uContainer},
	}
	cases := []struct {
		name        string
		gvk         schema.GroupVersionKind
		target      imageflowv1alpha1.SubscriptionTarget
		wantKind    string
		wantObserve bool
	}{
		{"deployment by container name is observable", deployGVK, t11uContainerTarget(t11uDeployRef()), observerKindDeployment, true},
		{"statefulset by container name is observable", appsv1.SchemeGroupVersion.WithKind(kindStatefulSet), t11uContainerTarget(t11uSTSRef()), observerKindStatefulSet, true},
		{"daemonset by container name is observable", appsv1.SchemeGroupVersion.WithKind(kindDaemonSet), t11uContainerTarget(t11uDSRef()), observerKindDaemonSet, true},
		{"imageFields locator on a built-in kind is NOT observable", deployGVK, imageflowv1alpha1.SubscriptionTarget{
			TargetRef:   t11uDeployRef(),
			ImageFields: []string{".spec.template.spec.containers[name=app].image"},
		}, "", false},
		{"built-in kind without container names is NOT observable", deployGVK, imageflowv1alpha1.SubscriptionTarget{
			TargetRef: t11uDeployRef(),
		}, "", false},
		{"third-party CRD is NOT observable", t11uEdgeGVK, edgeTarget, "", false},
		{"job by container name is observable", batchv1.SchemeGroupVersion.WithKind(kindJob), imageflowv1alpha1.SubscriptionTarget{
			TargetRef: imageflowv1alpha1.TargetReference{
				APIVersion: "batch/v1", Kind: "Job", Namespace: t11uNamespace, Name: "t11u-job",
			},
			ContainerNames: []string{t11uContainer},
		}, observerKindJob, true},
		{"cronjob by container name is observable", batchv1.SchemeGroupVersion.WithKind(kindCronJob), imageflowv1alpha1.SubscriptionTarget{
			TargetRef: imageflowv1alpha1.TargetReference{
				APIVersion: "batch/v1", Kind: "CronJob", Namespace: t11uNamespace, Name: "t11u-cronjob",
			},
			ContainerNames: []string{t11uContainer},
		}, observerKindCronJob, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kind, observable := observableBuiltinKind(tc.gvk, tc.target)
			if observable != tc.wantObserve || kind != tc.wantKind {
				t.Fatalf("observableBuiltinKind = (%q, %v), want (%q, %v)", kind, observable, tc.wantKind, tc.wantObserve)
			}
		})
	}
}

func TestObserverStatusPolicyRefDoesNotDisableNativeBuiltinObservation(t *testing.T) {
	// ADR-005 §5 final sentence: built-in kinds' rollout observation follows
	// ADR-006 native status even when a statusPolicyRef is declared; only the
	// HEALTH stage defers to the (v1.1) evaluator and stays Unknown.
	target := t11uContainerTarget(t11uDeployRef())
	target.StatusPolicyRef = t11uPolicyRef()
	sub := t11uSub([]imageflowv1alpha1.SubscriptionTarget{target}, imageflowv1alpha1.ImageSubscriptionStatus{
		Conditions:         t11uAuthorizedConditions(),
		Targets:            []imageflowv1alpha1.TargetDeliveryStatus{t11uSeededEntry(t11uDeployRef(), t11uRefA)},
		RolloutStatus:      imageflowv1alpha1.RolloutProgressing,
		ObservedGeneration: 1,
	})
	f := newT11uFixture(t, config.Config{}, t11uDeploySucceeded(), sub)
	f.applyStatus(sub, func() {})
	if _, err := f.reconcileSub(); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := f.getSub()
	entry := got.Status.Targets[0]
	if entry.RolloutStatus != imageflowv1alpha1.RolloutSucceeded {
		t.Fatalf("rolloutStatus = %q, want Succeeded (native observation despite policyRef)", entry.RolloutStatus)
	}
	if entry.HealthStatus != imageflowv1alpha1.HealthUnknown {
		t.Fatalf("healthStatus = %q, want honest Unknown", entry.HealthStatus)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, ConditionTypeStatusPolicyEvaluated)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonStatusPolicyEvaluatorDeferred {
		t.Fatalf("StatusPolicyEvaluated condition = %+v, want False/EvaluatorDeferred", cond)
	}
	if !strings.Contains(cond.Message, "legacy contract") && !strings.Contains(cond.Message, "rolloutKey/healthKey") {
		t.Fatalf("condition message %q must explain the legacy deferral or missing CUE keys", cond.Message)
	}
}

func TestObserverHonestUnknownForThirdPartyTarget(t *testing.T) {
	target := imageflowv1alpha1.SubscriptionTarget{
		TargetRef:      t11uEdgeRef(),
		ContainerNames: []string{t11uContainer},
	}
	sub := t11uSub([]imageflowv1alpha1.SubscriptionTarget{target}, imageflowv1alpha1.ImageSubscriptionStatus{
		Conditions:         t11uAuthorizedConditions(),
		Targets:            []imageflowv1alpha1.TargetDeliveryStatus{t11uSeededEntry(t11uEdgeRef(), t11uRefA)},
		RecentDeliveries:   []imageflowv1alpha1.DeliveryResult{t11uSeededRecord(t11uEdgeRef(), t11uRefB, t11uRefA, t11uStart.Add(-time.Minute))},
		RolloutStatus:      imageflowv1alpha1.RolloutProgressing,
		ObservedGeneration: 1,
	})
	f := newT11uFixture(t, config.Config{}, sub)
	f.applyStatus(sub, func() {})
	res, err := f.reconcileSub()
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("honest Unknown must not poll: RequeueAfter = %v", res.RequeueAfter)
	}
	got := f.getSub()
	entry := got.Status.Targets[0]
	if entry.RolloutStatus != imageflowv1alpha1.RolloutUnknown {
		t.Fatalf("rolloutStatus = %q, want honest Unknown", entry.RolloutStatus)
	}
	if entry.Reason != TargetReasonApplied || entry.Message != t11uEngineSteadyMessage {
		t.Fatalf("entry Reason/Message must stay engine-owned: %q/%q", entry.Reason, entry.Message)
	}
	if entry.ImageApplied != true || entry.HealthStatus != imageflowv1alpha1.HealthUnknown {
		t.Fatalf("imageApplied/health must stay independent: %+v", entry)
	}
	if got.Status.RolloutStatus != imageflowv1alpha1.RolloutUnknown {
		t.Fatalf("aggregate = %q, want Unknown", got.Status.RolloutStatus)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, ConditionTypeStatusPolicyEvaluated)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonStatusPolicyNotConfigured {
		t.Fatalf("StatusPolicyEvaluated condition = %+v, want False/NotConfigured", cond)
	}
	// The in-flight record for the honest-Unknown target stays in-flight (no
	// terminal result was observed) — the engine's Superseded marking remains
	// the only writer from here.
	if got.Status.RecentDeliveries[0].RolloutResult != imageflowv1alpha1.RolloutProgressing {
		t.Fatalf("record result = %q, want in-flight Progressing", got.Status.RecentDeliveries[0].RolloutResult)
	}
	// Dual-namespace events degrade to the subscription side only: the
	// third-party kind is not in the scheme, so the uncached target re-Get
	// fails and the target-side copy is honestly skipped.
	events := f.drainEvents()
	unknownEvents := t11uEventsWith(events, EventReasonRolloutUnknown)
	if len(unknownEvents) != 1 {
		t.Fatalf("events = %v, want exactly one subscription-side RolloutUnknown", events)
	}
	for _, want := range []string{"never fabricated", "ADR-005", "Determination: "} {
		if !strings.Contains(unknownEvents[0], want) {
			t.Fatalf("event %q must carry the honest-Unknown determination (%q)", unknownEvents[0], want)
		}
	}
	if v := testutil.ToFloat64(f.metrics.rolloutOutcomes.WithLabelValues(observerKindOther, observerResultUnknown)); v != 1 {
		t.Fatalf("rollout outcome metric = %v, want 1", v)
	}
	if v := testutil.ToFloat64(f.metrics.healthOutcomes.WithLabelValues(observerHealthUnknown)); v != 1 {
		t.Fatalf("health outcome metric = %v, want 1", v)
	}
}

func TestObserverStatusPolicyConditionTransitions(t *testing.T) {
	computed := &imageflowv1alpha1.ImageSubscriptionStatus{}
	setStatusPolicyCondition(computed, 0, nil, 7, t11uStart)
	cond := meta.FindStatusCondition(computed.Conditions, ConditionTypeStatusPolicyEvaluated)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonStatusPolicyNotConfigured {
		t.Fatalf("condition = %+v, want False/NotConfigured", cond)
	}
	if cond.ObservedGeneration != 7 || !cond.LastTransitionTime.Time.Equal(t11uStart) {
		t.Fatalf("condition = %+v, want observedGeneration 7 pinned to the injected clock", cond)
	}
	// Reason flip without a Status flip keeps LastTransitionTime (standard
	// condition convention) but picks up the new generation and message.
	// 2 policyRefs with no evalResults → legacy EvaluatorDeferred.
	setStatusPolicyCondition(computed, 2, nil, 8, t11uStart.Add(time.Hour))
	cond = meta.FindStatusCondition(computed.Conditions, ConditionTypeStatusPolicyEvaluated)
	if cond.Reason != ReasonStatusPolicyEvaluatorDeferred || cond.ObservedGeneration != 8 {
		t.Fatalf("condition = %+v, want EvaluatorDeferred at generation 8", cond)
	}
	if !cond.LastTransitionTime.Time.Equal(t11uStart) {
		t.Fatalf("LastTransitionTime must not move without a status flip: %+v", cond)
	}
	if !strings.Contains(cond.Message, "2 target(s)") {
		t.Fatalf("message %q must name the policyRef count", cond.Message)
	}
}

// ---------------------------------------------------------------------------
// AC1/AC6/AC9: full sync — imageApplied ⊥ rolloutStatus, dual-ns audit
// events, metrics, idempotence
// ---------------------------------------------------------------------------

func TestObserverReconcileDeploymentSucceededDualNamespaceEvents(t *testing.T) {
	target := t11uContainerTarget(t11uDeployRef())
	sub := t11uSub([]imageflowv1alpha1.SubscriptionTarget{target}, imageflowv1alpha1.ImageSubscriptionStatus{
		Conditions: t11uAuthorizedConditions(),
		Targets:    []imageflowv1alpha1.TargetDeliveryStatus{t11uSeededEntry(t11uDeployRef(), t11uRefA)},
		RecentDeliveries: []imageflowv1alpha1.DeliveryResult{
			t11uSeededRecord(t11uDeployRef(), t11uRefB, t11uRefA, t11uStart.Add(-time.Minute)),
		},
		RolloutStatus:      imageflowv1alpha1.RolloutProgressing,
		ObservedGeneration: 1,
	})
	f := newT11uFixture(t, config.Config{}, t11uDeploySucceeded(), sub)
	f.applyStatus(sub, func() {})

	res, err := f.reconcileSub()
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("succeeded rollout must not poll: RequeueAfter = %v", res.RequeueAfter)
	}
	got := f.getSub()
	entry := got.Status.Targets[0]
	// AC1: imageApplied=true coexists with an independent rollout verdict.
	if !entry.ImageApplied || entry.RolloutStatus != imageflowv1alpha1.RolloutSucceeded {
		t.Fatalf("entry = %+v, want imageApplied=true + rollout Succeeded", entry)
	}
	if entry.HealthStatus != imageflowv1alpha1.HealthUnknown {
		t.Fatalf("healthStatus = %q, want honest Unknown (rollout success never implies health)", entry.HealthStatus)
	}
	if entry.Reason != TargetReasonApplied || entry.Message != t11uEngineSteadyMessage {
		t.Fatalf("entry Reason/Message must stay engine-owned: %q/%q", entry.Reason, entry.Message)
	}
	if !entry.LastTransitionTime.Time.Equal(t11uStart) {
		t.Fatalf("LastTransitionTime = %v, want the injected clock", entry.LastTransitionTime)
	}
	if got.Status.RolloutStatus != imageflowv1alpha1.RolloutSucceeded {
		t.Fatalf("aggregate = %q, want Succeeded", got.Status.RolloutStatus)
	}
	// In-flight record healed to the terminal result with the observer reason.
	rec := got.Status.RecentDeliveries[0]
	if rec.RolloutResult != imageflowv1alpha1.RolloutSucceeded || rec.Reason != TargetReasonRolloutSucceeded {
		t.Fatalf("record = %+v, want healed Succeeded/RolloutSucceeded", rec)
	}
	if rec.HealthResult != imageflowv1alpha1.HealthUnknown {
		t.Fatalf("record health = %q, want Unknown", rec.HealthResult)
	}

	// AC6: dual-namespace events with the full audit payload.
	events := f.drainEvents()
	succeeded := t11uEventsWith(events, EventReasonRolloutSucceeded)
	if len(succeeded) != 2 {
		t.Fatalf("events = %v, want exactly two RolloutSucceeded (subscription side + target side)", events)
	}
	for _, ev := range succeeded {
		for _, want := range []string{
			"Normal " + EventReasonRolloutSucceeded,
			"apps/v1 Deployment " + t11uNamespace + "/" + t11uDeployName,
			"source ImageStreamTag " + t11uNamespace + "/" + t11uSourceLogical,
			"old image " + t11uRefB,
			"new image " + t11uRefA,
			"imageResolved: digest " + t11uDigestA,
			"imageApplied: true",
			"rollout: Succeeded",
			"health: Unknown",
			"succeeded per native deployment status",
			"ADR-005",
		} {
			if !strings.Contains(ev, want) {
				t.Fatalf("event %q missing audit fragment %q", ev, want)
			}
		}
	}

	// AC9: per-stage outcome metrics.
	if v := testutil.ToFloat64(f.metrics.rolloutOutcomes.WithLabelValues(observerKindDeployment, observerResultSucceeded)); v != 1 {
		t.Fatalf("rollout metric = %v, want 1", v)
	}
	if v := testutil.ToFloat64(f.metrics.healthOutcomes.WithLabelValues(observerHealthUnknown)); v != 1 {
		t.Fatalf("health metric = %v, want 1", v)
	}

	// Idempotence: the steady-state pass writes nothing and emits nothing.
	res2, err := f.reconcileSub()
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if res2.RequeueAfter != 0 {
		t.Fatalf("steady state must not poll: %v", res2.RequeueAfter)
	}
	if more := f.drainEvents(); len(more) != 0 {
		t.Fatalf("steady state emitted events: %v", more)
	}
	if v := testutil.ToFloat64(f.metrics.rolloutOutcomes.WithLabelValues(observerKindDeployment, observerResultSucceeded)); v != 1 {
		t.Fatalf("steady state re-counted the metric: %v", v)
	}
	after := f.getSub()
	if !equality.Semantic.DeepEqual(got.Status, after.Status) {
		t.Fatal("steady state changed the persisted status")
	}
}

func TestObserverReconcileDeploymentFailedKeepsImageAppliedTrue(t *testing.T) {
	deploy := t11uDeployment(t11uRefA, appsv1.DeploymentStatus{
		ObservedGeneration: 3, Replicas: 2, UpdatedReplicas: 2, AvailableReplicas: 0,
		Conditions: []appsv1.DeploymentCondition{{
			Type: appsv1.DeploymentAvailable, Status: corev1.ConditionFalse,
			Reason: "MinimumReplicasUnavailable",
		}},
	})
	pod := t11uPod(
		t11uOwnerRef("ReplicaSet", t11uRSName, t11uRSUID),
		t11uWaitingStatus(t11uRefA, t11uWaitingPullBackOff, "registry unreachable"))
	target := t11uContainerTarget(t11uDeployRef())
	sub := t11uSub([]imageflowv1alpha1.SubscriptionTarget{target}, imageflowv1alpha1.ImageSubscriptionStatus{
		Conditions: t11uAuthorizedConditions(),
		Targets:    []imageflowv1alpha1.TargetDeliveryStatus{t11uSeededEntry(t11uDeployRef(), t11uRefA)},
		RecentDeliveries: []imageflowv1alpha1.DeliveryResult{
			t11uSeededRecord(t11uDeployRef(), t11uRefB, t11uRefA, t11uStart.Add(-time.Minute)),
		},
		RolloutStatus:      imageflowv1alpha1.RolloutProgressing,
		ObservedGeneration: 1,
	})
	f := newT11uFixture(t, config.Config{}, deploy, t11uReplicaSet(), pod, sub)
	f.applyStatus(sub, func() {})
	if _, err := f.reconcileSub(); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := f.getSub()
	entry := got.Status.Targets[0]
	// AC1 pin: the field WAS written (imageApplied=true) while the release
	// FAILED — the two stages are independent surfaces.
	if !entry.ImageApplied || entry.RolloutStatus != imageflowv1alpha1.RolloutFailed {
		t.Fatalf("entry = %+v, want imageApplied=true + rollout Failed", entry)
	}
	if entry.Reason != TargetReasonApplied || entry.Message != t11uEngineSteadyMessage {
		t.Fatalf("entry Reason/Message must stay engine-owned: %q/%q", entry.Reason, entry.Message)
	}
	if got.Status.RolloutStatus != imageflowv1alpha1.RolloutFailed {
		t.Fatalf("aggregate = %q, want Failed", got.Status.RolloutStatus)
	}
	rec := got.Status.RecentDeliveries[0]
	if rec.RolloutResult != imageflowv1alpha1.RolloutFailed || rec.Reason != TargetReasonRolloutFailed {
		t.Fatalf("record = %+v, want healed Failed", rec)
	}
	events := f.drainEvents()
	failed := t11uEventsWith(events, EventReasonRolloutFailed)
	if len(failed) != 2 {
		t.Fatalf("events = %v, want dual-namespace RolloutFailed", events)
	}
	if !strings.HasPrefix(failed[0], "Warning "+EventReasonRolloutFailed) {
		t.Fatalf("rollout failure must be a Warning event: %q", failed[0])
	}
	for _, want := range []string{t11uPodName, t11uWaitingPullBackOff, "registry unreachable"} {
		if !strings.Contains(failed[0], want) {
			t.Fatalf("failure event %q must carry the pod-level WHY (%q)", failed[0], want)
		}
	}
	if v := testutil.ToFloat64(f.metrics.rolloutOutcomes.WithLabelValues(observerKindDeployment, observerResultFailed)); v != 1 {
		t.Fatalf("rollout failure metric = %v, want 1", v)
	}
}

func TestObserverReconcileProgressingKeepsPollingWithoutEvents(t *testing.T) {
	target := t11uContainerTarget(t11uDeployRef())
	sub := t11uSub([]imageflowv1alpha1.SubscriptionTarget{target}, imageflowv1alpha1.ImageSubscriptionStatus{
		Conditions:         t11uAuthorizedConditions(),
		Targets:            []imageflowv1alpha1.TargetDeliveryStatus{t11uSeededEntry(t11uDeployRef(), t11uRefA)},
		RolloutStatus:      imageflowv1alpha1.RolloutProgressing,
		ObservedGeneration: 1,
	})
	f := newT11uFixture(t, config.Config{}, t11uDeployProgressing(), sub)
	f.applyStatus(sub, func() {})
	res, err := f.reconcileSub()
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != waitingStateRequeue {
		t.Fatalf("in-flight rollout must poll: RequeueAfter = %v, want %v", res.RequeueAfter, waitingStateRequeue)
	}
	got := f.getSub()
	entry := got.Status.Targets[0]
	if entry.RolloutStatus != imageflowv1alpha1.RolloutProgressing {
		t.Fatalf("rolloutStatus = %q, want Progressing", entry.RolloutStatus)
	}
	// No rollout transition ⇒ no rollout events and no outcome metrics; the
	// only write is the standing StatusPolicyEvaluated condition.
	for _, ev := range f.drainEvents() {
		if strings.Contains(ev, EventReasonRolloutProgressing) {
			t.Fatalf("steady Progressing must not emit rollout events: %q", ev)
		}
	}
	if v := testutil.ToFloat64(f.metrics.rolloutOutcomes.WithLabelValues(observerKindDeployment, observerResultProgressing)); v != 0 {
		t.Fatalf("progressing metric = %v, want 0 (no transition happened)", v)
	}
	if meta.FindStatusCondition(got.Status.Conditions, ConditionTypeStatusPolicyEvaluated) == nil {
		t.Fatal("StatusPolicyEvaluated condition must exist after the first pass")
	}
}

func TestObserverT8GateBlocksAllWrites(t *testing.T) {
	target := t11uContainerTarget(t11uDeployRef())
	seed := imageflowv1alpha1.ImageSubscriptionStatus{
		Targets:            []imageflowv1alpha1.TargetDeliveryStatus{t11uSeededEntry(t11uDeployRef(), t11uRefA)},
		ObservedGeneration: 1,
	}
	t.Run("missing Forbidden condition requeues without touching status", func(t *testing.T) {
		sub := t11uSub([]imageflowv1alpha1.SubscriptionTarget{target}, *seed.DeepCopy())
		f := newT11uFixture(t, config.Config{}, t11uDeploySucceeded(), sub)
		f.applyStatus(sub, func() {})
		res, err := f.reconcileSub()
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if res.RequeueAfter != authzPendingRequeue {
			t.Fatalf("RequeueAfter = %v, want %v", res.RequeueAfter, authzPendingRequeue)
		}
		if events := f.drainEvents(); len(events) != 0 {
			t.Fatalf("gate must be silent: %v", events)
		}
		got := f.getSub()
		if !equality.Semantic.DeepEqual(seed, got.Status) {
			t.Fatalf("gate must not write status: %+v", got.Status)
		}
	})
	t.Run("Forbidden=True stops the observer entirely", func(t *testing.T) {
		st := *seed.DeepCopy()
		st.Conditions = []metav1.Condition{{
			Type: imageflowv1alpha1.ConditionTypeForbidden, Status: metav1.ConditionTrue,
			Reason: "Denied", ObservedGeneration: 1,
			LastTransitionTime: metav1.NewTime(t11uStart.Add(-time.Hour)),
		}}
		sub := t11uSub([]imageflowv1alpha1.SubscriptionTarget{target}, st)
		f := newT11uFixture(t, config.Config{}, t11uDeploySucceeded(), sub)
		f.applyStatus(sub, func() {})
		res, err := f.reconcileSub()
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if res.RequeueAfter != 0 {
			t.Fatalf("RequeueAfter = %v, want 0", res.RequeueAfter)
		}
		if events := f.drainEvents(); len(events) != 0 {
			t.Fatalf("gate must be silent: %v", events)
		}
		got := f.getSub()
		if !equality.Semantic.DeepEqual(st, got.Status) {
			t.Fatalf("gate must not write status: %+v", got.Status)
		}
	})
}

func TestObserverSkipsTerminalAndForeignEntries(t *testing.T) {
	forbiddenEntry := t11uSeededEntry(t11uDeployRef(), t11uRefA)
	forbiddenEntry.Reason = TargetReasonForbidden
	conflictEntry := t11uSeededEntry(t11uSTSRef(), t11uRefA)
	conflictEntry.RolloutStatus = imageflowv1alpha1.RolloutConflict
	conflictEntry.Reason = TargetReasonConflict
	supersededEntry := t11uSeededEntry(t11uDSRef(), t11uRefA)
	supersededEntry.RolloutStatus = imageflowv1alpha1.RolloutSuperseded
	noDeliveryEntry := imageflowv1alpha1.TargetDeliveryStatus{
		TargetRef: t11uEdgeRef(), RolloutStatus: imageflowv1alpha1.RolloutUnknown,
		HealthStatus: imageflowv1alpha1.HealthUnknown,
	}
	sub := t11uSub(nil, imageflowv1alpha1.ImageSubscriptionStatus{
		Conditions: t11uAuthorizedConditions(),
		Targets: []imageflowv1alpha1.TargetDeliveryStatus{
			forbiddenEntry, conflictEntry, supersededEntry, noDeliveryEntry,
		},
	})
	f := newT11uFixture(t, config.Config{})
	cases := []struct {
		name   string
		target imageflowv1alpha1.SubscriptionTarget
	}{
		{"T8 Forbidden entry is owned end to end by the authz lane", t11uContainerTarget(t11uDeployRef())},
		{"terminal Conflict is engine-owned (ADR-008 first-wins)", t11uContainerTarget(t11uSTSRef())},
		{"entry-level Superseded is terminal", t11uContainerTarget(t11uDSRef())},
		{"entry without a desired image was never delivered", imageflowv1alpha1.SubscriptionTarget{TargetRef: t11uEdgeRef()}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if out := f.reconciler.observeTarget(t11uCtx, sub, tc.target); out != nil {
				t.Fatalf("observeTarget = %+v, want nil (no observation domain)", out)
			}
		})
	}
}

func TestObserverStaleRolloutResetWhenFieldNoLongerApplied(t *testing.T) {
	// The persisted Succeeded describes the PREVIOUS image: a re-target whose
	// apply has not landed yet must not keep a stale success around.
	sts := t11uStatefulSet(t11uRefA, appsv1.StatefulSetStatus{
		ObservedGeneration: 2, Replicas: 2, ReadyReplicas: 2, UpdatedReplicas: 2,
		CurrentRevision: "r2", UpdateRevision: "r2",
	})
	entry := t11uSeededEntry(t11uSTSRef(), t11uRefB)
	entry.ObservedImage = t11uRefA
	entry.ImageApplied = false
	entry.RolloutStatus = imageflowv1alpha1.RolloutSucceeded
	target := t11uContainerTarget(t11uSTSRef())
	sub := t11uSub([]imageflowv1alpha1.SubscriptionTarget{target}, imageflowv1alpha1.ImageSubscriptionStatus{
		Conditions:         t11uAuthorizedConditions(),
		Targets:            []imageflowv1alpha1.TargetDeliveryStatus{entry},
		RolloutStatus:      imageflowv1alpha1.RolloutSucceeded,
		ObservedGeneration: 1,
	})
	f := newT11uFixture(t, config.Config{}, sts, sub)
	f.applyStatus(sub, func() {})
	if _, err := f.reconcileSub(); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := f.getSub()
	if got.Status.Targets[0].RolloutStatus != imageflowv1alpha1.RolloutUnknown {
		t.Fatalf("rolloutStatus = %q, want honest Unknown for the stale success", got.Status.Targets[0].RolloutStatus)
	}
	if got.Status.Targets[0].Reason != TargetReasonApplied ||
		got.Status.Targets[0].Message != t11uEngineSteadyMessage {
		t.Fatalf("entry Reason/Message must stay engine-owned: %+v", got.Status.Targets[0])
	}
	resetEvents := t11uEventsWith(f.drainEvents(), EventReasonRolloutUnknown)
	if len(resetEvents) != 2 || !strings.Contains(resetEvents[0], "currently references") {
		t.Fatalf("stale-reset event must explain the spec/observed mismatch: %v", resetEvents)
	}
}

func TestObserverNeverWritesPerTargetReasonMessage(t *testing.T) {
	// R3 contract (issue #12): the per-target Reason/Message fields are the
	// delivery engine's ImageApplied-stage surface (frozen T9 vocabulary,
	// pinned by T9/T10 tests). The observer writes rolloutStatus,
	// LastTransitionTime, the StatusPolicyEvaluated Condition and healed
	// records — never Reason/Message. The honest-Unknown WHY travels on the
	// Condition and on transition Events.
	target := t11uContainerTarget(t11uEdgeRef())
	entry := t11uSeededEntry(t11uEdgeRef(), t11uRefA)
	entry.RolloutStatus = imageflowv1alpha1.RolloutUnknown
	entry.Message = "engine steady state"
	sub := t11uSub([]imageflowv1alpha1.SubscriptionTarget{target}, imageflowv1alpha1.ImageSubscriptionStatus{
		Conditions:         t11uAuthorizedConditions(),
		Targets:            []imageflowv1alpha1.TargetDeliveryStatus{entry},
		RolloutStatus:      imageflowv1alpha1.RolloutUnknown,
		ObservedGeneration: 1,
	})
	f := newT11uFixture(t, config.Config{}, sub)
	f.applyStatus(sub, func() {})
	if _, err := f.reconcileSub(); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := f.getSub()
	if got.Status.Targets[0].Reason != TargetReasonApplied ||
		got.Status.Targets[0].Message != "engine steady state" {
		t.Fatalf("observer must not touch entry Reason/Message: %+v", got.Status.Targets[0])
	}
	if !got.Status.Targets[0].LastTransitionTime.Time.Equal(t11uStart.Add(-time.Minute)) {
		t.Fatalf("no rollout state change ⇒ LastTransitionTime must not move: %v", got.Status.Targets[0].LastTransitionTime)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, ConditionTypeStatusPolicyEvaluated)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonStatusPolicyNotConfigured {
		t.Fatalf("honest-Unknown WHY must live on the Condition: %+v", cond)
	}
	if events := f.drainEvents(); len(events) != 0 {
		t.Fatalf("steady Unknown must be silent: %v", events)
	}
	before := got.Status.DeepCopy()
	if _, err := f.reconcileSub(); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	after := f.getSub()
	if !equality.Semantic.DeepEqual(*before, after.Status) {
		t.Fatal("second pass must be a no-op")
	}
}

// ---------------------------------------------------------------------------
// record healing (in-flight → terminal) boundaries
// ---------------------------------------------------------------------------

func TestObserverHealsOnlyInFlightRecordsOfTheObservedImage(t *testing.T) {
	deployRef := t11uDeployRef()
	stsRef := t11uSTSRef()
	deployEntry := t11uSeededEntry(deployRef, t11uRefA)
	deployEntry.RolloutStatus = imageflowv1alpha1.RolloutSucceeded
	stsEntry := t11uSeededEntry(stsRef, t11uRefB)
	stsEntry.RolloutStatus = imageflowv1alpha1.RolloutUnknown

	inFlight := t11uSeededRecord(deployRef, t11uRefB, t11uRefA, t11uStart)
	superseded := t11uSeededRecord(deployRef, t11uRefC, t11uRefA, t11uStart.Add(-time.Minute))
	superseded.RolloutResult = imageflowv1alpha1.RolloutSuperseded
	conflict := t11uSeededRecord(deployRef, t11uRefB, t11uRefA, t11uStart.Add(-2*time.Minute))
	conflict.RolloutResult = imageflowv1alpha1.RolloutConflict
	foreignImage := t11uSeededRecord(deployRef, t11uRefA, t11uRefC, t11uStart.Add(-3*time.Minute))
	unknownTarget := t11uSeededRecord(stsRef, t11uRefA, t11uRefB, t11uStart.Add(-4*time.Minute))

	computed := &imageflowv1alpha1.ImageSubscriptionStatus{
		Targets: []imageflowv1alpha1.TargetDeliveryStatus{deployEntry, stsEntry},
		RecentDeliveries: []imageflowv1alpha1.DeliveryResult{
			inFlight, superseded, conflict, foreignImage, unknownTarget,
		},
	}
	outcomes := []observerOutcome{
		{ref: deployRef, kindLabel: observerKindDeployment, rollout: imageflowv1alpha1.RolloutSucceeded},
		{ref: stsRef, kindLabel: observerKindStatefulSet, rollout: imageflowv1alpha1.RolloutUnknown},
	}
	healInFlightRecords(computed, outcomes)
	recs := computed.RecentDeliveries
	if recs[0].RolloutResult != imageflowv1alpha1.RolloutSucceeded || recs[0].Reason != TargetReasonRolloutSucceeded {
		t.Fatalf("in-flight record = %+v, want healed Succeeded", recs[0])
	}
	if recs[1].RolloutResult != imageflowv1alpha1.RolloutSuperseded {
		t.Fatalf("superseded record = %+v, want untouched", recs[1])
	}
	if recs[2].RolloutResult != imageflowv1alpha1.RolloutConflict {
		t.Fatalf("conflict record = %+v, want untouched", recs[2])
	}
	if recs[3].RolloutResult != imageflowv1alpha1.RolloutProgressing {
		t.Fatalf("foreign-image record = %+v, want untouched (attribution mismatch)", recs[3])
	}
	if recs[4].RolloutResult != imageflowv1alpha1.RolloutProgressing {
		t.Fatalf("unknown-rollout record = %+v, want left in-flight", recs[4])
	}
	for i := range recs {
		if recs[i].HealthResult != imageflowv1alpha1.HealthUnknown {
			t.Fatalf("record %d health = %q, want honest Unknown", i, recs[i].HealthResult)
		}
	}

	// Failed heals symmetrically.
	failedComputed := &imageflowv1alpha1.ImageSubscriptionStatus{
		Targets:          []imageflowv1alpha1.TargetDeliveryStatus{deployEntry},
		RecentDeliveries: []imageflowv1alpha1.DeliveryResult{inFlight},
	}
	failedComputed.RecentDeliveries[0].RolloutResult = imageflowv1alpha1.RolloutProgressing
	healInFlightRecords(failedComputed, []observerOutcome{
		{ref: deployRef, kindLabel: observerKindDeployment, rollout: imageflowv1alpha1.RolloutFailed},
	})
	if got := failedComputed.RecentDeliveries[0]; got.RolloutResult != imageflowv1alpha1.RolloutFailed || got.Reason != TargetReasonRolloutFailed {
		t.Fatalf("failed heal = %+v, want Failed/RolloutFailed", got)
	}
}

// ---------------------------------------------------------------------------
// AC5 (unit): latest-wins supersede plans the dual-namespace event
// ---------------------------------------------------------------------------

func TestSupersededEventPlannedOnLatestWins(t *testing.T) {
	deployRef := t11uDeployRef()
	oldRecord := t11uSeededRecord(deployRef, t11uRefB, t11uRefA, t11uStart.Add(-time.Minute))
	newRecord := t11uSeededRecord(deployRef, t11uRefA, t11uRefB, t11uStart)
	targetObj := &unstructured.Unstructured{}
	targetObj.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind(kindDeployment))
	targetObj.SetNamespace(t11uNamespace)
	targetObj.SetName(t11uDeployName)
	outcomes := []deliveryOutcome{{ref: deployRef, targetObject: targetObj}}
	transition := &deliveryTransition{}

	merged := mergeRecentDeliveries(
		[]imageflowv1alpha1.DeliveryResult{oldRecord},
		[]imageflowv1alpha1.DeliveryResult{newRecord},
		5, outcomes, transition,
	)
	if len(merged) != 2 || merged[0].NewImage != t11uRefB {
		t.Fatalf("window = %+v, want the new delivery prepended", merged)
	}
	if merged[1].RolloutResult != imageflowv1alpha1.RolloutSuperseded {
		t.Fatalf("old record = %+v, want terminal Superseded", merged[1])
	}
	if len(transition.events) != 1 {
		t.Fatalf("planned events = %+v, want exactly one DeliverySuperseded", transition.events)
	}
	ev := transition.events[0]
	if ev.eventType != "Normal" || ev.reason != EventReasonDeliverySuperseded {
		t.Fatalf("event = %+v, want Normal %s", ev, EventReasonDeliverySuperseded)
	}
	if !ev.targetSide || ev.targetObject == nil {
		t.Fatalf("event must be dual-namespace: %+v", ev)
	}
	for _, want := range []string{
		"latest-wins", "ADR-008", "Superseded",
		t11uRefA, t11uRefB, t11uSourceLogical,
		"apps/v1 Deployment " + t11uNamespace + "/" + t11uDeployName,
		"no retry", "no cross-target rollback",
	} {
		if !strings.Contains(ev.message, want) {
			t.Fatalf("superseded message %q missing audit fragment %q", ev.message, want)
		}
	}

	// Bounding still applies with the new signature.
	truncating := &deliveryTransition{}
	clamped := mergeRecentDeliveries(
		[]imageflowv1alpha1.DeliveryResult{oldRecord},
		[]imageflowv1alpha1.DeliveryResult{newRecord},
		1, outcomes, truncating,
	)
	if len(clamped) != 1 || truncating.truncated != 1 {
		t.Fatalf("clamp = %+v truncated = %d, want one entry and truncated=1", clamped, truncating.truncated)
	}
}

// ---------------------------------------------------------------------------
// Engine steady state preserves the observer's rollout verdict (T9 mergeEntry
// contract — rolloutSet == nil keeps the persisted value; R3: reasons stay
// engine-owned on both sides)
// ---------------------------------------------------------------------------

func TestEngineSteadyStatePreservesObserverRolloutVerdict(t *testing.T) {
	f := newT11uFixture(t, config.Config{})
	eng := NewSubscriptionDeliveryReconciler(f.client, nil, f.recorder, f.clock, f.cfg, newDeliveryMetrics())
	deployRef := t11uDeployRef()
	source := sourceState{
		spec: t11uSourceRef(), name: t11uSourceLogical,
		ref: t11uRefA, digest: t11uDigestA,
		// Digest format (v1.2 T4): the written/compared value is the digest
		// reference itself; resolvedDigest stays empty.
		desiredImage: t11uRefA,
		created:      ptr.To(metav1.NewTime(t11uStart.Add(-90 * time.Second))),
	}
	base := t11uSeededEntry(deployRef, t11uRefA)
	base.RolloutStatus = imageflowv1alpha1.RolloutSucceeded // observer-written verdict
	base.LastTransitionTime = ptr.To(metav1.NewTime(t11uStart.Add(-time.Hour)))

	t.Run("steady-state converged pass keeps the observer verdict and plans nothing", func(t *testing.T) {
		out := eng.convergedOutcome(deliveryOutcome{ref: deployRef, converged: true, locator: t11uLocator}, source)
		merged, plan := mergeEntry(2, base, &out, source, t11uStart)
		if merged.Reason != TargetReasonApplied || !strings.Contains(merged.Message, "SSA field manager") {
			t.Fatalf("merged reason/message = %q/%q, want the engine Applied surface", merged.Reason, merged.Message)
		}
		if merged.RolloutStatus != imageflowv1alpha1.RolloutSucceeded {
			t.Fatalf("merged rollout = %q, want preserved Succeeded", merged.RolloutStatus)
		}
		if merged.HealthStatus != imageflowv1alpha1.HealthUnknown {
			t.Fatalf("merged health = %q, want untouched Unknown", merged.HealthStatus)
		}
		if !merged.LastTransitionTime.Time.Equal(t11uStart.Add(-time.Hour)) {
			t.Fatalf("LastTransitionTime moved without a state change: %v", merged.LastTransitionTime)
		}
		if plan.record != nil || len(plan.events) != 0 || plan.failureLabel != "" {
			t.Fatalf("steady state must plan nothing: %+v", plan)
		}
	})

	t.Run("a value-changing delivery seeds Progressing and keeps the engine WHY", func(t *testing.T) {
		out := eng.convergedOutcome(deliveryOutcome{ref: deployRef, converged: true, delivered: true, locator: t11uLocator}, source)
		merged, plan := mergeEntry(2, base, &out, source, t11uStart)
		if merged.Reason != TargetReasonApplied || merged.RolloutStatus != imageflowv1alpha1.RolloutProgressing {
			t.Fatalf("merged = %+v, want Applied/Progressing on a fresh delivery", merged)
		}
		if !merged.LastTransitionTime.Time.Equal(t11uStart) {
			t.Fatalf("state change must re-pin LastTransitionTime: %v", merged.LastTransitionTime)
		}
		if plan.record == nil || plan.record.RolloutResult != imageflowv1alpha1.RolloutProgressing {
			t.Fatalf("delivery must plan a record: %+v", plan.record)
		}
		if len(plan.events) != 1 || plan.events[0].reason != EventReasonImageApplied {
			t.Fatalf("delivery must plan the ImageApplied event: %+v", plan.events)
		}
	})

	t.Run("a delivery failure writes its own WHY", func(t *testing.T) {
		out := deliveryOutcome{ref: deployRef, reason: TargetReasonApplyFailed, message: "apply exploded", retryDelta: 1}
		merged, plan := mergeEntry(2, base, &out, source, t11uStart)
		if merged.Reason != TargetReasonApplyFailed || merged.Message != "apply exploded" {
			t.Fatalf("merged = %+v, want the delivery failure WHY", merged)
		}
		if merged.RetryCount != base.RetryCount+1 || merged.NextRetryTime == nil {
			t.Fatalf("retry bookkeeping = %+v, want delta applied", merged)
		}
		if plan.failureLabel == "" {
			t.Fatal("failure must plan its metric label")
		}
	})

}

// ---------------------------------------------------------------------------
// AC7/AC8 (unit): mirror delivery float-up — replacement, bounding, idempotence
// ---------------------------------------------------------------------------

func TestMergeDeliveryFloatEventsReplacesInFlightAndBounds(t *testing.T) {
	stored := []imageflowv1alpha1.MirrorStatusEvent{
		t11uImageChangedEv(t11uStart.Add(-time.Hour), t11uDigestA, t11uDigestB),
		t11uDeliveryEv(t11uStart, t11uDigestB, imageflowv1alpha1.RolloutProgressing),
	}
	rendered := []imageflowv1alpha1.MirrorStatusEvent{
		t11uDeliveryEv(t11uStart, t11uDigestB, imageflowv1alpha1.RolloutSucceeded),
	}
	merged, dropped := mergeDeliveryFloatEvents(stored, rendered, 5)
	if dropped != 0 {
		t.Fatalf("dropped = %d, want 0", dropped)
	}
	if len(merged) != 2 {
		t.Fatalf("merged = %+v, want the ImageChanged entry plus ONE Delivery view", merged)
	}
	deliveries := 0
	for _, ev := range merged {
		if ev.Type == imageflowv1alpha1.MirrorEventDelivery {
			deliveries++
			if ev.Result != imageflowv1alpha1.RolloutSucceeded {
				t.Fatalf("stored in-flight view was not replaced: %+v", ev)
			}
		}
	}
	if deliveries != 1 {
		t.Fatalf("deliveries = %d, want 1 (remove-then-insert on the stable identity)", deliveries)
	}
	// Deterministic idempotence: re-merging the merged window is a no-op.
	again, dropped2 := mergeDeliveryFloatEvents(merged, rendered, 5)
	if dropped2 != 0 || !equality.Semantic.DeepEqual(merged, again) {
		t.Fatalf("re-merge changed the window: %+v dropped=%d", again, dropped2)
	}

	// Bounding: the oldest entries leave the window and are reported.
	fiveOld := []imageflowv1alpha1.MirrorStatusEvent{
		t11uImageChangedEv(t11uStart.Add(-5*time.Hour), "", t11uDigestA),
		t11uImageChangedEv(t11uStart.Add(-4*time.Hour), "", t11uDigestA),
		t11uImageChangedEv(t11uStart.Add(-3*time.Hour), "", t11uDigestB),
		t11uImageChangedEv(t11uStart.Add(-2*time.Hour), "", t11uDigestC),
		t11uImageChangedEv(t11uStart.Add(-1*time.Hour), "", t11uDigestC),
	}
	twoNew := []imageflowv1alpha1.MirrorStatusEvent{
		t11uDeliveryEv(t11uStart.Add(2*time.Minute), t11uDigestB, imageflowv1alpha1.RolloutSucceeded),
		t11uDeliveryEv(t11uStart.Add(time.Minute), t11uDigestC, imageflowv1alpha1.RolloutFailed),
	}
	bounded, boundedDropped := mergeDeliveryFloatEvents(fiveOld, twoNew, 5)
	if len(bounded) != 5 || boundedDropped != 2 {
		t.Fatalf("bounded = %d entries dropped = %d, want 5/2", len(bounded), boundedDropped)
	}
	if bounded[0].Type != imageflowv1alpha1.MirrorEventDelivery || bounded[1].Type != imageflowv1alpha1.MirrorEventDelivery {
		t.Fatalf("newest-first order broken: %+v", bounded[:2])
	}
}

func TestRenderFloatEventsDedupesByIdentityNewestWins(t *testing.T) {
	sub := t11uSub(nil, imageflowv1alpha1.ImageSubscriptionStatus{
		RecentDeliveries: []imageflowv1alpha1.DeliveryResult{
			func() imageflowv1alpha1.DeliveryResult {
				r := t11uSeededRecord(t11uDeployRef(), t11uRefA, t11uRefB, t11uStart.Add(2*time.Second))
				r.RolloutResult = imageflowv1alpha1.RolloutSucceeded
				r.Reason = TargetReasonRolloutSucceeded
				return r
			}(),
			t11uSeededRecord(t11uDeployRef(), t11uRefA, t11uRefB, t11uStart.Add(time.Second)),
			t11uSeededRecord(t11uDeployRef(), t11uRefB, t11uRefC, t11uStart.Add(3*time.Second)),
		},
	})
	rendered := renderFloatEvents(sub)
	if len(rendered) != 2 {
		t.Fatalf("rendered = %+v, want 2 (the stale duplicate view collapses)", rendered)
	}
	first := rendered[0]
	if first.Type != imageflowv1alpha1.MirrorEventDelivery ||
		first.Result != imageflowv1alpha1.RolloutSucceeded ||
		first.OldDigest != t11uDigestA || first.NewDigest != t11uDigestB {
		t.Fatalf("first render = %+v, want the newest Succeeded view with bare digests", first)
	}
	if first.Subscription == nil || first.Subscription.Name != t11uSubName || first.Target == nil || *first.Target != (t11uDeployRef()) {
		t.Fatalf("first render = %+v, want subscription and target refs", first)
	}
	if !strings.Contains(first.Message, "Delivery summary") || !strings.Contains(first.Message, "ADR-005") {
		t.Fatalf("summary message %q must carry the honesty note", first.Message)
	}
	if rendered[1].NewDigest != t11uDigestC {
		t.Fatalf("second render = %+v, want the refB→refC delivery", rendered[1])
	}
}

func TestFloatUpDeliveriesWritesBothMirrorKinds(t *testing.T) {
	rec := t11uSeededRecord(t11uDeployRef(), t11uRefA, t11uRefB, t11uStart.Add(-time.Minute))
	rec.RolloutResult = imageflowv1alpha1.RolloutSucceeded
	rec.Reason = TargetReasonRolloutSucceeded
	sub := t11uSub([]imageflowv1alpha1.SubscriptionTarget{t11uContainerTarget(t11uDeployRef())},
		imageflowv1alpha1.ImageSubscriptionStatus{RecentDeliveries: []imageflowv1alpha1.DeliveryResult{rec}})
	consumers := []imageflowv1alpha1.ConsumerReference{{Namespace: t11uNamespace, Name: t11uSubName}}
	ist := t11uIST([]imageflowv1alpha1.MirrorStatusEvent{
		t11uImageChangedEv(t11uStart.Add(-time.Hour), t11uDigestA, t11uDigestB),
	}, consumers)
	it := t11uIT(t11uDigestB, []imageflowv1alpha1.MirrorStatusEvent{
		t11uImageChangedEv(t11uStart.Add(-time.Hour), "", t11uDigestB),
	})
	f := newT11uFixture(t, config.Config{}, ist, it, sub)
	f.applyStatus(ist, func() {})
	f.applyStatus(it, func() {})
	f.applyStatus(sub, func() {})

	f.reconciler.floatUpDeliveries(t11uCtx, types.NamespacedName{Namespace: t11uNamespace, Name: t11uSubName})

	gotIST := &imageflowv1alpha1.ImageStreamTag{}
	istKey := types.NamespacedName{Namespace: t11uNamespace, Name: ist.Name}
	if err := f.client.Get(t11uCtx, istKey, gotIST); err != nil {
		t.Fatalf("get ist: %v", err)
	}
	if len(gotIST.Status.Events) != 2 {
		t.Fatalf("ist events = %+v, want ImageChanged + Delivery", gotIST.Status.Events)
	}
	var delivery *imageflowv1alpha1.MirrorStatusEvent
	for i := range gotIST.Status.Events {
		if gotIST.Status.Events[i].Type == imageflowv1alpha1.MirrorEventDelivery {
			delivery = &gotIST.Status.Events[i]
		}
	}
	if delivery == nil {
		t.Fatal("ist has no Delivery summary")
	}
	if delivery.Result != imageflowv1alpha1.RolloutSucceeded || delivery.NewDigest != t11uDigestB || delivery.OldDigest != t11uDigestA {
		t.Fatalf("delivery summary = %+v, want the Succeeded refA→refB view", delivery)
	}
	if delivery.Subscription == nil || delivery.Subscription.Name != t11uSubName {
		t.Fatalf("delivery summary lacks the consumer reference: %+v", delivery.Subscription)
	}
	if !equality.Semantic.DeepEqual(gotIST.Status.Consumers, consumers) {
		t.Fatalf("float-up must not clobber consumers: %+v", gotIST.Status.Consumers)
	}
	gotIT := &imageflowv1alpha1.ImageTag{}
	itKey := types.NamespacedName{Namespace: t11uNamespace, Name: it.Name}
	if err := f.client.Get(t11uCtx, itKey, gotIT); err != nil {
		t.Fatalf("get it: %v", err)
	}
	found := false
	for _, ev := range gotIT.Status.Events {
		if ev.Type == imageflowv1alpha1.MirrorEventDelivery && ev.NewDigest == t11uDigestB {
			found = true
		}
	}
	if !found {
		t.Fatalf("image tag mirror lacks the digest-scoped Delivery summary: %+v", gotIT.Status.Events)
	}

	// Idempotence: a second float changes nothing.
	before := gotIST.Status.Events
	f.reconciler.floatUpDeliveries(t11uCtx, types.NamespacedName{Namespace: t11uNamespace, Name: t11uSubName})
	again := &imageflowv1alpha1.ImageStreamTag{}
	if err := f.client.Get(t11uCtx, istKey, again); err != nil {
		t.Fatalf("re-get ist: %v", err)
	}
	if !equality.Semantic.DeepEqual(before, again.Status.Events) {
		t.Fatal("second float-up changed the mirror window")
	}
	if events := f.drainEvents(); len(events) != 0 {
		t.Fatalf("float-up must be silent in steady state: %v", events)
	}
}

func TestFloatUpMissingMirrorIsSilentSkip(t *testing.T) {
	rec := t11uSeededRecord(t11uDeployRef(), t11uRefA, t11uRefB, t11uStart)
	sub := t11uSub([]imageflowv1alpha1.SubscriptionTarget{t11uContainerTarget(t11uDeployRef())},
		imageflowv1alpha1.ImageSubscriptionStatus{RecentDeliveries: []imageflowv1alpha1.DeliveryResult{rec}})
	f := newT11uFixture(t, config.Config{}, sub)
	f.applyStatus(sub, func() {})
	f.reconciler.floatUpDeliveries(t11uCtx, types.NamespacedName{Namespace: t11uNamespace, Name: t11uSubName})
	if events := f.drainEvents(); len(events) != 0 {
		t.Fatalf("missing mirrors must be a silent skip: %v", events)
	}
}

func TestFloatUpTruncationEmitsEventAndMetric(t *testing.T) {
	stored := []imageflowv1alpha1.MirrorStatusEvent{
		t11uImageChangedEv(t11uStart.Add(-5*time.Hour), "", t11uDigestA),
		t11uImageChangedEv(t11uStart.Add(-4*time.Hour), "", t11uDigestA),
		t11uImageChangedEv(t11uStart.Add(-3*time.Hour), "", t11uDigestB),
		t11uImageChangedEv(t11uStart.Add(-2*time.Hour), "", t11uDigestC),
		t11uImageChangedEv(t11uStart.Add(-1*time.Hour), "", t11uDigestC),
	}
	ist := t11uIST(stored, nil)
	f := newT11uFixture(t, config.Config{MirrorEventLimit: 5}, ist)
	f.applyStatus(ist, func() {})
	rendered := []imageflowv1alpha1.MirrorStatusEvent{
		t11uDeliveryEv(t11uStart.Add(2*time.Minute), t11uDigestB, imageflowv1alpha1.RolloutSucceeded),
		t11uDeliveryEv(t11uStart.Add(time.Minute), t11uDigestC, imageflowv1alpha1.RolloutFailed),
	}
	before := testutil.ToFloat64(mirrorEventsTruncated.WithLabelValues(mirrorKindImageStreamTag))
	f.reconciler.syncImageStreamTagFloat(t11uCtx, t11uNamespace, t11uStreamName, t11uTagName, rendered)
	after := testutil.ToFloat64(mirrorEventsTruncated.WithLabelValues(mirrorKindImageStreamTag))
	if after-before != 2 {
		t.Fatalf("truncation metric delta = %v, want 2", after-before)
	}
	events := f.drainEvents()
	truncated := t11uEventsWith(events, mirrorEventReasonSummaryTruncated)
	if len(truncated) != 1 {
		t.Fatalf("events = %v, want one EventSummaryTruncated", events)
	}
	if !strings.Contains(truncated[0], "bounded to 5 entries") || !strings.Contains(truncated[0], "2 older entries") {
		t.Fatalf("truncation event %q lacks the bound and the dropped count", truncated[0])
	}
	got := &imageflowv1alpha1.ImageStreamTag{}
	if err := f.client.Get(t11uCtx, types.NamespacedName{Namespace: t11uNamespace, Name: ist.Name}, got); err != nil {
		t.Fatalf("get ist: %v", err)
	}
	if len(got.Status.Events) != 5 {
		t.Fatalf("window = %d entries, want the bound 5", len(got.Status.Events))
	}
}

// ---------------------------------------------------------------------------
// label mappings (bounded metric vocabularies)
// ---------------------------------------------------------------------------

func TestObserverLabelMappings(t *testing.T) {
	if got := observerResultLabel(imageflowv1alpha1.RolloutSucceeded); got != observerResultSucceeded {
		t.Fatalf("succeeded label = %q", got)
	}
	if got := observerResultLabel(imageflowv1alpha1.RolloutFailed); got != observerResultFailed {
		t.Fatalf("failed label = %q", got)
	}
	if got := observerResultLabel(imageflowv1alpha1.RolloutConflict); got != observerResultUnknown {
		t.Fatalf("conflict must not invent a label: %q", got)
	}
	if got := observerHealthLabel(imageflowv1alpha1.HealthHealthy); got != observerHealthHealthy {
		t.Fatalf("healthy label = %q", got)
	}
	if got := observerHealthLabel(""); got != observerHealthUnknown {
		t.Fatalf("empty health label = %q", got)
	}
	if got := observerEventType(imageflowv1alpha1.RolloutFailed); got != "Warning" {
		t.Fatalf("failed event type = %q", got)
	}
	if got := observerEventType(imageflowv1alpha1.RolloutSucceeded); got != "Normal" {
		t.Fatalf("succeeded event type = %q", got)
	}
	if got := observerReasonFor(imageflowv1alpha1.RolloutFailed); got != TargetReasonRolloutFailed {
		t.Fatalf("failed reason = %q", got)
	}
	if got := observerEventReason(imageflowv1alpha1.RolloutSuperseded); got != EventReasonRolloutUnknown {
		t.Fatalf("unmapped statuses must fall back to the unknown event reason: %q", got)
	}
}

// ---------------------------------------------------------------------------
// T5: Job completion determination
// ---------------------------------------------------------------------------

const (
	t11uJobName  = "t11u-job"
	t11uBatchAPI = "batch/v1"
)

var t11uJobUID = types.UID("t11u-job-uid")

func t11uJobRef() imageflowv1alpha1.TargetReference {
	return imageflowv1alpha1.TargetReference{
		APIVersion: t11uBatchAPI, Kind: kindJob,
		Namespace: t11uNamespace, Name: t11uJobName,
	}
}

func t11uJob(image string, st batchv1.JobStatus) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: t11uNamespace, Name: t11uJobName,
			UID: t11uJobUID, Generation: 1,
		},
		Spec: batchv1.JobSpec{
			Template: t11uPodTemplate(image),
		},
		Status: st,
	}
}

// ---------------------------------------------------------------------------
// CronJob observation (T6): ownerReference-based Job discovery
// ---------------------------------------------------------------------------

const t11uCronJobName = "t11u-cronjob"

var t11uCronJobUID = types.UID("t11u-cronjob-uid")

func t11uCronJobRef() imageflowv1alpha1.TargetReference {
	return imageflowv1alpha1.TargetReference{
		APIVersion: t11uBatchAPI, Kind: kindCronJob,
		Namespace: t11uNamespace, Name: t11uCronJobName,
	}
}

func t11uCronJob(image string) *batchv1.CronJob {
	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: t11uNamespace, Name: t11uCronJobName,
			UID: t11uCronJobUID, Generation: 1,
		},
		Spec: batchv1.CronJobSpec{
			Schedule: "*/5 * * * *",
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Template: t11uPodTemplate(image),
				},
			},
		},
	}
}

// t11uJobOwnedBy creates a Job owned by the CronJob via ownerReferences.
func t11uJobOwnedBy(name, image string, cronJobUID types.UID, creationTime time.Time, st batchv1.JobStatus) *batchv1.Job {
	isController := true
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: t11uNamespace, Name: name,
			UID:               types.UID(name + "-uid"),
			CreationTimestamp: metav1.NewTime(creationTime),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: t11uBatchAPI, Kind: kindCronJob,
				Name: t11uCronJobName, UID: cronJobUID,
				Controller: &isController,
			}},
		},
		Spec: batchv1.JobSpec{
			Template: t11uPodTemplate(image),
		},
		Status: st,
	}
}

func TestObserverDetermineCronJobCompletionStates(t *testing.T) {
	now := metav1.NewTime(t11uStart)
	cases := []struct {
		name    string
		cronJob *batchv1.CronJob
		jobs    []*batchv1.Job // Jobs owned by the CronJob
		target  imageflowv1alpha1.SubscriptionTarget
		entry   imageflowv1alpha1.TargetDeliveryStatus
		want    imageflowv1alpha1.RolloutStatus
		wantSub string
	}{
		{
			name:    "has one completed Job → Succeeded",
			cronJob: t11uCronJob(t11uRefA),
			jobs: []*batchv1.Job{
				t11uJobOwnedBy("t11u-cj-job-1", t11uRefA, t11uCronJobUID, t11uStart.Add(-time.Hour), batchv1.JobStatus{
					Conditions: []batchv1.JobCondition{{
						Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
					}},
					CompletionTime: &now,
				}),
			},
			target:  t11uContainerTarget(t11uCronJobRef()),
			entry:   t11uSeededEntry(t11uCronJobRef(), t11uRefA),
			want:    imageflowv1alpha1.RolloutSucceeded,
			wantSub: "completed",
		},
		{
			name:    "has one failed Job → Failed",
			cronJob: t11uCronJob(t11uRefA),
			jobs: []*batchv1.Job{
				t11uJobOwnedBy("t11u-cj-job-1", t11uRefA, t11uCronJobUID, t11uStart.Add(-time.Hour), batchv1.JobStatus{
					Conditions: []batchv1.JobCondition{{
						Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
						Reason: "BackoffLimitExceeded",
					}},
				}),
			},
			target:  t11uContainerTarget(t11uCronJobRef()),
			entry:   t11uSeededEntry(t11uCronJobRef(), t11uRefA),
			want:    imageflowv1alpha1.RolloutFailed,
			wantSub: "BackoffLimitExceeded",
		},
		{
			name:    "multiple Jobs → selects most recent by creationTimestamp",
			cronJob: t11uCronJob(t11uRefA),
			jobs: []*batchv1.Job{
				// Older Job: completed
				t11uJobOwnedBy("t11u-cj-job-old", t11uRefA, t11uCronJobUID, t11uStart.Add(-2*time.Hour), batchv1.JobStatus{
					Conditions: []batchv1.JobCondition{{
						Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
					}},
					CompletionTime: ptr.To(metav1.NewTime(t11uStart.Add(-time.Hour))),
				}),
				// Newer Job: failed — this should be selected
				t11uJobOwnedBy("t11u-cj-job-new", t11uRefA, t11uCronJobUID, t11uStart.Add(-30*time.Minute), batchv1.JobStatus{
					Conditions: []batchv1.JobCondition{{
						Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
						Reason: "DeadlineExceeded",
					}},
				}),
			},
			target:  t11uContainerTarget(t11uCronJobRef()),
			entry:   t11uSeededEntry(t11uCronJobRef(), t11uRefA),
			want:    imageflowv1alpha1.RolloutFailed,
			wantSub: "DeadlineExceeded",
		},
		{
			name:    "no Jobs → Unknown with explanatory message",
			cronJob: t11uCronJob(t11uRefA),
			jobs:    nil,
			target:  t11uContainerTarget(t11uCronJobRef()),
			entry:   t11uSeededEntry(t11uCronJobRef(), t11uRefA),
			want:    imageflowv1alpha1.RolloutUnknown,
			wantSub: "no Jobs scheduled yet",
		},
		{
			name:    "spec no longer holds desired image → Unknown",
			cronJob: t11uCronJob(t11uRefA),
			jobs: []*batchv1.Job{
				t11uJobOwnedBy("t11u-cj-job-1", t11uRefA, t11uCronJobUID, t11uStart.Add(-time.Hour), batchv1.JobStatus{
					Active:    1,
					StartTime: &now,
				}),
			},
			target:  t11uContainerTarget(t11uCronJobRef()),
			entry:   t11uSeededEntry(t11uCronJobRef(), t11uRefB),
			want:    imageflowv1alpha1.RolloutUnknown,
			wantSub: "currently references",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			objs := []client.Object{tc.cronJob}
			for _, j := range tc.jobs {
				objs = append(objs, j)
			}
			f := newT11uFixture(t, config.Config{}, objs...)
			gvk := batchv1.SchemeGroupVersion.WithKind(kindCronJob)
			facts, found, err := f.reconciler.getWorkloadFacts(t11uCtx, gvk, tc.target.TargetRef)
			if err != nil || !found {
				t.Fatalf("getWorkloadFacts: found=%v err=%v", found, err)
			}
			det := determineRollout(facts, tc.target, tc.entry, nil)
			if det.status != tc.want {
				t.Fatalf("determineRollout = %q (%s), want %q", det.status, det.message, tc.want)
			}
			if tc.wantSub != "" && !strings.Contains(det.message, tc.wantSub) {
				t.Fatalf("message %q does not contain %q", det.message, tc.wantSub)
			}
		})
	}
}

func TestObserverDetermineJobCompletionStates(t *testing.T) {
	now := metav1.NewTime(t11uStart)
	cases := []struct {
		name    string
		job     *batchv1.Job
		target  imageflowv1alpha1.SubscriptionTarget
		entry   imageflowv1alpha1.TargetDeliveryStatus
		want    imageflowv1alpha1.RolloutStatus
		wantSub string
	}{
		{
			name: "succeeded when Complete condition is True",
			job: t11uJob(t11uRefA, batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{{
					Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
				}},
				CompletionTime: &now,
			}),
			target:  t11uContainerTarget(t11uJobRef()),
			entry:   t11uSeededEntry(t11uJobRef(), t11uRefA),
			want:    imageflowv1alpha1.RolloutSucceeded,
			wantSub: "completed",
		},
		{
			name: "failed when Failed condition is True",
			job: t11uJob(t11uRefA, batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{{
					Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
					Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit",
				}},
			}),
			target:  t11uContainerTarget(t11uJobRef()),
			entry:   t11uSeededEntry(t11uJobRef(), t11uRefA),
			want:    imageflowv1alpha1.RolloutFailed,
			wantSub: "BackoffLimitExceeded",
		},
		{
			name: "progressing when active pods exist",
			job: t11uJob(t11uRefA, batchv1.JobStatus{
				Active:    1,
				StartTime: &now,
			}),
			target:  t11uContainerTarget(t11uJobRef()),
			entry:   t11uSeededEntry(t11uJobRef(), t11uRefA),
			want:    imageflowv1alpha1.RolloutProgressing,
			wantSub: "active",
		},
		{
			name:    "unknown when no conditions and no active pods",
			job:     t11uJob(t11uRefA, batchv1.JobStatus{}),
			target:  t11uContainerTarget(t11uJobRef()),
			entry:   t11uSeededEntry(t11uJobRef(), t11uRefA),
			want:    imageflowv1alpha1.RolloutUnknown,
			wantSub: "has not reported",
		},
		{
			name:    "unknown when spec no longer holds desired image",
			job:     t11uJob(t11uRefA, batchv1.JobStatus{Active: 1}),
			target:  t11uContainerTarget(t11uJobRef()),
			entry:   t11uSeededEntry(t11uJobRef(), t11uRefB),
			want:    imageflowv1alpha1.RolloutUnknown,
			wantSub: "currently references",
		},
		{
			name:    "unknown when declared container missing from template",
			job:     t11uJob(t11uRefA, batchv1.JobStatus{Active: 1}),
			target:  imageflowv1alpha1.SubscriptionTarget{TargetRef: t11uJobRef(), ContainerNames: []string{"ghost"}},
			entry:   t11uSeededEntry(t11uJobRef(), t11uRefA),
			want:    imageflowv1alpha1.RolloutUnknown,
			wantSub: "not present in the pod template",
		},
		{
			name: "complete takes priority over failed when both present",
			job: t11uJob(t11uRefA, batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{
					{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
					{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"},
				},
				CompletionTime: &now,
			}),
			target:  t11uContainerTarget(t11uJobRef()),
			entry:   t11uSeededEntry(t11uJobRef(), t11uRefA),
			want:    imageflowv1alpha1.RolloutSucceeded,
			wantSub: "completed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newT11uFixture(t, config.Config{}, tc.job)
			gvk := batchv1.SchemeGroupVersion.WithKind(kindJob)
			facts, found, err := f.reconciler.getWorkloadFacts(t11uCtx, gvk, tc.target.TargetRef)
			if err != nil || !found {
				t.Fatalf("getWorkloadFacts: found=%v err=%v", found, err)
			}
			pods, err := f.reconciler.listWorkloadPods(t11uCtx, facts)
			if err != nil {
				t.Fatalf("listWorkloadPods: %v", err)
			}
			det := determineRollout(facts, tc.target, tc.entry, pods)
			if det.status != tc.want {
				t.Fatalf("determineRollout = %q (%s), want %q", det.status, det.message, tc.want)
			}
			if tc.wantSub != "" && !strings.Contains(det.message, tc.wantSub) {
				t.Fatalf("message %q does not contain %q", det.message, tc.wantSub)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// T7 (spec v1.6): completion-based health for Job/CronJob — health is derived
// directly from rollout status, no pod-scan invoked.
// ---------------------------------------------------------------------------

func TestObserverDetermineJobHealthFromCompletion(t *testing.T) {
	cases := []struct {
		name    string
		rollout imageflowv1alpha1.RolloutStatus
		want    imageflowv1alpha1.HealthStatus
	}{
		{
			name:    "healthy when rollout succeeded",
			rollout: imageflowv1alpha1.RolloutSucceeded,
			want:    imageflowv1alpha1.HealthHealthy,
		},
		{
			name:    "unhealthy when rollout failed",
			rollout: imageflowv1alpha1.RolloutFailed,
			want:    imageflowv1alpha1.HealthUnhealthy,
		},
		{
			name:    "unknown when rollout progressing",
			rollout: imageflowv1alpha1.RolloutProgressing,
			want:    imageflowv1alpha1.HealthUnknown,
		},
		{
			name:    "unknown when rollout unknown",
			rollout: imageflowv1alpha1.RolloutUnknown,
			want:    imageflowv1alpha1.HealthUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// For Job kind, determineHealth must NOT call scanPodsForHealth.
			// We pass nil pods to prove no pod-scan occurs (it would panic on
			// nil dereference if scanPodsForHealth were invoked).
			got := determineHealth(tc.rollout, nil, t11uRefA, observerKindJob)
			if got != tc.want {
				t.Fatalf("determineHealth(rollout=%q, kind=%q) = %q, want %q",
					tc.rollout, observerKindJob, got, tc.want)
			}
		})
	}
}

func TestObserverDetermineCronJobHealthFromCompletion(t *testing.T) {
	// CronJob health delegates to the same completion-based logic as Job.
	// The observer treats CronJob identically to Job for health determination:
	// health IS the completion result, no separate pod-level check needed.
	cases := []struct {
		name    string
		rollout imageflowv1alpha1.RolloutStatus
		want    imageflowv1alpha1.HealthStatus
	}{
		{
			name:    "healthy when rollout succeeded",
			rollout: imageflowv1alpha1.RolloutSucceeded,
			want:    imageflowv1alpha1.HealthHealthy,
		},
		{
			name:    "unhealthy when rollout failed",
			rollout: imageflowv1alpha1.RolloutFailed,
			want:    imageflowv1alpha1.HealthUnhealthy,
		},
		{
			name:    "unknown when rollout progressing",
			rollout: imageflowv1alpha1.RolloutProgressing,
			want:    imageflowv1alpha1.HealthUnknown,
		},
		{
			name:    "unknown when rollout unknown",
			rollout: imageflowv1alpha1.RolloutUnknown,
			want:    imageflowv1alpha1.HealthUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := determineHealth(tc.rollout, nil, t11uRefA, observerKindCronJob)
			if got != tc.want {
				t.Fatalf("determineHealth(rollout=%q, kind=%q) = %q, want %q",
					tc.rollout, observerKindCronJob, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// T8 (spec v1.6): completion-aware event messages for Job/CronJob — messages
// use "completion of" language instead of "rollout of" for batch workloads.
// ---------------------------------------------------------------------------

func TestObserverCompletionAwareEventMessagesForJob(t *testing.T) {
	now := metav1.NewTime(t11uStart)
	cases := []struct {
		name       string
		job        *batchv1.Job
		target     imageflowv1alpha1.SubscriptionTarget
		entry      imageflowv1alpha1.TargetDeliveryStatus
		wantStatus imageflowv1alpha1.RolloutStatus
		wantMsgs   []string // substrings that must appear in the message
	}{
		{
			name: "succeeded message includes completion time",
			job: t11uJob(t11uRefA, batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{{
					Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
				}},
				CompletionTime: &now,
			}),
			target:     t11uContainerTarget(t11uJobRef()),
			entry:      t11uSeededEntry(t11uJobRef(), t11uRefA),
			wantStatus: imageflowv1alpha1.RolloutSucceeded,
			wantMsgs:   []string{"completion of", "succeeded", "Job", t11uJobName, "completed at"},
		},
		{
			name: "failed message includes failure reason",
			job: t11uJob(t11uRefA, batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{{
					Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
					Reason: "BackoffLimitExceeded",
				}},
			}),
			target:     t11uContainerTarget(t11uJobRef()),
			entry:      t11uSeededEntry(t11uJobRef(), t11uRefA),
			wantStatus: imageflowv1alpha1.RolloutFailed,
			wantMsgs:   []string{"completion of", "failed", "Job", t11uJobName, "failed with reason", "BackoffLimitExceeded"},
		},
		{
			name: "progressing message includes active-since time",
			job: t11uJob(t11uRefA, batchv1.JobStatus{
				Active:    1,
				StartTime: &now,
			}),
			target:     t11uContainerTarget(t11uJobRef()),
			entry:      t11uSeededEntry(t11uJobRef(), t11uRefA),
			wantStatus: imageflowv1alpha1.RolloutProgressing,
			wantMsgs:   []string{"completion in progress", "Job", t11uJobName, "active since"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newT11uFixture(t, config.Config{}, tc.job)
			gvk := batchv1.SchemeGroupVersion.WithKind(kindJob)
			facts, found, err := f.reconciler.getWorkloadFacts(t11uCtx, gvk, tc.target.TargetRef)
			if err != nil || !found {
				t.Fatalf("getWorkloadFacts: found=%v err=%v", found, err)
			}
			pods, err := f.reconciler.listWorkloadPods(t11uCtx, facts)
			if err != nil {
				t.Fatalf("listWorkloadPods: %v", err)
			}
			det := determineRollout(facts, tc.target, tc.entry, pods)
			if det.status != tc.wantStatus {
				t.Fatalf("determineRollout = %q (%s), want %q", det.status, det.message, tc.wantStatus)
			}
			for _, want := range tc.wantMsgs {
				if !strings.Contains(det.message, want) {
					t.Errorf("message %q missing substring %q", det.message, want)
				}
			}
		})
	}
}

func TestObserverCompletionAwareEventMessagesForCronJob(t *testing.T) {
	now := metav1.NewTime(t11uStart)
	cases := []struct {
		name       string
		cronJob    *batchv1.CronJob
		jobs       []*batchv1.Job
		target     imageflowv1alpha1.SubscriptionTarget
		entry      imageflowv1alpha1.TargetDeliveryStatus
		wantStatus imageflowv1alpha1.RolloutStatus
		wantMsgs   []string
	}{
		{
			name:    "delegation message for completed Job",
			cronJob: t11uCronJob(t11uRefA),
			jobs: []*batchv1.Job{
				t11uJobOwnedBy("t11u-cj-job-1", t11uRefA, t11uCronJobUID, t11uStart.Add(-time.Hour), batchv1.JobStatus{
					Conditions: []batchv1.JobCondition{{
						Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
					}},
					CompletionTime: &now,
				}),
			},
			target:     t11uContainerTarget(t11uCronJobRef()),
			entry:      t11uSeededEntry(t11uCronJobRef(), t11uRefA),
			wantStatus: imageflowv1alpha1.RolloutSucceeded,
			wantMsgs:   []string{"observing most recent Job", "for CronJob", t11uCronJobName, "completion of", "succeeded"},
		},
		{
			name:       "no Jobs scheduled message",
			cronJob:    t11uCronJob(t11uRefA),
			jobs:       nil, // no Jobs
			target:     t11uContainerTarget(t11uCronJobRef()),
			entry:      t11uSeededEntry(t11uCronJobRef(), t11uRefA),
			wantStatus: imageflowv1alpha1.RolloutUnknown,
			wantMsgs:   []string{"no Jobs scheduled yet", "CronJob", t11uCronJobName, "completion stays Unknown"},
		},
		{
			name:    "delegation message for progressing Job",
			cronJob: t11uCronJob(t11uRefA),
			jobs: []*batchv1.Job{
				t11uJobOwnedBy("t11u-cj-job-active", t11uRefA, t11uCronJobUID, t11uStart.Add(-time.Minute), batchv1.JobStatus{
					Active:    1,
					StartTime: &now,
				}),
			},
			target:     t11uContainerTarget(t11uCronJobRef()),
			entry:      t11uSeededEntry(t11uCronJobRef(), t11uRefA),
			wantStatus: imageflowv1alpha1.RolloutProgressing,
			wantMsgs:   []string{"observing most recent Job", "for CronJob", t11uCronJobName, "completion in progress"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			objs := []client.Object{tc.cronJob}
			for _, j := range tc.jobs {
				objs = append(objs, j)
			}
			f := newT11uFixture(t, config.Config{}, objs...)
			gvk := batchv1.SchemeGroupVersion.WithKind(kindCronJob)
			facts, found, err := f.reconciler.getWorkloadFacts(t11uCtx, gvk, tc.target.TargetRef)
			if err != nil || !found {
				t.Fatalf("getWorkloadFacts: found=%v err=%v", found, err)
			}
			pods, err := f.reconciler.listWorkloadPods(t11uCtx, facts)
			if err != nil {
				t.Fatalf("listWorkloadPods: %v", err)
			}
			det := determineRollout(facts, tc.target, tc.entry, pods)
			if det.status != tc.wantStatus {
				t.Fatalf("determineRollout = %q (%s), want %q", det.status, det.message, tc.wantStatus)
			}
			for _, want := range tc.wantMsgs {
				if !strings.Contains(det.message, want) {
					t.Errorf("message %q missing substring %q", det.message, want)
				}
			}
		})
	}
}

func TestObserverReconcileJobSucceededDualNamespaceEvents(t *testing.T) {
	now := metav1.NewTime(t11uStart)
	job := t11uJob(t11uRefA, batchv1.JobStatus{
		Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
		}},
		CompletionTime: &now,
	})
	target := t11uContainerTarget(t11uJobRef())
	sub := t11uSub([]imageflowv1alpha1.SubscriptionTarget{target}, imageflowv1alpha1.ImageSubscriptionStatus{
		Conditions: t11uAuthorizedConditions(),
		Targets:    []imageflowv1alpha1.TargetDeliveryStatus{t11uSeededEntry(t11uJobRef(), t11uRefA)},
		RecentDeliveries: []imageflowv1alpha1.DeliveryResult{
			t11uSeededRecord(t11uJobRef(), t11uRefB, t11uRefA, t11uStart.Add(-time.Minute)),
		},
		RolloutStatus:      imageflowv1alpha1.RolloutProgressing,
		ObservedGeneration: 1,
	})
	f := newT11uFixture(t, config.Config{}, job, sub)
	f.applyStatus(sub, func() {})

	res, err := f.reconcileSub()
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("succeeded completion must not poll: RequeueAfter = %v", res.RequeueAfter)
	}
	got := f.getSub()
	entry := got.Status.Targets[0]
	if !entry.ImageApplied || entry.RolloutStatus != imageflowv1alpha1.RolloutSucceeded {
		t.Fatalf("entry = %+v, want imageApplied=true + rollout Succeeded", entry)
	}
	// Health should be Healthy for completed Job (T7).
	if entry.HealthStatus != imageflowv1alpha1.HealthHealthy {
		t.Fatalf("healthStatus = %q, want Healthy for completed Job", entry.HealthStatus)
	}

	// Dual-namespace events with completion-aware message.
	events := f.drainEvents()
	succeeded := t11uEventsWith(events, EventReasonRolloutSucceeded)
	if len(succeeded) != 2 {
		t.Fatalf("events = %v, want exactly two RolloutSucceeded (subscription side + target side)", events)
	}
	for _, ev := range succeeded {
		for _, want := range []string{
			"Normal " + EventReasonRolloutSucceeded,
			"completion of",
			"succeeded",
			"Job",
			t11uJobName,
			"completed at",
		} {
			if !strings.Contains(ev, want) {
				t.Fatalf("event %q missing completion-aware fragment %q", ev, want)
			}
		}
	}
}
