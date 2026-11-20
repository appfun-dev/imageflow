package controller

// v1.8-01 (issue #92, ADR-015 decisions 2/8/9) envtest suite: Argo Rollout
// delivery against a REAL API server with real Server-Side Apply.
//
//   - Lane A (TestRolloutDeliveryEnvtestSSAWritePath): the shared
//     environment WITH the Rollout CRD installed
//     (test/testdata/thirdparty/argoproj.io_rollouts.yaml) but NO
//     rollouts-controller running — admission of the Rollout CR through the
//     CRD schema, the SSA write path (image field written and OWNED by the
//     subscription's field manager), and steady-state convergence (a second
//     pass writes nothing).
//   - Lane B (TestRolloutDeliveryEnvtestKindNotInstalled): a dedicated
//     environment WITHOUT the Rollout CRD — a named Rollout target maps the
//     REST-mapping miss to the honest KindNotInstalled waiting state (spec
//     D9: no new classification enum), settling at the waiting requeue.
//
// The unit lane (subscription_delivery_controller_test.go) pins the 409
// escalation contract with an SSA-emulating fake; this suite proves the
// write path against the real apply machinery.

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
	"github.com/appfun-dev/imageflow/internal/delivery"
)

const (
	t9rSubName     = "checkout-live"
	t9rRolloutName = "checkout-api"
	t9rContainer   = "app"
	t9rPreImage    = "quay.io/example/checkout:live-20260831"
)

var (
	t9rDigestA = t5fRepo + "@sha256:" + strings.Repeat("3d", 32)
	t9rT0      = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
)

// t9rRollout builds the Rollout CR admitted through the vendored CRD schema.
func t9rRollout(ns, name, image string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "Rollout",
		"metadata":   map[string]any{"namespace": ns, "name": name},
		"spec": map[string]any{
			"replicas": int64(1),
			"selector": map[string]any{"matchLabels": map[string]any{"app": name}},
			"strategy": map[string]any{"canary": map[string]any{}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": name}},
				"spec": map[string]any{
					"containers": []any{map[string]any{"name": t9rContainer, "image": image}},
				},
			},
		},
	}}
}

func t9rGetRollout(t *testing.T, cl client.Client, ns, name string) *unstructured.Unstructured {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("argoproj.io/v1alpha1")
	u.SetKind("Rollout")
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, u); err != nil {
		t.Fatalf("get rollout %s: %v", name, err)
	}
	return u
}

func t9rRolloutImage(t *testing.T, u *unstructured.Unstructured) string {
	t.Helper()
	containers, found, err := unstructured.NestedSlice(u.Object, "spec", "template", "spec", "containers")
	if err != nil || !found || len(containers) == 0 {
		t.Fatalf("rollout pod-template nest missing (found=%v, err=%v): %v", found, err, u.Object)
	}
	c, _ := containers[0].(map[string]any)
	image, _ := c["image"].(string)
	return image
}

// t9rCreateAuthorizedSubscription mirrors t5fCreateAuthorizedSubscription
// with a named Rollout target (the T8 gate surface stamped in passed state,
// as the authz controller would leave it).
func t9rCreateAuthorizedSubscription(t *testing.T, cl client.Client, ns string) {
	t.Helper()
	sub := &imageflowv1alpha1.ImageSubscription{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: t9rSubName},
		Spec: imageflowv1alpha1.ImageSubscriptionSpec{
			Source: imageflowv1alpha1.SubscriptionSourceRef{
				Kind:      t9cSourceKind,
				Name:      t5fStream + ":" + t5fTag,
				Namespace: ns,
			},
			Targets: []imageflowv1alpha1.SubscriptionTarget{{
				TargetRef: imageflowv1alpha1.TargetReference{
					APIVersion: "argoproj.io/v1alpha1",
					Kind:       "Rollout",
					Namespace:  ns,
					Name:       t9rRolloutName,
				},
				ContainerNames: []string{t9rContainer},
			}},
		},
	}
	if err := cl.Create(context.Background(), sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	meta.SetStatusCondition(&sub.Status.Conditions, metav1.Condition{
		Type:               imageflowv1alpha1.ConditionTypeForbidden,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonAuthorized,
		Message:            "v1.8-01 fixture: creator passed the T8 gate",
		LastTransitionTime: metav1.NewTime(t9rT0),
		ObservedGeneration: sub.Generation,
	})
	if err := cl.Status().Update(context.Background(), sub); err != nil {
		t.Fatalf("stamp Forbidden condition: %v", err)
	}
}

func t9rReconcile(t *testing.T, reconciler *SubscriptionDeliveryReconciler, ns string) ctrl.Result {
	t.Helper()
	res, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: ns, Name: t9rSubName},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	return res
}

// Lane A: CRD installed, rollouts-controller absent.
func TestRolloutDeliveryEnvtestSSAWritePath(t *testing.T) {
	env := sharedBuildRunEnvtestEnv(t)
	rec := record.NewFakeRecorder(64)
	reconciler := NewSubscriptionDeliveryReconciler(
		env.client, env.client, rec, clock.NewFake(t9rT0), config.Config{}, nil)
	ns := brwCreateNamespace(t, env.client)

	// Admission: the vendored CRD schema admits the Rollout CR and preserves
	// the standard pod-template nest untouched (no controller normalizes it).
	if err := env.client.Create(context.Background(), t9rRollout(ns, t9rRolloutName, t9rPreImage)); err != nil {
		t.Fatalf("admission: create rollout: %v", err)
	}
	if image := t9rRolloutImage(t, t9rGetRollout(t, env.client, ns, t9rRolloutName)); image != t9rPreImage {
		t.Fatalf("admission must round-trip the pod-template nest, got image %q", image)
	}

	t5fCreateStream(t, env.client, ns, t5fStream, t9rDigestA)
	t9rCreateAuthorizedSubscription(t, env.client, ns)

	// The SSA write path: one pass writes the digest and OWNS the field.
	if res := t9rReconcile(t, reconciler, ns); res.RequeueAfter != 0 {
		t.Fatalf("delivery must settle silently, got %+v", res)
	}
	stored := t9rGetRollout(t, env.client, ns, t9rRolloutName)
	if image := t9rRolloutImage(t, stored); image != t9rDigestA {
		t.Fatalf("rollout image = %q, want digest %q", image, t9rDigestA)
	}
	selfManager := delivery.FieldManagerFor(ns, t9rSubName)
	owned := false
	for _, entry := range stored.GetManagedFields() {
		if entry.Manager == selfManager && entry.Operation == metav1.ManagedFieldsOperationApply {
			owned = true
		}
	}
	if !owned {
		t.Fatalf("image field must be owned by %q via SSA, got %v", selfManager, stored.GetManagedFields())
	}
	sub := t5fGetSub(t, env.client, ns, t9rSubName)
	if len(sub.Status.Targets) != 1 || sub.Status.Targets[0].Reason != TargetReasonApplied {
		t.Fatalf("expected one Applied entry, got %+v", sub.Status.Targets)
	}
	events := drainT9cEvents(rec)
	if v := len(t9cEventsWith(events, EventReasonImageApplied)); v == 0 {
		t.Fatalf("expected ImageApplied delivery events, got %v", events)
	}

	// Convergence against real SSA: a second pass writes NOTHING — the object
	// and the status keep their resource versions.
	rvObj := stored.GetResourceVersion()
	rvSub := sub.ResourceVersion
	if res := t9rReconcile(t, reconciler, ns); res.RequeueAfter != 0 {
		t.Fatalf("steady state must settle silently, got %+v", res)
	}
	if got := t9rGetRollout(t, env.client, ns, t9rRolloutName).GetResourceVersion(); got != rvObj {
		t.Fatalf("converged pass must not touch the rollout: rv %s -> %s", rvObj, got)
	}
	if got := t5fGetSub(t, env.client, ns, t9rSubName).ResourceVersion; got != rvSub {
		t.Fatalf("converged pass must not touch the subscription status: rv %s -> %s", rvSub, got)
	}
}

// Lane B: CRD absent — the REST-mapping miss is the honest KindNotInstalled
// waiting state (spec D9), settling at the waiting requeue with zero writes.
func TestRolloutDeliveryEnvtestKindNotInstalled(t *testing.T) {
	assets := buildRunWatcherEnvtestAssets(t)
	if assets == "" {
		t.Skip("envtest assets not found: set KUBEBUILDER_ASSETS or run `make envtest`")
	}
	env := &envtest.Environment{
		BinaryAssetsDirectory: assets,
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Logf("stop envtest: %v", err)
		}
	})
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := imageflowv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add imageflow scheme: %v", err)
	}
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	reconciler := NewSubscriptionDeliveryReconciler(
		cl, cl, record.NewFakeRecorder(64), clock.NewFake(t9rT0), config.Config{}, nil)
	ns := brwCreateNamespace(t, cl)
	t5fCreateStream(t, cl, ns, t5fStream, t9rDigestA)
	t9rCreateAuthorizedSubscription(t, cl, ns)

	res := t9rReconcile(t, reconciler, ns)
	if res.RequeueAfter != waitingStateRequeue {
		t.Fatalf("KindNotInstalled must settle at the waiting requeue, got %+v", res)
	}
	sub := t5fGetSub(t, cl, ns, t9rSubName)
	if len(sub.Status.Targets) != 1 || sub.Status.Targets[0].Reason != TargetReasonKindNotInstalled {
		t.Fatalf("expected the honest KindNotInstalled entry, got %+v", sub.Status.Targets)
	}
}
