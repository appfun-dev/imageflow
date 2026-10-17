package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/authz"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
	"github.com/appfun-dev/imageflow/internal/webhook"
)

// T8 subscription-authz reconciler unit suite (fake client + interceptor, per
// spec testing decision 2). Named cases pin the acceptance criteria:
// Forbidden condition + ZERO workload writes for denied targets, per-target
// granularity on targets change, fail-closed missing identity, unresolved
// verdicts requeue without touching the surface, steady-state write/event
// silence, flip transitions with Events, and the status merge contract that
// preserves delivery-engine-owned fields (T9).

const (
	t8RecheckPeriod  = 5 * time.Minute
	t8SubNamespace   = "team-ledger"
	t8SubName        = "ledger-stable"
	t8CreatorAnnoT8  = "user:ledger-owner@example.com"
	t8AllowedTarget  = "ledger-api"
	t8DeniedTarget   = "payroll-api"
	t8DeniedSARText  = `User "ledger-owner@example.com" cannot update resource "deployments"`
	t8DeploymentKind = "Deployment"
	// statusSubResource avoids repeating the literal across the package's test
	// files (goconst threshold is 3 per package).
	statusSubResource = "status"
)

// t8WriteAudit counts every mutating call the reconciler issues, split by
// whether it targets the subscription itself (allowed: status subresource +
// events) or anything else (forbidden by the zero-write guarantee).
type t8WriteAudit struct {
	sarCreates     int
	subscription   int
	otherObjects   int
	decide         func(sar *authorizationv1.SubjectAccessReview)
	sarCreateErrFn func() error
}

func (a *t8WriteAudit) count(obj client.Object, isStatus bool) {
	if _, ok := obj.(*imageflowv1alpha1.ImageSubscription); ok {
		if isStatus {
			a.subscription++
		}
		return
	}
	a.otherObjects++
}

func newT8ReconcilerFixture(t *testing.T, sub *imageflowv1alpha1.ImageSubscription,
	cfg config.Config, gateOverrides ...authz.Options,
) (*SubscriptionAuthzReconciler, *t8WriteAudit, *record.FakeRecorder, *clock.Fake) {
	t.Helper()
	audit := &t8WriteAudit{decide: func(sar *authorizationv1.SubjectAccessReview) {
		sar.Status.Allowed = true
	}}
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	fakeClk := clock.NewFake(start)

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		appsv1.AddToScheme, authorizationv1.AddToScheme, corev1.AddToScheme,
		imageflowv1alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("build scheme: %v", err)
		}
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sub).
		WithStatusSubresource(&imageflowv1alpha1.ImageSubscription{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object,
				_ ...client.CreateOption) error {
				if sar, ok := obj.(*authorizationv1.SubjectAccessReview); ok {
					audit.sarCreates++
					if audit.sarCreateErrFn != nil {
						if err := audit.sarCreateErrFn(); err != nil {
							return err
						}
					}
					audit.decide(sar)
					return nil
				}
				audit.count(obj, false)
				return c.Create(ctx, obj)
			},
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object,
				_ ...client.UpdateOption) error {
				audit.count(obj, false)
				return c.Update(ctx, obj)
			},
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object,
				p client.Patch, _ ...client.PatchOption) error {
				audit.count(obj, false)
				return c.Patch(ctx, obj, p)
			},
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object,
				_ ...client.DeleteOption) error {
				audit.count(obj, false)
				return c.Delete(ctx, obj)
			},
			SubResourceUpdate: func(ctx context.Context, c client.Client, subResource string,
				obj client.Object, _ ...client.SubResourceUpdateOption) error {
				audit.count(obj, subResource == statusSubResource)
				return c.SubResource(subResource).Update(ctx, obj)
			},
			SubResourcePatch: func(ctx context.Context, c client.Client, subResource string,
				obj client.Object, p client.Patch, _ ...client.SubResourcePatchOption) error {
				audit.count(obj, subResource == statusSubResource)
				return c.SubResource(subResource).Patch(ctx, obj, p)
			},
		}).Build()

	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{appsv1.SchemeGroupVersion})
	mapper.Add(appsv1.SchemeGroupVersion.WithKind(t8DeploymentKind), meta.RESTScopeNamespace)

	if cfg.SARRecheckPeriod == 0 {
		cfg.SARRecheckPeriod = t8RecheckPeriod
	}
	gateOpts := authz.Options{
		Clock:   fakeClk,
		TTL:     cfg.SARRecheckPeriod,
		Metrics: authz.NewMetrics(),
	}
	if len(gateOverrides) > 0 {
		override := gateOverrides[0]
		override.Clock = gateOpts.Clock
		override.TTL = gateOpts.TTL
		override.Metrics = gateOpts.Metrics
		gateOpts = override
	}
	gate := authz.NewGate(cl, mapper, gateOpts)
	recorder := record.NewFakeRecorder(64)
	reconciler := NewSubscriptionAuthzReconciler(cl, recorder, fakeClk, cfg, gate)
	return reconciler, audit, recorder, fakeClk
}

func t8SubscriptionFixture(targets ...string) *imageflowv1alpha1.ImageSubscription {
	sub := &imageflowv1alpha1.ImageSubscription{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   t8SubNamespace,
			Name:        t8SubName,
			Generation:  2,
			Annotations: map[string]string{webhook.CreatedByAnnotation: t8CreatorAnnoT8},
		},
		Spec: imageflowv1alpha1.ImageSubscriptionSpec{
			Source: imageflowv1alpha1.SubscriptionSourceRef{
				Kind:      "ImageStreamTag",
				Name:      "ledger-stream:stable",
				Namespace: "imageflow-system",
			},
		},
	}
	for _, name := range targets {
		sub.Spec.Targets = append(sub.Spec.Targets, imageflowv1alpha1.SubscriptionTarget{
			TargetRef: imageflowv1alpha1.TargetReference{
				APIVersion: "apps/v1",
				Kind:       t8DeploymentKind,
				Namespace:  t8SubNamespace,
				Name:       name,
			},
			ContainerNames: []string{"app"},
		})
	}
	return sub
}

func t8Request() ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: t8SubNamespace, Name: t8SubName,
	}}
}

func t8GetSub(t *testing.T, reconciler *SubscriptionAuthzReconciler) *imageflowv1alpha1.ImageSubscription {
	t.Helper()
	sub := &imageflowv1alpha1.ImageSubscription{}
	if err := reconciler.client.Get(context.Background(), t8Request().NamespacedName, sub); err != nil {
		t.Fatalf("get subscription: %v", err)
	}
	return sub
}

func t8ForbiddenCondition(sub *imageflowv1alpha1.ImageSubscription) *metav1.Condition {
	return meta.FindStatusCondition(sub.Status.Conditions, imageflowv1alpha1.ConditionTypeForbidden)
}

func drainT8Events(rec *record.FakeRecorder) []string {
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

// TestSubscriptionAuthzForbiddenTargetZeroWrites is the AC1 named case:
// a subscription with an unauthorized target gets the Forbidden condition,
// a per-target Reason=Forbidden entry, a Warning Event — and provably ZERO
// writes to any non-subscription object (interceptor audit).
func TestSubscriptionAuthzForbiddenTargetZeroWrites(t *testing.T) {
	sub := t8SubscriptionFixture(t8DeniedTarget)
	reconciler, audit, recorder, fakeClk := newT8ReconcilerFixture(t, sub, config.Config{
		SARRecheckPeriod: t8RecheckPeriod,
	})
	audit.decide = func(sar *authorizationv1.SubjectAccessReview) {
		sar.Status.Allowed = false
		sar.Status.Reason = t8DeniedSARText
	}

	result, err := reconciler.Reconcile(context.Background(), t8Request())
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != t8RecheckPeriod {
		t.Errorf("RequeueAfter = %v, want the recheck period %v", result.RequeueAfter, t8RecheckPeriod)
	}

	got := t8GetSub(t, reconciler)
	cond := t8ForbiddenCondition(got)
	if cond == nil {
		t.Fatal("Forbidden condition missing")
	}
	if cond.Status != metav1.ConditionTrue || cond.Reason != ReasonTargetsUnauthorized {
		t.Errorf("condition = %s/%s, want True/%s", cond.Status, cond.Reason, ReasonTargetsUnauthorized)
	}
	if !strings.Contains(cond.Message, t8DeniedTarget) || !strings.Contains(cond.Message, t8CreatorAnnoT8) {
		t.Errorf("condition message %q must name the denied target and the creator", cond.Message)
	}
	if cond.ObservedGeneration != 2 {
		t.Errorf("condition observedGeneration = %d, want 2", cond.ObservedGeneration)
	}
	if !cond.LastTransitionTime.Time.Equal(fakeClk.Now()) {
		t.Errorf("condition transition time %v must come from the injected clock %v",
			cond.LastTransitionTime, fakeClk.Now())
	}

	if len(got.Status.Targets) != 1 {
		t.Fatalf("per-target entries = %+v, want exactly the denied target", got.Status.Targets)
	}
	entry := got.Status.Targets[0]
	if entry.TargetRef.Name != t8DeniedTarget || entry.Reason != TargetReasonForbidden {
		t.Errorf("entry = %+v, want %s with reason %s", entry, t8DeniedTarget, TargetReasonForbidden)
	}
	if !strings.Contains(entry.Message, "cannot update") {
		t.Errorf("entry message %q must carry the SAR denial reason", entry.Message)
	}

	if audit.otherObjects != 0 {
		t.Errorf("writes to non-subscription objects = %d, want ZERO (zero-write guarantee)",
			audit.otherObjects)
	}
	if audit.subscription != 1 {
		t.Errorf("subscription status writes = %d, want exactly 1", audit.subscription)
	}

	events := drainT8Events(recorder)
	if len(events) != 1 {
		t.Fatalf("events = %v, want exactly one transition event", events)
	}
	if !strings.Contains(events[0], "Warning "+EventReasonTargetsForbidden) {
		t.Errorf("event = %q, want a Warning %s event", events[0], EventReasonTargetsForbidden)
	}
}

// TestSubscriptionAuthzAuthorizedCreatorPasses is the AC5 named case: with
// permission the re-check passes, no false Forbidden surfaces, and the state
// is positively observable (Forbidden=False/Authorized).
func TestSubscriptionAuthzAuthorizedCreatorPasses(t *testing.T) {
	sub := t8SubscriptionFixture(t8AllowedTarget)
	reconciler, audit, recorder, _ := newT8ReconcilerFixture(t, sub, config.Config{
		SARRecheckPeriod: t8RecheckPeriod,
	})

	if _, err := reconciler.Reconcile(context.Background(), t8Request()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	got := t8GetSub(t, reconciler)
	cond := t8ForbiddenCondition(got)
	if cond == nil {
		t.Fatal("Forbidden condition missing")
	}
	if cond.Status != metav1.ConditionFalse || cond.Reason != ReasonAuthorized {
		t.Errorf("condition = %s/%s, want False/%s (no false Forbidden)",
			cond.Status, cond.Reason, ReasonAuthorized)
	}
	if len(got.Status.Targets) != 0 {
		t.Errorf("per-target entries = %+v, want none for an authorized target", got.Status.Targets)
	}
	if audit.otherObjects != 0 {
		t.Errorf("writes to non-subscription objects = %d, want ZERO", audit.otherObjects)
	}
	if events := drainT8Events(recorder); len(events) != 0 {
		t.Errorf("events = %v, want none on the initial allowed path", events)
	}
}

// TestSubscriptionAuthzTargetsChangeGranularity is the AC2 named case: after
// a targets change only the unauthorized target is marked Forbidden while the
// authorized one is untouched — and after re-authorization the Forbidden
// husk entry is dropped again.
func TestSubscriptionAuthzTargetsChangeGranularity(t *testing.T) {
	sub := t8SubscriptionFixture(t8AllowedTarget)
	reconciler, audit, recorder, fakeClk := newT8ReconcilerFixture(t, sub, config.Config{
		SARRecheckPeriod: t8RecheckPeriod,
	})
	ctx := context.Background()

	if _, err := reconciler.Reconcile(ctx, t8Request()); err != nil {
		t.Fatalf("initial Reconcile() error = %v", err)
	}
	drainT8Events(recorder)

	// A targets change adds an unauthorized target (the webhook denies such
	// updates in production; the controller path covers webhook-bypassed or
	// pre-webhook objects and permission revocation between the timings).
	current := t8GetSub(t, reconciler)
	current.Spec.Targets = append(current.Spec.Targets, imageflowv1alpha1.SubscriptionTarget{
		TargetRef: imageflowv1alpha1.TargetReference{
			APIVersion: "apps/v1",
			Kind:       t8DeploymentKind,
			Namespace:  t8SubNamespace,
			Name:       t8DeniedTarget,
		},
		ContainerNames: []string{"worker"},
	})
	if err := reconciler.client.Update(ctx, current); err != nil {
		t.Fatalf("update targets: %v", err)
	}
	audit.decide = func(sar *authorizationv1.SubjectAccessReview) {
		sar.Status.Allowed = sar.Spec.ResourceAttributes.Name != t8DeniedTarget
		if !sar.Status.Allowed {
			sar.Status.Reason = t8DeniedSARText
		}
	}

	if _, err := reconciler.Reconcile(ctx, t8Request()); err != nil {
		t.Fatalf("Reconcile() after targets change: %v", err)
	}
	got := t8GetSub(t, reconciler)
	cond := t8ForbiddenCondition(got)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("condition = %+v, want True after adding an unauthorized target", cond)
	}
	if !strings.Contains(cond.Message, t8DeniedTarget) || strings.Contains(cond.Message, t8AllowedTarget) {
		t.Errorf("condition message %q must name only the denied target", cond.Message)
	}
	if len(got.Status.Targets) != 1 || got.Status.Targets[0].TargetRef.Name != t8DeniedTarget {
		t.Errorf("entries = %+v, want only the denied target marked", got.Status.Targets)
	}
	if audit.otherObjects != 0 {
		t.Errorf("writes to non-subscription objects = %d, want ZERO", audit.otherObjects)
	}

	// Re-grant: the next pass (after the cache TTL) flips back to authorized
	// and drops the pure-T8 husk entry entirely.
	audit.decide = func(sar *authorizationv1.SubjectAccessReview) { sar.Status.Allowed = true }
	fakeClk.Advance(t8RecheckPeriod)
	if _, err := reconciler.Reconcile(ctx, t8Request()); err != nil {
		t.Fatalf("Reconcile() after re-grant: %v", err)
	}
	got = t8GetSub(t, reconciler)
	cond = t8ForbiddenCondition(got)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonAuthorized {
		t.Fatalf("condition = %+v, want False/Authorized after re-grant", cond)
	}
	if len(got.Status.Targets) != 0 {
		t.Errorf("entries = %+v, want the cleared Forbidden husk dropped", got.Status.Targets)
	}
	events := drainT8Events(recorder)
	if len(events) != 2 {
		t.Fatalf("events = %v, want the forbidden and the recovery transition", events)
	}
	if !strings.Contains(events[0], "Warning "+EventReasonTargetsForbidden) ||
		!strings.Contains(events[1], "Normal "+EventReasonTargetsAuthorized) {
		t.Errorf("events = %v, want Warning %s then Normal %s",
			events, EventReasonTargetsForbidden, EventReasonTargetsAuthorized)
	}
}

// TestSubscriptionAuthzMissingIdentityFailsClosed: without a trusted
// created-by identity nothing may be written and the state says so explicitly
// (never silent).
func TestSubscriptionAuthzMissingIdentityFailsClosed(t *testing.T) {
	sub := t8SubscriptionFixture(t8AllowedTarget)
	sub.Annotations = nil
	reconciler, audit, recorder, _ := newT8ReconcilerFixture(t, sub, config.Config{
		SARRecheckPeriod: t8RecheckPeriod,
	})

	result, err := reconciler.Reconcile(context.Background(), t8Request())
	if err != nil {
		t.Fatalf("Reconcile() error = %v, want nil (the fail-closed state is persisted, not retried hot)", err)
	}
	if result.RequeueAfter != t8RecheckPeriod {
		t.Errorf("RequeueAfter = %v, want periodic re-checks to continue", result.RequeueAfter)
	}

	got := t8GetSub(t, reconciler)
	cond := t8ForbiddenCondition(got)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != ReasonMissingIdentity {
		t.Fatalf("condition = %+v, want True/%s", cond, ReasonMissingIdentity)
	}
	if !strings.Contains(cond.Message, webhook.CreatedByAnnotation) {
		t.Errorf("message %q must name the missing annotation", cond.Message)
	}
	if audit.otherObjects != 0 || audit.sarCreates != 0 {
		t.Errorf("audit = %+v, want zero SAR calls and zero workload writes", audit)
	}
	events := drainT8Events(recorder)
	if len(events) != 1 || !strings.Contains(events[0], "Warning "+EventReasonMissingIdentity) {
		t.Errorf("events = %v, want one Warning %s event", events, EventReasonMissingIdentity)
	}
}

// TestSubscriptionAuthzUnresolvedVerdictsRequeueWithoutSurfaceChange: a
// rate-limited (or otherwise unresolved) check keeps the persisted surface
// untouched and requeues with backoff — never fail open, never half-truth.
func TestSubscriptionAuthzUnresolvedVerdictsRequeueWithoutSurfaceChange(t *testing.T) {
	sub := t8SubscriptionFixture(t8AllowedTarget, t8DeniedTarget)
	reconciler, audit, recorder, _ := newT8ReconcilerFixture(t, sub,
		config.Config{SARRecheckPeriod: t8RecheckPeriod},
		authz.Options{LimiterQPS: 1, LimiterBurst: 1}) // second fresh check is shed

	result, err := reconciler.Reconcile(context.Background(), t8Request())
	if err == nil {
		t.Fatal("Reconcile() must surface unresolved verdicts as an error (backoff requeue)")
	}
	if !strings.Contains(err.Error(), authz.ReasonRateLimited) {
		t.Errorf("error = %v, want the rate-limit reason named", err)
	}
	if result != (ctrl.Result{}) {
		t.Errorf("result = %+v, want zero (the error drives the requeue)", result)
	}

	got := t8GetSub(t, reconciler)
	if len(got.Status.Conditions) != 0 || len(got.Status.Targets) != 0 {
		t.Errorf("status = %+v, want the persisted surface untouched on an incomplete picture", got.Status)
	}
	if audit.subscription != 0 || audit.otherObjects != 0 {
		t.Errorf("audit = %+v, want zero writes on an unresolved pass", audit)
	}
	if events := drainT8Events(recorder); len(events) != 0 {
		t.Errorf("events = %v, want none on an unresolved pass", events)
	}
}

// TestSubscriptionAuthzUnmappedTargetKindRequeues: a target kind the cluster
// does not serve keeps the surface untouched and requeues with backoff until
// the CRD exists (ADR-003: the subscription may predate its target).
func TestSubscriptionAuthzUnmappedTargetKindRequeues(t *testing.T) {
	sub := t8SubscriptionFixture(t8AllowedTarget)
	sub.Spec.Targets[0] = imageflowv1alpha1.SubscriptionTarget{
		TargetRef: imageflowv1alpha1.TargetReference{
			APIVersion: "example.io/v1",
			Kind:       "WebService",
			Namespace:  t8SubNamespace,
			Name:       "ledger-edge",
		},
		ImageFields: []string{".spec.runtime.containers[name=edge].image"},
	}
	reconciler, audit, _, _ := newT8ReconcilerFixture(t, sub, config.Config{
		SARRecheckPeriod: t8RecheckPeriod,
	})

	_, err := reconciler.Reconcile(context.Background(), t8Request())
	if err == nil {
		t.Fatal("Reconcile() must surface an unmapped target kind as an error (backoff requeue)")
	}
	if !strings.Contains(err.Error(), authz.ReasonKindNotInstalled) {
		t.Errorf("error = %v, want the KindNotInstalled reason named", err)
	}
	got := t8GetSub(t, reconciler)
	if len(got.Status.Conditions) != 0 {
		t.Errorf("conditions = %+v, want none while the verdict picture is incomplete", got.Status.Conditions)
	}
	if audit.subscription != 0 || audit.otherObjects != 0 || audit.sarCreates != 0 {
		t.Errorf("audit = %+v, want zero effects", audit)
	}
}

// TestSubscriptionAuthzSteadyStateIsSilent: repeated periodic re-checks with
// an unchanged verdict write nothing and emit nothing (no write amplification
// on the recheck cadence).
func TestSubscriptionAuthzSteadyStateIsSilent(t *testing.T) {
	sub := t8SubscriptionFixture(t8DeniedTarget)
	reconciler, audit, recorder, fakeClk := newT8ReconcilerFixture(t, sub, config.Config{
		SARRecheckPeriod: t8RecheckPeriod,
	})
	audit.decide = func(sar *authorizationv1.SubjectAccessReview) {
		sar.Status.Allowed = false
		sar.Status.Reason = t8DeniedSARText
	}
	ctx := context.Background()

	if _, err := reconciler.Reconcile(ctx, t8Request()); err != nil {
		t.Fatalf("first Reconcile() error = %v", err)
	}
	drainT8Events(recorder)
	statusWrites := audit.subscription
	sarCalls := audit.sarCreates

	for i := 0; i < 3; i++ {
		fakeClk.Advance(t8RecheckPeriod) // force cache expiry: fresh SAR each pass
		result, err := reconciler.Reconcile(ctx, t8Request())
		if err != nil {
			t.Fatalf("steady Reconcile() #%d error = %v", i, err)
		}
		if result.RequeueAfter != t8RecheckPeriod {
			t.Errorf("steady Reconcile() #%d RequeueAfter = %v", i, result.RequeueAfter)
		}
	}

	if audit.subscription != statusWrites {
		t.Errorf("status writes grew from %d to %d across steady re-checks, want no growth",
			statusWrites, audit.subscription)
	}
	if events := drainT8Events(recorder); len(events) != 0 {
		t.Errorf("events = %v, want silence across steady re-checks", events)
	}
	if audit.sarCreates <= sarCalls {
		t.Errorf("SAR creates = %d (was %d): expired cache entries must be re-verified each period",
			audit.sarCreates, sarCalls)
	}
	if audit.otherObjects != 0 {
		t.Errorf("writes to non-subscription objects = %d, want ZERO", audit.otherObjects)
	}
}

// TestSubscriptionAuthzPreservesDeliveryEngineFields pins the T8→T9 status
// merge contract: clearing a recovered target touches only the T8-owned
// Reason/Message fields; every delivery-engine-owned field survives.
func TestSubscriptionAuthzPreservesDeliveryEngineFields(t *testing.T) {
	sub := t8SubscriptionFixture(t8AllowedTarget)
	rollout := imageflowv1alpha1.RolloutProgressing
	desired := "quay.io/example/ledger@sha256:" + strings.Repeat("ab", 32)
	transition := metav1.NewTime(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	sub.Status.Targets = []imageflowv1alpha1.TargetDeliveryStatus{{
		TargetRef:          sub.Spec.Targets[0].TargetRef,
		DesiredImage:       desired,
		ImageApplied:       true,
		RolloutStatus:      rollout,
		HealthStatus:       imageflowv1alpha1.HealthUnknown,
		ObservedGeneration: 1,
		LastTransitionTime: &transition,
		Reason:             TargetReasonForbidden,
		Message:            t8DeniedSARText,
	}}
	sub.Status.Conditions = []metav1.Condition{{
		Type:               imageflowv1alpha1.ConditionTypeForbidden,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonTargetsUnauthorized,
		Message:            "stale denial",
		LastTransitionTime: transition,
	}}
	reconciler, audit, _, _ := newT8ReconcilerFixture(t, sub, config.Config{
		SARRecheckPeriod: t8RecheckPeriod,
	})

	if _, err := reconciler.Reconcile(context.Background(), t8Request()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	got := t8GetSub(t, reconciler)
	if len(got.Status.Targets) != 1 {
		t.Fatalf("entries = %+v, want the delivery-engine entry preserved", got.Status.Targets)
	}
	entry := got.Status.Targets[0]
	if entry.Reason != "" || entry.Message != "" {
		t.Errorf("entry reason/message = %q/%q, want cleared after recovery", entry.Reason, entry.Message)
	}
	if entry.DesiredImage != desired || !entry.ImageApplied || entry.RolloutStatus != rollout ||
		entry.HealthStatus != imageflowv1alpha1.HealthUnknown || entry.ObservedGeneration != 1 {
		t.Errorf("delivery-engine fields must survive untouched, got %+v", entry)
	}
	if entry.LastTransitionTime == nil || !entry.LastTransitionTime.Equal(&transition) {
		t.Errorf("delivery-engine transition time must survive, got %+v", entry.LastTransitionTime)
	}
	cond := t8ForbiddenCondition(got)
	if cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("condition = %+v, want False after recovery", cond)
	}
	if audit.otherObjects != 0 {
		t.Errorf("writes to non-subscription objects = %d, want ZERO", audit.otherObjects)
	}
}

// TestSubscriptionAuthzRequeueDefaultsToADR003Period: a zero-value config
// (e.g. an embedder that never parsed flags) still requeues at the ADR-003
// default instead of hot-looping.
func TestSubscriptionAuthzRequeueDefaultsToADR003Period(t *testing.T) {
	sub := t8SubscriptionFixture(t8AllowedTarget)
	reconciler, _, _, _ := newT8ReconcilerFixture(t, sub, config.Config{})
	reconciler.config = config.Config{} // the fixture normalizes; force the zero value

	result, err := reconciler.Reconcile(context.Background(), t8Request())
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != authz.DefaultRecheckPeriod {
		t.Errorf("RequeueAfter = %v, want the ADR-003 default %v",
			result.RequeueAfter, authz.DefaultRecheckPeriod)
	}
}

// TestSubscriptionAuthzNotFoundIsNoop: a deleted subscription reconciles to
// zero effects.
func TestSubscriptionAuthzNotFoundIsNoop(t *testing.T) {
	sub := t8SubscriptionFixture(t8AllowedTarget)
	reconciler, audit, recorder, _ := newT8ReconcilerFixture(t, sub, config.Config{
		SARRecheckPeriod: t8RecheckPeriod,
	})
	if err := reconciler.client.Delete(context.Background(), sub); err != nil {
		t.Fatalf("delete fixture: %v", err)
	}
	audit.subscription, audit.otherObjects, audit.sarCreates = 0, 0, 0

	result, err := reconciler.Reconcile(context.Background(), t8Request())
	if err != nil || result != (ctrl.Result{}) {
		t.Fatalf("Reconcile() = %+v, %v; want no-op", result, err)
	}
	if audit.sarCreates != 0 || audit.subscription != 0 || audit.otherObjects != 0 {
		t.Errorf("audit = %+v, want zero effects", audit)
	}
	if events := drainT8Events(recorder); len(events) != 0 {
		t.Errorf("events = %v, want none", events)
	}
}
