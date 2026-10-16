package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/config"
)

// T4 TTL GC unit tests: deterministic fake-clock advancement over the shared
// TaskObjectStatus decision surface. GC is uniform for both task kinds
// (Mapping and Import) — the Import cases here are the mechanism T7 re-verifies.

func TestTaskGCDeletesSuccessfulMappingAfterSuccessTTL(t *testing.T) {
	task := newTerminalMapping("gc-success", true, t4TestT0)
	env := newT4TestEnv(t, nil, task)
	gc := env.newTaskGC(gcKindMapping)
	ctxKey := types.NamespacedName{Namespace: t4TestNS, Name: "gc-success"}

	assertExists := func(stage string) {
		t.Helper()
		var m imageflowv1alpha1.ImageStreamMapping
		if err := env.client.Get(context.Background(), ctxKey, &m); err != nil {
			t.Fatalf("%s: task must still exist: %v", stage, err)
		}
	}

	// Just completed: full TTL remaining, nothing deleted.
	res, err := env.reconcileGC(gc, task)
	if err != nil {
		t.Fatalf("gc reconcile: %v", err)
	}
	if res.RequeueAfter != time.Hour {
		t.Fatalf("expected requeue after 1h, got %v", res.RequeueAfter)
	}
	assertExists("t0")

	// 59 minutes in: still retained.
	env.clock.Advance(59 * time.Minute)
	res, err = env.reconcileGC(gc, task)
	if err != nil {
		t.Fatalf("gc reconcile at 59m: %v", err)
	}
	if res.RequeueAfter != time.Minute {
		t.Fatalf("expected requeue after 1m remaining, got %v", res.RequeueAfter)
	}
	assertExists("t0+59m")

	// Past the success TTL: collected.
	env.clock.Advance(2 * time.Minute)
	res, err = env.reconcileGC(gc, task)
	if err != nil {
		t.Fatalf("gc reconcile at 61m: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Fatalf("deletion must not requeue: %+v", res)
	}
	var m imageflowv1alpha1.ImageStreamMapping
	getErr := env.client.Get(context.Background(), ctxKey, &m)
	if !apierrors.IsNotFound(getErr) {
		t.Fatalf("task must be deleted after success TTL, get err = %v", getErr)
	}

	events := drainFakeEvents(env.recorder)
	if n := countEventsWithReason(events, EventReasonTaskGarbageCollected); n != 1 {
		t.Fatalf("expected 1 TaskGarbageCollected event, got %d: %v", n, events)
	}
	gcEvent := findEventWithReason(events, EventReasonTaskGarbageCollected)
	for _, want := range []string{gcKindMapping, "gc-success", imageflowv1alpha1.ReasonSucceeded, "1h0m0s"} {
		if !strings.Contains(gcEvent, want) {
			t.Fatalf("GC event %q must mention %q", gcEvent, want)
		}
	}
	if v := t4CounterValue(t, env.metrics.gcDeletions, gcKindMapping); v != 1 {
		t.Fatalf("gc_deletions{mapping} = %v, want 1", v)
	}
}

func TestTaskGCRetainsFailedMappingForFailureTTL(t *testing.T) {
	task := newTerminalMapping("gc-failed", false, t4TestT0)
	env := newT4TestEnv(t, nil, task)
	gc := env.newTaskGC(gcKindMapping)
	ctxKey := types.NamespacedName{Namespace: t4TestNS, Name: "gc-failed"}

	// Past the SUCCESS TTL but inside the FAILURE TTL: retained for diagnosis.
	env.clock.Advance(time.Hour + time.Minute)
	res, err := env.reconcileGC(gc, task)
	if err != nil {
		t.Fatalf("gc reconcile at 1h1m: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("failed task must requeue for the remaining failure TTL, got %+v", res)
	}
	var m imageflowv1alpha1.ImageStreamMapping
	if err := env.client.Get(context.Background(), ctxKey, &m); err != nil {
		t.Fatalf("failed task must survive the success TTL: %v", err)
	}

	// Past the failure TTL (24h): collected.
	env.clock.Advance(23 * time.Hour)
	if _, err := env.reconcileGC(gc, task); err != nil {
		t.Fatalf("gc reconcile at 24h1m: %v", err)
	}
	if err := env.client.Get(context.Background(), ctxKey, &m); !apierrors.IsNotFound(err) {
		t.Fatalf("failed task must be deleted after failure TTL, get err = %v", err)
	}
	if v := t4CounterValue(t, env.metrics.gcDeletions, gcKindMapping); v != 1 {
		t.Fatalf("gc_deletions{mapping} = %v, want 1", v)
	}
}

func TestTaskGCIsUniformForImportKind(t *testing.T) {
	// The Import task never passes through the mapping reconciler: GC works
	// purely off the TaskObjectStatus shape, which is what T7 re-verifies.
	task := newTerminalImport("gc-import", true, t4TestT0)
	env := newT4TestEnv(t, nil, task)
	gc := env.newTaskGC(gcKindImport)

	env.clock.Advance(time.Hour + time.Minute)
	if _, err := env.reconcileGC(gc, task); err != nil {
		t.Fatalf("gc reconcile: %v", err)
	}
	var im imageflowv1alpha1.ImageStreamImport
	key := types.NamespacedName{Namespace: t4TestNS, Name: "gc-import"}
	if err := env.client.Get(context.Background(), key, &im); !apierrors.IsNotFound(err) {
		t.Fatalf("completed import task must be collected, get err = %v", err)
	}
	if v := t4CounterValue(t, env.metrics.gcDeletions, gcKindImport); v != 1 {
		t.Fatalf("gc_deletions{import} = %v, want 1", v)
	}
	if v := t4CounterValue(t, env.metrics.gcDeletions, gcKindMapping); v != 0 {
		t.Fatalf("gc_deletions{mapping} = %v, want 0", v)
	}
}

func TestTaskGCIgnoresNonTerminalAndUndatedTasks(t *testing.T) {
	t.Run("non-terminal task is untouched", func(t *testing.T) {
		task := newTestMapping("gc-running", t4RefA)
		env := newT4TestEnv(t, nil, task)
		gc := env.newTaskGC(gcKindMapping)

		env.clock.Advance(48 * time.Hour) // way past any TTL
		res, err := env.reconcileGC(gc, task)
		if err != nil {
			t.Fatalf("gc reconcile: %v", err)
		}
		if res != (ctrl.Result{}) {
			t.Fatalf("non-terminal task must not requeue: %+v", res)
		}
		var m imageflowv1alpha1.ImageStreamMapping
		key := types.NamespacedName{Namespace: t4TestNS, Name: "gc-running"}
		if err := env.client.Get(context.Background(), key, &m); err != nil {
			t.Fatalf("non-terminal task must not be deleted: %v", err)
		}
	})

	t.Run("terminal without completedAt is untouched", func(t *testing.T) {
		task := newTerminalMapping("gc-nodate", true, t4TestT0)
		task.Status.CompletedAt = nil // defensive: no GC surface
		env := newT4TestEnv(t, nil, task)
		gc := env.newTaskGC(gcKindMapping)

		env.clock.Advance(48 * time.Hour)
		if _, err := env.reconcileGC(gc, task); err != nil {
			t.Fatalf("gc reconcile: %v", err)
		}
		var m imageflowv1alpha1.ImageStreamMapping
		key := types.NamespacedName{Namespace: t4TestNS, Name: "gc-nodate"}
		if err := env.client.Get(context.Background(), key, &m); err != nil {
			t.Fatalf("task without completedAt must not be deleted: %v", err)
		}
	})
}

func TestTaskGCUnknownCompletionReasonUsesFailureTTL(t *testing.T) {
	task := newTerminalMapping("gc-weird", true, t4TestT0)
	task.Status.Conditions[0].Reason = "SomethingElse" // outside the frozen vocabulary
	env := newT4TestEnv(t, nil, task)
	gc := env.newTaskGC(gcKindMapping)
	key := types.NamespacedName{Namespace: t4TestNS, Name: "gc-weird"}

	env.clock.Advance(2 * time.Hour)
	if _, err := env.reconcileGC(gc, task); err != nil {
		t.Fatalf("gc reconcile at 2h: %v", err)
	}
	var m imageflowv1alpha1.ImageStreamMapping
	if err := env.client.Get(context.Background(), key, &m); err != nil {
		t.Fatalf("unknown reason must fall back to the conservative failure TTL: %v", err)
	}

	env.clock.Advance(23 * time.Hour)
	if _, err := env.reconcileGC(gc, task); err != nil {
		t.Fatalf("gc reconcile at 25h: %v", err)
	}
	if err := env.client.Get(context.Background(), key, &m); !apierrors.IsNotFound(err) {
		t.Fatalf("task must be collected after the failure TTL, get err = %v", err)
	}
}

func TestTaskGCReconcileMissingObjectIsNoError(t *testing.T) {
	env := newT4TestEnv(t, nil)
	gc := env.newTaskGC(gcKindMapping)
	gone := &imageflowv1alpha1.ImageStreamMapping{
		ObjectMeta: metav1.ObjectMeta{Namespace: t4TestNS, Name: "already-gone"},
	}
	if _, err := env.reconcileGC(gc, gone); err != nil {
		t.Fatalf("missing task must reconcile cleanly: %v", err)
	}
}

func TestTaskTTLDefaultsForZeroConfig(t *testing.T) {
	ok := &imageflowv1alpha1.TaskObjectStatus{Conditions: []metav1.Condition{{
		Type: imageflowv1alpha1.ConditionTypeComplete, Status: metav1.ConditionTrue,
		Reason: imageflowv1alpha1.ReasonSucceeded,
	}}}
	bad := &imageflowv1alpha1.TaskObjectStatus{Conditions: []metav1.Condition{{
		Type: imageflowv1alpha1.ConditionTypeComplete, Status: metav1.ConditionTrue,
		Reason: imageflowv1alpha1.ReasonFailed,
	}}}
	if got := taskTTL(config.Config{}, ok); got != defaultMappingTTLSuccess {
		t.Fatalf("zero config success TTL = %v, want %v", got, defaultMappingTTLSuccess)
	}
	if got := taskTTL(config.Config{}, bad); got != defaultMappingTTLFailure {
		t.Fatalf("zero config failure TTL = %v, want %v", got, defaultMappingTTLFailure)
	}
	custom := config.Config{MappingTTLSuccess: 30 * time.Second, MappingTTLFailure: time.Minute}
	if got := taskTTL(custom, ok); got != 30*time.Second {
		t.Fatalf("custom success TTL = %v, want 30s", got)
	}
	if got := taskTTL(custom, bad); got != time.Minute {
		t.Fatalf("custom failure TTL = %v, want 1m", got)
	}
}
