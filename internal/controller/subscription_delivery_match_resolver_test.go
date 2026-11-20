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

// Match-resolver unit tests for selector targets (issue #81, ADR-014
// decisions 2/12/13, spec D3/D5). Two seams are exercised:
//
//   - listerWatcherFactory: substituted with fcache.FakeControllerSource to
//     pin the membership answer (label re-check, namespace re-check,
//     terminal-state exclusion) and the informer-reuse registry;
//   - newDynamicListerWatcher: driven against the dynamic fake client to
//     pin that the selector travels INSIDE the List/Watch request options
//     (server-side filtering, decision 12) and the request is
//     namespace-scoped (decision 2).
//
// The real informer against a real API server is covered one layer up in
// test/integration (RegisterSubscriptionDelivery wires the production
// resolver); this lane owns the resolver's decisions.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	fcache "k8s.io/client-go/tools/cache/testing"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
)

const (
	t81rNamespace = "team-web"
	t81rOtherNS   = "team-billing"
	t81rAPIV1     = "v1"
	t81rKindPod   = "Pod"
	t81rAPIBatch  = "batch/v1"
	t81rKindJob   = "Job"
)

// t81rSelector builds a selector-form TargetReference for one namespace.
func t81rSelector(ns string, matchLabels map[string]string) imageflowv1alpha1.TargetReference {
	return imageflowv1alpha1.TargetReference{
		APIVersion:    t81rAPIV1,
		Kind:          t81rKindPod,
		Namespace:     ns,
		LabelSelector: &metav1.LabelSelector{MatchLabels: matchLabels},
	}
}

// t81rObject builds one unstructured object; status fields are set by the
// callers through unstructured helpers so the cache sees the same shape a
// real informer delivers.
func t81rObject(apiVersion, kind, ns, name string, objLabels map[string]string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(apiVersion)
	u.SetKind(kind)
	u.SetNamespace(ns)
	u.SetName(name)
	if objLabels != nil {
		u.SetLabels(objLabels)
	}
	return u
}

func t81rPodPhase(t *testing.T, u *unstructured.Unstructured, phase string) *unstructured.Unstructured {
	t.Helper()
	if err := unstructured.SetNestedField(u.Object, phase, "status", "phase"); err != nil {
		t.Fatalf("set pod phase: %v", err)
	}
	return u
}

func t81rJobConditions(t *testing.T, u *unstructured.Unstructured, conds ...[2]string) *unstructured.Unstructured {
	t.Helper()
	slice := make([]any, 0, len(conds))
	for _, c := range conds {
		slice = append(slice, map[string]any{"type": c[0], "status": c[1]})
	}
	if err := unstructured.SetNestedSlice(u.Object, slice, "status", "conditions"); err != nil {
		t.Fatalf("set job conditions: %v", err)
	}
	return u
}

// t81rFixture wires a matchResolver whose factory hands out pre-seeded
// FakeControllerSources (keyed by resource name) and records every factory
// invocation, so informer reuse is observable.
type t81rFixture struct {
	t        *testing.T
	resolver *matchResolver
	sources  map[string]*fcache.FakeControllerSource

	mu           sync.Mutex
	factoryCalls []string
}

func newT81rFixture(t *testing.T, seeds map[string][]runtime.Object) *t81rFixture {
	t.Helper()
	mapper := meta.NewDefaultRESTMapper(nil)
	mapper.Add(schema.GroupVersionKind{Version: "v1", Kind: "Pod"}, meta.RESTScopeNamespace)
	mapper.Add(schema.GroupVersionKind{Group: "batch", Version: "v1", Kind: "Job"}, meta.RESTScopeNamespace)

	f := &t81rFixture{t: t, sources: map[string]*fcache.FakeControllerSource{}}
	for resource, objs := range seeds {
		src := fcache.NewFakeControllerSource()
		for _, obj := range objs {
			src.Add(obj)
		}
		t.Cleanup(src.Shutdown)
		f.sources[resource] = src
	}
	f.resolver = newMatchResolver(mapper, f.factory, logr.Discard())
	return f
}

func (f *t81rFixture) factory(ctx context.Context, gvr schema.GroupVersionResource, namespace, selector string) cache.ListerWatcher {
	f.mu.Lock()
	f.factoryCalls = append(f.factoryCalls, gvr.Resource+"|"+namespace+"|"+selector)
	f.mu.Unlock()
	src, ok := f.sources[gvr.Resource]
	if !ok {
		f.t.Errorf("factory called for unseeded resource %s", gvr.Resource)
	}
	return src
}

func (f *t81rFixture) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.factoryCalls...)
}

func (f *t81rFixture) informerCount() int {
	f.resolver.mu.Lock()
	defer f.resolver.mu.Unlock()
	return len(f.resolver.informers)
}

// start runs resolver.Start in the background and waits until the run
// context is stored (getOrCreate rejects registrations before Start).
func (f *t81rFixture) start() {
	f.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = f.resolver.Start(ctx)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		f.resolver.mu.Lock()
		started := f.resolver.ctx != nil
		f.resolver.mu.Unlock()
		if started {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			f.t.Fatal("match resolver did not start within 5s")
		}
		time.Sleep(time.Millisecond)
	}
	f.t.Cleanup(func() {
		cancel()
		<-done
	})
}

func (f *t81rFixture) resolve(ref imageflowv1alpha1.TargetReference) ([]string, error) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return f.resolver.resolveSelector(ctx, ref)
}

func t81rEqualNames(t *testing.T, got, want []string, what string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s: got %v, want %v", what, got, want)
		}
	}
}

// TestMatchResolverExcludesTerminalPodsAndForeignObjects pins the
// membership answer of one tuple informer (ADR-014 decision 13):
// Completed Pods (phase Succeeded/Failed) leave the match set entirely, and
// because the FakeControllerSource does NOT filter server-side, the
// client-side label and namespace re-checks must keep the answer honest.
func TestMatchResolverExcludesTerminalPodsAndForeignObjects(t *testing.T) {
	labelsApp := map[string]string{"app": "checkout"}
	f := newT81rFixture(t, map[string][]runtime.Object{
		"pods": {
			t81rPodPhase(t, t81rObject(t81rAPIV1, t81rKindPod, t81rNamespace, "web-run", labelsApp), "Running"),
			t81rPodPhase(t, t81rObject(t81rAPIV1, t81rKindPod, t81rNamespace, "web-succeeded", labelsApp), "Succeeded"),
			t81rPodPhase(t, t81rObject(t81rAPIV1, t81rKindPod, t81rNamespace, "web-failed", labelsApp), "Failed"),
			t81rPodPhase(t, t81rObject(t81rAPIV1, t81rKindPod, t81rNamespace, "web-pending", labelsApp), "Pending"),
			t81rObject(t81rAPIV1, t81rKindPod, t81rNamespace, "web-nostatus", labelsApp),
			// Wrong labels: the source delivers everything, the resolver
			// must still answer only the matching set.
			t81rPodPhase(t, t81rObject(t81rAPIV1, t81rKindPod, t81rNamespace, "billing-run", map[string]string{"app": "billing"}), "Running"),
			// Wrong namespace: match domain is exactly one namespace
			// (decision 2), enforced client-side on top of the scoped
			// ListWatch.
			t81rPodPhase(t, t81rObject(t81rAPIV1, t81rKindPod, t81rOtherNS, "web-cross", labelsApp), "Running"),
		},
	})
	f.start()

	got, err := f.resolve(t81rSelector(t81rNamespace, map[string]string{"app": "checkout"}))
	if err != nil {
		t.Fatalf("resolveSelector: %v", err)
	}
	t81rEqualNames(t, got, []string{"web-nostatus", "web-pending", "web-run"}, "match set")

	// The factory must carry the rendered selector and the target namespace
	// (server-side filtering contract at the seam).
	calls := f.calls()
	if len(calls) != 1 {
		t.Fatalf("factory calls = %v, want exactly one", calls)
	}
	if calls[0] != "pods|"+t81rNamespace+"|app=checkout" {
		t.Fatalf("factory call = %q, want pods|%s|app=checkout", calls[0], t81rNamespace)
	}
}

// TestMatchResolverExcludesCompletedJobs pins decision 13 for Jobs: a Job is
// Completed when it carries Complete=True or Failed=True; False/Unknown
// conditions keep it inside the match set.
func TestMatchResolverExcludesCompletedJobs(t *testing.T) {
	labelsApp := map[string]string{"app": "checkout"}
	jobRef := imageflowv1alpha1.TargetReference{
		APIVersion:    t81rAPIBatch,
		Kind:          t81rKindJob,
		Namespace:     t81rNamespace,
		LabelSelector: &metav1.LabelSelector{MatchLabels: labelsApp},
	}
	f := newT81rFixture(t, map[string][]runtime.Object{
		"jobs": {
			t81rObject(t81rAPIBatch, t81rKindJob, t81rNamespace, "job-running", labelsApp),
			t81rJobConditions(t, t81rObject(t81rAPIBatch, t81rKindJob, t81rNamespace, "job-complete", labelsApp), [2]string{"Complete", "True"}),
			t81rJobConditions(t, t81rObject(t81rAPIBatch, t81rKindJob, t81rNamespace, "job-failed", labelsApp), [2]string{"Failed", "True"}),
			t81rJobConditions(t, t81rObject(t81rAPIBatch, t81rKindJob, t81rNamespace, "job-incomplete", labelsApp), [2]string{"Complete", "False"}),
			t81rJobConditions(t, t81rObject(t81rAPIBatch, t81rKindJob, t81rNamespace, "job-progressing", labelsApp), [2]string{"Failed", "False"}, [2]string{"Complete", "False"}),
		},
	})
	f.start()

	got, err := f.resolve(jobRef)
	if err != nil {
		t.Fatalf("resolveSelector: %v", err)
	}
	t81rEqualNames(t, got, []string{"job-incomplete", "job-progressing", "job-running"}, "job match set")
}

// TestMatchResolverReusesInformerPerCanonicalTuple pins the reuse registry:
// the SAME (GVK, namespace, selector) tuple — however many subscriptions
// declare it and however its matchLabels were ordered — creates exactly one
// informer; any component of the tuple creates a new one.
func TestMatchResolverReusesInformerPerCanonicalTuple(t *testing.T) {
	labelsApp := map[string]string{"app": "checkout"}
	f := newT81rFixture(t, map[string][]runtime.Object{
		"pods": {t81rPodPhase(t, t81rObject(t81rAPIV1, t81rKindPod, t81rNamespace, "web-run", labelsApp), "Running")},
	})
	f.start()

	first := t81rSelector(t81rNamespace, map[string]string{"app": "checkout"})
	if _, err := f.resolve(first); err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	// A separately declared, semantically identical selector (map order is
	// irrelevant; the canonical encoding sorts keys) reuses the informer.
	same := t81rSelector(t81rNamespace, map[string]string{"app": "checkout"})
	if imageflowv1alpha1.CanonicalTargetKey(first) != imageflowv1alpha1.CanonicalTargetKey(same) {
		t.Fatal("precondition: identical selectors must share a canonical key")
	}
	if _, err := f.resolve(same); err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if n := len(f.calls()); n != 1 {
		t.Fatalf("factory calls after identical tuple = %d, want 1 (informer reuse)", n)
	}
	if n := f.informerCount(); n != 1 {
		t.Fatalf("registered informers = %d, want 1", n)
	}

	// A different selector value is a different tuple.
	other := t81rSelector(t81rNamespace, map[string]string{"app": "billing"})
	if _, err := f.resolve(other); err != nil {
		t.Fatalf("third resolve: %v", err)
	}
	// A different namespace is a different tuple.
	crossNS := t81rSelector(t81rOtherNS, map[string]string{"app": "checkout"})
	if _, err := f.resolve(crossNS); err != nil {
		t.Fatalf("fourth resolve: %v", err)
	}
	if n := len(f.calls()); n != 3 {
		t.Fatalf("factory calls after two new tuples = %d, want 3", n)
	}
	if n := f.informerCount(); n != 3 {
		t.Fatalf("registered informers = %d, want 3", n)
	}
}

// TestNewDynamicListerWatcherCarriesSelectorServerSide pins ADR-014
// decision 12 on the production factory: the rendered selector goes INTO
// the List/Watch request options (the API server filters; the cache holds
// only the matching set) and both requests are namespace-scoped (decision
// 2). The fake dynamic client's restriction parsing proves what reached the
// wire.
func TestNewDynamicListerWatcherCarriesSelectorServerSide(t *testing.T) {
	dyn := dynamicfake.NewSimpleDynamicClient(clientgoscheme.Scheme)

	var mu sync.Mutex
	var listSel, listNS, watchSel, watchNS string
	dyn.PrependReactor("list", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		la := action.(clienttesting.ListAction)
		mu.Lock()
		listSel = la.GetListRestrictions().Labels.String()
		listNS = la.GetNamespace()
		mu.Unlock()
		return false, nil, nil
	})
	dyn.PrependWatchReactor("*", func(action clienttesting.Action) (bool, watch.Interface, error) {
		wa := action.(clienttesting.WatchAction)
		mu.Lock()
		watchSel = wa.GetWatchRestrictions().Labels.String()
		watchNS = wa.GetNamespace()
		mu.Unlock()
		return true, watch.NewFake(), nil
	})

	lw := newDynamicListerWatcher(dyn)(context.Background(),
		schema.GroupVersionResource{Version: "v1", Resource: "pods"}, t81rNamespace, "app=checkout")

	lwc := cache.ToListerWatcherWithContext(lw)
	if _, err := lwc.ListWithContext(context.Background(), metav1.ListOptions{}); err != nil {
		t.Fatalf("List: %v", err)
	}
	w, err := lwc.WatchWithContext(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	w.Stop()

	mu.Lock()
	defer mu.Unlock()
	if listSel != "app=checkout" || listNS != t81rNamespace {
		t.Fatalf("List request carried selector %q namespace %q, want app=checkout / %s", listSel, listNS, t81rNamespace)
	}
	if watchSel != "app=checkout" || watchNS != t81rNamespace {
		t.Fatalf("Watch request carried selector %q namespace %q, want app=checkout / %s", watchSel, watchNS, t81rNamespace)
	}
}

// TestMatchResolverOnChangeForwardsEventsIncludingTombstones pins the
// event-driven half of decision 3: every add/update/delete inside the
// filtered set reaches the installed sink, DELETE tombstones are unwrapped
// (their labels are what the mapping re-check needs), and non-unstructured
// payloads are dropped instead of panicking.
func TestMatchResolverOnChangeForwardsEventsIncludingTombstones(t *testing.T) {
	labelsApp := map[string]string{"app": "checkout"}
	f := newT81rFixture(t, map[string][]runtime.Object{
		"pods": {t81rPodPhase(t, t81rObject(t81rAPIV1, t81rKindPod, t81rNamespace, "web-run", labelsApp), "Running")},
	})
	f.start()

	events := make(chan string, 64)
	f.resolver.setOnChange(func(u *unstructured.Unstructured) {
		events <- u.GetName()
	})

	// First resolve registers the informer; drain the initial-sync adds so
	// only the mutations below are asserted.
	if _, err := f.resolve(t81rSelector(t81rNamespace, labelsApp)); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	drainT81rEvents(events)

	src := f.sources["pods"]
	added := t81rPodPhase(t, t81rObject(t81rAPIV1, t81rKindPod, t81rNamespace, "web-new", labelsApp), "Pending")
	modified := added.DeepCopy()
	if err := unstructured.SetNestedField(modified.Object, "Running", "status", "phase"); err != nil {
		t.Fatalf("set phase: %v", err)
	}
	src.Add(added)
	if !t81rWaitForEvent(events, "web-new") {
		t.Fatal("add event for web-new did not reach the sink")
	}
	src.Modify(modified)
	if !t81rWaitForEvent(events, "web-new") {
		t.Fatal("update event for web-new did not reach the sink")
	}
	src.Delete(modified)
	if !t81rWaitForEvent(events, "web-new") {
		t.Fatal("delete event for web-new did not reach the sink")
	}

	// Direct notify: tombstones unwrap to their object, foreign payloads
	// are dropped.
	tomb := t81rObject(t81rAPIV1, t81rKindPod, t81rNamespace, "web-tomb", labelsApp)
	f.resolver.notify(cache.DeletedFinalStateUnknown{Key: "web-tomb", Obj: tomb})
	if !t81rWaitForEvent(events, "web-tomb") {
		t.Fatal("tombstone payload did not reach the sink unwrapped")
	}
	f.resolver.notify("not-an-object")
	select {
	case got := <-events:
		t.Fatalf("non-unstructured payload reached the sink: %q", got)
	case <-time.After(50 * time.Millisecond):
	}
}

// drainT81rEvents empties everything already queued (initial informer sync
// replays the seed as adds).
func drainT81rEvents(ch chan string) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func t81rWaitForEvent(ch chan string, want string) bool {
	deadline := time.After(5 * time.Second)
	for {
		select {
		case got := <-ch:
			if got == want {
				return true
			}
		case <-deadline:
			return false
		}
	}
}

// TestMatchResolverGuards pins the defensive rejections: resolveSelector
// only accepts the selector form; before Start no informer can register;
// an unparsable selector surfaces as an error instead of matching
// everything.
func TestMatchResolverGuards(t *testing.T) {
	labelsApp := map[string]string{"app": "checkout"}
	f := newT81rFixture(t, map[string][]runtime.Object{
		"pods": {t81rObject(t81rAPIV1, t81rKindPod, t81rNamespace, "web-run", labelsApp)},
	})

	// Before Start: registration is refused.
	named := imageflowv1alpha1.TargetReference{
		APIVersion: t81rAPIV1, Kind: t81rKindPod, Namespace: t81rNamespace, Name: "web-run",
	}
	if _, err := f.resolve(named); err == nil || !strings.Contains(err.Error(), "selector-form") {
		t.Fatalf("named-form resolve error = %v, want selector-form rejection", err)
	}
	if _, err := f.resolve(t81rSelector(t81rNamespace, labelsApp)); err == nil || !strings.Contains(err.Error(), "not started") {
		t.Fatalf("pre-Start resolve error = %v, want not-started rejection", err)
	}

	f.start()
	broken := imageflowv1alpha1.TargetReference{
		APIVersion: t81rAPIV1,
		Kind:       t81rKindPod,
		Namespace:  t81rNamespace,
		LabelSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
			{Key: "app", Operator: metav1.LabelSelectorOperator("Bogus"), Values: []string{"checkout"}},
		}},
	}
	if _, err := f.resolve(broken); err == nil || !strings.Contains(err.Error(), "invalid label selector") {
		t.Fatalf("invalid selector error = %v, want invalid-label-selector", err)
	}
}

// TestIsCompletedObject is the decision-13 truth table in isolation.
func TestIsCompletedObject(t *testing.T) {
	labelsApp := map[string]string{"app": "checkout"}
	cases := []struct {
		name string
		obj  *unstructured.Unstructured
		want bool
	}{
		{"pod running", t81rPodPhaseNoT(t81rObject(t81rAPIV1, t81rKindPod, t81rNamespace, "p", labelsApp), "Running"), false},
		{"pod pending", t81rPodPhaseNoT(t81rObject(t81rAPIV1, t81rKindPod, t81rNamespace, "p", labelsApp), "Pending"), false},
		{"pod succeeded", t81rPodPhaseNoT(t81rObject(t81rAPIV1, t81rKindPod, t81rNamespace, "p", labelsApp), "Succeeded"), true},
		{"pod failed", t81rPodPhaseNoT(t81rObject(t81rAPIV1, t81rKindPod, t81rNamespace, "p", labelsApp), "Failed"), true},
		{"pod no status", t81rObject(t81rAPIV1, t81rKindPod, t81rNamespace, "p", labelsApp), false},
		{"job complete true", t81rJobConditionsNoT(t81rObject(t81rAPIBatch, t81rKindJob, t81rNamespace, "j", labelsApp), [2]string{"Complete", "True"}), true},
		{"job failed true", t81rJobConditionsNoT(t81rObject(t81rAPIBatch, t81rKindJob, t81rNamespace, "j", labelsApp), [2]string{"Failed", "True"}), true},
		{"job complete false", t81rJobConditionsNoT(t81rObject(t81rAPIBatch, t81rKindJob, t81rNamespace, "j", labelsApp), [2]string{"Complete", "False"}), false},
		{"job no conditions", t81rObject(t81rAPIBatch, t81rKindJob, t81rNamespace, "j", labelsApp), false},
		{"deployment succeeded phase is not terminal elsewhere", t81rPodPhaseNoT(t81rObject("apps/v1", "Deployment", t81rNamespace, "d", labelsApp), "Succeeded"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isCompletedObject(tc.obj); got != tc.want {
				t.Fatalf("isCompletedObject(%s) = %v, want %v", tc.obj.GetName(), got, tc.want)
			}
		})
	}
}

// t81rPodPhaseNoT / t81rJobConditionsNoT are panic-on-error variants for
// table literals (the helpers cannot carry *testing.T into a var block).
func t81rPodPhaseNoT(u *unstructured.Unstructured, phase string) *unstructured.Unstructured {
	_ = unstructured.SetNestedField(u.Object, phase, "status", "phase")
	return u
}

func t81rJobConditionsNoT(u *unstructured.Unstructured, conds ...[2]string) *unstructured.Unstructured {
	slice := make([]any, 0, len(conds))
	for _, c := range conds {
		slice = append(slice, map[string]any{"type": c[0], "status": c[1]})
	}
	_ = unstructured.SetNestedSlice(u.Object, slice, "status", "conditions")
	return u
}

// TestTargetRefMatchesObject pins the shared predicate behind both watch
// mappings: named form matches exactly one object; selector form matches
// the label set of the whole class; any GVK or namespace difference
// matches nothing; an unparsable selector fails closed.
func TestTargetRefMatchesObject(t *testing.T) {
	ns := t81rNamespace
	obj := t81rObject("apps/v1", "Deployment", ns, "web-1", map[string]string{"app": "checkout"})

	named := imageflowv1alpha1.TargetReference{APIVersion: "apps/v1", Kind: "Deployment", Namespace: ns, Name: "web-1"}
	selector := t81rSelectorRef("apps/v1", "Deployment", ns, map[string]string{"app": "checkout"})

	cases := []struct {
		name string
		ref  imageflowv1alpha1.TargetReference
		obj  *unstructured.Unstructured
		want bool
	}{
		{"named exact", named, obj, true},
		{"named other object", imageflowv1alpha1.TargetReference{APIVersion: "apps/v1", Kind: "Deployment", Namespace: ns, Name: "web-2"}, obj, false},
		{"selector labels match", selector, obj, true},
		{"selector labels miss", t81rSelectorRef("apps/v1", "Deployment", ns, map[string]string{"app": "billing"}), obj, false},
		{"object without labels", selector, t81rObject("apps/v1", "Deployment", ns, "web-3", nil), false},
		{"namespace differs", imageflowv1alpha1.TargetReference{APIVersion: "apps/v1", Kind: "Deployment", Namespace: t81rOtherNS, Name: "web-1"}, obj, false},
		{"kind differs", imageflowv1alpha1.TargetReference{APIVersion: "apps/v1", Kind: "StatefulSet", Namespace: ns, Name: "web-1"}, obj, false},
		{"apiVersion differs", imageflowv1alpha1.TargetReference{APIVersion: "apps/v1beta1", Kind: "Deployment", Namespace: ns, Name: "web-1"}, obj, false},
		{"selector form with a name behaves as named", imageflowv1alpha1.TargetReference{
			APIVersion: "apps/v1", Kind: "Deployment", Namespace: ns, Name: "web-1",
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "billing"}},
		}, obj, true},
		{"unparsable selector fails closed", t81rBrokenSelectorRef(), obj, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := targetRefMatchesObject(tc.ref, tc.obj.GetAPIVersion(), tc.obj.GetKind(), tc.obj); got != tc.want {
				t.Fatalf("targetRefMatchesObject = %v, want %v", got, tc.want)
			}
		})
	}
}

func t81rSelectorRef(apiVersion, kind, ns string, matchLabels map[string]string) imageflowv1alpha1.TargetReference {
	return imageflowv1alpha1.TargetReference{
		APIVersion:    apiVersion,
		Kind:          kind,
		Namespace:     ns,
		LabelSelector: &metav1.LabelSelector{MatchLabels: matchLabels},
	}
}

func t81rBrokenSelectorRef() imageflowv1alpha1.TargetReference {
	return imageflowv1alpha1.TargetReference{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Namespace:  t81rNamespace,
		LabelSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
			{Key: "app", Operator: metav1.LabelSelectorOperator("Bogus"), Values: []string{"checkout"}},
		}},
	}
}

// TestSelectorMatchesLabels pins the fail-closed contract: an empty
// selector matches everything (labels.Everything), a broken one matches
// nothing.
func TestSelectorMatchesLabels(t *testing.T) {
	objLabels := map[string]string{"app": "checkout"}
	if !selectorMatchesLabels(&metav1.LabelSelector{}, objLabels) {
		t.Fatal("empty selector must match every label set")
	}
	broken := t81rBrokenSelectorRef().LabelSelector
	if selectorMatchesLabels(broken, objLabels) {
		t.Fatal("unparsable selector must fail closed")
	}
	if selectorMatchesLabels(nil, objLabels) {
		t.Fatal("nil selector must fail closed (named form never calls this)")
	}
}
