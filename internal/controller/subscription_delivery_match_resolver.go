package controller

// Match resolution for label-selector targets (issue #81, ADR-014 decisions
// 2/3/12/13, spec D3/D5).
//
// A selector target names a CLASS of objects: same namespace, same GVK,
// labels matching the selector. Membership is continuous: this resolver
// keeps one informer per (GVK, namespace, selector) tuple whose ListWatch
// carries the selector server-side (decision 12), so the informer cache
// holds exactly the matching set and label changes arrive as add/delete
// events inside the filtered set — no polling, no full-kind watches.
// Subscriptions whose selector targets render to the same canonical key
// (CanonicalTargetKey selector form, #79) share one informer.
//
// Two products leave this resolver:
//
//   - resolveSelector: the membership snapshot the delivery engine converges
//     against (with terminal-state exclusion, decision 13), and
//   - onChange events: every add/update/delete inside any filtered set,
//     forwarded onto the delivery controller's match channel so membership
//     changes enqueue the affected subscriptions (decision 3 持续 reconcile).

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
)

// matchSyncTimeout bounds the initial cache sync of a freshly registered
// tuple informer; a sync that cannot complete (RBAC, API outage) surfaces
// as a transient failure so the reconcile retries with backoff instead of
// hanging the worker.
const matchSyncTimeout = 30 * time.Second

// matchResolverAPI is the seam the delivery engine resolves selector targets
// through. The production implementation is matchResolver (shared informers);
// unit tests substitute a fake-client lister with the same completed-state
// exclusion.
type matchResolverAPI interface {
	resolveSelector(ctx context.Context, ref imageflowv1alpha1.TargetReference) ([]string, error)
}

// listerWatcherFactory builds the server-side-filtered ListWatch of one
// tuple; the seam lets unit tests substitute a cache.FakeControllerSource
// while the production path is asserted to carry the selector in its options.
type listerWatcherFactory func(ctx context.Context, gvr schema.GroupVersionResource, namespace, selector string) cache.ListerWatcher

// newDynamicListerWatcher renders the production factory: namespace-scoped
// dynamic List/Watch with the label selector INSIDE the request options
// (ADR-014 decision 12 — the API server filters, the cache stays small).
func newDynamicListerWatcher(dyn dynamic.Interface) listerWatcherFactory {
	return func(ctx context.Context, gvr schema.GroupVersionResource, namespace, selector string) cache.ListerWatcher {
		return &cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				options.LabelSelector = selector
				return dyn.Resource(gvr).Namespace(namespace).List(ctx, options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				options.LabelSelector = selector
				return dyn.Resource(gvr).Namespace(namespace).Watch(ctx, options)
			},
		}
	}
}

// matchInformer is one registered tuple: the shared informer plus the sync
// probe resolveSelector waits on.
type matchInformer struct {
	key       string
	informer  cache.SharedIndexInformer
	hasSynced cache.InformerSynced
}

func (mi *matchInformer) run(ctx context.Context) {
	go mi.informer.Run(ctx.Done())
}

// matchResolver owns the per-tuple informer registry. It is a
// manager.Runnable: Start stores the run context; informers are created
// lazily on first resolve (subscriptions declare tuples, not the resolver)
// and stop with the manager.
type matchResolver struct {
	restMapper meta.RESTMapper
	factory    listerWatcherFactory
	logger     logr.Logger

	mu        sync.Mutex
	informers map[string]*matchInformer
	ctx       context.Context // nil until Start
	onChange  func(obj *unstructured.Unstructured)
}

func newMatchResolver(restMapper meta.RESTMapper, factory listerWatcherFactory, logger logr.Logger) *matchResolver {
	return &matchResolver{
		restMapper: restMapper,
		factory:    factory,
		logger:     logger,
		informers:  map[string]*matchInformer{},
	}
}

// Start blocks until the manager context ends; informers created later run
// under the stored context.
func (r *matchResolver) Start(ctx context.Context) error {
	r.mu.Lock()
	r.ctx = ctx
	for _, mi := range r.informers {
		mi.run(ctx)
	}
	r.mu.Unlock()
	<-ctx.Done()
	return nil
}

// setOnChange installs the delivery engine's match-channel sink. Called once
// during registration, before the manager starts.
func (r *matchResolver) setOnChange(fn func(obj *unstructured.Unstructured)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onChange = fn
}

// getOrCreate registers the tuple's informer on first use; later callers
// with the same canonical key (same GVK + namespace + selector, however many
// subscriptions declare it) share the one informer.
func (r *matchResolver) getOrCreate(ref imageflowv1alpha1.TargetReference) (*matchInformer, error) {
	key := imageflowv1alpha1.CanonicalTargetKey(ref)
	r.mu.Lock()
	defer r.mu.Unlock()
	if mi, ok := r.informers[key]; ok {
		return mi, nil
	}
	if r.ctx == nil {
		return nil, fmt.Errorf("match resolver not started: cannot register informer for %s", key)
	}
	gvk := schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind)
	mapping, err := r.restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, err
	}
	selector, err := metav1.LabelSelectorAsSelector(ref.LabelSelector)
	if err != nil {
		return nil, fmt.Errorf("invalid label selector on target %s: %w", key, err)
	}
	mi := &matchInformer{key: key}
	mi.informer = cache.NewSharedIndexInformer(
		r.factory(r.ctx, mapping.Resource, ref.Namespace, selector.String()),
		&unstructured.Unstructured{}, 0, cache.Indexers{})
	mi.hasSynced = mi.informer.HasSynced
	if _, err := mi.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    r.notify,
		UpdateFunc: func(_, obj any) { r.notify(obj) },
		DeleteFunc: r.notify,
	}); err != nil {
		return nil, fmt.Errorf("register match event handler for %s: %w", key, err)
	}
	r.informers[key] = mi
	mi.run(r.ctx)
	r.logger.V(1).Info("registered match informer", "tuple", key, "gvr", mapping.Resource.String())
	return mi, nil
}

// notify forwards one informer event to the delivery engine's match channel.
// Delete events may arrive as tombstones carrying the last matching labels —
// exactly what the mapping re-check needs to enqueue the losing subscription.
func (r *matchResolver) notify(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		if tomb, tombOK := obj.(cache.DeletedFinalStateUnknown); tombOK {
			u, ok = tomb.Obj.(*unstructured.Unstructured)
		}
		if !ok || u == nil {
			return
		}
	}
	r.mu.Lock()
	fn := r.onChange
	r.mu.Unlock()
	if fn != nil {
		fn(u)
	}
}

// resolveSelector returns the sorted names of the tuple's CURRENT match set:
// every cached object in the target namespace whose labels satisfy the
// selector, minus terminal-state objects (ADR-014 decision 13). The labels
// re-check against the cache costs nothing and keeps the answer honest even
// if a ListWatch ever delivered outside its filter.
func (r *matchResolver) resolveSelector(ctx context.Context, ref imageflowv1alpha1.TargetReference) ([]string, error) {
	if ref.LabelSelector == nil || ref.Name != "" {
		return nil, fmt.Errorf("resolveSelector requires a selector-form target, got %s",
			imageflowv1alpha1.CanonicalTargetKey(ref))
	}
	mi, err := r.getOrCreate(ref)
	if err != nil {
		return nil, err
	}
	syncCtx, cancel := context.WithTimeout(ctx, matchSyncTimeout)
	defer cancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), mi.hasSynced) {
		return nil, fmt.Errorf("match informer for %s did not sync within %s", mi.key, matchSyncTimeout)
	}
	selector, err := metav1.LabelSelectorAsSelector(ref.LabelSelector)
	if err != nil {
		return nil, fmt.Errorf("invalid label selector on target %s: %w", mi.key, err)
	}
	var names []string
	for _, item := range mi.informer.GetStore().List() {
		u, ok := item.(*unstructured.Unstructured)
		if !ok || u.GetNamespace() != ref.Namespace {
			continue
		}
		if !selector.Matches(labels.Set(u.GetLabels())) {
			continue
		}
		if isCompletedObject(u) {
			continue
		}
		names = append(names, u.GetName())
	}
	sort.Strings(names)
	return names, nil
}

// isCompletedObject implements ADR-014 decision 13: terminal workloads are
// outside the match set entirely — never matched, never delivered. A
// Completed Pod has phase Succeeded/Failed; a Completed Job has a
// Complete=True or Failed=True condition.
func isCompletedObject(u *unstructured.Unstructured) bool {
	switch u.GetKind() {
	case "Pod":
		phase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
		return phase == "Succeeded" || phase == "Failed"
	case "Job":
		conditions, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
		for _, c := range conditions {
			cond, ok := c.(map[string]any)
			if !ok {
				continue
			}
			condType, _, _ := unstructured.NestedString(cond, "type")
			condStatus, _, _ := unstructured.NestedString(cond, "status")
			if (condType == "Complete" || condType == "Failed") && condStatus == "True" {
				return true
			}
		}
	}
	return false
}

// selectorMatchesLabels renders a declared selector and matches it against
// one object's labels. An unparsable selector matches nothing (admission
// validation owns the user-facing error; matching must fail closed).
func selectorMatchesLabels(sel *metav1.LabelSelector, objLabels map[string]string) bool {
	parsed, err := metav1.LabelSelectorAsSelector(sel)
	if err != nil {
		return false
	}
	return parsed.Matches(labels.Set(objLabels))
}

// targetRefMatchesObject reports whether a target reference — named or
// selector form — points at the given object. Shared by the delivery and
// observer workload-event mappings (issue #81: selector targets map events
// for every object in their class).
func targetRefMatchesObject(ref imageflowv1alpha1.TargetReference, apiVersion, kind string, obj client.Object) bool {
	if ref.APIVersion != apiVersion || ref.Kind != kind || ref.Namespace != obj.GetNamespace() {
		return false
	}
	if ref.LabelSelector != nil && ref.Name == "" {
		return selectorMatchesLabels(ref.LabelSelector, obj.GetLabels())
	}
	return ref.Name == obj.GetName()
}
