package controller

// Watch surface of the delivery engine (ticket T9). Every trigger maps onto
// subscription reconcile requests; the reconciler itself is a full convergence
// pass, so the watches only decide WHEN passes run:
//
//   - ImageStream: the persisted Image Change (status.tags[].items) — a
//     Mapping (T4) or Import (T7) registration re-triggers delivery. Status
//     writes do not bump the generation, so the predicate compares the tag
//     associations explicitly.
//   - ImageSubscription (peer mapping): another subscription's spec change
//     (target dropped) or deletion can orphan field ownership this
//     subscription waits on — the ADR-008 exclusivity-reclaim trigger.
//     Status-only writes are ignored (they cannot change claims).
//   - Deployment/StatefulSet/DaemonSet: target created later ("目标后建"),
//     target spec edited (user reverted the image / added a container) and
//     target deleted (honest TargetNotFound report). Rollout status churn is
//     ignored via the generation gate — rollout observation is T11's lane and
//     must not drive delivery passes.
//
// Third-party kinds (T10's generic resolver lane) have no watch surface by
// design: their convergence rides the waiting-state requeue bounds of the
// engine, exactly like unwatchable events on built-in kinds.

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
)

// Built-in workload kind names (constants: the internal/controller package
// shares the goconst budget across every ticket's files).
const (
	kindDeployment  = "Deployment"
	kindStatefulSet = "StatefulSet"
	kindDaemonSet   = "DaemonSet"
)

// streamChangePredicate fires on stream creation/deletion and on updates that
// change the spec generation or the persisted tag associations (the Image
// Change surface — status writes never bump the generation).
type streamChangePredicate struct {
	predicate.Funcs
}

func (streamChangePredicate) Create(event.CreateEvent) bool { return true }
func (streamChangePredicate) Delete(event.DeleteEvent) bool { return true }

func (streamChangePredicate) Update(e event.UpdateEvent) bool {
	oldStream, ok1 := e.ObjectOld.(*imageflowv1alpha1.ImageStream)
	newStream, ok2 := e.ObjectNew.(*imageflowv1alpha1.ImageStream)
	if !ok1 || !ok2 {
		return false
	}
	if oldStream.Generation != newStream.Generation {
		return true
	}
	return !equality.Semantic.DeepEqual(oldStream.Status.Tags, newStream.Status.Tags)
}

// peerSubPredicate gates the peer-subscription mapping onto claim-relevant
// events: deletion (reclaim) and spec changes (target dropped). Creation is
// not mapped: a new subscription converges itself and cannot change the
// ownership state of established peers (first-wins protects them).
type peerSubPredicate struct {
	predicate.Funcs
}

func (peerSubPredicate) Create(event.CreateEvent) bool { return false }
func (peerSubPredicate) Delete(event.DeleteEvent) bool { return true }
func (peerSubPredicate) Update(e event.UpdateEvent) bool {
	return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration()
}

// workloadChangePredicate fires on target creation/deletion and on spec
// changes (generation bump). Status-only updates — the rollout churn of
// native controllers — are ignored.
type workloadChangePredicate struct {
	predicate.Funcs
}

func (workloadChangePredicate) Create(event.CreateEvent) bool { return true }
func (workloadChangePredicate) Delete(event.DeleteEvent) bool { return true }
func (workloadChangePredicate) Update(e event.UpdateEvent) bool {
	return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration()
}

// subsForStream maps an ImageStream event to every subscription sourcing from
// it (logical "<stream>:<tag>" name, source namespace match).
func (r *SubscriptionDeliveryReconciler) subsForStream(
	ctx context.Context,
	obj client.Object,
) []reconcile.Request {
	stream, ok := obj.(*imageflowv1alpha1.ImageStream)
	if !ok {
		return nil
	}
	subs, err := r.listSubscriptions(ctx)
	if err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range subs {
		src := subs[i].Spec.Source
		if src.Kind != mirrorSourceKindImageStreamTag || src.Namespace != stream.Namespace {
			continue
		}
		streamName, _, split := splitSubscriptionSourceName(src.Name)
		if split && streamName == stream.Name {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: subs[i].Namespace,
				Name:      subs[i].Name,
			}})
		}
	}
	return requests
}

// peerSubsForSub maps a subscription event (deletion or spec change) to every
// OTHER subscription sharing at least one target reference with it: the
// exclusivity-reclaim trigger (ADR-008: deleting/editing the occupying
// subscription lets the waiter's reconcile replay the Apply).
func (r *SubscriptionDeliveryReconciler) peerSubsForSub(
	ctx context.Context,
	obj client.Object,
) []reconcile.Request {
	changed, ok := obj.(*imageflowv1alpha1.ImageSubscription)
	if !ok {
		return nil
	}
	subs, err := r.listSubscriptions(ctx)
	if err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range subs {
		peer := &subs[i]
		if peer.Namespace == changed.Namespace && peer.Name == changed.Name {
			continue
		}
		if sharesTargetRef(peer, changed) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: peer.Namespace,
				Name:      peer.Name,
			}})
		}
	}
	return requests
}

// sharesTargetRef reports whether two subscriptions declare at least one
// identical target reference (all four fields equal).
func sharesTargetRef(a, b *imageflowv1alpha1.ImageSubscription) bool {
	for i := range a.Spec.Targets {
		for j := range b.Spec.Targets {
			if a.Spec.Targets[i].TargetRef == b.Spec.Targets[j].TargetRef {
				return true
			}
		}
	}
	return false
}

// subsForWorkload maps a built-in workload event to every subscription
// targeting that object. Cached typed objects carry no TypeMeta, so the kind
// is classified by Go type (the watch registration pins the GVK).
func (r *SubscriptionDeliveryReconciler) subsForWorkload(
	ctx context.Context,
	obj client.Object,
) []reconcile.Request {
	var kind string
	switch obj.(type) {
	case *appsv1.Deployment:
		kind = kindDeployment
	case *appsv1.StatefulSet:
		kind = kindStatefulSet
	case *appsv1.DaemonSet:
		kind = kindDaemonSet
	default:
		return nil
	}
	apiVersion := appsv1.SchemeGroupVersion.String()
	subs, err := r.listSubscriptions(ctx)
	if err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range subs {
		for j := range subs[i].Spec.Targets {
			ref := subs[i].Spec.Targets[j].TargetRef
			if ref.APIVersion == apiVersion && ref.Kind == kind &&
				ref.Namespace == obj.GetNamespace() && ref.Name == obj.GetName() {
				requests = append(requests, ctrl.Request{NamespacedName: types.NamespacedName{
					Namespace: subs[i].Namespace,
					Name:      subs[i].Name,
				}})
				break
			}
		}
	}
	return requests
}

// listSubscriptions lists all subscriptions (cached client). Mapping failures
// are logged and swallowed: a missed mapping is bounded by the engine's
// waiting-state requeues and the informer resync.
func (r *SubscriptionDeliveryReconciler) listSubscriptions(
	ctx context.Context,
) ([]imageflowv1alpha1.ImageSubscription, error) {
	var subs imageflowv1alpha1.ImageSubscriptionList
	if err := r.client.List(ctx, &subs); err != nil {
		log.FromContext(ctx).Error(err, "cannot list ImageSubscriptions for the delivery watch mapping")
		return nil, err
	}
	return subs.Items, nil
}
