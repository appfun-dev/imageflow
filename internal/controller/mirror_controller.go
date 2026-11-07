package controller

// Mirror synthesis controller (ticket T5, ADR-002 R4/R9/R10).
//
// One registration unit in the Registrar pattern: it watches ImageStream (the
// single source of truth) and synthesizes the read-only mirror views —
// ImageStreamTag (per tag, current digest) and ImageTag (per digest anywhere
// in the bounded per-tag histories) — in the stream's namespace. Mirror
// status carries the current association (image{digest,imageReference,
// artifactRef,created}), the source stream's observedGeneration, the bounded
// event summary (ImageChanged entries recomputed from stream history;
// Delivery entries written by the subscription controller are preserved —
// this synthesis loop is T11's durable write entry point) and read-only
// consumer references derived from ImageSubscription objects.
//
// Ownership boundaries (parallel-ticket coordination):
//   - ImageStream.status is READ-ONLY input here; writes belong to the
//     mapping/import paths (T4/T9). This controller never writes streams.
//   - ImageArtifact objects are READ-ONLY input; creation belongs to the
//     registration paths (T4). Absence is tolerated: artifactRef stays empty
//     and the ArtifactLinked condition reports ArtifactNotFound until the
//     artifact appears (an ImageArtifact watch re-triggers synthesis).
//   - Mirror objects (ImageStreamTag/ImageTag) are owned EXCLUSIVELY by this
//     controller. Specs are written once at create time and never mutated:
//     the CRD CEL rule (self == oldSelf) rejects every spec update, including
//     this controller's own, so the create path must get the spec right.
//     Status writes go through the status subresource only.
//
// Deletion semantics: every mirror carries a controlling ownerRef to its
// stream, so Kubernetes GC cascades on stream deletion in a real cluster. In
// addition, the NotFound reconcile path sweeps leftover mirrors explicitly,
// which makes cascade behavior deterministic in environments without a
// garbage-collector controller (envtest) and covers mirrors created while the
// manager was down.

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
)

// mirrorControllerName is the production controller name (Named()). It shows
// up as the `controller` label on controller-runtime metrics and workqueue
// metrics. Integration tests must build their instance under a different
// name: controller-runtime validates controller names against a
// process-global set, and the shared manager_test.go environment builds every
// registered controller in the same test process.
const mirrorControllerName = "mirror"

// Label values of the imageflow_mirror_* metrics (bounded vocabularies, see
// docs/metrics-contract.md "T5 mirror metrics").
const (
	mirrorKindImageStreamTag = "ImageStreamTag"
	mirrorKindImageTag       = "ImageTag"

	mirrorActionCreate      = "create"
	mirrorActionUpdate      = "update"
	mirrorActionStatusWrite = "status_update"
	mirrorActionDelete      = "delete"

	mirrorResultSuccess = "success"
	mirrorResultError   = "error"
)

var (
	mirrorObjectWrites = promauto.With(metrics.Registry).NewCounterVec(prometheus.CounterOpts{
		Name: "imageflow_mirror_object_writes_total",
		Help: "Total mirror object writes performed by the synthesis controller. " +
			"kind=ImageStreamTag|ImageTag, action=create|update|status_update|delete, " +
			"result=success|error. No-op reconciles (idempotent synthesis) perform no write " +
			"and are not counted.",
	}, []string{"kind", "action", "result"})

	mirrorEventsTruncated = promauto.With(metrics.Registry).NewCounterVec(prometheus.CounterOpts{
		Name: "imageflow_mirror_events_truncated_total",
		Help: "Total mirror status event entries dropped by the bounded event window " +
			"(--mirror-event-limit, CRD cap 5) when a status write occurred. Overflow lives " +
			"in kube Events and the audit log (ADR-008). kind=ImageStreamTag|ImageTag.",
	}, []string{"kind"})

	mirrorSynthesisLag = promauto.With(metrics.Registry).NewHistogramVec(prometheus.HistogramOpts{
		Name: "imageflow_mirror_synthesis_lag_seconds",
		Help: "Age of the mirrored catalog association at synthesis time (clock now minus " +
			"the association's created timestamp), observed when a mirror status write " +
			"occurred. Measures how far the mirror trails the source of truth. " +
			"kind=ImageStreamTag|ImageTag.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300},
	}, []string{"kind"})
)

func init() {
	Registrars = append(Registrars, mirrorRegistrar{})
}

type mirrorRegistrar struct{}

func (mirrorRegistrar) Register(mgr ctrl.Manager, opts Options) error {
	return RegisterMirror(mgr, mirrorControllerName, opts)
}

// RBAC of the whole manager, regenerated into config/rbac/role.yaml by
// `make manifests`. The imagestreams rule is read-only: stream status belongs
// to the mapping/import paths (T4/T9), never to this controller. The core
// pods rule lives with its owning controller (the subscription observer scans
// pods for rollout health); the T12 RBAC consolidation removed the scaffold
// configmaps rule together with the example controller that needed it.
//
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagestreams,verbs=get;list;watch
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagestreamtags,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagestreamtags/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagetags,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagetags/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imageartifacts,verbs=get;list;watch
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagesubscriptions,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// RegisterMirror wires the mirror synthesis controller onto mgr under the
// given controller name. The name is a parameter because controller-runtime
// enforces process-global controller-name uniqueness; the integration suite
// runs a second instance next to the full manager under a distinct name.
func RegisterMirror(mgr ctrl.Manager, name string, opts Options) error {
	// Defensive registration guard: builder.Complete resolves the watched
	// GVKs against the manager's scheme at registration time, so a manager
	// whose scheme lacks the imageflow API types cannot host this controller.
	// The shared manager_test.go environment boots the full manager with a
	// minimal scheme (and no CRDs installed); mirrors are meaningless there,
	// so skip loudly instead of failing controller.Setup for the whole
	// process. Production (cmd/main.go) always registers the scheme, and
	// informer setup additionally tolerates missing CRDs at runtime
	// (watch sources retry NoKindMatchError until cancellation).
	for _, kind := range []string{"ImageStream", "ImageStreamTag", "ImageTag", "ImageSubscription", "ImageArtifact"} {
		gvk := imageflowv1alpha1.GroupVersion.WithKind(kind)
		if !mgr.GetScheme().Recognizes(gvk) {
			mgr.GetLogger().Error(
				fmt.Errorf("manager scheme does not recognize %s", gvk),
				"skipping mirror controller registration")
			return nil
		}
	}

	timeSource := opts.Clock
	if timeSource == nil {
		timeSource = clock.Real{}
	}
	r := &mirrorReconciler{
		client:     mgr.GetClient(),
		recorder:   mgr.GetEventRecorderFor("imageflow-mirror"),
		clock:      timeSource,
		eventLimit: effectiveMirrorEventLimit(opts.Config.MirrorEventLimit),
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&imageflowv1alpha1.ImageStream{}).
		Owns(&imageflowv1alpha1.ImageStreamTag{}).
		Owns(&imageflowv1alpha1.ImageTag{}).
		Watches(&imageflowv1alpha1.ImageSubscription{},
			handler.EnqueueRequestsFromMapFunc(r.streamsForSubscription)).
		Watches(&imageflowv1alpha1.ImageArtifact{},
			handler.EnqueueRequestsFromMapFunc(r.streamsForArtifact)).
		Complete(r)
}

type mirrorReconciler struct {
	client     client.Client
	recorder   record.EventRecorder
	clock      clock.Clock
	eventLimit int
}

// Reconcile synthesizes all mirror views of one ImageStream (request key =
// stream namespace/name). The loop is idempotent: desired state is recomputed
// from the source of truth on every pass and written only when it differs
// from the stored objects (no write amplification on repeated reconciles).
func (r *mirrorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	stream := &imageflowv1alpha1.ImageStream{}
	if err := r.client.Get(ctx, req.NamespacedName, stream); err != nil {
		if apierrors.IsNotFound(err) {
			// Source of truth is gone: sweep leftover mirrors (ownerRef GC
			// covers real clusters; this makes the cascade deterministic in
			// envtest and handles misses).
			return ctrl.Result{}, r.cleanupMirrors(ctx, req.Namespace, req.Name)
		}
		return ctrl.Result{}, fmt.Errorf("get source ImageStream: %w", err)
	}

	existingISTs, existingITs, err := r.listStreamMirrors(ctx, stream.Namespace, stream.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	subs, err := r.listSubscriptions(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	artifactExists, err := r.artifactLookup(ctx, stream)
	if err != nil {
		return ctrl.Result{}, err
	}

	desiredISTs, desiredITs := synthesizeMirrors(stream, subs, artifactExists, r.clock.Now())

	if err := r.applyImageStreamTags(ctx, stream, existingISTs, desiredISTs); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.applyImageTags(ctx, stream, existingITs, desiredITs); err != nil {
		return ctrl.Result{}, err
	}

	logger.V(1).Info("mirror synthesis reconciled",
		"stream", req.NamespacedName,
		"imageStreamTags", len(desiredISTs),
		"imageTags", len(desiredITs))
	return ctrl.Result{}, nil
}

// applyImageStreamTags upserts the desired ImageStreamTag mirrors and prunes
// stored ones whose tag association left the stream status.
func (r *mirrorReconciler) applyImageStreamTags(
	ctx context.Context,
	stream *imageflowv1alpha1.ImageStream,
	existing, desired []imageflowv1alpha1.ImageStreamTag,
) error {
	desiredNames := make(map[string]struct{}, len(desired))
	for i := range desired {
		d := &desired[i]
		desiredNames[d.Name] = struct{}{}
		cur := findImageStreamTag(existing, d.Name)
		if cur == nil {
			if err := r.createImageStreamTag(ctx, d); err != nil {
				return err
			}
			continue
		}
		if err := r.syncImageStreamTag(ctx, stream, cur, d); err != nil {
			return err
		}
	}
	for i := range existing {
		cur := &existing[i]
		if _, keep := desiredNames[cur.Name]; keep {
			continue
		}
		if err := r.deleteMirror(ctx, cur, mirrorKindImageStreamTag); err != nil {
			return err
		}
	}
	return nil
}

// applyImageTags upserts the desired ImageTag mirrors and prunes stored ones
// whose digest left every bounded tag history of the stream.
func (r *mirrorReconciler) applyImageTags(
	ctx context.Context,
	stream *imageflowv1alpha1.ImageStream,
	existing, desired []imageflowv1alpha1.ImageTag,
) error {
	desiredNames := make(map[string]struct{}, len(desired))
	for i := range desired {
		d := &desired[i]
		desiredNames[d.Name] = struct{}{}
		cur := findImageTag(existing, d.Name)
		if cur == nil {
			if err := r.createImageTag(ctx, d); err != nil {
				return err
			}
			continue
		}
		if err := r.syncImageTag(ctx, stream, cur, d); err != nil {
			return err
		}
	}
	for i := range existing {
		cur := &existing[i]
		if _, keep := desiredNames[cur.Name]; keep {
			continue
		}
		if err := r.deleteMirror(ctx, cur, mirrorKindImageTag); err != nil {
			return err
		}
	}
	return nil
}

func (r *mirrorReconciler) createImageStreamTag(ctx context.Context, d *imageflowv1alpha1.ImageStreamTag) error {
	obj := d.DeepCopy()
	obj.Status = imageflowv1alpha1.ImageStreamTagStatus{} // status subresource: written below
	if err := r.client.Create(ctx, obj); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// A different object occupies this index name (spec mismatch is
			// unreachable behind the deterministic encoder short of hash
			// collisions; names are indexes only, specs are authoritative and
			// CEL-immutable, so this object is left untouched and reported).
			r.reportNameCollision(ctx, d, mirrorKindImageStreamTag)
			return nil
		}
		mirrorObjectWrites.WithLabelValues(mirrorKindImageStreamTag, mirrorActionCreate, mirrorResultError).Inc()
		return fmt.Errorf("create ImageStreamTag mirror %s/%s: %w", d.Namespace, d.Name, err)
	}
	mirrorObjectWrites.WithLabelValues(mirrorKindImageStreamTag, mirrorActionCreate, mirrorResultSuccess).Inc()

	status := d.Status
	merged, dropped := mergeMirrorEvents(nil, status.Events, r.eventLimit)
	status.Events = merged
	obj.Status = status
	if err := r.client.Status().Update(ctx, obj); err != nil {
		mirrorObjectWrites.WithLabelValues(mirrorKindImageStreamTag, mirrorActionStatusWrite, mirrorResultError).Inc()
		return fmt.Errorf("write ImageStreamTag mirror status %s/%s: %w", obj.Namespace, obj.Name, err)
	}
	mirrorObjectWrites.WithLabelValues(mirrorKindImageStreamTag, mirrorActionStatusWrite, mirrorResultSuccess).Inc()
	r.observeSynthesisLag(mirrorKindImageStreamTag, status.Image)
	r.recordTruncation(ctx, obj, mirrorKindImageStreamTag, dropped)
	return nil
}

func (r *mirrorReconciler) createImageTag(ctx context.Context, d *imageflowv1alpha1.ImageTag) error {
	obj := d.DeepCopy()
	obj.Status = imageflowv1alpha1.ImageTagStatus{} // status subresource: written below
	if err := r.client.Create(ctx, obj); err != nil {
		if apierrors.IsAlreadyExists(err) {
			r.reportNameCollision(ctx, d, mirrorKindImageTag)
			return nil
		}
		mirrorObjectWrites.WithLabelValues(mirrorKindImageTag, mirrorActionCreate, mirrorResultError).Inc()
		return fmt.Errorf("create ImageTag mirror %s/%s: %w", d.Namespace, d.Name, err)
	}
	mirrorObjectWrites.WithLabelValues(mirrorKindImageTag, mirrorActionCreate, mirrorResultSuccess).Inc()

	status := d.Status
	merged, dropped := mergeMirrorEvents(nil, status.Events, r.eventLimit)
	status.Events = merged
	obj.Status = status
	if err := r.client.Status().Update(ctx, obj); err != nil {
		mirrorObjectWrites.WithLabelValues(mirrorKindImageTag, mirrorActionStatusWrite, mirrorResultError).Inc()
		return fmt.Errorf("write ImageTag mirror status %s/%s: %w", obj.Namespace, obj.Name, err)
	}
	mirrorObjectWrites.WithLabelValues(mirrorKindImageTag, mirrorActionStatusWrite, mirrorResultSuccess).Inc()
	r.observeSynthesisLag(mirrorKindImageTag, status.Image)
	r.recordTruncation(ctx, obj, mirrorKindImageTag, dropped)
	return nil
}

// syncImageStreamTag converges one stored mirror onto the desired state:
// spec integrity check (never mutated — CEL self==oldSelf rejects every spec
// update, including ours), ownerRef upkeep (user-writable metadata can drift;
// the update keeps the spec byte-identical so CEL passes) and a status write
// only when the merged desired status differs from the stored one.
func (r *mirrorReconciler) syncImageStreamTag(
	ctx context.Context,
	stream *imageflowv1alpha1.ImageStream,
	cur, d *imageflowv1alpha1.ImageStreamTag,
) error {
	if cur.Spec != d.Spec {
		r.reportNameCollision(ctx, cur, mirrorKindImageStreamTag)
		return nil
	}
	if err := r.ensureOwner(ctx, cur, stream, mirrorKindImageStreamTag); err != nil {
		return err
	}

	desiredStatus := d.Status.MirrorObjectStatus
	merged, dropped := mergeMirrorEvents(cur.Status.Events, desiredStatus.Events, r.eventLimit)
	desiredStatus.Events = merged
	desiredStatus.Conditions = mergeMirrorConditions(cur.Status.Conditions, desiredStatus.Conditions)
	if equality.Semantic.DeepEqual(cur.Status.MirrorObjectStatus, desiredStatus) {
		return nil // idempotency gate: no write amplification
	}

	upd := cur.DeepCopy()
	upd.Status.MirrorObjectStatus = desiredStatus
	if err := r.client.Status().Update(ctx, upd); err != nil {
		mirrorObjectWrites.WithLabelValues(mirrorKindImageStreamTag, mirrorActionStatusWrite, mirrorResultError).Inc()
		return fmt.Errorf("update ImageStreamTag mirror status %s/%s: %w", cur.Namespace, cur.Name, err)
	}
	mirrorObjectWrites.WithLabelValues(mirrorKindImageStreamTag, mirrorActionStatusWrite, mirrorResultSuccess).Inc()
	r.observeSynthesisLag(mirrorKindImageStreamTag, desiredStatus.Image)
	r.recordTruncation(ctx, upd, mirrorKindImageStreamTag, dropped)
	return nil
}

func (r *mirrorReconciler) syncImageTag(
	ctx context.Context,
	stream *imageflowv1alpha1.ImageStream,
	cur, d *imageflowv1alpha1.ImageTag,
) error {
	if cur.Spec != d.Spec {
		r.reportNameCollision(ctx, cur, mirrorKindImageTag)
		return nil
	}
	if err := r.ensureOwner(ctx, cur, stream, mirrorKindImageTag); err != nil {
		return err
	}

	desiredStatus := d.Status.MirrorObjectStatus
	merged, dropped := mergeMirrorEvents(cur.Status.Events, desiredStatus.Events, r.eventLimit)
	desiredStatus.Events = merged
	desiredStatus.Conditions = mergeMirrorConditions(cur.Status.Conditions, desiredStatus.Conditions)
	if equality.Semantic.DeepEqual(cur.Status.MirrorObjectStatus, desiredStatus) {
		return nil // idempotency gate: no write amplification
	}

	upd := cur.DeepCopy()
	upd.Status.MirrorObjectStatus = desiredStatus
	if err := r.client.Status().Update(ctx, upd); err != nil {
		mirrorObjectWrites.WithLabelValues(mirrorKindImageTag, mirrorActionStatusWrite, mirrorResultError).Inc()
		return fmt.Errorf("update ImageTag mirror status %s/%s: %w", cur.Namespace, cur.Name, err)
	}
	mirrorObjectWrites.WithLabelValues(mirrorKindImageTag, mirrorActionStatusWrite, mirrorResultSuccess).Inc()
	r.observeSynthesisLag(mirrorKindImageTag, desiredStatus.Image)
	r.recordTruncation(ctx, upd, mirrorKindImageTag, dropped)
	return nil
}

// ensureOwner restores the controlling ownerRef when it drifted off a mirror
// (metadata is the user-writable surface; the ownerRef is not negotiable —
// cascade GC depends on it). The spec is sent back byte-identical, which the
// CEL transition rule accepts.
func (r *mirrorReconciler) ensureOwner(
	ctx context.Context,
	cur client.Object,
	stream *imageflowv1alpha1.ImageStream,
	kind string,
) error {
	if metav1.IsControlledBy(cur, stream) {
		return nil
	}
	patch := cur.DeepCopyObject().(client.Object)
	patch.SetOwnerReferences(append(patch.GetOwnerReferences(), mirrorOwnerReference(stream)))
	if err := r.client.Update(ctx, patch); err != nil {
		mirrorObjectWrites.WithLabelValues(kind, mirrorActionUpdate, mirrorResultError).Inc()
		return fmt.Errorf("restore ownerRef on %s mirror %s/%s: %w", kind, cur.GetNamespace(), cur.GetName(), err)
	}
	mirrorObjectWrites.WithLabelValues(kind, mirrorActionUpdate, mirrorResultSuccess).Inc()
	// Propagate the fresh resourceVersion so a status write in the same pass
	// does not run into an optimistic-concurrency conflict.
	cur.SetResourceVersion(patch.GetResourceVersion())
	return nil
}

func (r *mirrorReconciler) deleteMirror(ctx context.Context, obj client.Object, kind string) error {
	if err := r.client.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		mirrorObjectWrites.WithLabelValues(kind, mirrorActionDelete, mirrorResultError).Inc()
		return fmt.Errorf("delete stale %s mirror %s/%s: %w", kind, obj.GetNamespace(), obj.GetName(), err)
	}
	mirrorObjectWrites.WithLabelValues(kind, mirrorActionDelete, mirrorResultSuccess).Inc()
	return nil
}

// cleanupMirrors deletes every mirror of a stream that no longer exists.
// NotFound races with the Kubernetes GC are tolerated.
func (r *mirrorReconciler) cleanupMirrors(ctx context.Context, namespace, streamName string) error {
	ists, its, err := r.listStreamMirrors(ctx, namespace, streamName)
	if err != nil {
		return err
	}
	for i := range ists {
		if err := r.deleteMirror(ctx, &ists[i], mirrorKindImageStreamTag); err != nil {
			return err
		}
	}
	for i := range its {
		if err := r.deleteMirror(ctx, &its[i], mirrorKindImageTag); err != nil {
			return err
		}
	}
	if n := len(ists) + len(its); n > 0 {
		log.FromContext(ctx).V(1).Info("swept mirrors of a deleted stream",
			"stream", streamName, "namespace", namespace, "objects", n)
	}
	return nil
}

// listStreamMirrors returns the stored mirrors of one stream in its namespace.
// Mirrors are listed per namespace and filtered in memory on spec.stream; at
// v1 catalog scale this avoids field-index setup and works identically on the
// cached client and the fake client.
func (r *mirrorReconciler) listStreamMirrors(
	ctx context.Context,
	namespace, streamName string,
) (ists []imageflowv1alpha1.ImageStreamTag, its []imageflowv1alpha1.ImageTag, err error) {
	istList := &imageflowv1alpha1.ImageStreamTagList{}
	if err := r.client.List(ctx, istList, client.InNamespace(namespace)); err != nil {
		return nil, nil, fmt.Errorf("list ImageStreamTag mirrors: %w", err)
	}
	for i := range istList.Items {
		if istList.Items[i].Spec.Stream == streamName {
			ists = append(ists, istList.Items[i])
		}
	}
	itList := &imageflowv1alpha1.ImageTagList{}
	if err := r.client.List(ctx, itList, client.InNamespace(namespace)); err != nil {
		return nil, nil, fmt.Errorf("list ImageTag mirrors: %w", err)
	}
	for i := range itList.Items {
		if itList.Items[i].Spec.Stream == streamName {
			its = append(its, itList.Items[i])
		}
	}
	return ists, its, nil
}

// listSubscriptions lists subscriptions cluster-wide: consumers of a catalog
// stream may live in any namespace (ADR-002 R3 cross-namespace sources).
func (r *mirrorReconciler) listSubscriptions(ctx context.Context) ([]imageflowv1alpha1.ImageSubscription, error) {
	subList := &imageflowv1alpha1.ImageSubscriptionList{}
	if err := r.client.List(ctx, subList); err != nil {
		return nil, fmt.Errorf("list ImageSubscriptions: %w", err)
	}
	return subList.Items, nil
}

// artifactLookup probes the cluster-scoped ImageArtifact of every digest in
// the stream status (memoized per reconcile). Absence is a normal state
// (registration belongs to T4/T9), anything else is an error.
func (r *mirrorReconciler) artifactLookup(
	ctx context.Context,
	stream *imageflowv1alpha1.ImageStream,
) (func(digest string) bool, error) {
	known := map[string]bool{}
	for _, tagEntry := range stream.Status.Tags {
		for _, item := range tagEntry.Items {
			if _, probed := known[item.Digest]; probed {
				continue
			}
			name := ArtifactName(item.Digest)
			if name == "" {
				known[item.Digest] = false
				continue
			}
			artifact := &imageflowv1alpha1.ImageArtifact{}
			err := r.client.Get(ctx, client.ObjectKey{Name: name}, artifact)
			switch {
			case err == nil:
				known[item.Digest] = true
			case apierrors.IsNotFound(err):
				known[item.Digest] = false
			default:
				return nil, fmt.Errorf("get ImageArtifact %s: %w", name, err)
			}
		}
	}
	return func(digest string) bool { return known[digest] }, nil
}

// streamsForSubscription maps a subscription event to the reconcile request
// of the stream it sources from, so consumer references re-synthesize when
// subscriptions are created, changed or deleted anywhere in the cluster.
func (r *mirrorReconciler) streamsForSubscription(_ context.Context, obj client.Object) []reconcile.Request {
	sub, ok := obj.(*imageflowv1alpha1.ImageSubscription)
	if !ok {
		return nil
	}
	src := sub.Spec.Source
	if src.Kind != mirrorSourceKindImageStreamTag {
		return nil
	}
	stream, _, ok := splitSubscriptionSourceName(src.Name)
	if !ok {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Namespace: src.Namespace, Name: stream},
	}}
}

// streamsForArtifact maps an ImageArtifact event (registration or removal by
// the T4/T9 paths) to the streams whose mirrors reference that digest, so
// artifactRef and the ArtifactLinked condition re-synthesize.
func (r *mirrorReconciler) streamsForArtifact(ctx context.Context, obj client.Object) []reconcile.Request {
	artifact, ok := obj.(*imageflowv1alpha1.ImageArtifact)
	if !ok {
		return nil
	}
	digest := artifact.Spec.Digest
	if digest == "" {
		return nil
	}
	found := map[types.NamespacedName]struct{}{}

	itList := &imageflowv1alpha1.ImageTagList{}
	if err := r.client.List(ctx, itList); err != nil {
		log.FromContext(ctx).Error(err, "cannot map ImageArtifact to ImageTag mirrors", "artifact", artifact.Name)
		return nil
	}
	for i := range itList.Items {
		if itList.Items[i].Spec.Digest == digest {
			found[types.NamespacedName{
				Namespace: itList.Items[i].Namespace,
				Name:      itList.Items[i].Spec.Stream,
			}] = struct{}{}
		}
	}

	istList := &imageflowv1alpha1.ImageStreamTagList{}
	if err := r.client.List(ctx, istList); err != nil {
		log.FromContext(ctx).Error(err, "cannot map ImageArtifact to ImageStreamTag mirrors", "artifact", artifact.Name)
		return nil
	}
	for i := range istList.Items {
		img := istList.Items[i].Status.Image
		if img != nil && img.Digest == digest {
			found[types.NamespacedName{
				Namespace: istList.Items[i].Namespace,
				Name:      istList.Items[i].Spec.Stream,
			}] = struct{}{}
		}
	}

	requests := make([]reconcile.Request, 0, len(found))
	for key := range found {
		requests = append(requests, reconcile.Request{NamespacedName: key})
	}
	return requests
}

// observeSynthesisLag records how far the freshly synthesized association
// trails the wall clock (ADR-002: mirror lag is seconds).
func (r *mirrorReconciler) observeSynthesisLag(kind string, img *imageflowv1alpha1.MirrorImage) {
	if img == nil || img.Created == nil {
		return
	}
	lag := r.clock.Since(img.Created.Time)
	if lag < 0 {
		lag = 0
	}
	mirrorSynthesisLag.WithLabelValues(kind).Observe(lag.Seconds())
}

// recordTruncation surfaces bounded-window overflow on the mirror object and
// in metrics (ADR-008: overflow falls to kube Events and audit logs). It is
// only called when a status write actually happened, so steady-state
// reconciles never spam Events.
func (r *mirrorReconciler) recordTruncation(ctx context.Context, obj client.Object, kind string, dropped int) {
	if dropped <= 0 {
		return
	}
	mirrorEventsTruncated.WithLabelValues(kind).Add(float64(dropped))
	r.recorder.Eventf(obj, corev1.EventTypeNormal, mirrorEventReasonSummaryTruncated,
		"mirror event summary is bounded to %d entries; %d older entries left the window "+
			"(full history lives in kube Events and the cluster audit log, ADR-008)",
		r.eventLimit, dropped)
	log.FromContext(ctx).V(1).Info("mirror event summary truncated",
		"kind", kind, "namespace", obj.GetNamespace(), "name", obj.GetName(), "dropped", dropped)
}

// reportNameCollision reports the (astronomically unlikely) case of two
// distinct mirror facts encoding to one object name. Names are indexes only,
// specs are authoritative and CEL-immutable, so the occupying object is never
// touched; the Event and log give operators the audit trail. obj is a typed
// mirror object carrying the colliding name (used only as the Event
// involved-object).
func (r *mirrorReconciler) reportNameCollision(ctx context.Context, obj runtime.Object, kind string) {
	log.FromContext(ctx).Info("mirror name collision: index name occupied by a different spec; leaving object untouched",
		"kind", kind)
	r.recorder.Eventf(obj, corev1.EventTypeWarning, "MirrorNameCollision",
		"This %s mirror name is occupied by an object with a different spec. Mirror names are indexes only, "+
			"specs are authoritative and immutable (CEL); the occupying object was left untouched.", kind)
}

func findImageStreamTag(
	items []imageflowv1alpha1.ImageStreamTag,
	name string,
) *imageflowv1alpha1.ImageStreamTag {
	for i := range items {
		if items[i].Name == name {
			return &items[i]
		}
	}
	return nil
}

func findImageTag(items []imageflowv1alpha1.ImageTag, name string) *imageflowv1alpha1.ImageTag {
	for i := range items {
		if items[i].Name == name {
			return &items[i]
		}
	}
	return nil
}
