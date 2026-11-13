package controller

// Subscription delivery engine (ticket T9, ADR-005/006/008): Image Change
// driven Server-Side Apply writes to built-in workload kinds.
//
// # Trigger chain
//
// The persisted form of an Image Change is ImageStream.status.tags[].items[0]
// (spec decision 9: no in-memory events). This controller watches the source
// ImageStreams of all subscriptions, so a Mapping (T4) or Import (T7)
// registration re-triggers delivery; subscription create/update, workload
// create/spec-change/delete and peer-subscription spec-change/delete events
// cover restart recovery, "target created later" and exclusivity reclaim.
// Every pass is a full convergence pass against persisted state: dedup,
// retry and restart recovery all fall out of reconcile idempotence.
//
// # Write contract (ADR-005, ADR-012)
//
// The catalog association (ImageStream.status.tags[].items[0]) is ALWAYS a
// digest reference; the engine refuses anything else at resolve time.
// spec.imageReferenceFormat (v1.2, ADR-012 §Decision 1/2, spec decisions
// 1/2/7) then shapes the value WRITTEN to target fields: Digest keeps the v1
// <repo>@sha256:<64hex> form, Tag writes <repo>:<tag>, DigestWithTag writes
// the OCI composite <repo>:<tag>@sha256:<64hex> — each guarded last-mile
// against its format grammar. Tag/DigestWithTag additionally report the
// resolved digest reference on status.targets[].resolvedDigest for
// observability and drift detection. Built-in adapters (ADR-006 first batch:
// Deployment/StatefulSet/DaemonSet) locate images by containerNames in the
// pod template — required, non-empty, no default guessing. Writes are minimal
// Server-Side Apply configurations that never touch unrelated fields.
//
// # Field-level exclusivity (ADR-008)
//
// Each subscription applies under one stable field manager
// (delivery.FieldManagerFor: imageflow.appfun.dev/sub-<ns>-<name>); the
// target's managedFields are the exclusivity registry. Before writing, the
// engine inspects the registry on a FRESH (uncached) read:
//
//   - another ImageFlow subscription owns a declared field AND still live-
//     claims it (its spec targets the same field) => terminal Conflict
//     condition, ZERO writes (first-wins);
//   - the owning ImageFlow subscription is gone or no longer claims the
//     field (deleted / target dropped from its spec) => orphaned registry
//     entry: reconcile replays the Apply with force and ownership becomes
//     deterministic — this is the ADR-008 "独占回收" and "目标后建" path;
//   - only user/GitOps managers own the field => the declaring subscription
//     takes it over (unforced apply first; a 409 round trip re-classifies
//     against a fresh read so concurrent subscription races converge to one
//     owner plus one terminal Conflict).
//
// Equal-and-owned fields produce NO write at all (no patch, no rollout, no
// API write amplification). The workload annotation
// imageflow.appfun.dev/managed-by is written with a static value as an
// audit-facing summary only (per-subscription values would turn the
// annotation itself into a conflict surface between co-owning subscriptions).
//
// # T8 authorization gate (frozen contract, issue #9)
//
// Before ANY SSA write to target T: the subscription's Forbidden condition
// must be present and not True, and T must have no status.targets[] entry
// with Reason=Forbidden — re-checked every reconcile (the persisted surface
// can flip on periodic re-verification). A missing Forbidden condition means
// the gate has not established its surface yet: zero writes + requeue
// (Unknown ⇒ never write). When the sub-level condition is True this
// controller performs no reads of targets and no status writes at all.
//
// # Status merge contract (unit-test pinned by T8)
//
// T8 owns ONLY Reason/Message of entries whose Reason is "Forbidden".
// This engine owns, for every other entry: desiredImage, observedImage,
// imageApplied, rolloutStatus, healthStatus, retryCount, nextRetryTime,
// observedGeneration, lastTransitionTime — plus status.observedGeneration,
// status.rolloutStatus (worst-state aggregate) and recentDeliveries.
// Forbidden entries are preserved untouched; stale entries of removed targets
// are pruned except Forbidden ones (T8 lifecycle). Ownership split with T11
// (rollout observer): T9 seeds rolloutStatus=Progressing when a value-changing
// apply lands, sets the terminal Conflict state, and initializes empty
// rolloutStatus/healthStatus to Unknown; T11 owns every observation-driven
// transition (Progressing→Succeeded/Failed, health) and must recompute the
// aggregate with the exported AggregateRolloutStatus on its own writes.
//
// # Events (ADR-003 §4: dual-namespace audit)
//
// Every value-changing delivery writes Normal ImageApplied Events in BOTH the
// subscription namespace and the target namespace. Failure surfaces
// (TargetNotFound / ContainerNotFound / ApplyForbidden / Unsupported /
// SourceUnresolved / InvalidImageReference / ...) emit Warning Events on the
// subscription, transition-deduplicated against the persisted per-target
// reason so steady-state reconciles never spam. Rollout-outcome Events and
// the mirror status Delivery float-up belong to T11.

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/authz"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
	"github.com/appfun-dev/imageflow/internal/delivery"
	"github.com/appfun-dev/imageflow/internal/imagefields"
)

// Production controller and recorder names. Integration tests build their
// instances under distinct names: controller-runtime validates controller
// names process-globally.
const (
	subscriptionDeliveryControllerName = "subscription-delivery"
	recorderNameSubscriptionDelivery   = "imageflow-subscription-delivery"
)

// Stable per-target reason vocabulary (status.targets[].reason). Frozen as
// the T9 machine-readable failure/state surface; T10 (generic resolver) and
// T11 (observer) build on it, they do not redefine it.
const (
	// TargetReasonApplied means every declared field holds the desired digest
	// reference and this subscription owns the fields (SSA registry).
	TargetReasonApplied = "Applied"
	// TargetReasonConflict is the ADR-008 terminal state: another LIVE
	// subscription owns a declared field (first-wins), zero writes performed.
	TargetReasonConflict = "Conflict"
	// TargetReasonUnsupported reports a Kind outside the built-in adapter set
	// (ADR-006) or without any registered adapter — honestly, never guessed.
	TargetReasonUnsupported = "Unsupported"
	// TargetReasonTargetNotFound means the workload object does not exist
	// (waiting state: delivery replays when the target appears — "目标后建").
	TargetReasonTargetNotFound = "TargetNotFound"
	// TargetReasonContainerNotFound means a declared container name is absent
	// from the target's pod template (locator discipline, spec decision 6).
	TargetReasonContainerNotFound = "ContainerNotFound"
	// TargetReasonApplyForbidden reports the API server denying THIS
	// controller's write (controller-SA RBAC problem; distinct from the T8
	// creator-authorization Forbidden surface).
	TargetReasonApplyForbidden = "ApplyForbidden"
	// TargetReasonApplyFailed reports a transient apply/read failure; the
	// pass requeues with backoff and retryCount/nextRetryTime are persisted.
	TargetReasonApplyFailed = "ApplyFailed"
	// TargetReasonSourceUnresolved means the catalog association for the
	// source stream:tag is not available yet (stream/tag/association missing).
	TargetReasonSourceUnresolved = "SourceUnresolved"
	// TargetReasonInvalidImageReference guards the ADR-005 §2 write contract:
	// the resolved reference is not a digest reference; zero writes.
	TargetReasonInvalidImageReference = "InvalidImageReference"
	// TargetReasonInvalidTarget reports a malformed target declaration
	// (missing or ambiguous locator); webhook-enforced, defensively re-checked.
	TargetReasonInvalidTarget = "InvalidTarget"
	// TargetReasonInvalidImageField reports an imageFields path that cannot be
	// resolved against the live object (grammar violation, missing field,
	// non-string leaf — ADR-005 §1; exercised once T10 registers the resolver).
	TargetReasonInvalidImageField = "InvalidImageField"
	// TargetReasonKindNotInstalled means the target GVK is not served by the
	// cluster (CRD absent); waiting state, mirrors the T8 gate vocabulary.
	TargetReasonKindNotInstalled = "KindNotInstalled"
)

// Event reasons of the delivery surface (Event.reason vocabulary). Every
// value-changing delivery is audited in BOTH namespaces (ADR-003 §4).
const (
	// EventReasonImageApplied announces one delivery: Normal, dual-namespace
	// (subscription object and target workload object).
	EventReasonImageApplied = "ImageApplied"
	// EventReasonDeliveryConflict announces the terminal first-wins Conflict:
	// Warning on the subscription (zero writes were performed).
	EventReasonDeliveryConflict = "DeliveryConflict"
	// EventReasonUnsupportedTarget announces an Unsupported per-target reason:
	// Warning on the subscription.
	EventReasonUnsupportedTarget = "UnsupportedTarget"
	// EventReasonDeliveryHistoryTruncated announces recentDeliveries overflow;
	// dropped entries live in kube Events and the audit log (ADR-008).
	EventReasonDeliveryHistoryTruncated = "DeliveryHistoryTruncated"
)

// Condition reasons of the T9-owned subscription conditions.
const (
	// ReasonFieldOwnershipConflict is used with Conflict=True.
	ReasonFieldOwnershipConflict = "FieldOwnershipConflict"
	// ReasonNoConflicts is used with Conflict=False.
	ReasonNoConflicts = "NoConflicts"
	// ReasonUnsupportedTargets is used with Unsupported=True.
	ReasonUnsupportedTargets = "UnsupportedTargets"
	// ReasonAllTargetsSupported is used with Unsupported=False.
	ReasonAllTargetsSupported = "AllTargetsSupported"
)

// Requeue intervals of the waiting states. Constants, not flags: they are
// safety nets under the event-driven triggers (watches deliver the real
// latency; these bounds only cover kinds/events no watch can see).
const (
	// authzPendingRequeue is used while the T8 gate has not established the
	// Forbidden surface yet (frozen contract: Unknown ⇒ zero writes + requeue).
	authzPendingRequeue = 5 * time.Second
	// waitingStateRequeue re-triggers converging waiting states (source
	// unresolved, target not found, container missing, kind not installed).
	waitingStateRequeue = 30 * time.Second
)

// retryBackoffBase and retryBackoffMax bound the persisted nextRetryTime
// estimate of transient failures (the real requeue rides the workqueue
// backoff of the returned error).
const (
	retryBackoffBase = 5 * time.Second
	retryBackoffMax  = 5 * time.Minute
)

func init() {
	Registrars = append(Registrars, subscriptionDeliveryRegistrar{})
}

type subscriptionDeliveryRegistrar struct{}

func (subscriptionDeliveryRegistrar) Register(mgr ctrl.Manager, opts Options) error {
	return RegisterSubscriptionDelivery(mgr, subscriptionDeliveryControllerName, opts)
}

// RBAC of the whole manager, regenerated into config/rbac/role.yaml by
// `make manifests`. The delivery engine holds the ADR-003 controller-SA
// write surface: cluster-level update/patch on the whitelisted built-in
// workload kinds ONLY (SSA writes use patch; update is included per the
// frozen ADR-003 wording), plus the subscription status subresource and
// Events in both namespaces (dual-namespace audit, ADR-003 §4). No wildcard
// verbs, no other workload kinds.
//
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=create;get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagesubscriptions,verbs=get;list;watch
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagesubscriptions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagestreams,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// RegisterSubscriptionDelivery wires the delivery engine onto mgr under the
// given controller name (parameterized because controller-runtime enforces
// process-global controller-name uniqueness; integration suites run their
// instance under a distinct name).
func RegisterSubscriptionDelivery(mgr ctrl.Manager, name string, opts Options) error {
	// Scheme-recognition guard (platform pattern): builder.Complete resolves
	// watched GVKs against the manager scheme at registration time, so a
	// manager without the imageflow kinds (shared manager_test.go environment)
	// must skip loudly instead of aborting the whole process. The watched
	// built-in workload kinds come from clientgoscheme, which every manager
	// in this repo registers.
	if missing := missingImageFlowKinds(mgr, kindImageSubscription, kindImageStream); len(missing) > 0 {
		logSkippedRegistration(name, missing)
		return nil
	}

	timeSource := opts.Clock
	if timeSource == nil {
		timeSource = clock.Real{}
	}
	r := NewSubscriptionDeliveryReconciler(
		mgr.GetClient(),
		mgr.GetAPIReader(),
		mgr.GetEventRecorderFor(recorderNameSubscriptionDelivery),
		timeSource,
		opts.Config,
		sharedDeliveryMetrics(),
	)
	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&imageflowv1alpha1.ImageSubscription{}).
		Watches(&imageflowv1alpha1.ImageStream{},
			handler.EnqueueRequestsFromMapFunc(r.subsForStream),
			builder.WithPredicates(streamChangePredicate{})).
		Watches(&imageflowv1alpha1.ImageSubscription{},
			handler.EnqueueRequestsFromMapFunc(r.peerSubsForSub),
			builder.WithPredicates(peerSubPredicate{})).
		Watches(&appsv1.Deployment{},
			handler.EnqueueRequestsFromMapFunc(r.subsForWorkload),
			builder.WithPredicates(workloadChangePredicate{})).
		Watches(&appsv1.StatefulSet{},
			handler.EnqueueRequestsFromMapFunc(r.subsForWorkload),
			builder.WithPredicates(workloadChangePredicate{})).
		Watches(&appsv1.DaemonSet{},
			handler.EnqueueRequestsFromMapFunc(r.subsForWorkload),
			builder.WithPredicates(workloadChangePredicate{})).
		Complete(r)
}

// SubscriptionDeliveryReconciler converges the declared image fields of one
// subscription's targets onto the catalog's current digest association.
type SubscriptionDeliveryReconciler struct {
	client    client.Client
	apiReader client.Reader // uncached: fresh reads guard every write decision
	recorder  record.EventRecorder
	clock     clock.Clock
	config    config.Config
	metrics   *deliveryMetrics
}

// NewSubscriptionDeliveryReconciler builds the reconciler. apiReader may be
// nil or the same client as cl (tests); in the manager it is
// mgr.GetAPIReader() so write decisions and terminal-effect dedup read
// directly from the API server (the mandatory cache-lag guard, 6c8920b).
// A nil metrics set is replaced with an unregistered one (tests).
func NewSubscriptionDeliveryReconciler(
	cl client.Client,
	apiReader client.Reader,
	recorder record.EventRecorder,
	clk clock.Clock,
	cfg config.Config,
	metrics *deliveryMetrics,
) *SubscriptionDeliveryReconciler {
	if apiReader == nil {
		apiReader = cl
	}
	if clk == nil {
		clk = clock.Real{}
	}
	if metrics == nil {
		metrics = newDeliveryMetrics()
	}
	return &SubscriptionDeliveryReconciler{
		client:    cl,
		apiReader: apiReader,
		recorder:  recorder,
		clock:     clk,
		config:    cfg,
		metrics:   metrics,
	}
}

// Reconcile runs one delivery convergence pass over a single subscription.
func (r *SubscriptionDeliveryReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var sub imageflowv1alpha1.ImageSubscription
	if err := r.client.Get(ctx, req.NamespacedName, &sub); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get subscription: %w", err)
	}
	if !sub.DeletionTimestamp.IsZero() {
		// Deleting: no finalizer, no writes. Orphaned SSA ownership entries
		// are reclaimed by any waiting subscription's forced replay
		// (ADR-008 独占回收), never by a destructive "release" apply.
		return ctrl.Result{}, nil
	}

	// T8 frozen contract: the object surface gates every write. Missing
	// condition = gate has not established its surface = Unknown = zero
	// writes + requeue. Forbidden=True = zero writes to every target; T8's
	// periodic re-verification flips the surface and re-triggers this
	// controller through the subscription watch.
	forbidden := meta.FindStatusCondition(sub.Status.Conditions, imageflowv1alpha1.ConditionTypeForbidden)
	if forbidden == nil {
		logger.V(1).Info("authorization surface not established yet: zero writes, requeue",
			"subscription", req.NamespacedName)
		return ctrl.Result{RequeueAfter: authzPendingRequeue}, nil
	}
	if forbidden.Status == metav1.ConditionTrue {
		logger.V(1).Info("subscription is Forbidden (T8 gate): zero writes",
			"subscription", req.NamespacedName, "reason", forbidden.Reason)
		return ctrl.Result{}, nil
	}

	source := r.resolveSource(ctx, &sub)
	if source.err != nil {
		return ctrl.Result{}, source.err
	}
	source.spec = sub.Spec.Source
	source.name = fmt.Sprintf("%s in namespace %s", sub.Spec.Source.Name, sub.Spec.Source.Namespace)

	fieldManager := delivery.FieldManagerFor(sub.Namespace, sub.Name)
	pass := r.deliverAllTargets(ctx, &sub, source, fieldManager)

	transition, err := r.syncDeliveryStatus(ctx, req.NamespacedName, source, pass)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("sync delivery status: %w", err)
	}
	r.emitPlannedEffects(&sub, transition)

	logger.V(1).Info("subscription delivery pass complete",
		"subscription", req.NamespacedName,
		"targets", len(pass.outcomes),
		"delivered", transition.deliveredCount,
		"statusWritten", transition.wrote)
	return ctrl.Result{RequeueAfter: pass.requeue}, pass.joinedError()
}

// effectiveDeliveryFormat resolves spec.imageReferenceFormat: an empty value
// means Digest (mutating-webhook defaulting, ADR-012 §3), so subscriptions
// predating v1.2 keep the v1 digest-only behavior. An unknown value degrades
// to Digest at the render/guard sites (the v1-safe direction).
func effectiveDeliveryFormat(format imageflowv1alpha1.ImageReferenceFormat) imageflowv1alpha1.ImageReferenceFormat {
	if format == "" {
		return imageflowv1alpha1.ImageReferenceFormatDigest
	}
	return format
}

// formatReferencePatterns maps each imageReferenceFormat to the reference
// grammar the engine writes (ADR-012 §6, spec decision 3) — the same shapes
// the T3 webhook enforces on persisted status.targets[].desiredImage.
var formatReferencePatterns = map[imageflowv1alpha1.ImageReferenceFormat]*regexp.Regexp{
	imageflowv1alpha1.ImageReferenceFormatDigest:        digestRefPatternRegexp,
	imageflowv1alpha1.ImageReferenceFormatTag:           regexp.MustCompile(`^[^@\s]+:[^@\s]+$`),
	imageflowv1alpha1.ImageReferenceFormatDigestWithTag: regexp.MustCompile(`^[^@\s]+:[^@\s]+@sha256:[0-9a-f]{64}$`),
}

// referenceMatchesFormat is the format-aware last-mile write guard: the value
// the engine is about to apply must match the grammar of the selected format.
func referenceMatchesFormat(format imageflowv1alpha1.ImageReferenceFormat, ref string) bool {
	pattern, ok := formatReferencePatterns[effectiveDeliveryFormat(format)]
	return ok && pattern.MatchString(ref)
}

// desiredImageForFormat renders (spec decisions 1/2): (1) the reference the
// engine writes to and compares against target fields, and (2) the
// status.targets[].resolvedDigest observability value ("" for Digest, where
// DesiredImage already carries the digest). digestRef must already be a
// digest reference (the resolve-time catalog contract); a malformed input
// defensively degrades to the v1 digest behavior. The tag comes from the
// logical source name "<stream>:<tag>" and equals ImageStreamTag.spec.tag by
// construction (the mirror is synthesized from the same pair), so no extra
// object read is needed (ADR-012 §5).
func desiredImageForFormat(
	format imageflowv1alpha1.ImageReferenceFormat,
	digestRef string,
	tag string,
) (desired, resolvedDigest string) {
	switch effectiveDeliveryFormat(format) {
	case imageflowv1alpha1.ImageReferenceFormatTag:
		repo, _, err := SplitDigestReference(digestRef)
		if err != nil {
			return digestRef, ""
		}
		return repo + ":" + tag, digestRef
	case imageflowv1alpha1.ImageReferenceFormatDigestWithTag:
		repo, digest, err := SplitDigestReference(digestRef)
		if err != nil {
			return digestRef, ""
		}
		return repo + ":" + tag + "@" + digest, digestRef
	default:
		return digestRef, ""
	}
}

// sourceState is the resolved catalog association of the subscription source.
type sourceState struct {
	// spec is the subscription source as declared (recentDeliveries §12.9
	// shape echoes it).
	spec imageflowv1alpha1.SubscriptionSourceRef
	// name renders the source for Event messages.
	name string
	// ref is the digest reference of the catalog association (only when
	// reason is empty); it stays the §12.9 recentDeliveries NewImage truth
	// regardless of the selected format.
	ref string
	// digest of the association.
	digest string
	// format is the effective spec.imageReferenceFormat ("" resolved to
	// Digest so pre-v1.2 objects behave exactly as v1).
	format imageflowv1alpha1.ImageReferenceFormat
	// desiredImage is the formatted reference the engine writes to and
	// compares against target fields (spec decisions 1/2/7): the digest
	// reference itself for Digest, <repo>:<tag> for Tag, the OCI composite
	// <repo>:<tag>@sha256:<hex> for DigestWithTag.
	desiredImage string
	// resolvedDigest reports the digest truth on
	// status.targets[].resolvedDigest for Tag/DigestWithTag (observability
	// and drift detection); empty for Digest (DesiredImage already carries
	// the digest there).
	resolvedDigest string
	// created is when the association was recorded (Image Change time, the
	// start of the end-to-end delivery latency).
	created *metav1.Time
	// reason/message describe why the source could not be resolved (one of
	// the per-target reason values); empty when resolved.
	reason  string
	message string
	// waiting requests the waiting-state requeue.
	waiting bool
	// err is a transient catalog read failure (aborts the pass with backoff).
	err error
}

// resolveSource maps spec.source (kind ImageStreamTag, logical
// "<stream>:<tag>" name) onto the current ImageStream.status association.
// The stream — not the mirror — is read: ImageStream.status is the single
// source of truth (ADR-002), mirrors are synthesized views.
func (r *SubscriptionDeliveryReconciler) resolveSource(
	ctx context.Context,
	sub *imageflowv1alpha1.ImageSubscription,
) sourceState {
	src := sub.Spec.Source
	format := effectiveDeliveryFormat(sub.Spec.ImageReferenceFormat)
	if src.Kind != mirrorSourceKindImageStreamTag {
		return sourceState{
			reason: TargetReasonSourceUnresolved,
			message: fmt.Sprintf("source kind %q is not supported (v1 enum: ImageStreamTag); "+
				"zero writes until spec.source is fixed", src.Kind),
		}
	}
	stream, tag, ok := splitSubscriptionSourceName(src.Name)
	if !ok {
		return sourceState{
			reason: TargetReasonSourceUnresolved,
			message: fmt.Sprintf("source name %q is not in the logical <stream>:<tag> form; "+
				"zero writes until spec.source is fixed", src.Name),
		}
	}
	var ist imageflowv1alpha1.ImageStream
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: src.Namespace, Name: stream}, &ist); err != nil {
		if apierrors.IsNotFound(err) {
			return sourceState{
				reason: TargetReasonSourceUnresolved,
				message: fmt.Sprintf("ImageStream %q not found in namespace %q (yet); "+
					"delivery replays when the stream and its tag association appear", stream, src.Namespace),
				waiting: true,
			}
		}
		return sourceState{err: fmt.Errorf("get source ImageStream %s/%s: %w", src.Namespace, stream, err)}
	}
	for i := range ist.Status.Tags {
		if ist.Status.Tags[i].Tag != tag {
			continue
		}
		items := ist.Status.Tags[i].Items
		if len(items) == 0 {
			break
		}
		current := items[0]
		if current.ImageReference == "" {
			return sourceState{
				reason: TargetReasonSourceUnresolved,
				message: fmt.Sprintf("tag %q of ImageStream %s/%s carries no image reference; "+
					"waiting for a complete registration", tag, src.Namespace, stream),
				waiting: true,
			}
		}
		if !digestRefPatternRegexp.MatchString(current.ImageReference) {
			// The catalog association is a digest reference under EVERY
			// format (formats shape what is written, not what the catalog
			// stores; the digest is also needed for resolvedDigest and the
			// DigestWithTag composite). The catalog patterns make this
			// unreachable; the engine refuses instead of trusting a tag
			// reference (ADR-005 §2, ADR-012 §5).
			return sourceState{
				reason: TargetReasonInvalidImageReference,
				message: fmt.Sprintf("catalog association %q of %s:%s is not a digest reference "+
					"<repo>@sha256:<64hex>; tag references are never trusted (ADR-005, ADR-012)",
					current.ImageReference, stream, tag),
			}
		}
		desired, resolvedDigest := desiredImageForFormat(format, current.ImageReference, tag)
		return sourceState{
			ref:            current.ImageReference,
			digest:         current.Digest,
			format:         format,
			desiredImage:   desired,
			resolvedDigest: resolvedDigest,
			created:        current.Created.DeepCopy(),
		}
	}
	return sourceState{
		reason: TargetReasonSourceUnresolved,
		message: fmt.Sprintf("no image is registered for tag %q of ImageStream %s/%s yet "+
			"(register via ImageStreamMapping or ImageStreamImport); delivery replays on the "+
			"Image Change", tag, src.Namespace, stream),
		waiting: true,
	}
}

// deliveryPass is the collected per-target work of one reconcile.
type deliveryPass struct {
	outcomes []deliveryOutcome
	requeue  time.Duration
	errs     []error
}

func (p *deliveryPass) joinedError() error {
	return errors.Join(p.errs...)
}

// deliverAllTargets runs the per-target engine over the deduplicated spec
// targets. One target's failure never blocks the others; transient failures
// are collected and joined into the reconcile error (workqueue backoff).
func (r *SubscriptionDeliveryReconciler) deliverAllTargets(
	ctx context.Context,
	sub *imageflowv1alpha1.ImageSubscription,
	source sourceState,
	fieldManager string,
) deliveryPass {
	pass := deliveryPass{outcomes: make([]deliveryOutcome, 0, len(sub.Spec.Targets))}
	seen := make(map[imageflowv1alpha1.TargetReference]bool, len(sub.Spec.Targets))
	for i := range sub.Spec.Targets {
		target := &sub.Spec.Targets[i]
		if seen[target.TargetRef] {
			// Duplicate target references (webhook lane; defensive): one
			// outcome per distinct target keeps entries and records unique.
			log.FromContext(ctx).V(1).Info("skipping duplicate target reference in spec.targets",
				"subscription", sub.Namespace+"/"+sub.Name,
				"target", authz.FormatTarget(target.TargetRef))
			continue
		}
		seen[target.TargetRef] = true

		outcome := r.deliverTarget(ctx, sub, source, target, fieldManager)
		pass.outcomes = append(pass.outcomes, outcome)
		if outcome.err != nil {
			pass.errs = append(pass.errs, outcome.err)
		}
		if outcome.requeue > 0 && (pass.requeue == 0 || outcome.requeue < pass.requeue) {
			pass.requeue = outcome.requeue
		}
	}
	return pass
}

// deliverTarget converges ONE target and returns the computed outcome. The
// only side effect performed here is the SSA apply itself; status writes,
// Events and metrics are planned and executed by syncDeliveryStatus /
// emitPlannedEffects against fresh reads (dedup survives cache lag and
// restarts because every decision is re-derived from persisted state).
func (r *SubscriptionDeliveryReconciler) deliverTarget(
	ctx context.Context,
	sub *imageflowv1alpha1.ImageSubscription,
	source sourceState,
	target *imageflowv1alpha1.SubscriptionTarget,
	fieldManager string,
) deliveryOutcome {
	ref := target.TargetRef
	out := deliveryOutcome{ref: ref}

	// T8 entry-level gate (frozen contract): a persisted Reason=Forbidden
	// entry means zero writes for this target; the entry belongs to T8 and is
	// preserved untouched.
	if idx := findTargetEntry(sub.Status.Targets, ref); idx >= 0 &&
		sub.Status.Targets[idx].Reason == TargetReasonForbidden {
		out.preserveEntry = true
		return out
	}

	hasContainers := len(target.ContainerNames) > 0
	hasPaths := len(target.ImageFields) > 0
	switch {
	case hasContainers && hasPaths:
		return out.failed(TargetReasonInvalidTarget, fmt.Sprintf(
			"target %s declares both containerNames and imageFields: the locators are mutually "+
				"exclusive (spec decision 6); zero writes", authz.FormatTarget(ref)))
	case !hasContainers && !hasPaths:
		return out.failed(TargetReasonInvalidTarget, fmt.Sprintf(
			"target %s declares neither containerNames nor imageFields: no locator, no guessing; "+
				"zero writes", authz.FormatTarget(ref)))
	}

	if source.reason != "" {
		// Source not resolved (or violating the write contract): honest
		// per-target report, zero writes.
		out = out.failed(source.reason, source.message)
		if source.waiting {
			out.requeue = waitingStateRequeue
		}
		return out
	}

	// The source resolved: the desired (formatted) reference is reportable on
	// every outcome from here on, even failures (users see WHAT would be
	// delivered).
	out.setDesired = true
	out.desiredImage = source.desiredImage

	gvk := schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind)
	kindAdapter, pathAdapter, class, supported := delivery.Classify(gvk, hasPaths)
	if !supported {
		out = out.failed(TargetReasonUnsupported, delivery.UnsupportedMessage(gvk, class))
		return out
	}

	plan, obj, failure := r.planDelivery(ctx, out, target, source, gvk,
		kindAdapter, pathAdapter, hasPaths)
	if failure != nil {
		return *failure
	}
	out.targetObject = obj

	observed, uniform := observedSummary(plan.Fields)
	// ImageApplied comparison (spec decision 7): each format compares the
	// reference IT writes — the digest for Digest, the tag string only for
	// Tag (drift stays visible via resolvedDigest), the composite for
	// DigestWithTag.
	allEqual := uniform && observed == source.desiredImage
	out.setObserved = true
	out.observedImage = observed

	// Field-level exclusivity inspection on the fresh managedFields
	// (ADR-008: managedFields 即独占登记表).
	ownership := delivery.InspectOwnership(obj.GetManagedFields(), plan.Fields, fieldManager)
	conflicted := conflictingPaths(ownership)
	if len(conflicted) > 0 {
		if claimant, live := r.findLiveClaimant(ctx, sub, ref, conflicted); live {
			// First-wins: a LIVE subscription owns and still claims the
			// field. Terminal Conflict, ZERO writes (the later subscriber).
			out = out.failed(TargetReasonConflict, fmt.Sprintf(
				"image field(s) %s of target %s are owned by subscription %s (field manager %s): "+
					"first-wins exclusivity (ADR-008), zero writes performed; reclaim by deleting or "+
					"editing the occupying subscription",
				strings.Join(conflicted, ", "), authz.FormatTarget(ref),
				claimant, strings.Join(occupyingManagers(ownership), ", ")))
			out.conflictNow = true
			out.setImageApplied = true
			out.imageApplied = allEqual
			out.setDesired = true
			out.desiredImage = source.desiredImage
			out.setObserved = true
			out.observedImage = observed
			rollout := imageflowv1alpha1.RolloutConflict
			out.rolloutSet = &rollout
			return out
		}
		// Orphaned registry entry (occupant deleted or no longer claiming):
		// replay the Apply with force so ownership becomes deterministic
		// (ADR-008 独占回收; also covers "target created after" flows).
	}

	ownedBySelf := allOwnership(ownership, delivery.OwnershipSelf)
	if allEqual && ownedBySelf {
		// Equal-and-owned: NO write of any kind (AC: 相等不产生 patch).
		out.converged = true
		out.locator = locatorSummary(target)
		return r.convergedOutcome(out, source)
	}

	force := len(conflicted) > 0 // orphaned ImageFlow ownership detected above
	applyErr := r.applyOnce(ctx, plan, fieldManager, force)
	if applyErr != nil && apierrors.IsConflict(applyErr) && !force {
		// Server-side race: someone claimed ownership between the fresh read
		// and the apply. Re-classify on a NEW fresh read: a live subscription
		// claimant means terminal Conflict (the 409 wrote nothing); anything
		// else is a user/GitOps manager this subscription takes over from.
		r.metrics.applies.WithLabelValues(applyResultConflict).Inc()
		fresh := &unstructured.Unstructured{}
		fresh.SetGroupVersionKind(gvk)
		getErr := r.apiReader.Get(ctx,
			types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, fresh)
		if getErr != nil {
			// Fail closed: without a fresh ownership picture a forced apply
			// could stomp a live subscription's claim. Retry with backoff.
			out = out.transient(TargetReasonApplyFailed, fmt.Sprintf(
				"re-read target %s after an apply conflict: %v", authz.FormatTarget(ref), getErr))
			return out
		}
		reowned := delivery.InspectOwnership(fresh.GetManagedFields(), plan.Fields, fieldManager)
		reconflicted := conflictingPaths(reowned)
		if _, live := r.findLiveClaimant(ctx, sub, ref, reconflicted); live {
			out = out.failed(TargetReasonConflict, fmt.Sprintf(
				"image field(s) %s of target %s were claimed by another subscription while this "+
					"apply was in flight (field manager %s): first-wins exclusivity (ADR-008), "+
					"zero writes performed", strings.Join(reconflicted, ", "),
				authz.FormatTarget(ref), strings.Join(occupyingManagers(reowned), ", ")))
			out.conflictNow = true
			out.setImageApplied = true
			out.imageApplied = allEqual
			out.setDesired = true
			out.desiredImage = source.desiredImage
			out.setObserved = true
			out.observedImage = observed
			rollout := imageflowv1alpha1.RolloutConflict
			out.rolloutSet = &rollout
			return out
		}
		out.targetObject = fresh
		applyErr = r.applyOnce(ctx, plan, fieldManager, true)
	}
	if applyErr != nil {
		r.metrics.applies.WithLabelValues(applyResultError).Inc()
		switch {
		case apierrors.IsForbidden(applyErr):
			out = out.transient(TargetReasonApplyForbidden, fmt.Sprintf(
				"the API server denied this controller's write to %s: %v; fix the manager RBAC "+
					"(cluster-level update/patch on the whitelisted kinds, ADR-003)",
				authz.FormatTarget(ref), applyErr))
			return out
		case apierrors.IsNotFound(applyErr):
			out = out.failed(TargetReasonTargetNotFound, fmt.Sprintf(
				"target %s disappeared mid-delivery; delivery replays when it is recreated",
				authz.FormatTarget(ref)))
			out.requeue = waitingStateRequeue
			return out
		case apierrors.IsInvalid(applyErr) && ref.Kind == "Job" && strings.Contains(applyErr.Error(), "field is immutable"):
			// Job.spec.template is immutable after creation. Delete the Job
			// then re-apply via SSA to create it with the new image. We must
			// wait for the delete to fully propagate before re-applying,
			// otherwise SSA sees the existing Job and rejects the template
			// change.
			delObj := &unstructured.Unstructured{}
			delObj.SetGroupVersionKind(schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind))
			delObj.SetName(ref.Name)
			delObj.SetNamespace(ref.Namespace)
			// Use Background propagation with grace=0 to force-delete the
			// Job immediately. Default graceful deletion leaves the Job in
			// Terminating state while pods finish, blocking SSA recreate.
			graceZero := int64(0)
			propagation := metav1.DeletePropagationBackground
			delErr := r.client.Delete(ctx, delObj, client.GracePeriodSeconds(graceZero),
				client.PropagationPolicy(propagation))
			if delErr != nil && !apierrors.IsNotFound(delErr) {
				out = out.transient(TargetReasonApplyFailed, fmt.Sprintf(
					"failed to delete immutable Job %s for image update: %v",
					authz.FormatTarget(ref), delErr))
				return out
			}
			// Verify deletion completed before re-applying. The API server
			// delete is synchronous but finalizers or GC may delay removal.
			verifyObj := &unstructured.Unstructured{}
			verifyObj.SetGroupVersionKind(schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind))
			verifyErr := r.apiReader.Get(ctx, types.NamespacedName{
				Namespace: ref.Namespace, Name: ref.Name}, verifyObj)
			if verifyErr == nil {
				// Job still exists — cannot recreate in this pass. Return
				// transient so the next reconcile retries after deletion
				// completes.
				out = out.transient(TargetReasonApplyFailed, fmt.Sprintf(
					"deleted immutable Job %s but it still exists; retrying on next reconcile",
					authz.FormatTarget(ref)))
				out.requeue = waitingStateRequeue
				return out
			}
			if !apierrors.IsNotFound(verifyErr) {
				out = out.transient(TargetReasonApplyFailed, fmt.Sprintf(
					"failed to verify deletion of Job %s: %v",
					authz.FormatTarget(ref), verifyErr))
				return out
			}
			// Job is gone. Build a recreate payload from the original
			// target object (captured by planDelivery before the failed
			// apply). We need the full spec (command, args, env, etc.)
			// because SSA CREATE has no server state to merge with.
			// Only strip server-generated metadata.labels (controller-uid,
			// job-name) which are tied to the old UID.
			if out.targetObject == nil {
				out = out.transient(TargetReasonApplyFailed, fmt.Sprintf(
					"deleted immutable Job %s but have no original spec to recreate from",
					authz.FormatTarget(ref)))
				return out
			}
			recreateObj := out.targetObject.DeepCopy()
			// Clean metadata for fresh create.
			recreateObj.SetResourceVersion("")
			recreateObj.SetUID("")
			recreateObj.SetCreationTimestamp(metav1.Time{})
			recreateObj.SetManagedFields(nil)
			recreateObj.SetLabels(nil) // strip server-generated labels
			// Keep only our managed-by annotation.
			recreateObj.SetAnnotations(map[string]string{
				delivery.ManagedByAnnotation: delivery.ManagedByAnnotationValue,
			})
			// Update container images to the desired value from the
			// original SSA plan (which has the correct new image).
			planContainers, _, _ := unstructured.NestedSlice(plan.Apply.Object,
				"spec", "template", "spec", "containers")
			if len(planContainers) > 0 {
				targetContainers, found, _ := unstructured.NestedSlice(recreateObj.Object,
					"spec", "template", "spec", "containers")
				if found {
					// Build a map of container name → desired image from the plan.
					imageByName := make(map[string]string)
					for _, pc := range planContainers {
						pcMap, ok := pc.(map[string]interface{})
						if !ok {
							continue
						}
						name, _ := pcMap["name"].(string)
						img, _ := pcMap["image"].(string)
						if name != "" && img != "" {
							imageByName[name] = img
						}
					}
					for i, tc := range targetContainers {
						tcMap, ok := tc.(map[string]interface{})
						if !ok {
							continue
						}
						name, _ := tcMap["name"].(string)
						if newImg, ok := imageByName[name]; ok {
							tcMap["image"] = newImg
							targetContainers[i] = tcMap
						}
					}
					_ = unstructured.SetNestedSlice(recreateObj.Object, targetContainers,
						"spec", "template", "spec", "containers")
				}
			}
			freshPlan := &delivery.Plan{
				Apply:  recreateObj,
				Fields: plan.Fields,
			}
			recreateErr := r.applyOnce(ctx, freshPlan, fieldManager, false)
			if recreateErr != nil {
				out = out.transient(TargetReasonApplyFailed, fmt.Sprintf(
					"deleted immutable Job %s but failed to recreate: %v",
					authz.FormatTarget(ref), recreateErr))
				return out
			}
			// Recreated successfully — fall through to success path below.
			r.metrics.applies.WithLabelValues(applyResultSuccess).Inc()
		default:
			out = out.transient(TargetReasonApplyFailed, fmt.Sprintf(
				"server-side apply to %s failed: %v", authz.FormatTarget(ref), applyErr))
			return out
		}
	}
	r.metrics.applies.WithLabelValues(applyResultSuccess).Inc()

	// The apply landed. valueChanged is decided from the pre-apply fresh
	// read: a value-changing apply is THE delivery; an equal-value apply only
	// claimed/re-asserted ownership (takeover of an orphan or user field).
	out.converged = true
	out.delivered = !allEqual
	out.claimedOwnership = allEqual
	out.previousValue = observed
	out.locator = locatorSummary(target)
	return r.convergedOutcome(out, source)
}

// planDelivery performs the pre-write half of the write contract for one
// target (ADR-005 §1): a fresh uncached read of the live object, adapter
// field planning, and honest validation of every declared leaf before any
// write is attempted. It returns the plan and the live object, or — when the
// target cannot be read, planned or validated — a failure outcome and nil
// plan (zero writes).
func (r *SubscriptionDeliveryReconciler) planDelivery(
	ctx context.Context,
	out deliveryOutcome,
	target *imageflowv1alpha1.SubscriptionTarget,
	source sourceState,
	gvk schema.GroupVersionKind,
	kindAdapter delivery.KindAdapter,
	pathAdapter delivery.PathAdapter,
	hasPaths bool,
) (*delivery.Plan, *unstructured.Unstructured, *deliveryOutcome) {
	ref := target.TargetRef
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	if err := r.apiReader.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, obj); err != nil {
		switch {
		case apierrors.IsNotFound(err):
			out = out.failed(TargetReasonTargetNotFound, fmt.Sprintf(
				"target %s does not exist (yet); delivery replays when it is created "+
					"(独占回收/目标后建, ADR-008)", authz.FormatTarget(ref)))
			out.requeue = waitingStateRequeue
			return nil, nil, &out
		case meta.IsNoMatchError(err):
			out = out.failed(TargetReasonKindNotInstalled, fmt.Sprintf(
				"kind %s is not served by this cluster (CRD absent?); delivery is impossible "+
					"until the kind exists", authz.FormatTarget(ref)))
			out.requeue = waitingStateRequeue
			return nil, nil, &out
		default:
			out = out.transient(TargetReasonApplyFailed, fmt.Sprintf(
				"read target %s: %v", authz.FormatTarget(ref), err))
			return nil, nil, &out
		}
	}
	out.targetObject = obj

	var plan *delivery.Plan
	var planErr error
	if hasPaths {
		plan, planErr = pathAdapter.Plan(obj, target.ImageFields, source.desiredImage)
	} else {
		plan, planErr = kindAdapter.Plan(obj, target.ContainerNames, source.desiredImage)
	}
	if planErr != nil {
		var missing *delivery.ContainersNotFoundError
		var pathViolation *delivery.PathViolationError
		switch {
		case errors.As(planErr, &missing):
			out = out.failed(TargetReasonContainerNotFound, fmt.Sprintf(
				"%v; the locator is exact (no default guessing, spec decision 6)", missing))
			out.requeue = waitingStateRequeue
			return nil, obj, &out
		case errors.As(planErr, &pathViolation):
			out = out.failed(TargetReasonInvalidImageField, pathViolation.Error())
			return nil, obj, &out
		default:
			out = out.failed(TargetReasonInvalidTarget, fmt.Sprintf(
				"cannot plan the image fields of target %s: %v", authz.FormatTarget(ref), planErr))
			return nil, obj, &out
		}
	}
	for _, f := range plan.Fields {
		if !f.Found {
			// The write contract verifies the live object before writing and
			// never guesses (spec decision 3): an absent or non-string leaf
			// is an honest failure, not a default.
			out = out.failed(TargetReasonInvalidImageField, fmt.Sprintf(
				"image field %s of target %s is absent or not a string in the live object; "+
					"zero writes (ADR-005 §1)", f.Path, authz.FormatTarget(ref)))
			return nil, obj, &out
		}
	}

	// ADR-005 §2 + ADR-012 §6 last-mile guard: the engine only ever writes a
	// value matching the grammar of the selected imageReferenceFormat,
	// whatever upstream produced.
	if !referenceMatchesFormat(source.format, source.desiredImage) {
		out = out.failed(TargetReasonInvalidImageReference, fmt.Sprintf(
			"resolved reference %q does not match the imageReferenceFormat=%s grammar; "+
				"zero writes (ADR-005 §2, ADR-012 §6)", source.desiredImage, source.format))
		return nil, obj, &out
	}
	return plan, obj, nil
}

// convergedOutcome fills the per-target entry fields of a converged target
// (desired formatted reference present in every declared field, owned by this
// subscription).
func (r *SubscriptionDeliveryReconciler) convergedOutcome(
	out deliveryOutcome,
	source sourceState,
) deliveryOutcome {
	out.setDesired = true
	out.desiredImage = source.desiredImage
	out.setObserved = true
	out.observedImage = source.desiredImage
	out.setImageApplied = true
	out.imageApplied = true
	out.reason = TargetReasonApplied
	if source.resolvedDigest != "" {
		// Tag/DigestWithTag: the fields hold the formatted reference; the
		// message keeps the digest truth visible alongside it.
		out.message = fmt.Sprintf("all declared image fields (%s) hold %s (digest %s) and are "+
			"owned by this subscription's SSA field manager (ADR-008 exclusivity registry)",
			out.locator, source.desiredImage, source.digest)
	} else {
		out.message = fmt.Sprintf("all declared image fields (%s) hold digest %s and are owned by "+
			"this subscription's SSA field manager (ADR-008 exclusivity registry)",
			out.locator, source.digest)
	}
	out.resetRetry = true
	if out.delivered {
		// A value-changing apply starts a native rollout: seed Progressing.
		// T11 owns every observation-driven transition from here.
		progressing := imageflowv1alpha1.RolloutProgressing
		out.rolloutSet = &progressing
	}
	return out
}

// deliveryOutcome is the computed per-target result of one reconcile pass.
// Field writes to the persisted entry are expressed as explicit set/preserve
// operations so the status merge can re-apply them onto a FRESH read inside
// the retry-on-conflict closure without clobbering fields other writers
// (T8's Forbidden Reason/Message, T11's observations) changed meanwhile.
type deliveryOutcome struct {
	ref imageflowv1alpha1.TargetReference

	// preserveEntry marks a T8-owned Forbidden entry: zero writes to the
	// target AND zero writes to the entry.
	preserveEntry bool

	setDesired      bool
	desiredImage    string
	setObserved     bool
	observedImage   string
	setImageApplied bool
	imageApplied    bool

	// rolloutSet forces the per-target rollout state when non-nil (delivery
	// consequences only: Progressing on a value-changing apply, terminal
	// Conflict); nil preserves the persisted state (initialized to Unknown
	// when empty).
	rolloutSet *imageflowv1alpha1.RolloutStatus

	reason  string
	message string

	// locator is the human-readable locator summary (containers/paths) for
	// messages.
	locator string

	// resetRetry zeroes retryCount/nextRetryTime (converged targets);
	// retryDelta increments them (transient failures).
	resetRetry bool
	retryDelta int32

	// Facts for the record/event/metric planning inside the status merge.
	delivered        bool // a value-changing apply landed this pass
	claimedOwnership bool // an equal-value apply claimed ownership this pass
	previousValue    string
	converged        bool // target ended the pass equal-and-owned
	conflictNow      bool // terminal Conflict detected this pass

	// targetObject is the fresh target read, used as the target-namespace
	// Event involved object (nil when the object was never read).
	targetObject *unstructured.Unstructured

	// err bubbles a transient failure into the reconcile error (backoff).
	err error
	// requeue requests the waiting-state requeue interval.
	requeue time.Duration
}

// failed returns the outcome of a deterministic (non-transient) failure: an
// explicit per-target reason/message, zero writes, no retry bookkeeping.
func (o deliveryOutcome) failed(reason, message string) deliveryOutcome {
	o.reason = reason
	o.message = message
	return o
}

// transient returns the outcome of a retryable failure: reason/message plus
// retry bookkeeping and the reconcile error that drives the workqueue
// backoff.
func (o deliveryOutcome) transient(reason, message string) deliveryOutcome {
	o = o.failed(reason, message)
	o.retryDelta = 1
	o.err = fmt.Errorf("%s: %s", reason, message)
	return o
}

// locatorSummary renders the target locator for user-facing messages.
func locatorSummary(target *imageflowv1alpha1.SubscriptionTarget) string {
	if len(target.ContainerNames) > 0 {
		return "containers [" + strings.Join(target.ContainerNames, ", ") + "]"
	}
	return "fields [" + strings.Join(target.ImageFields, ", ") + "]"
}

// applyOnce performs the SSA apply with the subscription's stable field
// manager. The apply configuration declares ONLY the image fields (plus the
// static audit annotation), so SSA never touches unrelated fields (ADR-005).
func (r *SubscriptionDeliveryReconciler) applyOnce(
	ctx context.Context,
	plan *delivery.Plan,
	fieldManager string,
	force bool,
) error {
	patchOpts := []client.PatchOption{client.FieldOwner(fieldManager)}
	if force {
		patchOpts = append(patchOpts, client.ForceOwnership)
	}
	applied := plan.Apply.DeepCopy()
	return r.client.Patch(ctx, applied, client.Apply, patchOpts...)
}

// observedSummary reduces the declared fields' observed values to one
// reported value: the uniform value when every field agrees, otherwise the
// first declared field's value (documented rule; imageApplied is only ever
// true when the values are uniform AND equal the digest).
func observedSummary(fields []delivery.FieldPlan) (value string, uniform bool) {
	uniform = true
	for i, f := range fields {
		if i == 0 {
			value = f.Observed
			continue
		}
		if f.Observed != value {
			uniform = false
		}
	}
	return value, uniform
}

// conflictingPaths lists the declared paths owned by ANOTHER ImageFlow
// subscription field manager.
func conflictingPaths(ownership []delivery.FieldOwnership) []string {
	var out []string
	for _, o := range ownership {
		if o.Ownership == delivery.OwnershipOtherSubscription {
			out = append(out, o.Path)
		}
	}
	return out
}

// occupyingManagers lists the distinct other-subscription field managers
// found in an ownership inspection (diagnosable Conflict messages).
func occupyingManagers(ownership []delivery.FieldOwnership) []string {
	seen := map[string]bool{}
	var out []string
	for _, o := range ownership {
		for _, m := range o.SubscriptionManagers {
			if !seen[m] {
				seen[m] = true
				out = append(out, m)
			}
		}
	}
	return out
}

// allOwnership reports whether every inspected path has exactly the wanted
// classification.
func allOwnership(ownership []delivery.FieldOwnership, want delivery.Ownership) bool {
	for _, o := range ownership {
		if o.Ownership != want {
			return false
		}
	}
	return len(ownership) > 0
}

// findLiveClaimant looks for ANOTHER live subscription whose spec still
// claims one of the contested fields of target ref. Live-claimant analysis —
// not the raw managedFields entry — decides between a first-wins Conflict
// (occupant alive and claiming) and an orphaned-registry takeover (occupant
// deleted or target dropped from its spec): the spec of live subscriptions
// is the truth, managedFields can lag it (ADR-008 独占回收 = delete/edit the
// occupying subscription, the waiter's reconcile replays the Apply).
func (r *SubscriptionDeliveryReconciler) findLiveClaimant(
	ctx context.Context,
	self *imageflowv1alpha1.ImageSubscription,
	ref imageflowv1alpha1.TargetReference,
	contested []string,
) (claimant string, live bool) {
	if len(contested) == 0 {
		return "", false
	}
	var subs imageflowv1alpha1.ImageSubscriptionList
	if err := r.client.List(ctx, &subs); err != nil {
		// Fail closed: without the live-subscription picture a takeover
		// could stomp a live owner. The pass reports the conflict as live
		// (zero writes) and requeues through the normal triggers.
		log.FromContext(ctx).Error(err, "cannot list subscriptions for the live-claimant check; "+
			"treating contested ownership as live (zero writes)")
		return "unknown (subscription list unavailable)", true
	}
	for i := range subs.Items {
		other := &subs.Items[i]
		if other.Namespace == self.Namespace && other.Name == self.Name {
			continue
		}
		if !other.DeletionTimestamp.IsZero() {
			continue // deleting: not a live claimant
		}
		for j := range other.Spec.Targets {
			t := &other.Spec.Targets[j]
			if t.TargetRef != ref {
				continue
			}
			if claimsOverlap(t, contested) {
				return other.Namespace + "/" + other.Name, true
			}
		}
	}
	return "", false
}

// claimsOverlap reports whether one candidate target declaration claims any
// of the contested canonical field paths. containerNames locators are
// rendered into the same canonical pod-template path grammar imageFields
// uses, so mixed-locator overlaps are detected too.
func claimsOverlap(target *imageflowv1alpha1.SubscriptionTarget, contested []string) bool {
	claims := make([]string, 0, len(target.ContainerNames)+len(target.ImageFields))
	for _, name := range target.ContainerNames {
		claims = append(claims, fmt.Sprintf(".spec.template.spec.containers[name=%s].image", name))
	}
	for _, raw := range target.ImageFields {
		if parsed, err := imagefields.Parse(raw); err == nil {
			claims = append(claims, parsed.String())
		} else {
			claims = append(claims, raw)
		}
	}
	for _, claim := range claims {
		for _, path := range contested {
			if claim == path {
				return true
			}
		}
	}
	return false
}
