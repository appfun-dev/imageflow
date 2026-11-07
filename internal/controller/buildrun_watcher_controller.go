package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
)

// BuildRun Watcher (T1 infrastructure + T3 reconcile core, ADR-004 Amended +
// ADR-011): feature-gated optional controller that watches Shipwright BuildRun
// objects and auto-registers build outputs into the ImageFlow catalog.
//
// Shipwright types are deliberately NOT vendored: the watcher consumes
// BuildRuns as unstructured objects (zero compile-time hard dependency,
// ADR-001), reading the two status fields the ecosystem relies on —
// .status.output.digest and .status.buildSpec.output.image (docs/quickstart.md).

const (
	// shipwrightBuildCRDGroup is the API group for Shipwright Build resources.
	shipwrightBuildCRDGroup = "shipwright.io"
	// shipwrightBuildCRDVersion is the API version checked at startup.
	shipwrightBuildCRDVersion = "v1beta1"
	// shipwrightBuildCRDKind is the kind whose presence gates watcher startup.
	shipwrightBuildCRDKind = "Build"

	// buildRunTargetAnnotation marks a BuildRun for auto-registration; the
	// value is JSON {"stream":"<imagestream>","tag":"<tag>"} (ADR-011 §2).
	buildRunTargetAnnotation = "buildrun.imageflow.appfun.dev/target"

	// buildRunRegisteredConditionType tracks per-BuildRun registration state on
	// the BuildRun status (ADR-011 §3 reuses the Mapping condition system).
	buildRunRegisteredConditionType = "Registered"

	// Registered condition reasons (taxonomy mirrors the Mapping controller:
	// failure codes are also the event-message prefix).
	buildRunReasonSucceeded             = "Succeeded"
	buildRunReasonMissingDigest         = "MissingDigest"
	buildRunReasonStreamNotFound        = "StreamNotFound"
	buildRunReasonInvalidTarget         = "InvalidTarget"
	buildRunReasonMissingImageReference = "MissingImageReference"
	buildRunWatcherControllerName       = "buildrun-watcher"
	buildRunWatcherRecorderName         = "buildrun-watcher"
)

// shipwrightBuildGVK is the GroupVersionKind used for CRD existence detection.
var shipwrightBuildGVK = schema.GroupVersionKind{
	Group:   shipwrightBuildCRDGroup,
	Version: shipwrightBuildCRDVersion,
	Kind:    shipwrightBuildCRDKind,
}

// buildRunGVK is the GroupVersionKind of watched Shipwright BuildRun objects.
var buildRunGVK = schema.GroupVersionKind{
	Group:   shipwrightBuildCRDGroup,
	Version: shipwrightBuildCRDVersion,
	Kind:    "BuildRun",
}

// buildRunTarget is the parsed form of buildRunTargetAnnotation (ADR-011 §2).
type buildRunTarget struct {
	Stream string `json:"stream"`
	Tag    string `json:"tag"`
}

type buildRunWatcherRegistrar struct{}

func init() {
	Registrars = append(Registrars, &buildRunWatcherRegistrar{})
}

// Register conditionally registers the BuildRun watcher controller.
//   - If EnableBuildRunWatcher is false, registration is skipped entirely (no informer created).
//   - If true, the Shipwright Build CRD must exist; otherwise an error is returned
//     to prevent manager startup with a misconfigured cluster.
func (buildRunWatcherRegistrar) Register(mgr ctrl.Manager, opts Options) error {
	if !opts.Config.EnableBuildRunWatcher {
		return nil
	}

	// CRD existence gate: verify shipwright.io/v1beta1 Build is available.
	mapper := mgr.GetRESTMapper()
	if mapper == nil {
		return fmt.Errorf("buildrun watcher: RESTMapper not available on manager")
	}

	_, err := mapper.RESTMapping(shipwrightBuildGVK.GroupKind(), shipwrightBuildGVK.Version)
	if err != nil {
		return fmt.Errorf(
			"buildrun watcher: --enable-buildrun-watcher=true but %s/%s %s CRD not found in cluster: %w",
			shipwrightBuildCRDGroup, shipwrightBuildCRDVersion, shipwrightBuildCRDKind, err,
		)
	}

	return setupBuildRunWatcherController(mgr, opts)
}

func setupBuildRunWatcherController(mgr ctrl.Manager, opts Options) error {
	watchPrototype := &unstructured.Unstructured{}
	watchPrototype.SetGroupVersionKind(buildRunGVK)
	return ctrl.NewControllerManagedBy(mgr).
		For(watchPrototype).
		Named(buildRunWatcherControllerName).
		Complete(NewBuildRunReconciler(
			mgr.GetClient(),
			mgr.GetAPIReader(),
			mgr.GetEventRecorderFor(buildRunWatcherRecorderName),
			opts.Clock,
			opts.Config,
		))
}

// BuildRunReconciler auto-registers outputs of succeeded, annotated Shipwright
// BuildRuns into the ImageFlow catalog (ticket #16, ADR-011 §3-4).
//
// Contract:
//   - filter: annotation buildrun.imageflow.appfun.dev/target AND Succeeded=True
//   - digest gate: .status.output.digest non-empty -> register with
//     RegistrationSourceBuildRun and stamp Registered=True/Succeeded
//   - empty digest -> Registered=False/MissingDigest + Warning Event
//   - invalid annotation / missing stream / missing image reference ->
//     Registered=False with the matching reason + Warning Event
//   - idempotent: re-reconciling the same (stream, tag, digest) state is a
//     no-op — no duplicate stream history, Events, or condition re-stamps
//   - failure states are terminal for the BuildRun (no requeue); transient
//     API errors requeue via the returned error
type BuildRunReconciler struct {
	client    client.Client
	apiReader client.Reader
	recorder  record.EventRecorder
	clock     clock.Clock
	cfg       config.Config
}

// NewBuildRunReconciler wires the watcher reconciler. apiReader is the
// uncached reader used to guard against cache lag on the watcher's own status
// writes (same pattern as the Mapping controller); nil falls back to cl, and a
// nil clock falls back to the real clock.
func NewBuildRunReconciler(
	cl client.Client,
	apiReader client.Reader,
	recorder record.EventRecorder,
	clk clock.Clock,
	cfg config.Config,
) *BuildRunReconciler {
	if apiReader == nil {
		apiReader = cl
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &BuildRunReconciler{
		client:    cl,
		apiReader: apiReader,
		recorder:  recorder,
		clock:     clk,
		cfg:       cfg,
	}
}

func (r *BuildRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	br := &unstructured.Unstructured{}
	br.SetGroupVersionKind(buildRunGVK)
	if err := r.client.Get(ctx, req.NamespacedName, br); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Filter: only annotated BuildRuns participate.
	rawTarget, annotated := br.GetAnnotations()[buildRunTargetAnnotation]
	if !annotated {
		return ctrl.Result{}, nil
	}
	// Filter: only succeeded BuildRuns register; running/failed ones are
	// ignored entirely (no condition, no events).
	if !isBuildRunSucceeded(br) {
		return ctrl.Result{}, nil
	}

	target, err := parseBuildRunTarget(rawTarget)
	if err != nil {
		return r.fail(ctx, req.NamespacedName, buildRunReasonInvalidTarget, err.Error())
	}

	digest, _, _ := unstructured.NestedString(br.Object, "status", "output", "digest")
	if digest == "" {
		return r.fail(ctx, req.NamespacedName, buildRunReasonMissingDigest,
			"BuildRun succeeded but .status.output.digest is empty; build strategy may not provide system results")
	}

	imageRef, err := buildRunImageReference(br, digest)
	if err != nil {
		return r.fail(ctx, req.NamespacedName, buildRunReasonMissingImageReference, err.Error())
	}

	if _, err := RegisterImage(ctx, r.client, r.recorder, r.clock, r.cfg, Registration{
		Namespace:      br.GetNamespace(),
		Stream:         target.Stream,
		Tag:            target.Tag,
		ImageReference: imageRef,
		Digest:         digest,
		Source:         RegistrationSourceBuildRun,
		SourceTask:     br.GetName(),
	}); err != nil {
		if errors.Is(err, ErrStreamNotFound) {
			return r.fail(ctx, req.NamespacedName, buildRunReasonStreamNotFound,
				fmt.Sprintf("ImageStream %q not found in namespace %q; create the stream first or fix annotation %q",
					target.Stream, br.GetNamespace(), buildRunTargetAnnotation))
		}
		// Transient API failure: requeue.
		return ctrl.Result{}, err
	}

	return r.succeed(ctx, req.NamespacedName, target, imageRef)
}

// succeed stamps Registered=True/Succeeded and emits the success audit event
// exactly once per condition transition. The message is a pure function of the
// desired end state (stream, tag, reference) — never of this run's outcome —
// because BuildRuns are long-lived and re-reconcile on every status change: an
// outcome-dependent message would flip on every idempotent re-run and emit
// duplicate events (change-vs-noop semantics live on the ImageChanged event
// emitted by RegisterImage).
func (r *BuildRunReconciler) succeed(
	ctx context.Context,
	key types.NamespacedName,
	target buildRunTarget,
	imageRef string,
) (ctrl.Result, error) {
	message := fmt.Sprintf("Registered %s onto %s:%s", imageRef, target.Stream, target.Tag)

	applied, err := r.applyRegisteredCondition(ctx, key, buildRunCondition{
		status:  "True",
		reason:  buildRunReasonSucceeded,
		message: message,
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if applied {
		r.recorder.Eventf(newBuildRunRef(key), "Normal", EventReasonBuildRunRegistered, "%s", message)
	}
	return ctrl.Result{}, nil
}

// fail stamps Registered=False/<reason> and emits the BuildRunFailed Warning
// audit event exactly once per condition transition. Failures are terminal for
// the BuildRun — no requeue.
func (r *BuildRunReconciler) fail(
	ctx context.Context,
	key types.NamespacedName,
	reason, message string,
) (ctrl.Result, error) {
	applied, err := r.applyRegisteredCondition(ctx, key, buildRunCondition{
		status:  "False",
		reason:  reason,
		message: message,
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if applied {
		r.recorder.Eventf(newBuildRunRef(key), "Warning", EventReasonBuildRunFailed, "%s: %s", reason, message)
	}
	return ctrl.Result{}, nil
}

// buildRunCondition is the desired Registered condition state.
type buildRunCondition struct {
	status  string
	reason  string
	message string
}

// applyRegisteredCondition persists the desired Registered condition on the
// BuildRun status and reports whether a write happened. Idempotence: when the
// persisted condition already matches (status/reason/message), the call is a
// no-op — no status update, and callers skip the audit event. The uncached
// read guards against cache lag from the watcher's own previous status write
// re-triggering a reconcile (same pattern as the Mapping controller).
func (r *BuildRunReconciler) applyRegisteredCondition(
	ctx context.Context,
	key types.NamespacedName,
	desired buildRunCondition,
) (bool, error) {
	latest := &unstructured.Unstructured{}
	latest.SetGroupVersionKind(buildRunGVK)
	if err := r.apiReader.Get(ctx, key, latest); err != nil {
		return false, err
	}
	if registeredConditionMatches(latest, desired) {
		return false, nil
	}

	updated := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &unstructured.Unstructured{}
		current.SetGroupVersionKind(buildRunGVK)
		if err := r.apiReader.Get(ctx, key, current); err != nil {
			return err
		}
		if registeredConditionMatches(current, desired) {
			updated = false
			return nil
		}
		upsertRegisteredCondition(current, desired, r.clock.Now())
		updated = true
		return r.client.Status().Update(ctx, current)
	})
	if err != nil {
		return false, err
	}
	return updated, nil
}

// registeredConditionMatches reports whether the persisted Registered
// condition already equals the desired state.
func registeredConditionMatches(br *unstructured.Unstructured, desired buildRunCondition) bool {
	conds, found, err := unstructured.NestedSlice(br.Object, "status", "conditions")
	if err != nil || !found {
		return false
	}
	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if !ok || m["type"] != buildRunRegisteredConditionType {
			continue
		}
		return m["status"] == desired.status &&
			m["reason"] == desired.reason &&
			m["message"] == desired.message
	}
	return false
}

// upsertRegisteredCondition inserts or replaces the Registered condition,
// preserving lastTransitionTime while the status stays unchanged and re-stamping
// it from the injected clock on transitions (Mapping condition semantics).
func upsertRegisteredCondition(br *unstructured.Unstructured, desired buildRunCondition, now time.Time) {
	conds, _, _ := unstructured.NestedSlice(br.Object, "status", "conditions")
	out := make([]interface{}, 0, len(conds)+1)
	replaced := false
	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if !ok || m["type"] != buildRunRegisteredConditionType {
			out = append(out, c)
			continue
		}
		ltt, _ := m["lastTransitionTime"].(string)
		if ltt == "" || m["status"] != desired.status {
			ltt = now.UTC().Format(time.RFC3339)
		}
		out = append(out, map[string]interface{}{
			"type":               buildRunRegisteredConditionType,
			"status":             desired.status,
			"reason":             desired.reason,
			"message":            desired.message,
			"lastTransitionTime": ltt,
			"observedGeneration": br.GetGeneration(),
		})
		replaced = true
	}
	if !replaced {
		out = append(out, map[string]interface{}{
			"type":               buildRunRegisteredConditionType,
			"status":             desired.status,
			"reason":             desired.reason,
			"message":            desired.message,
			"lastTransitionTime": now.UTC().Format(time.RFC3339),
			"observedGeneration": br.GetGeneration(),
		})
	}
	_ = unstructured.SetNestedSlice(br.Object, out, "status", "conditions")
}

// isBuildRunSucceeded reports whether the BuildRun carries Succeeded=True
// (Shipwright status condition).
func isBuildRunSucceeded(br *unstructured.Unstructured) bool {
	conds, found, err := unstructured.NestedSlice(br.Object, "status", "conditions")
	if err != nil || !found {
		return false
	}
	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if m["type"] == "Succeeded" {
			return m["status"] == "True"
		}
	}
	return false
}

// parseBuildRunTarget validates the target annotation (ADR-011 §2). The
// admission webhook (T2) rejects malformed annotations up front; the watcher
// still fails loudly instead of silently ignoring them, because webhooks can
// be bypassed or disabled.
func parseBuildRunTarget(raw string) (buildRunTarget, error) {
	var target buildRunTarget
	if err := json.Unmarshal([]byte(raw), &target); err != nil {
		return buildRunTarget{}, fmt.Errorf(
			"annotation %q is not valid JSON (want {\"stream\":\"<name>\",\"tag\":\"<tag>\"}): %v",
			buildRunTargetAnnotation, err)
	}
	if target.Stream == "" || target.Tag == "" {
		return buildRunTarget{}, fmt.Errorf(
			"annotation %q must set non-empty stream and tag, got %q",
			buildRunTargetAnnotation, raw)
	}
	return target, nil
}

// buildRunImageReference derives the canonical digest reference
// <repo>@<digest> from the build output image. The image comes from
// .status.buildSpec.output.image (Shipwright copies the resolved build spec
// into status) with .spec.output.image (BuildRun-level override) as fallback;
// the tag is stripped because TagEvent.ImageReference only accepts digest
// references (ADR-004). Registry ports are preserved.
func buildRunImageReference(br *unstructured.Unstructured, digest string) (string, error) {
	image, _, _ := unstructured.NestedString(br.Object, "status", "buildSpec", "output", "image")
	if image == "" {
		image, _, _ = unstructured.NestedString(br.Object, "spec", "output", "image")
	}
	if image == "" {
		return "", fmt.Errorf(
			"BuildRun succeeded with digest %s but neither .status.buildSpec.output.image nor .spec.output.image is set; cannot derive the image repository",
			digest)
	}
	return stripImageTag(image) + "@" + digest, nil
}

// stripImageTag removes a :tag suffix without touching a registry port: only a
// colon after the last slash starts a tag.
func stripImageTag(ref string) string {
	lastSlash := strings.LastIndex(ref, "/")
	if idx := strings.LastIndex(ref, ":"); idx > lastSlash {
		return ref[:idx]
	}
	return ref
}

// newBuildRunRef builds the minimal object reference for event recording. The
// watcher's events describe a specific BuildRun even when the object was
// concurrently deleted.
func newBuildRunRef(key types.NamespacedName) *unstructured.Unstructured {
	ref := &unstructured.Unstructured{}
	ref.SetGroupVersionKind(buildRunGVK)
	ref.SetNamespace(key.Namespace)
	ref.SetName(key.Name)
	return ref
}
