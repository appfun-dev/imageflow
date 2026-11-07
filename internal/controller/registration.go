package controller

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
)

// This file implements the shared catalog registration machinery (ticket T4):
// content-addressed ImageArtifact dedup, idempotent ImageStream status
// association with bounded per-tag history, and the Image Change surface
// (kube Events + metrics). The Mapping push path (T4) uses it directly; the
// Import pull path (T7) reuses RegisterImage with RegistrationSourceImport so
// both registration paths share identical semantics (spec decision 9, ADR-004).
//
// Image Change is an internal logical event, never an in-memory-only signal:
// the persisted form is ImageStream.status.tags[].items (most recent first).
// Change detection compares the incoming digest against items[0].Digest, so
// dedup, retry and controller-restart recovery all fall out of reconcile
// idempotence against persisted state (spec decision 9: no reliance on
// in-memory events).

// RegistrationSource identifies which task path produced a registration. It is
// surfaced in Event messages so an Image Change is attributable to its origin
// (Mapping push vs Import pull).
type RegistrationSource string

const (
	// RegistrationSourceMapping is the push path (ImageStreamMapping, T4).
	RegistrationSourceMapping RegistrationSource = "ImageStreamMapping"
	// RegistrationSourceImport is the pull path (ImageStreamImport, T7).
	RegistrationSourceImport RegistrationSource = "ImageStreamImport"
	// RegistrationSourceBuildRun is the build-output path (Shipwright BuildRun
	// watcher, T3 / ADR-011).
	RegistrationSourceBuildRun RegistrationSource = "BuildRun"
)

// Event reasons emitted on the catalog side. The vocabulary is shared by both
// registration paths so downstream tooling sees one stable set.
const (
	// EventReasonImageChanged announces a real image change: the (stream, tag)
	// association moved to a different digest (or was established for the first
	// time). The message attributes the source task (Mapping vs Import).
	EventReasonImageChanged = "ImageChanged"
	// EventReasonTagHistoryOverflow announces bounded-history overflow: entries
	// dropped from ImageStream.status.tags[].items land in Events (ADR-008).
	EventReasonTagHistoryOverflow = "TagHistoryOverflow"
	// EventReasonImageArtifactCreated announces first-time creation of the
	// cluster-scoped ImageArtifact for a digest.
	EventReasonImageArtifactCreated = "ImageArtifactCreated"
	// EventReasonMappingRegistered is the success audit event on the Mapping task.
	EventReasonMappingRegistered = "MappingRegistered"
	// EventReasonMappingFailed is the failure audit event on the Mapping task.
	EventReasonMappingFailed = "MappingFailed"
	// EventReasonBuildRunRegistered is the success audit event on a watched
	// BuildRun whose output was registered into the catalog (T3, ADR-011).
	EventReasonBuildRunRegistered = "BuildRunRegistered"
	// EventReasonBuildRunFailed is the failure audit event on a watched
	// BuildRun whose output could not be registered; the message is prefixed
	// with the failure code (MissingDigest/StreamNotFound/...), mirroring the
	// Mapping failure taxonomy.
	EventReasonBuildRunFailed = "BuildRunFailed"
	// EventReasonTaskGarbageCollected announces TTL deletion of a task object.
	EventReasonTaskGarbageCollected = "TaskGarbageCollected"
)

// RegistrationOutcome classifies one registration attempt.
type RegistrationOutcome string

const (
	// RegistrationChanged means the (stream, tag) association moved to the new
	// digest: a real Image Change was recorded (prepended to bounded history).
	RegistrationChanged RegistrationOutcome = "changed"
	// RegistrationNoop means the (stream, tag) already referenced the digest:
	// idempotent re-registration, no Image Change, no status write.
	RegistrationNoop RegistrationOutcome = "noop"
)

// ErrStreamNotFound reports that the target ImageStream does not exist. Callers
// treat it as a terminal task failure (diagnosable, not retryable): waiting
// forever would keep garbage tasks alive with no completion signal.
var ErrStreamNotFound = errors.New("stream not found")

var (
	digestPatternRegexp    = regexp.MustCompile(imageflowv1alpha1.DigestPattern)
	digestRefPatternRegexp = regexp.MustCompile(imageflowv1alpha1.DigestReferencePattern)
)

// ArtifactNameForDigest derives the cluster-scoped ImageArtifact object name
// for a digest: sha256:<64hex> -> sha256-<64hex> (RFC 1123, content-addressed
// naming convention from ADR-002 R2 / T2 handoff).
func ArtifactNameForDigest(digest string) (string, error) {
	if !digestPatternRegexp.MatchString(digest) {
		return "", fmt.Errorf("invalid digest %q: want sha256:<64 lowercase hex chars>", digest)
	}
	return strings.Replace(digest, ":", "-", 1), nil
}

// SplitDigestReference parses a digest reference <repo>@sha256:<64hex> into its
// repository and digest parts. Tag-only references are rejected: a registration
// without a digest cannot produce an Image Change and would poison the
// downstream chain (ADR-004).
func SplitDigestReference(ref string) (repo string, digest string, err error) {
	if !digestRefPatternRegexp.MatchString(ref) {
		return "", "", fmt.Errorf(
			"image reference %q is not a digest reference: want <repo>@sha256:<64 lowercase hex chars>", ref)
	}
	at := strings.LastIndex(ref, "@")
	return ref[:at], ref[at+1:], nil
}

// Registration is one catalog registration request, normalized by the caller.
type Registration struct {
	// Namespace of the task and of the target ImageStream (same namespace,
	// ADR-002 R7).
	Namespace string
	// Stream is the target ImageStream name.
	Stream string
	// Tag to associate the image with.
	Tag string
	// ImageReference is the canonical digest reference <repo>@sha256:<64hex>.
	ImageReference string
	// Digest is sha256:<64hex>, the change-detection key.
	Digest string
	// Source attributes the registration path (Mapping/Import) in audit output.
	Source RegistrationSource
	// SourceTask is the name of the task object that produced the registration.
	SourceTask string
}

// RegistrationResult reports what one idempotent registration did.
type RegistrationResult struct {
	// Outcome is changed (real Image Change recorded) or noop (idempotent
	// re-registration).
	Outcome RegistrationOutcome
	// ArtifactName is the cluster-scoped ImageArtifact for the digest
	// (created now or reused).
	ArtifactName string
	// ArtifactCreated is true when this call created the ImageArtifact.
	ArtifactCreated bool
	// OldDigest is the previous current digest of (stream, tag), empty on first
	// association. Only meaningful when Outcome == RegistrationChanged.
	OldDigest string
}

// RegisterImage performs one idempotent catalog registration. It is safe to
// call repeatedly (retry, requeue, controller restart): effects are keyed off
// persisted state, so a re-run after a partially completed registration
// converges without duplicate Image Changes, duplicate artifacts or duplicate
// Events. Errors are classified for callers: ErrStreamNotFound is terminal for
// the task; every other error is transient and must be requeued.
func RegisterImage(
	ctx context.Context,
	cl client.Client,
	recorder record.EventRecorder,
	clk clock.Clock,
	cfg config.Config,
	reg Registration,
) (RegistrationResult, error) {
	// Fail fast on a missing stream so a failed registration leaves no catalog
	// side effects (the stream is re-read under retry-on-conflict below anyway).
	var probe imageflowv1alpha1.ImageStream
	if err := cl.Get(ctx, types.NamespacedName{Namespace: reg.Namespace, Name: reg.Stream}, &probe); err != nil {
		if apierrors.IsNotFound(err) {
			return RegistrationResult{}, fmt.Errorf("%w: ImageStream %q not found in namespace %q",
				ErrStreamNotFound, reg.Stream, reg.Namespace)
		}
		return RegistrationResult{}, fmt.Errorf("probe stream: %w", err)
	}

	artifactName, artifactCreated, err := ensureImageArtifact(ctx, cl, recorder, reg)
	if err != nil {
		return RegistrationResult{}, err
	}

	changed, oldDigest, err := associateTagDigest(ctx, cl, recorder, clk, cfg, reg)
	if err != nil {
		return RegistrationResult{}, err
	}

	outcome := RegistrationNoop
	if changed {
		outcome = RegistrationChanged
	}
	return RegistrationResult{
		Outcome:         outcome,
		ArtifactName:    artifactName,
		ArtifactCreated: artifactCreated,
		OldDigest:       oldDigest,
	}, nil
}

// ensureImageArtifact creates the content-addressed cluster-scoped ImageArtifact
// if absent, and reuses it otherwise. Global dedup: one artifact per digest
// (ADR-002 R2). The spec is create-once (CEL self == oldSelf), so an existing
// artifact is never updated — not its spec, and not its metadata: user
// annotations are the only user-writable surface and the controller preserves
// them by never writing the object again.
func ensureImageArtifact(
	ctx context.Context,
	cl client.Client,
	recorder record.EventRecorder,
	reg Registration,
) (name string, created bool, err error) {
	name, err = ArtifactNameForDigest(reg.Digest)
	if err != nil {
		return "", false, fmt.Errorf("derive artifact name: %w", err)
	}

	var existing imageflowv1alpha1.ImageArtifact
	switch getErr := cl.Get(ctx, types.NamespacedName{Name: name}, &existing); {
	case getErr == nil:
		// Reuse. Deliberately no write: spec is immutable (CEL create-once) and
		// annotations belong to the user (ADR-002 R2).
		return name, false, nil
	case !apierrors.IsNotFound(getErr):
		return "", false, fmt.Errorf("get image artifact %q: %w", name, getErr)
	}

	artifact := &imageflowv1alpha1.ImageArtifact{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: imageflowv1alpha1.ImageArtifactSpec{
			Digest:         reg.Digest,
			ImageReference: reg.ImageReference,
		},
	}
	if createErr := cl.Create(ctx, artifact); createErr != nil {
		if apierrors.IsAlreadyExists(createErr) {
			// A concurrent registration (another Mapping, or the Import path)
			// won the create race. Same digest, same content address: reuse.
			return name, false, nil
		}
		return "", false, fmt.Errorf("create image artifact %q: %w", name, createErr)
	}
	recorder.Eventf(artifact, corev1.EventTypeNormal, EventReasonImageArtifactCreated,
		"Created cluster-scoped ImageArtifact for %s (source: %s %s/%s)",
		reg.Digest, reg.Source, reg.Namespace, reg.SourceTask)
	return name, true, nil
}

// associateTagDigest records reg.Digest as the current association of
// (stream, tag) in ImageStream.status via the status subresource, guarded by
// retry-on-conflict: both registration paths (T4 Mapping, T7 Import) write this
// one surface, and a conflict means a concurrent writer — the closure re-reads
// and recomputes, which is also what makes concurrent same-digest registrations
// dedup to a single Image Change. Retention is bounded by
// Config.StreamTagHistoryLimit, clamped to the CRD cap
// (StreamTagHistoryMaxItems): overflow entries fall to kube Events (ADR-008).
//
// The catalog side deliberately uses read-modify-write + retry-on-conflict, not
// SSA: status.tags[].items is an atomic list (whole-array ownership), and
// SSA field-level exclusivity is the delivery-side contract (T9, ADR-008).
func associateTagDigest(
	ctx context.Context,
	cl client.Client,
	recorder record.EventRecorder,
	clk clock.Clock,
	cfg config.Config,
	reg Registration,
) (changed bool, oldDigest string, err error) {
	limit := historyLimit(cfg)
	var stream imageflowv1alpha1.ImageStream
	var dropped []imageflowv1alpha1.TagEvent

	writeErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		changed, oldDigest, dropped = false, "", nil
		if getErr := cl.Get(ctx, types.NamespacedName{Namespace: reg.Namespace, Name: reg.Stream}, &stream); getErr != nil {
			if apierrors.IsNotFound(getErr) {
				return fmt.Errorf("%w: ImageStream %q not found in namespace %q",
					ErrStreamNotFound, reg.Stream, reg.Namespace)
			}
			return fmt.Errorf("get stream: %w", getErr)
		}

		idx := -1
		for i := range stream.Status.Tags {
			if stream.Status.Tags[i].Tag == reg.Tag {
				idx = i
				break
			}
		}
		if idx == -1 {
			stream.Status.Tags = append(stream.Status.Tags, imageflowv1alpha1.NamedTagEventList{Tag: reg.Tag})
			idx = len(stream.Status.Tags) - 1
		}

		items := stream.Status.Tags[idx].Items
		if len(items) > 0 && items[0].Digest == reg.Digest {
			// Idempotent: the tag already points at this digest. No write, no
			// Image Change, no Events (pipeline-retry semantics, user story 16).
			return nil
		}
		if len(items) > 0 {
			oldDigest = items[0].Digest
		}

		merged := make([]imageflowv1alpha1.TagEvent, 0, len(items)+1)
		merged = append(merged, imageflowv1alpha1.TagEvent{
			Digest:         reg.Digest,
			ImageReference: reg.ImageReference,
			Created:        metav1.NewTime(clk.Now()),
			Generation:     stream.Generation,
		})
		merged = append(merged, items...)
		if len(merged) > limit {
			dropped = append([]imageflowv1alpha1.TagEvent(nil), merged[limit:]...)
			merged = merged[:limit]
		}
		stream.Status.Tags[idx].Items = merged
		stream.Status.ObservedGeneration = stream.Generation
		changed = true
		return cl.Status().Update(ctx, &stream)
	})
	if writeErr != nil {
		return false, "", writeErr
	}

	if changed {
		previous := "none"
		if oldDigest != "" {
			previous = oldDigest
		}
		recorder.Eventf(&stream, corev1.EventTypeNormal, EventReasonImageChanged,
			"Tag %q now references %s (previous: %s); source: %s %s/%s",
			reg.Tag, reg.Digest, previous, reg.Source, reg.Namespace, reg.SourceTask)
	}
	for _, d := range dropped {
		recorder.Eventf(&stream, corev1.EventTypeNormal, EventReasonTagHistoryOverflow,
			"Tag %q history exceeded bounded depth %d; dropped %s (%s) from status — "+
				"complete history lives in Events and the audit log pipeline (ADR-008)",
			reg.Tag, limit, d.Digest, d.ImageReference)
	}
	return changed, oldDigest, nil
}

// historyLimit returns the effective per-tag retention depth: the configured
// flag, clamped into [1, StreamTagHistoryMaxItems]. A flag above the CRD cap
// would make the API server reject the status write (structural etcd guard),
// so the controller never exceeds the cap regardless of configuration.
func historyLimit(cfg config.Config) int {
	limit := cfg.StreamTagHistoryLimit
	if limit <= 0 || limit > imageflowv1alpha1.StreamTagHistoryMaxItems {
		return imageflowv1alpha1.StreamTagHistoryMaxItems
	}
	return limit
}
