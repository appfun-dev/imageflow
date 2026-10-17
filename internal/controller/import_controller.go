package controller

// ImageStreamImport controller (ticket T7, pull registration path).
//
// Lifecycle: a user (on-demand) or the scheduled-import controller creates an
// ImageStreamImport task -> this controller records startedAt, resolves the
// external reference to an immutable digest through the frozen S2 registry seam
// (internal/registry), performs the idempotent catalog registration through the
// SHARED T4 machinery (RegisterImage with RegistrationSourceImport, so both
// registration paths have identical semantics: spec decision 9, ADR-004) and
// writes the terminal Complete condition with a diagnosable reason. TTL GC
// (task_gc_controller.go, T4) removes terminal tasks later — this file never
// reimplements GC. Every state transition is audited with kube Events.
//
// Diagnosability contract (user story 11): failures are classified into a
// frozen code vocabulary that separates network, credential and manifest
// problems. The code prefixes the Complete condition message (the same shape
// T4 uses for the Mapping path) and labels the imageflow_import_tasks_total
// metric. Classification is driven by the S2 error taxonomy (errors.Is against
// the registry sentinels) so no error text is parsed.
//
// Credential boundary (user story 12, ADR-007): pull secrets are read through
// the UNCACHED manager API reader (deliberately not the client cache: a Secret
// informer would list/watch every secret in the cluster) and are handed to the
// S2 client as registry.Credentials. Nothing derived from a secret ever reaches
// status, Events, log records or metric labels — the S2 client already scrubs
// credential material from its errors, and this controller only ever formats
// those scrubbed errors plus non-secret identifiers (registry host, secret
// name).
//
// Retry semantics: registry-side failures are TERMINAL for the task (the whole
// point of the failure TTL is that the diagnosable reason stays inspectable),
// while Kubernetes API failures requeue with backoff. The scheduled path
// provides the retry cadence for transient registry problems: the next tick
// creates a fresh task. Change detection and dedup are keyed off persisted
// catalog state (see registration.go), so retries never duplicate an Image
// Change.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
	"github.com/appfun-dev/imageflow/internal/registry"
)

// Event recorder component names (Event.source.component) of the T7 units.
const (
	recorderNameImport          = "imageflow-import"
	recorderNameScheduledImport = "imageflow-scheduled-import"
)

// kindImageStream is the ImageFlow kind name used by the scheme-recognition
// guard of the scheduled-import controller.
const kindImageStream = "ImageStream"

// importSourceKindDockerImage is the only ImageSourceReference kind in v1
// (CRD enum + default); the controller states it explicitly so an unexpected
// kind is a diagnosable failure instead of a silent misimport.
const importSourceKindDockerImage = "DockerImage"

// T7 Event reasons. The catalog-side Image Change vocabulary belongs to T4
// (registration.go) and is shared by both paths; the reasons below are the
// import lane's own audit surface.
const (
	// EventReasonImportSucceeded is the success audit event on the Import task.
	EventReasonImportSucceeded = "ImportSucceeded"
	// EventReasonImportFailed is the failure audit event on the Import task.
	EventReasonImportFailed = "ImportFailed"
	// EventReasonTagDriftDetected announces that a scheduled or on-demand
	// import found the external tag pointing at a DIFFERENT digest than the
	// catalog: a real Image Change whose origin is the pull path. It is emitted
	// in addition to the shared ImageChanged event so drift is attributable by
	// reason alone (Mapping-sourced changes never carry it).
	EventReasonTagDriftDetected = "TagDriftDetected"
	// EventReasonScheduledImportCreated announces a task created by the
	// scheduled-import controller on the stream it polls.
	EventReasonScheduledImportCreated = "ScheduledImportCreated"
	// EventReasonScheduledImportError reports a scheduled task that could not
	// be created (never silent, user story 32 pattern).
	EventReasonScheduledImportError = "ScheduledImportError"
)

// Failure taxonomy of the Import path: the code prefixes the Complete
// condition message and labels imageflow_import_tasks_total. The condition
// reason vocabulary itself stays frozen to Succeeded/Failed (T2). Network,
// credential and manifest problems are distinct codes (user story 11).
const (
	// importFailPullSecretNotFound: spec.pullSecretRef names a secret that does
	// not exist (credential problem, local).
	importFailPullSecretNotFound = "PullSecretNotFound"
	// importFailPullSecretInvalid: the secret is not a usable dockerconfigjson
	// for the target registry (credential problem, local).
	importFailPullSecretInvalid = "PullSecretInvalid"
	// importFailUnauthorized: the registry rejected the request (401/403)
	// (credential problem, remote).
	importFailUnauthorized = "Unauthorized"
	// importFailManifestNotFound: the registry has no such manifest/tag/repo
	// (404) (manifest problem).
	importFailManifestNotFound = "ManifestNotFound"
	// importFailDigestMismatch: served content does not hash to the expected
	// digest (manifest problem).
	importFailDigestMismatch = "DigestMismatch"
	// importFailRegistryDenied: the registry host is outside
	// --registry-allowlist (SSRF guard, zero network I/O).
	importFailRegistryDenied = "RegistryDenied"
	// importFailTimeout: the registry operation exceeded its deadline
	// (network problem).
	importFailTimeout = "RegistryTimeout"
	// importFailNetwork: the registry endpoint could not be reached at all —
	// DNS, routing, refused connection, TLS (network problem).
	importFailNetwork = "NetworkUnreachable"
	// importFailRegistry: any other non-2xx answer (429, 5xx) or protocol
	// violation.
	importFailRegistry = "RegistryError"
	// importFailUnsupportedSource: spec.from.kind is outside the v1 vocabulary.
	importFailUnsupportedSource = "UnsupportedImageSourceKind"
)

func init() {
	Registrars = append(Registrars, &importRegistrar{})
}

// ImportControllerNames carries the controller names T7 builds. controller-runtime
// validates controller names against a process-global set, so a second manager
// built in the same process (integration suites) must use distinct names.
type ImportControllerNames struct {
	// Import is the ImageStreamImport reconciler controller name.
	Import string
	// Scheduled is the scheduled-import (ImageStream polling) controller name.
	Scheduled string
}

// DefaultImportControllerNames returns the production controller names.
func DefaultImportControllerNames() ImportControllerNames {
	return ImportControllerNames{
		Import:    "import",
		Scheduled: "scheduled-import",
	}
}

// ImportDeps carries the injectable dependencies of the T7 registration unit.
// The zero value is the production wiring: the S2 client is built from
// opts.Config (allowlist) with controller-runtime's metrics registry.
type ImportDeps struct {
	// Registry overrides the S2 client. Tests inject a client bound to a
	// registry double; nil builds the production client.
	Registry registry.Client
	// RegistryTransport overrides the HTTP transport of the BUILT client
	// (ignored when Registry is set).
	RegistryTransport http.RoundTripper
	// RegistryRequestTimeout bounds one logical registry operation of the
	// BUILT client; <= 0 selects the S2 default (30s).
	RegistryRequestTimeout time.Duration
}

// SetupImportControllers registers both T7 controllers with mgr: the
// ImageStreamImport reconciler (execution) and the scheduled-import controller
// (cadence). Registrars use it for production wiring; integration suites call
// it directly with test-unique names and an injected registry client.
//
// TTL GC is NOT registered here: task_gc_controller.go (T4) already covers the
// ImageStreamImport kind and is wired by its own registration unit.
func SetupImportControllers(mgr ctrl.Manager, opts Options, names ImportControllerNames, deps ImportDeps) error {
	// Mandatory scheme-recognition guard (ENV platform fact 1): builder.Complete
	// resolves the watched GVKs against the manager scheme AT REGISTRATION time,
	// so a manager whose scheme lacks the imageflow kinds would abort here and
	// take the whole manager down. Skip loudly instead; production registers
	// api/v1alpha1 in cmd/main.go init(), so a skip there is a wiring regression.
	if missing := missingImageFlowKinds(mgr, kindImageStreamImport, kindImageStream); len(missing) > 0 {
		logSkippedRegistration(names.Import+"/"+names.Scheduled, missing)
		return nil
	}

	reg, err := importRegistryClient(opts, deps)
	if err != nil {
		return fmt.Errorf("build import registry client: %w", err)
	}

	clk := opts.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	metrics := sharedImportMetrics()

	importReconciler := NewImportReconciler(
		mgr.GetClient(),
		mgr.GetAPIReader(),
		mgr.GetEventRecorderFor(recorderNameImport),
		clk,
		opts.Config,
		reg,
		metrics,
	)
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&imageflowv1alpha1.ImageStreamImport{}).
		Named(names.Import).
		Complete(importReconciler); err != nil {
		return fmt.Errorf("build %s controller: %w", names.Import, err)
	}

	scheduledReconciler := NewScheduledImportReconciler(
		mgr.GetClient(),
		mgr.GetEventRecorderFor(recorderNameScheduledImport),
		clk,
		opts.Config,
		metrics,
	)
	// Owns() re-evaluates the schedule as soon as one of its tasks completes
	// (the ownerRef below is a controlling one); due-ness itself is recomputed
	// from persisted state on every pass, so the controller is restart-safe.
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&imageflowv1alpha1.ImageStream{}).
		Owns(&imageflowv1alpha1.ImageStreamImport{}).
		Named(names.Scheduled).
		Complete(scheduledReconciler); err != nil {
		return fmt.Errorf("build %s controller: %w", names.Scheduled, err)
	}
	return nil
}

// importRegistryClient returns the injected S2 client or builds the production
// one: allowlist from --registry-allowlist (SSRF guard), metrics against
// controller-runtime's registry, durations measured with the injected clock.
func importRegistryClient(opts Options, deps ImportDeps) (registry.Client, error) {
	if deps.Registry != nil {
		return deps.Registry, nil
	}
	return registry.NewClient(registry.Options{
		Allowlist:      opts.Config.RegistryAllowlist,
		Registerer:     ctrlmetrics.Registry,
		Clock:          opts.Clock,
		Transport:      deps.RegistryTransport,
		RequestTimeout: deps.RegistryRequestTimeout,
	})
}

type importRegistrar struct{}

func (importRegistrar) Register(mgr ctrl.Manager, opts Options) error {
	return SetupImportControllers(mgr, opts, DefaultImportControllerNames(), ImportDeps{})
}

// ImportReconciler reconciles ImageStreamImport task objects (pull path).
type ImportReconciler struct {
	client    client.Client
	apiReader client.Reader // uncached: pull-secret reads must not start a Secret informer
	recorder  record.EventRecorder
	clock     clock.Clock
	config    config.Config
	images    registry.Client
	metrics   *importMetrics
}

// NewImportReconciler builds the reconciler. apiReader may be the same client
// as cl (tests); in the manager it is mgr.GetAPIReader() so pull secrets are
// read directly from the API server. A nil metrics set is replaced with an
// unregistered one (tests); a nil registry client is a programming error and
// panics, because every reconcile would fail with a nil dereference anyway.
func NewImportReconciler(
	cl client.Client,
	apiReader client.Reader,
	recorder record.EventRecorder,
	clk clock.Clock,
	cfg config.Config,
	images registry.Client,
	metrics *importMetrics,
) *ImportReconciler {
	if apiReader == nil {
		apiReader = cl
	}
	if clk == nil {
		clk = clock.Real{}
	}
	if metrics == nil {
		metrics = newImportMetrics()
	}
	if images == nil {
		panic("NewImportReconciler: registry client is required (seam S2)")
	}
	return &ImportReconciler{
		client:    cl,
		apiReader: apiReader,
		recorder:  recorder,
		clock:     clk,
		config:    cfg,
		images:    images,
		metrics:   metrics,
	}
}

// RBAC of the whole manager, regenerated into config/rbac/role.yaml by
// `make manifests`. The secrets rule is deliberately `get` only (no list, no
// watch): pull secrets are read through the uncached API reader, so the
// controller never needs — and never gets — cluster-wide secret visibility
// (ADR-007 credential boundary).
//
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagestreamimports,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagestreamimports/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagestreams,verbs=get;list;watch
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagestreams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imageartifacts,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get

// Reconcile processes one ImageStreamImport task. Terminal tasks reconcile to
// zero effects; non-terminal tasks resolve the external reference, register the
// result through the shared catalog machinery and end in Complete=True with
// reason Succeeded or Failed plus a diagnosable message.
func (r *ImportReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var task imageflowv1alpha1.ImageStreamImport
	if err := r.client.Get(ctx, req.NamespacedName, &task); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil // deleted (e.g. by TTL GC) before we got to it
		}
		return ctrl.Result{}, fmt.Errorf("get import task: %w", err)
	}
	if taskIsTerminal(&task.Status.TaskObjectStatus) {
		return ctrl.Result{}, nil // idempotence: a finished task has zero effects
	}

	if err := r.ensureStartedAt(ctx, req.NamespacedName); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("record startedAt: %w", err)
	}

	if kind := task.Spec.From.Kind; kind != "" && kind != importSourceKindDockerImage {
		return r.failTask(ctx, &task, importFailure{
			code: importFailUnsupportedSource,
			message: fmt.Sprintf("%s: spec.from.kind %q is not supported; v1 imports images through the "+
				"standard OCI / Docker Registry v2 API only (kind %q, ADR-007)",
				importFailUnsupportedSource, kind, importSourceKindDockerImage),
		})
	}

	ref, err := registry.ParseReference(task.Spec.From.Name)
	if err != nil {
		return r.failTask(ctx, &task, importFailure{
			code: failCodeInvalidImageReference,
			message: fmt.Sprintf("%s: spec.from.name %q is not a valid image reference: %v — want "+
				`"<registry>/<repository>:<tag>" or "<registry>/<repository>@sha256:<64hex>" `+
				"with an explicit registry host", failCodeInvalidImageReference, task.Spec.From.Name, err),
		})
	}

	creds, failure, err := r.credentialsFor(ctx, &task, ref.Registry)
	if err != nil {
		return ctrl.Result{}, err
	}
	if failure != nil {
		return r.failTask(ctx, &task, *failure)
	}

	digest, failure, err := r.resolveDigest(ctx, ref, creds)
	if err != nil {
		return ctrl.Result{}, err
	}
	if failure != nil {
		return r.failTask(ctx, &task, *failure)
	}

	imageRef := registry.Reference{Registry: ref.Registry, Repository: ref.Repository, Digest: digest}.String()
	res, err := RegisterImage(ctx, r.client, r.recorder, r.clock, r.config, Registration{
		Namespace:      task.Namespace,
		Stream:         task.Spec.Stream,
		Tag:            task.Spec.Tag,
		ImageReference: imageRef,
		Digest:         digest,
		Source:         RegistrationSourceImport,
		SourceTask:     task.Name,
	})
	if err != nil {
		if errors.Is(err, ErrStreamNotFound) {
			return r.failTask(ctx, &task, importFailure{
				code: failCodeStreamNotFound,
				message: fmt.Sprintf("%s: %v; create the stream first or fix spec.stream",
					failCodeStreamNotFound, err),
			})
		}
		// Transient Kubernetes API failure: requeue with backoff. Persisted-state
		// dedup makes the retry safe (no duplicate Image Change).
		return ctrl.Result{}, fmt.Errorf("register imported image: %w", err)
	}

	message := importSuccessMessage(&task, digest, imageRef, res)
	completed, err := r.completeTask(ctx, req.NamespacedName, importCompletion{
		reason:         imageflowv1alpha1.ReasonSucceeded,
		message:        message,
		digest:         digest,
		imageReference: imageRef,
		artifactRef:    res.ArtifactName,
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("write completion status: %w", err)
	}

	r.metrics.tasks.WithLabelValues(importResultSuccess, imageflowv1alpha1.ReasonSucceeded).Inc()
	r.metrics.registrations.WithLabelValues(importOutcomeFor(res.Outcome)).Inc()
	r.observeTaskDuration(completed, importResultSuccess)
	r.recorder.Eventf(completed, corev1.EventTypeNormal, EventReasonImportSucceeded, "%s", message)
	r.recordDrift(ctx, completed, res, digest)
	logger.Info("import complete",
		"stream", task.Spec.Stream, "tag", task.Spec.Tag, "from", task.Spec.From.Name,
		"digest", digest, "outcome", res.Outcome, "artifact", res.ArtifactName,
		"trigger", importTrigger(completed))
	return ctrl.Result{}, nil
}

// recordDrift emits the import-lane drift Event on the stream when the imported
// digest replaced a different one. First associations are not drift (the shared
// ImageChanged event already announces them); a noop registration emits nothing
// ("无漂移时不产生", ticket AC).
func (r *ImportReconciler) recordDrift(
	ctx context.Context,
	task *imageflowv1alpha1.ImageStreamImport,
	res RegistrationResult,
	digest string,
) {
	if res.Outcome != RegistrationChanged || res.OldDigest == "" {
		return
	}
	var stream imageflowv1alpha1.ImageStream
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.Stream}
	if err := r.client.Get(ctx, key, &stream); err != nil {
		// The registration itself succeeded; a lost stream only costs the extra
		// audit Event. Never fail the task for it.
		log.FromContext(ctx).Error(err, "cannot emit drift event: stream unreadable",
			"stream", key)
		return
	}
	r.recorder.Eventf(&stream, corev1.EventTypeNormal, EventReasonTagDriftDetected,
		"Import detected tag drift on %q: %s -> %s (external source %s; trigger: %s; task %s/%s)",
		task.Spec.Tag, res.OldDigest, digest, task.Spec.From.Name, importTrigger(task),
		task.Namespace, task.Name)
}

// credentialsFor resolves the pull credentials of one task. It returns a
// terminal failure when the referenced secret is unusable, a transient error
// when the API server could not answer, and the zero Credentials for anonymous
// imports. Secret CONTENT never leaves this function: only registry.Credentials
// (self-redacting) and scrubbed error text flow onward.
func (r *ImportReconciler) credentialsFor(
	ctx context.Context,
	task *imageflowv1alpha1.ImageStreamImport,
	registryHost string,
) (registry.Credentials, *importFailure, error) {
	if task.Spec.PullSecretRef == nil {
		return registry.Credentials{}, nil, nil
	}
	name := task.Spec.PullSecretRef.Name
	var secret corev1.Secret
	key := types.NamespacedName{Namespace: task.Namespace, Name: name}
	if err := r.apiReader.Get(ctx, key, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return registry.Credentials{}, &importFailure{
				code: importFailPullSecretNotFound,
				message: fmt.Sprintf("%s: pull secret %q referenced by spec.pullSecretRef does not exist "+
					"in namespace %q", importFailPullSecretNotFound, name, task.Namespace),
			}, nil
		}
		return registry.Credentials{}, nil, fmt.Errorf("read pull secret %q: %w", name, err)
	}
	creds, err := registry.CredentialsFromSecret(&secret, registryHost)
	if err != nil {
		// err is credential-free by construction (S2 scrubs server echoes and
		// never includes the payload); only the secret name and host are added.
		return registry.Credentials{}, &importFailure{
			code: importFailPullSecretInvalid,
			message: fmt.Sprintf("%s: pull secret %q cannot authenticate to registry %q: %v",
				importFailPullSecretInvalid, name, registryHost, err),
		}, nil
	}
	return creds, nil, nil
}

// resolveDigest turns the task's external reference into an immutable digest.
// Tag references resolve through a manifest HEAD (ResolveDigest); digest
// references are verified against the served payload (FetchManifest), so a
// manifest that does not exist — or does not hash to the requested digest — is
// a diagnosable failure instead of a poisoned catalog entry.
func (r *ImportReconciler) resolveDigest(
	ctx context.Context,
	ref registry.Reference,
	creds registry.Credentials,
) (string, *importFailure, error) {
	if ref.Digest != "" {
		manifest, err := r.images.FetchManifest(ctx, registry.Reference{
			Registry:   ref.Registry,
			Repository: ref.Repository,
			Digest:     ref.Digest,
		}, creds)
		if err != nil {
			return r.classifyResolveFailure(ctx, err)
		}
		return manifest.Digest, nil, nil
	}
	digest, err := r.images.ResolveDigest(ctx, ref, creds)
	if err != nil {
		return r.classifyResolveFailure(ctx, err)
	}
	return digest, nil, nil
}

// classifyResolveFailure maps an S2 error onto the import failure taxonomy. A
// cancelled caller context (manager shutdown) is NOT terminal: the task stays
// open and is retried after the restart.
func (r *ImportReconciler) classifyResolveFailure(
	ctx context.Context,
	err error,
) (string, *importFailure, error) {
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		return "", nil, fmt.Errorf("resolve image reference: %w", err)
	}
	failure := classifyRegistryError(err)
	return "", &failure, nil
}

// classifyRegistryError maps the frozen S2 error taxonomy onto the import
// failure codes. It uses errors.Is/errors.As only — never error text — so the
// mapping survives message changes in the S2 package.
func classifyRegistryError(err error) importFailure {
	switch {
	case errors.Is(err, registry.ErrDenied):
		return importFailure{
			code: importFailRegistryDenied,
			message: fmt.Sprintf("%s: %v — the registry host must be listed in --registry-allowlist "+
				"(SSRF guard fails closed, ADR-007)", importFailRegistryDenied, err),
		}
	case errors.Is(err, registry.ErrUnauthorized):
		return importFailure{
			code: importFailUnauthorized,
			message: fmt.Sprintf("%s: %v — verify the referenced pull secret grants pull access to this "+
				"repository; credential material is never echoed back", importFailUnauthorized, err),
		}
	case errors.Is(err, registry.ErrNotFound):
		return importFailure{
			code: importFailManifestNotFound,
			message: fmt.Sprintf("%s: %v — check that the repository and tag exist and are readable with "+
				"the given credentials", importFailManifestNotFound, err),
		}
	case errors.Is(err, registry.ErrDigestMismatch):
		return importFailure{
			code: importFailDigestMismatch,
			message: fmt.Sprintf("%s: %v — the registry served content that does not hash to the requested "+
				"digest; refusing to register it", importFailDigestMismatch, err),
		}
	case errors.Is(err, registry.ErrInvalidReference):
		return importFailure{
			code:    failCodeInvalidImageReference,
			message: fmt.Sprintf("%s: %v", failCodeInvalidImageReference, err),
		}
	case errors.Is(err, registry.ErrTimeout), errors.Is(err, context.DeadlineExceeded):
		return importFailure{
			code:    importFailTimeout,
			message: fmt.Sprintf("%s: %v — the registry did not answer in time", importFailTimeout, err),
		}
	}
	if isTimeoutError(err) {
		return importFailure{
			code:    importFailTimeout,
			message: fmt.Sprintf("%s: %v — the registry did not answer in time", importFailTimeout, err),
		}
	}
	var httpErr *registry.HTTPError
	if errors.As(err, &httpErr) {
		return importFailure{
			code: importFailRegistry,
			message: fmt.Sprintf("%s: %v — the registry answered with a non-success status; retry later "+
				"or check the registry health", importFailRegistry, err),
		}
	}
	if isTransportError(err) {
		return importFailure{
			code: importFailNetwork,
			message: fmt.Sprintf("%s: %v — the registry endpoint could not be reached; check DNS, routing, "+
				"TLS trust and that the registry is up", importFailNetwork, err),
		}
	}
	return importFailure{
		code:    importFailRegistry,
		message: fmt.Sprintf("%s: %v — unexpected registry client failure", importFailRegistry, err),
	}
}

// isTimeoutError reports a net-level timeout that the S2 client wrapped without
// its ErrTimeout sentinel.
func isTimeoutError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// isTransportError reports a failure of the HTTP round trip itself (connection
// refused, DNS failure, TLS handshake failure): a network problem, not a
// registry answer.
func isTransportError(err error) bool {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// importFailure is a terminal, diagnosable task failure.
type importFailure struct {
	// code is the frozen failure-taxonomy identifier (message prefix + metric
	// reason label).
	code string
	// message is the full human-readable diagnosis, credential-free by
	// construction.
	message string
}

// importTrigger names what created a task, for audit messages.
func importTrigger(task *imageflowv1alpha1.ImageStreamImport) string {
	if task.Labels[LabelImportSource] == LabelValueScheduledImport {
		return "scheduled"
	}
	return "on-demand"
}

func importOutcomeFor(outcome RegistrationOutcome) string {
	if outcome == RegistrationChanged {
		return importOutcomeChanged
	}
	return importOutcomeNoop
}

// importSuccessMessage is the Complete condition message of a successful
// import. It states the resolved digest, whether the catalog moved (drift) and
// what the previous association was, so the task alone answers "what happened".
func importSuccessMessage(
	task *imageflowv1alpha1.ImageStreamImport,
	digest, imageRef string,
	res RegistrationResult,
) string {
	trigger := importTrigger(task)
	if res.Outcome == RegistrationNoop {
		return fmt.Sprintf("No drift: %s:%s already references %s (imported from %s; trigger: %s) — "+
			"the external tag was re-verified, no Image Change recorded",
			task.Spec.Stream, task.Spec.Tag, digest, task.Spec.From.Name, trigger)
	}
	if res.OldDigest == "" {
		return fmt.Sprintf("Imported %s as %s onto %s:%s; first association recorded (trigger: %s)",
			task.Spec.From.Name, imageRef, task.Spec.Stream, task.Spec.Tag, trigger)
	}
	return fmt.Sprintf("Imported %s as %s onto %s:%s; tag drift detected, image change recorded "+
		"(previous: %s; trigger: %s)",
		task.Spec.From.Name, imageRef, task.Spec.Stream, task.Spec.Tag, res.OldDigest, trigger)
}

// failTask writes the terminal failure status, records metrics and emits the
// audit Event. A failed task is terminal (no requeue): it stays around for the
// failure TTL (default 24h) so the diagnosable reason can be inspected.
func (r *ImportReconciler) failTask(
	ctx context.Context,
	task *imageflowv1alpha1.ImageStreamImport,
	failure importFailure,
) (ctrl.Result, error) {
	completed, err := r.completeTask(ctx, client.ObjectKeyFromObject(task), importCompletion{
		reason:  imageflowv1alpha1.ReasonFailed,
		message: failure.message,
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("write failure status: %w", err)
	}
	r.metrics.tasks.WithLabelValues(importResultFailure, failure.code).Inc()
	r.observeTaskDuration(completed, importResultFailure)
	r.recorder.Eventf(completed, corev1.EventTypeWarning, EventReasonImportFailed, "%s", failure.message)
	log.FromContext(ctx).Info("import failed",
		"stream", task.Spec.Stream, "tag", task.Spec.Tag, "from", task.Spec.From.Name,
		"reason", failure.code, "trigger", importTrigger(completed))
	return ctrl.Result{}, nil
}

// importCompletion is the terminal status payload (Complete=True, reason from
// the frozen Succeeded/Failed vocabulary, diagnosable message, import results).
type importCompletion struct {
	reason         string
	message        string
	digest         string
	imageReference string
	artifactRef    string
}

// completeTask writes the terminal status through the status subresource with
// retry-on-conflict, pinning every timestamp to the injected clock (seam S3).
func (r *ImportReconciler) completeTask(
	ctx context.Context,
	key types.NamespacedName,
	c importCompletion,
) (*imageflowv1alpha1.ImageStreamImport, error) {
	var fresh imageflowv1alpha1.ImageStreamImport
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := r.client.Get(ctx, key, &fresh); err != nil {
			return err
		}
		now := r.clock.Now()
		st := &fresh.Status
		if st.StartedAt == nil {
			startedAt := metav1.NewTime(now)
			st.StartedAt = &startedAt
		}
		completedAt := metav1.NewTime(now)
		st.CompletedAt = &completedAt
		if c.digest != "" {
			st.Digest = c.digest
		}
		if c.imageReference != "" {
			st.ImageReference = c.imageReference
		}
		if c.artifactRef != "" {
			st.ArtifactRef = c.artifactRef
		}
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type:               imageflowv1alpha1.ConditionTypeComplete,
			Status:             metav1.ConditionTrue,
			Reason:             c.reason,
			Message:            c.message,
			ObservedGeneration: fresh.Generation,
		})
		// SetStatusCondition stamps LastTransitionTime from the wall clock; pin
		// it to the injected clock so TTL GC and tests are deterministic.
		if cond := meta.FindStatusCondition(st.Conditions, imageflowv1alpha1.ConditionTypeComplete); cond != nil {
			cond.LastTransitionTime = metav1.NewTime(now)
		}
		return r.client.Status().Update(ctx, &fresh)
	})
	if err != nil {
		return nil, err
	}
	return &fresh, nil
}

// ensureStartedAt persists startedAt (once) before any effect is performed, so
// the import duration survives a mid-import crash.
func (r *ImportReconciler) ensureStartedAt(ctx context.Context, key types.NamespacedName) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh imageflowv1alpha1.ImageStreamImport
		if err := r.client.Get(ctx, key, &fresh); err != nil {
			return err
		}
		if fresh.Status.StartedAt != nil || taskIsTerminal(&fresh.Status.TaskObjectStatus) {
			return nil
		}
		now := metav1.NewTime(r.clock.Now())
		fresh.Status.StartedAt = &now
		return r.client.Status().Update(ctx, &fresh)
	})
}

func (r *ImportReconciler) observeTaskDuration(completed *imageflowv1alpha1.ImageStreamImport, result string) {
	st := &completed.Status
	if st.StartedAt == nil || st.CompletedAt == nil {
		return
	}
	r.metrics.duration.WithLabelValues(result).Observe(st.CompletedAt.Sub(st.StartedAt.Time).Seconds())
}
