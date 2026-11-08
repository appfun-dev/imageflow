package controller

// T5 (issue #26, ADR-012) envtest suite: the complete delivery flow of all
// three imageReferenceFormat values verified against a real API server —
// written container image values, status.targets[] (desiredImage /
// resolvedDigest / imageApplied), recentDeliveries records, delivery Events,
// format switching and the downgrade protection. subscription_delivery_
// controller_test.go covers the format branches against an SSA-emulating fake
// client (T4); this suite proves the same contracts against real Server-Side
// Apply and real admission webhooks:
//
//   - Lane A (TestDeliveryFormatEnvtestDigest*/Tag*/DigestWithTag*): the
//     shared buildrun-watcher environment WITHOUT webhooks, reconciler driven
//     directly with a FakeRecorder and the injected clock. A subscription
//     whose imageReferenceFormat is unset KEEPS it unset here (no mutating
//     webhook), which isolates the controller-side effectiveDeliveryFormat
//     fallback — the backward-compatibility path for pre-v1.2 objects (AC4).
//   - Lane B (TestDeliveryFormatEnvtestFormatDowngradeRejected): a dedicated
//     environment with the generated webhook configurations installed and the
//     production Register seam serving them, plus the delivery controller on a
//     manager — the format upgrade is admitted and re-delivered, the
//     downgrade to Digest is rejected by vimagesubscription.kb.io and never
//     reaches the workload (AC5).

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	crwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
	"github.com/appfun-dev/imageflow/internal/webhook"
)

const (
	t5fStream    = "checkout"
	t5fDeploy    = "checkout-api"
	t5fContainer = "app"
	t5fTag       = "live"
	t5fRepo      = "quay.io/example/checkout"
	t5fPreImage  = "quay.io/example/checkout:live-20260831"
)

var (
	t5fDigestA    = t5fRepo + "@sha256:" + strings.Repeat("1b", 32)
	t5fDigestB    = t5fRepo + "@sha256:" + strings.Repeat("2c", 32)
	t5fTagRef     = t5fRepo + ":" + t5fTag
	t5fCompositeA = t5fTagRef + "@sha256:" + strings.Repeat("1b", 32)
	t5fCompositeB = t5fTagRef + "@sha256:" + strings.Repeat("2c", 32)
	t5fT0         = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
)

// --- lane A fixture: hand-driven reconciler, no webhooks ---------------------

type t5fFixture struct {
	reconciler *SubscriptionDeliveryReconciler
	client     client.Client
	recorder   *record.FakeRecorder
	clock      *clock.Fake
	ns         string
}

func newT5fFixture(t *testing.T) *t5fFixture {
	t.Helper()
	env := sharedBuildRunEnvtestEnv(t)
	rec := record.NewFakeRecorder(64)
	clk := clock.NewFake(t5fT0)
	reconciler := NewSubscriptionDeliveryReconciler(
		env.client, env.client, rec, clk, config.Config{}, nil)
	return &t5fFixture{
		reconciler: reconciler,
		client:     env.client,
		recorder:   rec,
		clock:      clk,
		ns:         brwCreateNamespace(t, env.client),
	}
}

//nolint:unparam // test helper: name param kept for API consistency with production patterns
func t5fCreateStream(t *testing.T, cl client.Client, ns, name, ref string) {
	t.Helper()
	stream := &imageflowv1alpha1.ImageStream{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
	}
	if err := cl.Create(context.Background(), stream); err != nil {
		t.Fatalf("create imagestream: %v", err)
	}
	t5fSetStreamRef(t, cl, ns, name, ref, t5fT0.Add(-time.Minute))
}

// t5fSetStreamRef re-points the catalog association at a new digest, the way
// a Mapping/Import registration would (Image Change trigger).
func t5fSetStreamRef(t *testing.T, cl client.Client, ns, name, ref string, created time.Time) {
	t.Helper()
	stream := &imageflowv1alpha1.ImageStream{}
	key := types.NamespacedName{Namespace: ns, Name: name}
	if err := cl.Get(context.Background(), key, stream); err != nil {
		t.Fatalf("get imagestream %s: %v", key, err)
	}
	stream.Status.Tags = []imageflowv1alpha1.NamedTagEventList{{
		Tag: t5fTag,
		Items: []imageflowv1alpha1.TagEvent{{
			ImageReference: ref,
			Digest:         t5fDigestPart(ref),
			Created:        metav1.NewTime(created),
		}},
	}}
	if err := cl.Status().Update(context.Background(), stream); err != nil {
		t.Fatalf("update imagestream status: %v", err)
	}
}

func t5fDigestPart(ref string) string {
	_, digest, _ := strings.Cut(ref, "@")
	return digest
}

//nolint:unparam // test helper: name param kept for API consistency with production patterns
func t5fCreateDeployment(t *testing.T, cl client.Client, ns, name, image string) {
	t.Helper()
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(0)),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: t5fContainer, Image: image}},
				},
			},
		},
	}
	if err := cl.Create(context.Background(), deploy); err != nil {
		t.Fatalf("create deployment: %v", err)
	}
}

// t5fCreateAuthorizedSubscription creates the subscription and stamps the T8
// gate surface in its passed state (Forbidden=False) — the precondition for
// any delivery write, normally produced by the authz controller.
func t5fCreateAuthorizedSubscription(
	t *testing.T, cl client.Client, ns, name string,
	format imageflowv1alpha1.ImageReferenceFormat,
) {
	t.Helper()
	sub := &imageflowv1alpha1.ImageSubscription{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: imageflowv1alpha1.ImageSubscriptionSpec{
			Source: imageflowv1alpha1.SubscriptionSourceRef{
				Kind:      t9cSourceKind,
				Name:      t5fStream + ":" + t5fTag,
				Namespace: ns,
			},
			Targets: []imageflowv1alpha1.SubscriptionTarget{{
				TargetRef: imageflowv1alpha1.TargetReference{
					APIVersion: t9cAPIApps,
					Kind:       t9cKindDeploy,
					Namespace:  ns,
					Name:       t5fDeploy,
				},
				ContainerNames: []string{t5fContainer},
			}},
			ImageReferenceFormat: format,
		},
	}
	if err := cl.Create(context.Background(), sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	meta.SetStatusCondition(&sub.Status.Conditions, metav1.Condition{
		Type:               imageflowv1alpha1.ConditionTypeForbidden,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonAuthorized,
		Message:            "t5 fixture: creator passed the T8 gate",
		LastTransitionTime: metav1.NewTime(t5fT0),
		ObservedGeneration: sub.Generation,
	})
	if err := cl.Status().Update(context.Background(), sub); err != nil {
		t.Fatalf("stamp Forbidden condition: %v", err)
	}
}

// t5fReconcile drives one delivery pass; a successful flow settles silently
// (no error, no requeue).
func t5fReconcile(t *testing.T, f *t5fFixture, subName string) {
	t.Helper()
	res, err := f.reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: f.ns, Name: subName},
	})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("delivery must settle silently, got %+v", res)
	}
}

func t5fGetSub(t *testing.T, cl client.Client, ns, name string) *imageflowv1alpha1.ImageSubscription {
	t.Helper()
	sub := &imageflowv1alpha1.ImageSubscription{}
	key := types.NamespacedName{Namespace: ns, Name: name}
	if err := cl.Get(context.Background(), key, sub); err != nil {
		t.Fatalf("get subscription %s: %v", key, err)
	}
	return sub
}

// t5fDeploymentImageE is the polling-safe variant (empty string on any read
// error); t5fDeploymentImage is the fatal-on-error variant for lane A.
func t5fDeploymentImageE(cl client.Client, ns, name string) string {
	deploy := &appsv1.Deployment{}
	key := types.NamespacedName{Namespace: ns, Name: name}
	if err := cl.Get(context.Background(), key, deploy); err != nil {
		return ""
	}
	if len(deploy.Spec.Template.Spec.Containers) == 0 {
		return ""
	}
	return deploy.Spec.Template.Spec.Containers[0].Image
}

//nolint:unparam // test helper: name param kept for API consistency with production patterns
func t5fDeploymentImage(t *testing.T, cl client.Client, ns, name string) string {
	t.Helper()
	deploy := &appsv1.Deployment{}
	key := types.NamespacedName{Namespace: ns, Name: name}
	if err := cl.Get(context.Background(), key, deploy); err != nil {
		t.Fatalf("get deployment %s: %v", key, err)
	}
	return deploy.Spec.Template.Spec.Containers[0].Image
}

func t5fDeploymentRV(t *testing.T, cl client.Client, ns, name string) string {
	t.Helper()
	deploy := &appsv1.Deployment{}
	key := types.NamespacedName{Namespace: ns, Name: name}
	if err := cl.Get(context.Background(), key, deploy); err != nil {
		t.Fatalf("get deployment %s: %v", key, err)
	}
	return deploy.ResourceVersion
}

// t5fUpdateFormat switches spec.imageReferenceFormat with conflict retry —
// lane B runs the delivery controller concurrently, so the status can move
// between the read and the write.
func t5fUpdateFormat(
	t *testing.T, cl client.Client, ns, name string,
	format imageflowv1alpha1.ImageReferenceFormat,
) error {
	t.Helper()
	key := types.NamespacedName{Namespace: ns, Name: name}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sub := &imageflowv1alpha1.ImageSubscription{}
		if err := cl.Get(context.Background(), key, sub); err != nil {
			return err
		}
		sub.Spec.ImageReferenceFormat = format
		return cl.Update(context.Background(), sub)
	})
}

// --- AC1 + AC4: Digest and unset format keep the v1 behavior -----------------

// TestDeliveryFormatEnvtestDigestKeepsV1Behavior pins backward compatibility
// against real SSA: an explicit Digest format AND a pre-v1.2 object whose
// imageReferenceFormat is unset both write the plain digest reference, with
// resolvedDigest staying empty. The webhook-less environment keeps the unset
// field unset on the stored object, proving the controller-side
// effectiveDeliveryFormat fallback — not admission defaulting — preserves v1.
func TestDeliveryFormatEnvtestDigestKeepsV1Behavior(t *testing.T) {
	for _, tc := range []struct {
		name    string
		subName string
		format  imageflowv1alpha1.ImageReferenceFormat
	}{
		{"unset format (pre-v1.2 object)", "format-unset", ""},
		{"explicit Digest", "format-digest", imageflowv1alpha1.ImageReferenceFormatDigest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newT5fFixture(t)
			t5fCreateStream(t, f.client, f.ns, t5fStream, t5fDigestA)
			t5fCreateDeployment(t, f.client, f.ns, t5fDeploy, t5fPreImage)
			t5fCreateAuthorizedSubscription(t, f.client, f.ns, tc.subName, tc.format)

			t5fReconcile(t, f, tc.subName)

			if image := t5fDeploymentImage(t, f.client, f.ns, t5fDeploy); image != t5fDigestA {
				t.Fatalf("container image = %q, want the v1 digest reference %q", image, t5fDigestA)
			}
			sub := t5fGetSub(t, f.client, f.ns, tc.subName)
			if tc.format == "" && sub.Spec.ImageReferenceFormat != "" {
				t.Fatalf("no mutating webhook here: stored format must stay unset, got %q",
					sub.Spec.ImageReferenceFormat)
			}
			entry := t9cEntry(sub, t5fDeploy)
			if entry == nil || entry.DesiredImage != t5fDigestA || entry.ObservedImage != t5fDigestA ||
				entry.ResolvedDigest != "" || !entry.ImageApplied || entry.Reason != TargetReasonApplied ||
				entry.RolloutStatus != imageflowv1alpha1.RolloutProgressing {
				t.Fatalf("Digest entry mismatch (resolvedDigest must stay empty): %+v", entry)
			}
			if len(sub.Status.RecentDeliveries) != 1 ||
				sub.Status.RecentDeliveries[0].NewImage != t5fDigestA ||
				sub.Status.RecentDeliveries[0].Reason != TargetReasonApplied {
				t.Fatalf("delivery record must carry the digest reference: %+v",
					sub.Status.RecentDeliveries)
			}
			events := drainFakeEvents(f.recorder)
			if len(events) != 2 || countEventsWithReason(events, EventReasonImageApplied) != 2 {
				t.Fatalf("expected dual-namespace ImageApplied events, got %v", events)
			}
		})
	}
}

// --- AC2: Tag writes repo:tag, resolvedDigest reports the digest truth -------

// TestDeliveryFormatEnvtestTagWritesTagAndResolvedDigest pins the Tag
// contract end-to-end: the container image and desired/observed status carry
// <repo>:<tag> while resolvedDigest reports the digest truth; ImageApplied
// compares the tag string (spec decision 7) — a new digest behind the same
// tag rewrites nothing, records no delivery and flips resolvedDigest only.
func TestDeliveryFormatEnvtestTagWritesTagAndResolvedDigest(t *testing.T) {
	f := newT5fFixture(t)
	const subName = "format-tag"
	t5fCreateStream(t, f.client, f.ns, t5fStream, t5fDigestA)
	t5fCreateDeployment(t, f.client, f.ns, t5fDeploy, t5fPreImage)
	t5fCreateAuthorizedSubscription(t, f.client, f.ns, subName, imageflowv1alpha1.ImageReferenceFormatTag)

	t5fReconcile(t, f, subName)

	if image := t5fDeploymentImage(t, f.client, f.ns, t5fDeploy); image != t5fTagRef {
		t.Fatalf("container image = %q, want the tag reference %q", image, t5fTagRef)
	}
	sub := t5fGetSub(t, f.client, f.ns, subName)
	entry := t9cEntry(sub, t5fDeploy)
	if entry == nil || entry.DesiredImage != t5fTagRef || entry.ObservedImage != t5fTagRef ||
		entry.ResolvedDigest != t5fDigestA || !entry.ImageApplied || entry.Reason != TargetReasonApplied {
		t.Fatalf("Tag entry mismatch: %+v", entry)
	}
	if len(sub.Status.RecentDeliveries) != 1 || sub.Status.RecentDeliveries[0].NewImage != t5fDigestA {
		t.Fatalf("record must keep the digest truth (NewImage pattern is digest-only): %+v",
			sub.Status.RecentDeliveries)
	}
	events := drainFakeEvents(f.recorder)
	if len(events) != 2 || countEventsWithReason(events, EventReasonImageApplied) != 2 ||
		!strings.Contains(events[0], t5fTagRef) {
		t.Fatalf("delivery events must name the written tag reference: %v", events)
	}

	// Steady state: tag-string equality suppresses every write (no apply, no
	// status write, no events).
	f.clock.Advance(time.Minute)
	deployRV := t5fDeploymentRV(t, f.client, f.ns, t5fDeploy)
	subRV := t5fGetSub(t, f.client, f.ns, subName).ResourceVersion
	t5fReconcile(t, f, subName)
	if rv := t5fDeploymentRV(t, f.client, f.ns, t5fDeploy); rv != deployRV {
		t.Fatalf("steady state must not rewrite the workload (resourceVersion %s -> %s)", deployRV, rv)
	}
	if rv := t5fGetSub(t, f.client, f.ns, subName).ResourceVersion; rv != subRV {
		t.Fatalf("steady state must not rewrite status (resourceVersion %s -> %s)", subRV, rv)
	}
	if events := drainFakeEvents(f.recorder); len(events) != 0 {
		t.Fatalf("steady state must be event-free, got %v", events)
	}

	// Tag drift (spec decision 7): the new digest behind the same tag flips
	// resolvedDigest only — ImageApplied compares the tag string, so the
	// workload keeps the tag, no delivery is recorded, no events are emitted.
	f.clock.Advance(time.Minute)
	t5fSetStreamRef(t, f.client, f.ns, t5fStream, t5fDigestB, t5fT0.Add(2*time.Minute))
	t5fReconcile(t, f, subName)
	if rv := t5fDeploymentRV(t, f.client, f.ns, t5fDeploy); rv != deployRV {
		t.Fatalf("same tag string must not rewrite the workload (resourceVersion %s -> %s)", deployRV, rv)
	}
	if image := t5fDeploymentImage(t, f.client, f.ns, t5fDeploy); image != t5fTagRef {
		t.Fatalf("tag drift must not rewrite the field, got %q", image)
	}
	sub = t5fGetSub(t, f.client, f.ns, subName)
	entry = t9cEntry(sub, t5fDeploy)
	if entry == nil || entry.DesiredImage != t5fTagRef || entry.ResolvedDigest != t5fDigestB ||
		!entry.ImageApplied || entry.Reason != TargetReasonApplied {
		t.Fatalf("drift entry mismatch: %+v", entry)
	}
	if len(sub.Status.RecentDeliveries) != 1 {
		t.Fatalf("a same-tag drift is not a delivery: %+v", sub.Status.RecentDeliveries)
	}
	if events := drainFakeEvents(f.recorder); len(events) != 0 {
		t.Fatalf("a same-tag drift must be event-free, got %v", events)
	}
}

// --- AC3: DigestWithTag writes the composite reference -----------------------

// TestDeliveryFormatEnvtestDigestWithTagWritesComposite pins the composite
// contract end-to-end: the container image carries <repo>:<tag>@sha256:<hex>
// and resolvedDigest the plain digest reference; a digest change re-renders
// the composite, so it is a real value-changing delivery (second record).
func TestDeliveryFormatEnvtestDigestWithTagWritesComposite(t *testing.T) {
	f := newT5fFixture(t)
	const subName = "format-composite"
	t5fCreateStream(t, f.client, f.ns, t5fStream, t5fDigestA)
	t5fCreateDeployment(t, f.client, f.ns, t5fDeploy, t5fPreImage)
	t5fCreateAuthorizedSubscription(t, f.client, f.ns, subName,
		imageflowv1alpha1.ImageReferenceFormatDigestWithTag)

	t5fReconcile(t, f, subName)

	if image := t5fDeploymentImage(t, f.client, f.ns, t5fDeploy); image != t5fCompositeA {
		t.Fatalf("container image = %q, want the composite %q", image, t5fCompositeA)
	}
	sub := t5fGetSub(t, f.client, f.ns, subName)
	entry := t9cEntry(sub, t5fDeploy)
	if entry == nil || entry.DesiredImage != t5fCompositeA || entry.ObservedImage != t5fCompositeA ||
		entry.ResolvedDigest != t5fDigestA || !entry.ImageApplied || entry.Reason != TargetReasonApplied {
		t.Fatalf("composite entry mismatch: %+v", entry)
	}
	events := drainFakeEvents(f.recorder)
	if len(events) != 2 || countEventsWithReason(events, EventReasonImageApplied) != 2 ||
		!strings.Contains(events[0], t5fCompositeA) {
		t.Fatalf("delivery events must name the written composite: %v", events)
	}

	// A new digest re-renders the composite: value-changing delivery.
	f.clock.Advance(time.Minute)
	t5fSetStreamRef(t, f.client, f.ns, t5fStream, t5fDigestB, t5fT0.Add(2*time.Minute))
	t5fReconcile(t, f, subName)
	if image := t5fDeploymentImage(t, f.client, f.ns, t5fDeploy); image != t5fCompositeB {
		t.Fatalf("container image = %q, want the new composite %q", image, t5fCompositeB)
	}
	sub = t5fGetSub(t, f.client, f.ns, subName)
	entry = t9cEntry(sub, t5fDeploy)
	if entry == nil || entry.DesiredImage != t5fCompositeB || entry.ResolvedDigest != t5fDigestB ||
		!entry.ImageApplied {
		t.Fatalf("re-delivered composite entry mismatch: %+v", entry)
	}
	if len(sub.Status.RecentDeliveries) != 2 || sub.Status.RecentDeliveries[0].NewImage != t5fDigestB {
		t.Fatalf("composite re-delivery must be recorded: %+v", sub.Status.RecentDeliveries)
	}
	// Dual-namespace per reason: the value-changing delivery plus the
	// latest-wins Superseded audit of the in-flight previous record (ADR-008).
	events = drainFakeEvents(f.recorder)
	if countEventsWithReason(events, EventReasonImageApplied) != 2 ||
		countEventsWithReason(events, EventReasonDeliverySuperseded) != 2 {
		t.Fatalf("composite re-delivery events mismatch: %v", events)
	}
}

// --- AC5: format switching + downgrade protection, full admission chain ------

// TestDeliveryFormatEnvtestFormatDowngradeRejected runs the delivery
// controller and ALL admission webhooks against one API server: a Tag
// subscription delivers the tag reference, upgrading to DigestWithTag is
// admitted and re-delivers the composite, and the downgrade back to Digest is
// rejected by vimagesubscription.kb.io — the rejected update never reaches
// the controller, so the workload keeps the composite.
func TestDeliveryFormatEnvtestFormatDowngradeRejected(t *testing.T) {
	assets := buildRunWatcherEnvtestAssets(t)
	if assets == "" {
		t.Skip("envtest assets not found: set KUBEBUILDER_ASSETS or run `make envtest`")
	}
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	env := &envtest.Environment{
		BinaryAssetsDirectory: assets,
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			Paths: []string{filepath.Join("..", "..", "config", "webhook")},
		},
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Logf("stop envtest: %v", err)
		}
	})

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := imageflowv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add imageflow scheme: %v", err)
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
		Controller:             ctrlconfig.Controller{SkipNameValidation: ptr.To(true)},
		WebhookServer: crwebhook.NewServer(crwebhook.Options{
			Host:    env.WebhookInstallOptions.LocalServingHost,
			Port:    env.WebhookInstallOptions.LocalServingPort,
			CertDir: env.WebhookInstallOptions.LocalServingCertDir,
		}),
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	webhook.Register(mgr)
	if err := RegisterSubscriptionDelivery(mgr, "t5-format-delivery",
		Options{Clock: clock.Real{}, Config: config.Config{}}); err != nil {
		t.Fatalf("register delivery controller: %v", err)
	}

	mgrCtx, mgrCancel := context.WithCancel(context.Background())
	t.Cleanup(mgrCancel)
	mgrErr := make(chan error, 1)
	go func() { mgrErr <- mgr.Start(mgrCtx) }()
	started := mgr.GetWebhookServer().StartedChecker()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if err := started(nil); err == nil {
			break
		}
		select {
		case err := <-mgrErr:
			t.Fatalf("manager exited before the webhook server started: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("webhook server did not start within 20s")
		}
		time.Sleep(100 * time.Millisecond)
	}

	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ns := brwCreateNamespace(t, cl)
	const subName = "downgrade-guard"
	t5fCreateStream(t, cl, ns, t5fStream, t5fDigestA)
	t5fCreateDeployment(t, cl, ns, t5fDeploy, t5fPreImage)
	t5fCreateAuthorizedSubscription(t, cl, ns, subName, imageflowv1alpha1.ImageReferenceFormatTag)

	// The delivery controller serves the Tag subscription end-to-end.
	brwEventually(t, "tag reference delivered", func() error {
		if image := t5fDeploymentImageE(cl, ns, t5fDeploy); image != t5fTagRef {
			return fmt.Errorf("container image = %q, want %q", image, t5fTagRef)
		}
		return nil
	})
	brwEventually(t, "tag delivery reported on status", func() error {
		sub := &imageflowv1alpha1.ImageSubscription{}
		if err := cl.Get(context.Background(),
			types.NamespacedName{Namespace: ns, Name: subName}, sub); err != nil {
			return err
		}
		entry := t9cEntry(sub, t5fDeploy)
		if entry == nil || entry.Reason != TargetReasonApplied || !entry.ImageApplied ||
			entry.DesiredImage != t5fTagRef || entry.ResolvedDigest != t5fDigestA {
			return fmt.Errorf("tag entry not converged: %+v", entry)
		}
		return nil
	})

	// Upgrade Tag → DigestWithTag is admitted (lateral move) and the
	// controller re-delivers the composite reference.
	if err := t5fUpdateFormat(t, cl, ns, subName,
		imageflowv1alpha1.ImageReferenceFormatDigestWithTag); err != nil {
		t.Fatalf("upgrade to DigestWithTag must be admitted: %v", err)
	}
	brwEventually(t, "composite delivered after upgrade", func() error {
		if image := t5fDeploymentImageE(cl, ns, t5fDeploy); image != t5fCompositeA {
			return fmt.Errorf("container image = %q, want %q", image, t5fCompositeA)
		}
		return nil
	})

	// Downgrade DigestWithTag → Digest is rejected by the validating webhook
	// (T3, #24): the reference shape would silently change for every target.
	err = t5fUpdateFormat(t, cl, ns, subName, imageflowv1alpha1.ImageReferenceFormatDigest)
	if err == nil {
		t.Fatalf("format downgrade to Digest must be rejected")
	}
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "downgrade") ||
		!strings.Contains(err.Error(), "vimagesubscription.kb.io") {
		t.Fatalf("downgrade rejection must come from vimagesubscription.kb.io: %v", err)
	}

	// The rejected downgrade never reached the controller: the workload keeps
	// the composite reference.
	brwConsistently(t, 2*time.Second, "workload keeps composite after rejected downgrade", func() error {
		if image := t5fDeploymentImageE(cl, ns, t5fDeploy); image != t5fCompositeA {
			return fmt.Errorf("workload rewritten despite rejected downgrade: %q", image)
		}
		return nil
	})
}
