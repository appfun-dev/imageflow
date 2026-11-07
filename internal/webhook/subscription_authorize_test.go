package webhook

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/authz"
	"github.com/appfun-dev/imageflow/internal/clock"
)

// T8 admission-authorization unit suite (fake client + interceptor, per spec
// testing decision 2: SAR 门两个方向 allow/deny). These tests pin the handler
// behavior of the advisory admission half of the gate: denials and unresolved
// checks surface as admission WARNINGS (enforcement is the controller-side
// Forbidden condition + zero writes), targets-change re-checks run, and the
// T3 shape rules stay untouched, rejection-owning and first in line.

const (
	t8CreatorValue = "user:t8-creator@example.com"
	t8CreatorUser  = "t8-creator@example.com"
	t8SACreator    = "serviceAccount:team-cicd:pipeline-bot"
	t8SourceName   = "billing-api:stable"
	t8AppTarget    = "billing-api"
	t8DenyMessage  = `User "t8-creator@example.com" cannot update resource "deployments"`
)

type t8SarRecorder struct {
	calls     int
	users     []string
	decide    func(sar *authorizationv1.SubjectAccessReview)
	createErr error
}

func newT8AuthorizerFixture(t *testing.T, opts authz.Options) (*subscriptionValidator, *t8SarRecorder) {
	t.Helper()
	rec := &t8SarRecorder{decide: func(sar *authorizationv1.SubjectAccessReview) {
		sar.Status.Allowed = true
	}}
	cl := fake.NewClientBuilder().
		WithScheme(t8Scheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object,
				_ ...client.CreateOption) error {
				sar, ok := obj.(*authorizationv1.SubjectAccessReview)
				if !ok {
					return c.Create(ctx, obj)
				}
				rec.calls++
				rec.users = append(rec.users, sar.Spec.User)
				if rec.createErr != nil {
					return rec.createErr
				}
				rec.decide(sar)
				return nil
			},
		}).Build()

	start, err := time.Parse(time.RFC3339, "2026-06-01T00:00:00Z")
	if err != nil {
		t.Fatalf("parse fixture start: %v", err)
	}
	if opts.Clock == nil {
		opts.Clock = clock.NewFake(start)
	}
	if opts.Metrics == nil {
		opts.Metrics = authz.NewMetrics()
	}
	authorizer := &subscriptionAuthorizer{gate: authz.NewGate(cl, t8Mapper(), opts)}
	return &subscriptionValidator{authorizer: authorizer}, rec
}

func t8Scheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}
	if err := authorizationv1.AddToScheme(s); err != nil {
		t.Fatalf("add authorization scheme: %v", err)
	}
	return s
}

func t8Mapper() meta.RESTMapper {
	m := meta.NewDefaultRESTMapper([]schema.GroupVersion{appsv1.SchemeGroupVersion})
	m.Add(appsv1.SchemeGroupVersion.WithKind("Deployment"), meta.RESTScopeNamespace)
	m.Add(appsv1.SchemeGroupVersion.WithKind("StatefulSet"), meta.RESTScopeNamespace)
	return m
}

func t8Target(name, kind string) imageflowv1alpha1.SubscriptionTarget {
	return imageflowv1alpha1.SubscriptionTarget{
		TargetRef: imageflowv1alpha1.TargetReference{
			APIVersion: "apps/v1",
			Kind:       kind,
			Namespace:  "team-billing",
			Name:       name,
		},
		ContainerNames: []string{"app"},
	}
}

func t8Subscription(name, createdBy string, targets ...imageflowv1alpha1.SubscriptionTarget) *imageflowv1alpha1.ImageSubscription {
	sub := &imageflowv1alpha1.ImageSubscription{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-billing", Name: name},
		Spec: imageflowv1alpha1.ImageSubscriptionSpec{
			Source: imageflowv1alpha1.SubscriptionSourceRef{
				Kind:      "ImageStreamTag",
				Name:      t8SourceName,
				Namespace: "imageflow-system",
			},
			Targets: targets,
		},
	}
	if createdBy != "" {
		sub.Annotations = map[string]string{CreatedByAnnotation: createdBy}
	}
	return sub
}

func TestValidateCreateAllowsAuthorizedCreator(t *testing.T) {
	validator, rec := newT8AuthorizerFixture(t, authz.Options{})
	sub := t8Subscription("billing-stable", t8CreatorValue, t8Target(t8AppTarget, "Deployment"))

	warnings, err := validator.ValidateCreate(context.Background(), sub)

	if err != nil {
		t.Fatalf("ValidateCreate() error = %v, want allowed", err)
	}
	if len(warnings) != 0 {
		t.Errorf("warnings = %v, want none", warnings)
	}
	if rec.calls != 1 || rec.users[0] != t8CreatorUser {
		t.Errorf("SAR calls = %d for users %v, want exactly one check of %q",
			rec.calls, rec.users, t8CreatorUser)
	}
}

func TestValidateCreateWarnsOnUnauthorizedTarget(t *testing.T) {
	validator, rec := newT8AuthorizerFixture(t, authz.Options{})
	rec.decide = func(sar *authorizationv1.SubjectAccessReview) {
		sar.Status.Allowed = false
		sar.Status.Reason = t8DenyMessage
	}
	sub := t8Subscription("billing-stable", t8CreatorValue, t8Target(t8AppTarget, "Deployment"))

	warnings, err := validator.ValidateCreate(context.Background(), sub)

	if err != nil {
		t.Fatalf("ValidateCreate() error = %v; the advisory half must not deny "+
			"(enforcement is the controller-side Forbidden condition)", err)
	}
	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want exactly one authorization warning", warnings)
	}
	for _, want := range []string{t8CreatorUser, t8AppTarget, "lacks update permission",
		t8DenyMessage, "ZERO writes", "Forbidden"} {
		if !strings.Contains(warnings[0], want) {
			t.Errorf("warning %q must mention %q", warnings[0], want)
		}
	}
}

func TestValidateCreateWarnsWithoutTrustedIdentity(t *testing.T) {
	validator, rec := newT8AuthorizerFixture(t, authz.Options{})
	sub := t8Subscription("no-identity", "", t8Target(t8AppTarget, "Deployment"))

	warnings, err := validator.ValidateCreate(context.Background(), sub)

	if err != nil {
		t.Fatalf("ValidateCreate() error = %v, want the advisory warning path", err)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "MissingIdentity") {
		t.Fatalf("warnings = %v, want one warning announcing the fail-closed MissingIdentity state",
			warnings)
	}
	if !strings.Contains(warnings[0], CreatedByAnnotation) {
		t.Errorf("warning %q must name the missing annotation", warnings[0])
	}
	if rec.calls != 0 {
		t.Errorf("SAR calls = %d, want 0 (no identity, nothing to check)", rec.calls)
	}
}

func TestValidateCreateWarnsOnMalformedIdentity(t *testing.T) {
	validator, _ := newT8AuthorizerFixture(t, authz.Options{})
	sub := t8Subscription("bad-identity", "robot:not-a-known-kind", t8Target(t8AppTarget, "Deployment"))

	warnings, err := validator.ValidateCreate(context.Background(), sub)

	if err != nil {
		t.Fatalf("ValidateCreate() error = %v, want the advisory warning path", err)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "unknown identity kind") {
		t.Fatalf("warnings = %v, want one warning carrying the parse failure", warnings)
	}
}

func TestValidateUpdateWithoutTargetsChangeSkipsSAR(t *testing.T) {
	validator, rec := newT8AuthorizerFixture(t, authz.Options{})
	old := t8Subscription("billing-stable", t8CreatorValue, t8Target(t8AppTarget, "Deployment"))
	updated := old.DeepCopy()
	updated.Annotations = map[string]string{"team-note": "unrelated edit"}

	_, err := validator.ValidateUpdate(context.Background(), old, updated)

	if err != nil {
		t.Fatalf("ValidateUpdate() error = %v, want allowed", err)
	}
	if rec.calls != 0 {
		t.Errorf("SAR calls = %d, want 0 when spec.targets is unchanged "+
			"(RBAC drift is the periodic re-verification's job)", rec.calls)
	}
}

func TestValidateUpdateWithNewUnauthorizedTargetWarns(t *testing.T) {
	validator, rec := newT8AuthorizerFixture(t, authz.Options{})
	rec.decide = func(sar *authorizationv1.SubjectAccessReview) {
		if sar.Spec.ResourceAttributes.Name == "audit-log" {
			sar.Status.Allowed = false
			sar.Status.Reason = `cannot update resource "statefulsets"`
			return
		}
		sar.Status.Allowed = true
	}
	old := t8Subscription("billing-stable", t8CreatorValue, t8Target(t8AppTarget, "Deployment"))
	updated := old.DeepCopy()
	updated.Spec.Targets = append(updated.Spec.Targets, t8Target("audit-log", "StatefulSet"))

	warnings, err := validator.ValidateUpdate(context.Background(), old, updated)

	if err != nil {
		t.Fatalf("ValidateUpdate() error = %v, want the advisory warning path", err)
	}
	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want exactly one authorization warning", warnings)
	}
	if !strings.Contains(warnings[0], "audit-log") {
		t.Errorf("warning %q must name the new denied target", warnings[0])
	}
	if strings.Contains(warnings[0], t8DenyMessage) {
		t.Errorf("warning %q must not misreport the authorized target", warnings[0])
	}
	if rec.calls != 2 {
		t.Errorf("SAR calls = %d, want 2 (both targets re-checked on targets change)", rec.calls)
	}
}

func TestValidateCreateRateLimitedDefersToControllerReverification(t *testing.T) {
	validator, rec := newT8AuthorizerFixture(t, authz.Options{LimiterQPS: 1, LimiterBurst: 1})
	sub := t8Subscription("burst", t8CreatorValue,
		t8Target("burst-one", "Deployment"), t8Target("burst-two", "StatefulSet"))

	warnings, err := validator.ValidateCreate(context.Background(), sub)

	if err != nil {
		t.Fatalf("ValidateCreate() error = %v, want the advisory warning path", err)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "burst-two") ||
		!strings.Contains(warnings[0], authz.ReasonRateLimited) {
		t.Fatalf("warnings = %v, want one warning naming the shed target and the RateLimited code",
			warnings)
	}
	if !strings.Contains(warnings[0], "zero writes") {
		t.Errorf("warning %q must state the fail-closed contract", warnings[0])
	}
	if rec.calls != 1 {
		t.Errorf("SAR calls = %d, want 1 (the shed check must not reach the API)", rec.calls)
	}
}

func TestValidateCreateSARFailureWarns(t *testing.T) {
	validator, sarRec := newT8AuthorizerFixture(t, authz.Options{})
	sarRec.createErr = errors.New("apiserver unreachable")
	sub := t8Subscription("outage", t8SACreator, t8Target(t8AppTarget, "Deployment"))

	warnings, err := validator.ValidateCreate(context.Background(), sub)

	if err != nil {
		t.Fatalf("ValidateCreate() error = %v, want the advisory warning path", err)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], authz.ReasonSubjectAccessReviewFailed) {
		t.Fatalf("warnings = %v, want one warning carrying the SAR failure code", warnings)
	}
}

func TestValidateCreateUnmappedKindAllowsWithWarning(t *testing.T) {
	validator, rec := newT8AuthorizerFixture(t, authz.Options{})
	thirdParty := imageflowv1alpha1.SubscriptionTarget{
		TargetRef: imageflowv1alpha1.TargetReference{
			APIVersion: "example.io/v1",
			Kind:       "WebService",
			Namespace:  "team-billing",
			Name:       "edge-fabric",
		},
		ImageFields: []string{".spec.runtime.containers[name=edge].image"},
	}
	sub := t8Subscription("crd-pending", t8CreatorValue, thirdParty)

	warnings, err := validator.ValidateCreate(context.Background(), sub)

	if err != nil {
		t.Fatalf("ValidateCreate() error = %v, want allowed-with-warning for an uninstalled kind", err)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "edge-fabric") {
		t.Fatalf("warnings = %v, want one warning naming the target", warnings)
	}
	if !strings.Contains(warnings[0], authz.ReasonKindNotInstalled) ||
		!strings.Contains(warnings[0], "re-verifies") {
		t.Errorf("warning %q must carry the KindNotInstalled code and point at the "+
			"periodic re-verification", warnings[0])
	}
	if rec.calls != 0 {
		t.Errorf("SAR calls = %d, want 0 for an unmapped kind", rec.calls)
	}
}

func TestShapeViolationsPrecedeAuthorization(t *testing.T) {
	validator, rec := newT8AuthorizerFixture(t, authz.Options{})
	rec.decide = func(sar *authorizationv1.SubjectAccessReview) {
		sar.Status.Allowed = false
	}
	sub := t8Subscription("bad-shape", t8CreatorValue, t8Target(t8AppTarget, "Deployment"))
	// Both locators set: a T3 shape violation, which must surface unchanged
	// and before any SAR is attempted (additive integration proof).
	sub.Spec.Targets[0].ImageFields = []string{".spec.template.spec.containers[name=app].image"}

	_, err := validator.ValidateCreate(context.Background(), sub)

	if !apierrors.IsInvalid(err) {
		t.Fatalf("error = %v, want the T3 Invalid shape rejection", err)
	}
	if rec.calls != 0 {
		t.Errorf("SAR calls = %d, want 0 (shape runs first)", rec.calls)
	}
}

func TestShapeOnlyValidatorWithoutAuthorizer(t *testing.T) {
	validator := &subscriptionValidator{}
	sub := t8Subscription("no-gate", "", t8Target(t8AppTarget, "Deployment"))

	if _, err := validator.ValidateCreate(context.Background(), sub); err != nil {
		t.Fatalf("zero-value validator must stay shape-only, got %v", err)
	}
}

func TestSARSubjectMapsBothIdentityKinds(t *testing.T) {
	user, err := SARSubject(t8CreatorValue)
	if err != nil {
		t.Fatalf("user subject: %v", err)
	}
	if user.Username != t8CreatorUser || len(user.Groups) != 0 {
		t.Errorf("user subject = %+v, want username %q without groups", user, t8CreatorUser)
	}

	sa, err := SARSubject(t8SACreator)
	if err != nil {
		t.Fatalf("service-account subject: %v", err)
	}
	if sa.Username != "system:serviceaccount:team-cicd:pipeline-bot" {
		t.Errorf("SA username = %q", sa.Username)
	}
	wantGroups := "system:serviceaccounts,system:serviceaccounts:team-cicd"
	if strings.Join(sa.Groups, ",") != wantGroups {
		t.Errorf("SA groups = %v, want %s", sa.Groups, wantGroups)
	}

	if _, err := SARSubject(""); err == nil {
		t.Error("empty annotation value must fail closed")
	}
	if _, err := SARSubject("serviceAccount:broken"); err == nil {
		t.Error("malformed annotation value must fail closed")
	}
}
