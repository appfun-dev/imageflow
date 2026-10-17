package authz

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
)

// Shared test fixtures of the T8 gate unit suite (fake client + interceptor,
// per spec testing decision 2: "SAR 门两个方向（allow/deny）").

const (
	kindDeployment  = "Deployment"
	apiAppsV1       = "apps/v1"
	testNamespace   = "team-payments"
	testUser        = "alice@example.com"
	testSAUser      = "system:serviceaccount:team-ci:deploy-bot"
	testSANamespace = "team-ci"
	fakeStart       = "2026-03-01T00:00:00Z"
)

// sarCallLog records every SubjectAccessReview the gate created and lets each
// test decide the evaluation outcome per request.
type sarCallLog struct {
	calls     int
	requests  []*authorizationv1.SubjectAccessReview
	decide    func(sar *authorizationv1.SubjectAccessReview)
	createErr error
}

func (l *sarCallLog) allowAll() {
	l.decide = func(sar *authorizationv1.SubjectAccessReview) {
		sar.Status.Allowed = true
		sar.Status.Reason = `RBAC: allowed with RoleBinding "team-admin"`
	}
}

func (l *sarCallLog) denyAll() {
	l.decide = func(sar *authorizationv1.SubjectAccessReview) {
		sar.Status.Allowed = false
		sar.Status.Reason = `RBAC: forbidden for ` + testUser
	}
}

// denyByName denies only SARs targeting the given workload names.
func (l *sarCallLog) denyByName(names ...string) {
	denied := make(map[string]bool, len(names))
	for _, n := range names {
		denied[n] = true
	}
	l.decide = func(sar *authorizationv1.SubjectAccessReview) {
		if denied[sar.Spec.ResourceAttributes.Name] {
			sar.Status.Allowed = false
			sar.Status.Reason = `RBAC: no binding grants update on ` +
				sar.Spec.ResourceAttributes.Name
			return
		}
		sar.Status.Allowed = true
	}
}

// newGateFixture builds a Gate backed by a fake client whose interceptor
// short-circuits SubjectAccessReview creates (they are virtual objects: the
// API server evaluates and discards them), an apps/v1-only RESTMapper and a
// fake clock starting at a fixed instant.
func newGateFixture(t *testing.T, opts Options) (*Gate, *sarCallLog, *clock.Fake) {
	t.Helper()
	log := &sarCallLog{}
	log.allowAll()

	start, err := time.Parse(time.RFC3339, fakeStart)
	if err != nil {
		t.Fatalf("parse fixture start: %v", err)
	}
	fakeClk := clock.NewFake(start)

	cl := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object,
				_ ...client.CreateOption) error {
				sar, ok := obj.(*authorizationv1.SubjectAccessReview)
				if !ok {
					return c.Create(ctx, obj)
				}
				log.calls++
				log.requests = append(log.requests, sar.DeepCopy())
				if log.createErr != nil {
					return log.createErr
				}
				if log.decide != nil {
					log.decide(sar)
				}
				return nil
			},
		}).Build()

	if opts.Clock == nil {
		opts.Clock = fakeClk
	}
	if opts.Metrics == nil {
		opts.Metrics = NewMetrics()
	}
	gate := NewGate(cl, testMapper(), opts)
	return gate, log, fakeClk
}

func testScheme(t *testing.T) *runtime.Scheme {
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

// testMapper resolves only apps/v1 Deployment: every other kind exercises the
// KindNotInstalled path.
func testMapper() meta.RESTMapper {
	m := meta.NewDefaultRESTMapper([]schema.GroupVersion{appsv1.SchemeGroupVersion})
	m.Add(appsv1.SchemeGroupVersion.WithKind(kindDeployment), meta.RESTScopeNamespace)
	return m
}

func deploymentTarget(name string) imageflowv1alpha1.TargetReference {
	return imageflowv1alpha1.TargetReference{
		APIVersion: apiAppsV1,
		Kind:       kindDeployment,
		Namespace:  testNamespace,
		Name:       name,
	}
}

func userSubject() Subject {
	return Subject{Username: testUser}
}

func saSubject() Subject {
	return Subject{
		Username: testSAUser,
		Groups:   ServiceAccountGroups(testSANamespace),
	}
}
