package webhook

import (
	"context"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
)

// v1.7-03 (#80) envtest suite: the namespace-level SubjectAccessReview gate
// for selector targets verified end-to-end against a real API server — the
// mutating admission pins the creator identity (imageflow.appfun.dev/
// created-by), the validating admission runs one UNNAMED SAR per selector
// target through the real RBAC evaluator (envtest boots the API server with
// --authorization-mode=RBAC), a definitive deny rejects the admission as
// Forbidden (403), and granting the creator update on the target resource
// admits it. subscription_authorize_test.go covers the gate semantics against
// a fake SAR client; this suite covers the wiring and the RBAC truth.
//
// The environment is shared with the BuildRun/subscription admission suites
// (started once in TestMain, only when KUBEBUILDER_ASSETS is set).

// envtestSelectorSubscription builds an admissible ImageSubscription whose
// single target locates Deployments by label selector in the shared envtest
// namespace.
func envtestSelectorSubscription(name string) *imageflowv1alpha1.ImageSubscription {
	return &imageflowv1alpha1.ImageSubscription{
		ObjectMeta: metav1.ObjectMeta{Namespace: envtestNS, Name: name},
		Spec: imageflowv1alpha1.ImageSubscriptionSpec{
			Source: imageflowv1alpha1.SubscriptionSourceRef{
				Kind:      sourceKindImageStreamTag,
				Name:      "payments-api:stable",
				Namespace: "imageflow-system",
			},
			Targets: []imageflowv1alpha1.SubscriptionTarget{{
				TargetRef: imageflowv1alpha1.TargetReference{
					APIVersion:    "apps/v1",
					Kind:          "Deployment",
					Namespace:     envtestNS,
					LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "payments-api"}},
				},
				ContainerNames: []string{"payments-api"},
			}},
		},
	}
}

// newSARUser provisions a user that may create subscriptions in the shared
// namespace; whether it may also UPDATE the target kind (apps/Deployments) is
// the SAR contract under test and is controlled per case. Distinct cases must
// use distinct users: the gate caches SAR verdicts per (user, GVK,
// namespace), so a cached deny would otherwise leak into the allow case.
func newSARUser(t *testing.T, username string, grantTargetUpdate bool) client.Client {
	t.Helper()
	ctx := context.Background()

	user, err := envtestEnv.AddUser(envtest.User{Name: username}, envtestCfg)
	if err != nil {
		t.Fatalf("add envtest user %q: %v", username, err)
	}

	rules := []rbacv1.PolicyRule{{
		APIGroups: []string{imageflowv1alpha1.GroupVersion.Group},
		Resources: []string{"imagesubscriptions"},
		Verbs:     []string{"create"},
	}}
	if grantTargetUpdate {
		rules = append(rules, rbacv1.PolicyRule{
			APIGroups: []string{"apps"},
			Resources: []string{"deployments"},
			Verbs:     []string{"get", "update"},
		})
	}
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Namespace: envtestNS, Name: "sar-" + username},
		Rules:      rules,
	}
	if err := envtestClient.Create(ctx, role); err != nil {
		t.Fatalf("create role for %q: %v", username, err)
	}
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: envtestNS, Name: "sar-" + username},
		Subjects: []rbacv1.Subject{{
			Kind:     rbacv1.UserKind,
			Name:     username,
			APIGroup: rbacv1.GroupName,
		}},
		RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: role.Name},
	}
	if err := envtestClient.Create(ctx, binding); err != nil {
		t.Fatalf("create role binding for %q: %v", username, err)
	}

	userClient, err := client.New(user.Config(), client.Options{Scheme: envtestClient.Scheme()})
	if err != nil {
		t.Fatalf("new client for %q: %v", username, err)
	}
	return userClient
}

func TestSelectorTargetSARAdmission(t *testing.T) {
	requireBuildRunAdmissionEnv(t)
	ctx := context.Background()

	t.Run("denied namespace-level SAR rejects the admission as Forbidden", func(t *testing.T) {
		// The creator may create subscriptions but holds no grant on
		// apps/Deployments: the unnamed SAR comes back denied.
		userClient := newSARUser(t, "sar-deny-user", false)
		err := userClient.Create(ctx, envtestSelectorSubscription("sar-selector-denied"))
		if err == nil {
			t.Fatal("selector target whose creator lacks update on the target kind must be rejected")
		}
		if !strings.Contains(err.Error(), "vimagesubscription.kb.io") {
			t.Fatalf("rejection must come from the subscription webhook (vimagesubscription.kb.io), got: %v", err)
		}
		if !apierrors.IsForbidden(err) {
			t.Fatalf("selector-target denial must surface as Forbidden (403), got: %v", err)
		}
		if !strings.Contains(err.Error(), "sar-deny-user") {
			t.Fatalf("rejection must name the denied creator, got: %v", err)
		}
		if !strings.Contains(err.Error(), "selector(app=payments-api)") {
			t.Fatalf("rejection must name the denied selector target by canonical key, got: %v", err)
		}
	})

	t.Run("allowed namespace-level SAR admits the subscription", func(t *testing.T) {
		// The creator holds update on apps/Deployments in the namespace: the
		// unnamed SAR allows and the subscription persists.
		userClient := newSARUser(t, "sar-allow-user", true)
		sub := envtestSelectorSubscription("sar-selector-allowed")
		if err := userClient.Create(ctx, sub); err != nil {
			t.Fatalf("creator with namespace-wide update on the target resource must be admitted: %v", err)
		}
		got := &imageflowv1alpha1.ImageSubscription{}
		if err := envtestClient.Get(ctx, client.ObjectKeyFromObject(sub), got); err != nil {
			t.Fatalf("admitted subscription must persist: %v", err)
		}
		if creator := got.GetAnnotations()[CreatedByAnnotation]; creator != "user:sar-allow-user" {
			t.Fatalf("mutating admission must pin the creator identity, got %q", creator)
		}
	})
}
