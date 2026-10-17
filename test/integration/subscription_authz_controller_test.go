/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package integration

import (
	"context"
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/authz"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
	"github.com/appfun-dev/imageflow/internal/controller"
	iwebhook "github.com/appfun-dev/imageflow/internal/webhook"
)

// T8 named cases: the subscription authorization gate against a real API
// server (object surface = seam S1), driven by the injected clock (seam S3)
// and interceptor-simulated SAR verdicts.
//
// Layering note (verified platform fact, supersedes the ticket premise):
// controller-runtime v0.21 envtest boots kube-apiserver with
// --authorization-mode=RBAC (pkg/internal/testing/controlplane/apiserver.go),
// NOT AlwaysAllow — so the manager-driven specs below exercise REAL RBAC
// evaluation end to end (RoleBinding grant -> allowed, revoke -> denied,
// re-grant -> flip back), while the hand-built specs stay deterministic via
// interceptor-decided SARs on the injected clock. The kind layer (e2e named
// case t8-e2e-*) still owns the full-cluster proof with the production
// manager image and flags.
//
// Suite coverage: the Forbidden surface on the API object, the ZERO-write
// guarantee (interceptor audit of every mutating call), per-target
// granularity on targets change, deterministic periodic re-verification and
// cache TTL behavior on the injected clock, the allow direction against the
// real SAR API (no false Forbidden), the real-RBAC revocation/re-grant flip
// within the recheck period, and the SAR cache hit-rate metric exposure on
// the process-global registry.
//
// Controller names carry the "-t8" suffix: controller-runtime validates
// controller names process-globally and other suites in this package boot
// their own managers.
var _ = Describe("T8 subscription authorization gate", Label("t8"), Ordered, func() {
	var (
		t8Env     *envtest.Environment
		restCfg   *rest.Config
		k8s       client.Client
		watchCl   client.WithWatch
		ctx       context.Context
		cancel    context.CancelFunc
		ns        string
		mgrCancel context.CancelFunc
		mgrDone   chan struct{}
	)

	const (
		t8ControllerName = "subscription-authz-t8"
		t8KindDeployment = "Deployment"
		t8APIAppsV1      = "apps/v1"
		t8SourceKind     = "ImageStreamTag"
		t8StatusSubRes   = "status"
		t8HandPeriod     = 5 * time.Minute
		t8ManagerPeriod  = 2 * time.Second
		t8SAIdentity     = "serviceAccount:t8-ci-namespace:deploy-bot"
		t8UserIdentity   = "user:t8-operator@example.org"
		t8OperatorUser   = "t8-operator@example.org"

		t8RevocableIdentity = "user:t8-revocable@example.net"
		t8RevocableUser     = "t8-revocable@example.net"
		t8StreamName        = "ledger-stream:stable"
		t8DenyReason        = `User "system:serviceaccount:t8-ci-namespace:deploy-bot" ` +
			`cannot update resource "deployments" in API group "apps" in the namespace`
	)

	t8Hex := strings.Repeat("8a", 32)

	BeforeAll(func() {
		assets := os.Getenv("KUBEBUILDER_ASSETS")
		if assets == "" {
			Skip("KUBEBUILDER_ASSETS not set")
		}
		t8Env = &envtest.Environment{
			BinaryAssetsDirectory: assets,
			CRDDirectoryPaths: []string{
				"../../config/crd/bases",
			},
			ErrorIfCRDPathMissing: true,
		}
		var err error
		restCfg, err = t8Env.Start()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(t8Env.Stop()).To(Succeed())
		})

		scheme := runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(imageflowv1alpha1.AddToScheme(scheme)).To(Succeed())
		k8s, err = client.New(restCfg, client.Options{Scheme: scheme})
		Expect(err).NotTo(HaveOccurred())
		watchCl, err = client.NewWithWatch(restCfg, client.Options{Scheme: scheme})
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel = context.WithCancel(context.Background())
		DeferCleanup(cancel)

		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "t8-authz-"}}
		Expect(k8s.Create(ctx, namespace)).To(Succeed())
		ns = namespace.Name
	})

	// --- helpers ----------------------------------------------------------

	// t8Audit counts mutating calls of the reconciler under test: SAR creates
	// (intercepted and decided in-process) and any write that is NOT the
	// subscription's own status subresource (forbidden by the zero-write
	// guarantee — the gate must never touch workloads or main resources).
	t8Audit := struct {
		sarCreates      int
		statusWrites    int
		forbiddenWrites int
	}{}

	t8Mapper := func() meta.RESTMapper {
		m := meta.NewDefaultRESTMapper([]schema.GroupVersion{appsv1.SchemeGroupVersion})
		m.Add(appsv1.SchemeGroupVersion.WithKind(t8KindDeployment), meta.RESTScopeNamespace)
		return m
	}

	t8TargetRef := func(name string) imageflowv1alpha1.TargetReference {
		return imageflowv1alpha1.TargetReference{
			APIVersion: t8APIAppsV1,
			Kind:       t8KindDeployment,
			Namespace:  ns,
			Name:       name,
		}
	}

	t8NewSubscription := func(name, createdBy string, targets ...string) *imageflowv1alpha1.ImageSubscription {
		sub := &imageflowv1alpha1.ImageSubscription{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      name,
				Annotations: map[string]string{
					iwebhook.CreatedByAnnotation: createdBy,
				},
			},
			Spec: imageflowv1alpha1.ImageSubscriptionSpec{
				Source: imageflowv1alpha1.SubscriptionSourceRef{
					Kind:      t8SourceKind,
					Name:      t8StreamName,
					Namespace: ns,
				},
			},
		}
		for _, target := range targets {
			sub.Spec.Targets = append(sub.Spec.Targets, imageflowv1alpha1.SubscriptionTarget{
				TargetRef:      t8TargetRef(target),
				ContainerNames: []string{"app"},
			})
		}
		return sub
	}

	t8CreateDeployment := func(name string) *appsv1.Deployment {
		deploy := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To(int32(0)),
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"t8-app": name},
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"t8-app": name}},
					Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name:  "app",
						Image: "quay.io/example/ledger@sha256:" + t8Hex,
					}}},
				},
			},
		}
		Expect(k8s.Create(ctx, deploy)).To(Succeed())
		return deploy
	}

	// t8HandBuiltReconciler assembles the production reconciler against the
	// real API server with an intercepted client: SAR creates are decided
	// in-process (envtest itself would AlwaysAllow), and every mutating call
	// is audited for the zero-write guarantee.
	t8HandBuiltReconciler := func(
		decide func(sar *authorizationv1.SubjectAccessReview),
		fakeClk *clock.Fake,
	) (*controller.SubscriptionAuthzReconciler, *record.FakeRecorder) {
		audit := &t8Audit
		intercepted := interceptor.NewClient(watchCl, interceptor.Funcs{
			Create: func(c context.Context, cl client.WithWatch, obj client.Object,
				_ ...client.CreateOption) error {
				if sar, ok := obj.(*authorizationv1.SubjectAccessReview); ok {
					audit.sarCreates++
					decide(sar)
					return nil
				}
				audit.forbiddenWrites++
				return cl.Create(c, obj)
			},
			Update: func(c context.Context, cl client.WithWatch, obj client.Object,
				_ ...client.UpdateOption) error {
				audit.forbiddenWrites++
				return cl.Update(c, obj)
			},
			Patch: func(c context.Context, cl client.WithWatch, obj client.Object,
				p client.Patch, _ ...client.PatchOption) error {
				audit.forbiddenWrites++
				return cl.Patch(c, obj, p)
			},
			Delete: func(c context.Context, cl client.WithWatch, obj client.Object,
				_ ...client.DeleteOption) error {
				audit.forbiddenWrites++
				return cl.Delete(c, obj)
			},
			SubResourceUpdate: func(c context.Context, cl client.Client, subResource string,
				obj client.Object, _ ...client.SubResourceUpdateOption) error {
				if subResource == t8StatusSubRes {
					if _, ok := obj.(*imageflowv1alpha1.ImageSubscription); ok {
						audit.statusWrites++
						return cl.SubResource(subResource).Update(c, obj)
					}
				}
				audit.forbiddenWrites++
				return cl.SubResource(subResource).Update(c, obj)
			},
		})
		cfg := config.Config{SARRecheckPeriod: t8HandPeriod}
		gate := authz.NewGate(intercepted, t8Mapper(), authz.Options{
			Clock:   fakeClk,
			TTL:     cfg.SARRecheckPeriod,
			Metrics: authz.NewMetrics(),
		})
		recorder := record.NewFakeRecorder(64)
		return controller.NewSubscriptionAuthzReconciler(
			intercepted, recorder, fakeClk, cfg, gate), recorder
	}

	t8Request := func(name string) ctrl.Request {
		return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}
	}

	t8GetSub := func(name string) *imageflowv1alpha1.ImageSubscription {
		sub := &imageflowv1alpha1.ImageSubscription{}
		Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, sub)).To(Succeed())
		return sub
	}

	t8Condition := func(sub *imageflowv1alpha1.ImageSubscription) *metav1.Condition {
		return meta.FindStatusCondition(sub.Status.Conditions,
			imageflowv1alpha1.ConditionTypeForbidden)
	}

	t8DrainEvents := func(rec *record.FakeRecorder) []string {
		var out []string
		for {
			select {
			case e := <-rec.Events:
				out = append(out, e)
			default:
				return out
			}
		}
	}

	t8ResetAudit := func() {
		t8Audit.sarCreates, t8Audit.statusWrites, t8Audit.forbiddenWrites = 0, 0, 0
	}

	// t8GrantDeploymentUpdate binds a real RBAC Role (update on
	// deployments.apps in the test namespace) to a User identity — the
	// standard grant shape ADR-003 expects application teams to hold in the
	// target namespace.
	t8GrantDeploymentUpdate := func(roleName, bindingName, userName string) {
		role := &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: roleName},
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{"apps"},
				Resources: []string{"deployments"},
				Verbs:     []string{"get", "update"},
			}},
		}
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(role), &rbacv1.Role{}); err != nil {
			Expect(k8s.Create(ctx, role)).To(Succeed())
		}
		binding := &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: bindingName},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "Role",
				Name:     roleName,
			},
			Subjects: []rbacv1.Subject{{
				Kind: "User",
				Name: userName,
			}},
		}
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(binding), &rbacv1.RoleBinding{}); err != nil {
			Expect(k8s.Create(ctx, binding)).To(Succeed())
		}
	}

	t8EventReasonsFor := func(subName string) []string {
		var list corev1.EventList
		Expect(k8s.List(ctx, &list, client.InNamespace(ns))).To(Succeed())
		var reasons []string
		for _, event := range list.Items {
			if event.InvolvedObject.Name == subName {
				reasons = append(reasons, event.Reason)
			}
		}
		return reasons
	}

	// --- hand-built reconciler specs (deterministic, injected clock) -------

	Context("with hand-built reconcilers against the API server", func() {
		It("t8-envtest-denied-target-produces-forbidden-condition-and-zero-target-writes", func() {
			deploy := t8CreateDeployment("t8-denied-ledger")
			rvBefore := deploy.ResourceVersion
			sub := t8NewSubscription("t8-ac1-denied", t8SAIdentity, "t8-denied-ledger")
			Expect(k8s.Create(ctx, sub)).To(Succeed())

			fakeClk := clock.NewFake(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
			t8ResetAudit()
			reconciler, recorder := t8HandBuiltReconciler(
				func(sar *authorizationv1.SubjectAccessReview) {
					sar.Status.Allowed = false
					sar.Status.Reason = t8DenyReason
				}, fakeClk)

			result, err := reconciler.Reconcile(ctx, t8Request("t8-ac1-denied"))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(t8HandPeriod),
				"every pass must schedule the next periodic re-verification")

			got := t8GetSub("t8-ac1-denied")
			cond := t8Condition(got)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal("TargetsUnauthorized"))
			Expect(cond.Message).To(ContainSubstring("t8-denied-ledger"))
			Expect(got.Status.Targets).To(HaveLen(1))
			Expect(got.Status.Targets[0].Reason).To(Equal(controller.TargetReasonForbidden))
			Expect(got.Status.Targets[0].Message).To(ContainSubstring("cannot update"))

			By("the zero-write guarantee holds: no write to anything but the subscription status")
			Expect(t8Audit.forbiddenWrites).To(BeZero())
			Expect(t8Audit.statusWrites).To(Equal(1))
			Expect(t8Audit.sarCreates).To(Equal(1))
			after := &appsv1.Deployment{}
			Expect(k8s.Get(ctx, client.ObjectKeyFromObject(deploy), after)).To(Succeed())
			Expect(after.ResourceVersion).To(Equal(rvBefore), "the target workload must be untouched")

			events := t8DrainEvents(recorder)
			Expect(events).To(HaveLen(1))
			Expect(events[0]).To(ContainSubstring("Warning " + controller.EventReasonTargetsForbidden))
		})

		It("t8-envtest-targets-change-marks-only-the-unauthorized-target", func() {
			sub := t8NewSubscription("t8-ac2-granular", t8UserIdentity, "t8-allowed-ledger")
			Expect(k8s.Create(ctx, sub)).To(Succeed())

			fakeClk := clock.NewFake(time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC))
			t8ResetAudit()
			reconciler, recorder := t8HandBuiltReconciler(
				func(sar *authorizationv1.SubjectAccessReview) {
					sar.Status.Allowed = sar.Spec.ResourceAttributes.Name != "t8-revoked-ledger"
					if !sar.Status.Allowed {
						sar.Status.Reason = t8DenyReason
					}
				}, fakeClk)

			By("first pass: the single authorized target yields no false Forbidden")
			_, err := reconciler.Reconcile(ctx, t8Request("t8-ac2-granular"))
			Expect(err).NotTo(HaveOccurred())
			got := t8GetSub("t8-ac2-granular")
			Expect(t8Condition(got).Status).To(Equal(metav1.ConditionFalse))
			t8DrainEvents(recorder)

			By("a targets change adds an unauthorized target (webhook-bypass / revocation path)")
			got.Spec.Targets = append(got.Spec.Targets, imageflowv1alpha1.SubscriptionTarget{
				TargetRef:      t8TargetRef("t8-revoked-ledger"),
				ContainerNames: []string{"sidecar"},
			})
			Expect(k8s.Update(ctx, got)).To(Succeed())

			result, err := reconciler.Reconcile(ctx, t8Request("t8-ac2-granular"))
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Duration(t8HandPeriod)))

			got = t8GetSub("t8-ac2-granular")
			cond := t8Condition(got)
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Message).To(ContainSubstring("t8-revoked-ledger"))
			Expect(cond.Message).NotTo(ContainSubstring("t8-allowed-ledger"),
				"the authorized target must not be implicated")
			Expect(got.Status.Targets).To(HaveLen(1),
				"only the unauthorized target gets a Forbidden entry")
			Expect(got.Status.Targets[0].TargetRef.Name).To(Equal("t8-revoked-ledger"))
			Expect(t8Audit.forbiddenWrites).To(BeZero())
		})

		It("t8-envtest-periodic-recheck-flips-state-on-injected-clock-when-permissions-change", func() {
			sub := t8NewSubscription("t8-ac3-flip", t8SAIdentity, "t8-flip-ledger")
			Expect(k8s.Create(ctx, sub)).To(Succeed())

			fakeClk := clock.NewFake(time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC))
			t8ResetAudit()
			allowed := true
			reconciler, recorder := t8HandBuiltReconciler(
				func(sar *authorizationv1.SubjectAccessReview) {
					sar.Status.Allowed = allowed
					if !allowed {
						sar.Status.Reason = t8DenyReason
					}
				}, fakeClk)

			By("initially authorized: the persisted state says so")
			_, err := reconciler.Reconcile(ctx, t8Request("t8-ac3-flip"))
			Expect(err).NotTo(HaveOccurred())
			Expect(t8Condition(t8GetSub("t8-ac3-flip")).Status).To(Equal(metav1.ConditionFalse))
			Expect(t8Audit.sarCreates).To(Equal(1))

			By("permission revoked; inside the cache TTL the cached verdict is still served")
			allowed = false
			_, err = reconciler.Reconcile(ctx, t8Request("t8-ac3-flip"))
			Expect(err).NotTo(HaveOccurred())
			Expect(t8Audit.sarCreates).To(Equal(1), "a cache hit must not call the SAR API")
			Expect(t8Condition(t8GetSub("t8-ac3-flip")).Status).To(Equal(metav1.ConditionFalse))

			By("the injected clock passes the recheck period: the revocation flips the state")
			fakeClk.Advance(t8HandPeriod)
			_, err = reconciler.Reconcile(ctx, t8Request("t8-ac3-flip"))
			Expect(err).NotTo(HaveOccurred())
			Expect(t8Audit.sarCreates).To(Equal(2), "the expired entry must be re-verified")
			got := t8GetSub("t8-ac3-flip")
			Expect(t8Condition(got).Status).To(Equal(metav1.ConditionTrue))
			Expect(got.Status.Targets).To(HaveLen(1))
			events := t8DrainEvents(recorder)
			Expect(events).To(HaveLen(1))
			Expect(events[0]).To(ContainSubstring("Warning " + controller.EventReasonTargetsForbidden))

			By("permission granted again: the next periodic re-check flips the state back")
			allowed = true
			fakeClk.Advance(t8HandPeriod)
			_, err = reconciler.Reconcile(ctx, t8Request("t8-ac3-flip"))
			Expect(err).NotTo(HaveOccurred())
			Expect(t8Audit.sarCreates).To(Equal(3))
			got = t8GetSub("t8-ac3-flip")
			Expect(t8Condition(got).Status).To(Equal(metav1.ConditionFalse))
			Expect(got.Status.Targets).To(BeEmpty(), "the Forbidden husk entry is dropped on recovery")
			events = t8DrainEvents(recorder)
			Expect(events).To(HaveLen(1))
			Expect(events[0]).To(ContainSubstring("Normal " + controller.EventReasonTargetsAuthorized))
			Expect(t8Audit.forbiddenWrites).To(BeZero())
		})
	})

	// --- manager-driven specs (production registrar, real SAR API) ---------

	Context("with the T8 controller running in a manager", Ordered, func() {
		BeforeAll(func() {
			scheme := runtime.NewScheme()
			Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
			Expect(imageflowv1alpha1.AddToScheme(scheme)).To(Succeed())
			mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
				Scheme:  scheme,
				Metrics: metricsserver.Options{BindAddress: "0"},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(controller.RegisterSubscriptionAuthz(mgr, t8ControllerName, controller.Options{
				Clock:  clock.Real{},
				Config: config.Config{SARRecheckPeriod: t8ManagerPeriod},
			})).To(Succeed())

			var mgrCtx context.Context
			mgrCtx, mgrCancel = context.WithCancel(ctx)
			mgrDone = make(chan struct{})
			go func() {
				defer GinkgoRecover()
				defer close(mgrDone)
				Expect(mgr.Start(mgrCtx)).To(Succeed())
			}()
			Expect(mgr.GetCache().WaitForCacheSync(mgrCtx)).To(BeTrue())
		})

		AfterAll(func() {
			if mgrCancel != nil {
				mgrCancel()
				Eventually(mgrDone, 30*time.Second, 100*time.Millisecond).Should(BeClosed())
			}
		})

		It("t8-envtest-authorized-creator-passes-real-sar-without-false-forbidden", func() {
			// The envtest apiserver evaluates SAR with a real RBAC authorizer:
			// this spec pins the ALLOW direction end to end through the
			// production registrar (watch loop, real RESTMapper, real SAR API,
			// real RoleBinding) — an authorized creator must never see a
			// false Forbidden (AC5).
			t8CreateDeployment("t8-real-ledger")
			t8GrantDeploymentUpdate("t8-ac5-role", "t8-ac5-binding", t8OperatorUser)
			sub := t8NewSubscription("t8-ac5-allowed", t8UserIdentity, "t8-real-ledger")
			Expect(k8s.Create(ctx, sub)).To(Succeed())

			Eventually(func(g Gomega) {
				cond := t8Condition(t8GetSub("t8-ac5-allowed"))
				g.Expect(cond).NotTo(BeNil(), "the gate must report the authorized state")
				g.Expect(cond.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(cond.Reason).To(Equal(controller.ReasonAuthorized))
			}, 60*time.Second, 200*time.Millisecond).Should(Succeed())

			var list corev1.EventList
			Consistently(func(g Gomega) {
				g.Expect(k8s.List(ctx, &list, client.InNamespace(ns))).To(Succeed())
				for _, event := range list.Items {
					if event.InvolvedObject.Name != "t8-ac5-allowed" {
						continue
					}
					g.Expect(event.Reason).NotTo(Equal(controller.EventReasonTargetsForbidden),
						"an authorized creator must never produce a Forbidden event")
				}
			}, 5*time.Second, time.Second).Should(Succeed())
		})

		It("t8-envtest-real-rbac-revocation-flips-state-within-recheck-period", func() {
			// AC3 on a REAL authorizer: grant -> authorized, revoke -> the
			// periodic re-verification flips the persisted state to Forbidden
			// within one recheck period (+ cache TTL), re-grant -> flips back.
			// "Delivery stops" is asserted structurally: the gate never
			// writes to targets and T9 consumes this surface (kind-layer
			// delivery-stop case lands with T9/T12).
			t8CreateDeployment("t8-revoke-ledger")
			const role, binding = "t8-revoke-role", "t8-revoke-binding"
			t8GrantDeploymentUpdate(role, binding, t8RevocableUser)
			sub := t8NewSubscription("t8-ac3-real-flip", t8RevocableIdentity, "t8-revoke-ledger")
			Expect(k8s.Create(ctx, sub)).To(Succeed())

			By("granted: the gate reports authorized")
			Eventually(func(g Gomega) {
				cond := t8Condition(t8GetSub("t8-ac3-real-flip"))
				g.Expect(cond).NotTo(BeNil())
				g.Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			}, 60*time.Second, 200*time.Millisecond).Should(Succeed())

			By("revoked: the periodic re-verification flips to Forbidden within one period")
			Expect(k8s.Delete(ctx, &rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: binding},
			})).To(Succeed())
			Eventually(func(g Gomega) {
				cond := t8Condition(t8GetSub("t8-ac3-real-flip"))
				g.Expect(cond).NotTo(BeNil())
				g.Expect(cond.Status).To(Equal(metav1.ConditionTrue),
					"revocation must flip the state within the %s recheck period", t8ManagerPeriod)
				g.Expect(cond.Reason).To(Equal(controller.ReasonTargetsUnauthorized))
				got := t8GetSub("t8-ac3-real-flip")
				g.Expect(got.Status.Targets).To(HaveLen(1))
				g.Expect(got.Status.Targets[0].Reason).To(Equal(controller.TargetReasonForbidden))
			}, 60*time.Second, 200*time.Millisecond).Should(Succeed())

			By("the flip is audited with a Warning event on the subscription")
			Eventually(func(g Gomega) {
				g.Expect(t8EventReasonsFor("t8-ac3-real-flip")).
					To(ContainElement(controller.EventReasonTargetsForbidden))
			}, 30*time.Second, 250*time.Millisecond).Should(Succeed())

			By("re-granted: the next periodic re-verification flips back to authorized")
			t8GrantDeploymentUpdate(role, binding, t8RevocableUser)
			Eventually(func(g Gomega) {
				cond := t8Condition(t8GetSub("t8-ac3-real-flip"))
				g.Expect(cond).NotTo(BeNil())
				g.Expect(cond.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(cond.Reason).To(Equal(controller.ReasonAuthorized))
				got := t8GetSub("t8-ac3-real-flip")
				g.Expect(got.Status.Targets).To(BeEmpty(),
					"the Forbidden entry is cleared on recovery")
			}, 60*time.Second, 200*time.Millisecond).Should(Succeed())
			Eventually(func(g Gomega) {
				g.Expect(t8EventReasonsFor("t8-ac3-real-flip")).
					To(ContainElement(controller.EventReasonTargetsAuthorized))
			}, 30*time.Second, 250*time.Millisecond).Should(Succeed())
		})

		It("t8-envtest-sar-cache-hit-rate-metric-is-exposed", func() {
			// The manager-driven gate reports through the process-wide
			// collector set registered against controller-runtime's metrics
			// registry: the first check of a target misses, the re-reconcile
			// triggered by the status write hits inside the TTL — both
			// directions of the AC6 hit-rate metric must be observable.
			lookups := func() map[string]float64 {
				values := map[string]float64{}
				families, err := ctrlmetrics.Registry.Gather()
				Expect(err).NotTo(HaveOccurred())
				for _, family := range families {
					if family.GetName() != "imageflow_subscription_sar_cache_lookups_total" {
						continue
					}
					for _, m := range family.GetMetric() {
						for _, label := range m.GetLabel() {
							if label.GetName() == "result" {
								values[label.GetValue()] = m.GetCounter().GetValue()
							}
						}
					}
				}
				return values
			}
			before := lookups()

			sub := t8NewSubscription("t8-ac6-metrics", t8SAIdentity, "t8-metric-ledger")
			// No RBAC grant for the SA identity: the real authorizer denies,
			// which exercises the denied-verdict counters alongside the
			// cache hit/miss pair asserted below.
			Expect(k8s.Create(ctx, sub)).To(Succeed())
			Eventually(func(g Gomega) {
				g.Expect(t8Condition(t8GetSub("t8-ac6-metrics"))).NotTo(BeNil())
			}, 60*time.Second, 200*time.Millisecond).Should(Succeed())

			Eventually(func(g Gomega) {
				after := lookups()
				g.Expect(after["miss"]).To(BeNumerically(">", before["miss"]),
					"the fresh target check must register a cache miss")
				g.Expect(after["hit"]).To(BeNumerically(">", before["hit"]),
					"the status-write re-reconcile inside the TTL must register a cache hit")
			}, 30*time.Second, 250*time.Millisecond).Should(Succeed())

			families, err := ctrlmetrics.Registry.Gather()
			Expect(err).NotTo(HaveOccurred())
			names := map[string]bool{}
			for _, family := range families {
				names[family.GetName()] = true
			}
			Expect(names).To(HaveKey("imageflow_subscription_sar_checks_total"),
				"the verdict counter of the T8 metrics section must be registered")
		})
	})
})
