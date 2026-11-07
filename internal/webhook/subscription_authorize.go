package webhook

import (
	"context"
	"flag"
	"fmt"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/authz"
	"github.com/appfun-dev/imageflow/internal/clock"
)

// Admission-time half of the subscription authorization gate (ticket T8,
// ADR-003 timings 1+2): every CREATE, and every UPDATE that changes
// spec.targets, re-verifies via SubjectAccessReview that the created-by
// identity holds the update verb on every target.
//
// Enforcement split (ADR-003: "不满足 → 订阅报 Forbidden Condition 且对该
// 目标零写入"): the authoritative consequence of a denial is the persisted
// Forbidden condition plus the zero-write guarantee, both owned by the
// controller-side gate (internal/controller subscription-authz). This
// admission half is the immediate-feedback surface: findings are returned as
// admission WARNINGS (visible in kubectl apply output), never as denials, so
//
//   - a subscription may legitimately be created before its RBAC grant or
//     before its target kind/object exists (ADR-003 double insurance) and
//     self-heals into delivery once the grant lands — no recreate needed;
//   - the security property ("写不动创建者无权的 Workload") never depends on
//     the webhook: the controller gate fails closed independently;
//   - the T3 shape rules keep sole ownership of admission rejection.
//
// Warning cases: denied targets (naming them and the RBAC remedy), a missing
// or malformed created-by identity (the controller will report
// Forbidden/MissingIdentity), rate-limited or failed SAR checks (the
// controller re-verifies and fails closed), and target kinds the cluster does
// not serve yet.
//
// The authorizer is additive to the T3 shape rules: shape violations are
// reported first and unchanged; authorization only runs once the shape is
// valid. The identity subject is the created-by ANNOTATION (not the current
// requester): the mutating webhook runs before validation and pins the
// annotation to the creator on every write, so admission always checks the
// creator per ADR-003 — including updates by other principals.
//
// Updates that do not change spec.targets skip the SAR check entirely: RBAC
// drift on an unchanged target set is owned by the controller-side periodic
// re-verification, and admission adds no SAR load for unrelated edits.

// sarRecheckPeriodFlag is the process flag (bound in cmd/main.go via
// internal/config) sizing the admission-side SAR cache TTL. The webhook
// registration seam reads the flag surface instead of manager options — the
// same no-shared-file-edit pattern the webhook enablement gate established in
// internal/controller/webhook.go — so one TTL semantics governs both halves
// of the gate in a production process.
const sarRecheckPeriodFlag = "sar-recheck-period"

// subscriptionAuthorizer performs the admission-time SAR checks through a
// cached, rate-limited authz.Gate and maps every non-allowed finding to an
// admission warning.
type subscriptionAuthorizer struct {
	gate *authz.Gate
}

// newSubscriptionAuthorizer builds the admission authorizer from the manager:
// its client creates SubjectAccessReviews and its RESTMapper resolves target
// kinds. The gate uses the real clock (admission lives in wall time) and the
// --sar-recheck-period flag as its cache TTL.
func newSubscriptionAuthorizer(mgr ctrl.Manager) *subscriptionAuthorizer {
	return &subscriptionAuthorizer{
		gate: authz.NewGate(mgr.GetClient(), mgr.GetRESTMapper(), authz.Options{
			Clock: clock.Real{},
			TTL:   recheckPeriodFromFlags(),
		}),
	}
}

// recheckPeriodFromFlags reads the SAR recheck period from the process flag
// surface, falling back to the ADR-003 default when the flag is unbound
// (programmatic embedders, tests).
func recheckPeriodFromFlags() time.Duration {
	if f := flag.Lookup(sarRecheckPeriodFlag); f != nil {
		if d, err := time.ParseDuration(f.Value.String()); err == nil && d > 0 {
			return d
		}
	}
	return authz.DefaultRecheckPeriod
}

// authorizeCreate is the "创建 admission" timing (ADR-003): every target of a
// new subscription is checked as the object is admitted.
func (a *subscriptionAuthorizer) authorizeCreate(
	ctx context.Context,
	sub *imageflowv1alpha1.ImageSubscription,
) (admission.Warnings, error) {
	return a.authorizeTargets(ctx, sub)
}

// authorizeUpdate is the "targets 变更" timing (ADR-003): the check re-runs
// only when spec.targets actually changed.
func (a *subscriptionAuthorizer) authorizeUpdate(
	ctx context.Context,
	oldSub, newSub *imageflowv1alpha1.ImageSubscription,
) (admission.Warnings, error) {
	if apiequality.Semantic.DeepEqual(oldSub.Spec.Targets, newSub.Spec.Targets) {
		return nil, nil
	}
	return a.authorizeTargets(ctx, newSub)
}

// authorizeTargets runs the gate over every target and renders the findings
// as admission warnings. It never denies: the persisted Forbidden surface
// and the zero-write guarantee are enforced by the controller-side gate
// regardless of what happens here.
func (a *subscriptionAuthorizer) authorizeTargets(
	ctx context.Context,
	sub *imageflowv1alpha1.ImageSubscription,
) (admission.Warnings, error) {
	subject, err := SARSubject(sub.GetAnnotations()[CreatedByAnnotation])
	if err != nil {
		return admission.Warnings{fmt.Sprintf(
			"[imageflow] no trusted creator identity: %v. The subscription will report "+
				"Forbidden (MissingIdentity) and receive zero writes to every target until "+
				"the created-by annotation records a verifiable creator (ADR-003).", err)}, nil
	}

	refs := make([]imageflowv1alpha1.TargetReference, 0, len(sub.Spec.Targets))
	for i := range sub.Spec.Targets {
		refs = append(refs, sub.Spec.Targets[i].TargetRef)
	}
	verdicts := a.gate.CheckTargets(ctx, subject, refs)

	var warnings admission.Warnings
	var denied, unresolved []authz.TargetVerdict
	for _, verdict := range verdicts {
		switch verdict.Verdict {
		case authz.VerdictDenied:
			denied = append(denied, verdict)
		case authz.VerdictUnknown:
			unresolved = append(unresolved, verdict)
		}
	}

	if len(denied) > 0 {
		warnings = append(warnings, fmt.Sprintf(
			"[imageflow] creator %q lacks update permission on %d of %d target(s): %s. "+
				"The subscription reports Forbidden and ImageFlow performs ZERO writes to these "+
				"targets (ADR-003); grant standard RBAC update in the target namespace(s) and "+
				"the periodic re-verification flips the state without a recreate.",
			subject.Username, len(denied), len(refs), authz.FormatDeniedTargets(denied)))
	}
	for _, verdict := range unresolved {
		warnings = append(warnings, fmt.Sprintf(
			"[imageflow] target %s could not be authorization-checked at admission (%s). "+
				"The subscription controller re-verifies it periodically and performs zero "+
				"writes until it is authorized.",
			authz.FormatTarget(verdict.Target), summarizeUnresolved(verdict.Reason)))
	}
	return warnings, nil
}

// summarizeUnresolved caps an unresolved-verdict reason for the warning text;
// the reason already carries its stable code as a prefix.
func summarizeUnresolved(reason string) string {
	const summaryLength = 160
	return authz.TruncateMessage(reason, summaryLength)
}
