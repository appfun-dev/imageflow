package webhook

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/authz"
	"github.com/appfun-dev/imageflow/internal/clock"
)

// Admission-time half of the subscription authorization gate (ticket T8,
// ADR-003 timings 1+2): every CREATE, and every UPDATE that changes
// spec.targets, re-verifies via SubjectAccessReview that the created-by
// identity holds the update verb on every target. Denials fail closed:
//
//   - denied target(s)            → 403 Forbidden, listing the targets and
//     the standard-RBAC remedy (the ADR-003 "feature, not a defect" path);
//   - rate-limited check(s)       → 429 TooManyRequests (retry shortly; the
//     limiter sheds load, the gate never fails open);
//   - SAR API failure             → 503 ServiceUnavailable (retry shortly);
//   - target kind not installed   → allowed WITH a warning: a subscription
//     may legitimately be created before its target CRD (ADR-003 double
//     insurance), delivery is structurally impossible while the kind is
//     absent, and the controller-side periodic re-verification picks the
//     target up once the kind exists.
//
// The authorizer is additive to the T3 shape rules: shape violations are
// reported first and unchanged; authorization only runs once the shape is
// valid. The identity subject is the created-by ANNOTATION (not the current
// requester): the mutating webhook runs before validation and pins the
// annotation to the creator on every write, so admission always authorizes
// the creator per ADR-003 — including updates by other principals.
//
// Updates that do not change spec.targets skip the SAR check entirely: RBAC
// drift on an unchanged target set is owned by the controller-side periodic
// re-verification, and admission adds no SAR load for unrelated edits.

// sarRecheckPeriodFlag is the process flag (bound in cmd/main.go via
// internal/config) sizing the admission-side SAR cache TTL. The webhook
// registration seam reads the flag surface instead of manager options — the
// same no-shared-file-edit pattern the webhook enablement gate established in
// internal/controller/webhook.go — so one cache TTL governs both halves of
// the gate in a production process.
const sarRecheckPeriodFlag = "sar-recheck-period"

// admissionRetryAfterSeconds hints retry timing on rate-limited admissions.
const admissionRetryAfterSeconds = 2

// subscriptionGR identifies the ImageSubscription resource in admission
// denials.
var subscriptionGR = schema.GroupResource{
	Group:    imageflowv1alpha1.GroupVersion.Group,
	Resource: "imagesubscriptions",
}

// subscriptionAuthorizer performs the admission-time SAR checks through a
// cached, rate-limited authz.Gate.
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
// new subscription is checked before the object is accepted.
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

// authorizeTargets runs the gate over every target and maps the verdict
// picture onto admission responses (fail closed on everything except the
// structurally-undeliverable KindNotInstalled case).
func (a *subscriptionAuthorizer) authorizeTargets(
	ctx context.Context,
	sub *imageflowv1alpha1.ImageSubscription,
) (admission.Warnings, error) {
	subject, err := SARSubject(sub.GetAnnotations()[CreatedByAnnotation])
	if err != nil {
		return nil, apierrors.NewForbidden(subscriptionGR, sub.Name, fmt.Errorf(
			"subscription authorization requires a trusted created-by identity (ADR-003): %v", err))
	}

	refs := make([]imageflowv1alpha1.TargetReference, 0, len(sub.Spec.Targets))
	for i := range sub.Spec.Targets {
		refs = append(refs, sub.Spec.Targets[i].TargetRef)
	}
	verdicts := a.gate.CheckTargets(ctx, subject, refs)

	var warnings admission.Warnings
	var denied, rateLimited, failed []authz.TargetVerdict
	for _, verdict := range verdicts {
		switch verdict.Verdict {
		case authz.VerdictDenied:
			denied = append(denied, verdict)
		case authz.VerdictUnknown:
			switch {
			case strings.HasPrefix(verdict.Reason, authz.ReasonKindNotInstalled):
				warnings = append(warnings, fmt.Sprintf(
					"target %s could not be authorization-checked: %s. The subscription "+
						"controller re-verifies it periodically and performs zero writes "+
						"until it is authorized.",
					authz.FormatTarget(verdict.Target), verdict.Reason))
			case strings.HasPrefix(verdict.Reason, authz.ReasonRateLimited):
				rateLimited = append(rateLimited, verdict)
			default:
				failed = append(failed, verdict)
			}
		}
	}

	switch {
	case len(denied) > 0:
		return warnings, apierrors.NewForbidden(subscriptionGR, sub.Name, fmt.Errorf(
			"creator %q lacks update permission on %d of %d target(s): %s. ImageFlow "+
				"subscriptions never write to unauthorized targets (ADR-003); grant standard "+
				"RBAC update in the target namespace(s) first",
			subject.Username, len(denied), len(refs), authz.FormatDeniedTargets(denied)))
	case len(rateLimited) > 0:
		return warnings, apierrors.NewTooManyRequests(fmt.Sprintf(
			"subscription authorization is rate limited (%d of %d target checks shed); "+
				"retry shortly — the authorization gate never fails open",
			len(rateLimited), len(refs)), admissionRetryAfterSeconds)
	case len(failed) > 0:
		return warnings, apierrors.NewServiceUnavailable(fmt.Sprintf(
			"subscription authorization could not be verified for %d of %d target(s) "+
				"(%s: %s); retry shortly — the authorization gate never fails open",
			len(failed), len(refs), authz.FormatTarget(failed[0].Target),
			authz.TruncateMessage(failed[0].Reason, authz.MaxTargetReasonLength)))
	}
	return warnings, nil
}
