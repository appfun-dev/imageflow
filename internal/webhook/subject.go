package webhook

import (
	"fmt"

	"github.com/appfun-dev/imageflow/internal/authz"
)

// SARSubject maps a created-by annotation value to the SubjectAccessReview
// subject the T8 authorization gate checks (ADR-003). It is the single
// identity-to-subject mapping shared by the admission-side authorizer and the
// controller-side subscription-authz gate so both timings always evaluate the
// exact same identity:
//
//   - user identities carry the recorded username only — the frozen T3
//     annotation model records no groups for plain users, so group-only grants
//     (e.g. system:masters) are invisible to the gate; binding creators
//     directly (User or ServiceAccount subjects) is the supported model;
//   - serviceAccount identities additionally carry the structural groups the
//     API server attaches to every request from that account
//     (system:serviceaccounts, system:serviceaccounts:<ns>), so bindings to
//     those groups are evaluated exactly as for a live request.
//
// An empty or malformed value is an error: callers fail closed (admission
// denial / Forbidden condition), never open.
func SARSubject(createdByValue string) (authz.Subject, error) {
	if createdByValue == "" {
		return authz.Subject{}, fmt.Errorf("annotation %s is absent: no trusted creator identity "+
			"was recorded at admission", CreatedByAnnotation)
	}
	parsed, err := ParseCreatedBy(createdByValue)
	if err != nil {
		return authz.Subject{}, err
	}
	subject := authz.Subject{Username: parsed.Username}
	if parsed.Kind == CreatedByKindServiceAccount {
		subject.Groups = authz.ServiceAccountGroups(parsed.ServiceAccountNamespace)
	}
	return subject, nil
}
