package webhook

import (
	"fmt"
	"strings"

	authenticationv1 "k8s.io/api/authentication/v1"
)

// CreatedByAnnotation records the authenticated identity that created an
// ImageSubscription (ADR-003). The subscription controller's
// SubjectAccessReview gate (T8) uses it as the authorization subject. It is
// injected by the mutating webhook on create and forced back to the stored
// value on every update, so it always names the creator and can never be
// re-attributed by a later writer.
const CreatedByAnnotation = "imageflow.appfun.dev/created-by"

// Identity kinds carried in the CreatedByAnnotation value (ADR-003:
// user / serviceAccount).
const (
	// CreatedByKindUser marks a non-service-account identity (human, OIDC,
	// node, ...). The remainder of the value is the raw Kubernetes username.
	CreatedByKindUser = "user"
	// CreatedByKindServiceAccount marks a service-account identity. The
	// remainder of the value is "<namespace>:<name>".
	CreatedByKindServiceAccount = "serviceAccount"
)

// serviceAccountUsernamePrefix is the canonical Kubernetes username prefix of
// service-account identities.
const serviceAccountUsernamePrefix = "system:serviceaccount:"

// CreatedByValue encodes the admission requester into the created-by
// annotation value:
//
//   - "serviceAccount:<namespace>:<name>" for service-account identities
//     (username system:serviceaccount:<namespace>:<name>);
//   - "user:<username>" for every other identity, preserving the username
//     verbatim (it may itself contain colons, e.g. system:node:n-0 or
//     OIDC subjects).
//
// An empty username yields "": the webhook then never injects (and strips any
// user-supplied) annotation, because there is no trusted identity to record.
func CreatedByValue(user authenticationv1.UserInfo) string {
	if user.Username == "" {
		return ""
	}
	if ns, name, ok := splitServiceAccountUsername(user.Username); ok {
		return CreatedByKindServiceAccount + ":" + ns + ":" + name
	}
	return CreatedByKindUser + ":" + user.Username
}

// CreatedBy is the parsed form of the CreatedByAnnotation value. T8
// (subscription authorization gate) consumes it to build SubjectAccessReview
// requests: Username is always the Kubernetes username to submit to SAR,
// regardless of identity kind.
type CreatedBy struct {
	// Kind is CreatedByKindUser or CreatedByKindServiceAccount.
	Kind string
	// Username is the Kubernetes username for SubjectAccessReview: the raw
	// username for user identities, the reconstructed
	// system:serviceaccount:<namespace>:<name> form for service accounts.
	Username string
	// ServiceAccountNamespace is set for CreatedByKindServiceAccount only.
	ServiceAccountNamespace string
	// ServiceAccountName is set for CreatedByKindServiceAccount only.
	ServiceAccountName string
}

// ParseCreatedBy parses a created-by annotation value back into an identity.
// It rejects malformed or unknown-kind values so consumers never act on a
// corrupted annotation.
func ParseCreatedBy(value string) (*CreatedBy, error) {
	kind, rest, found := strings.Cut(value, ":")
	if !found || rest == "" {
		return nil, fmt.Errorf("created-by value %q is not in \"<kind>:<identity>\" form", value)
	}
	switch kind {
	case CreatedByKindServiceAccount:
		ns, name, ok := strings.Cut(rest, ":")
		if !ok || ns == "" || name == "" || strings.Contains(name, ":") {
			return nil, fmt.Errorf("created-by value %q is not a valid %s identity"+
				" (expected %s:<namespace>:<name>)", value, CreatedByKindServiceAccount, CreatedByKindServiceAccount)
		}
		return &CreatedBy{
			Kind:                    CreatedByKindServiceAccount,
			Username:                serviceAccountUsernamePrefix + ns + ":" + name,
			ServiceAccountNamespace: ns,
			ServiceAccountName:      name,
		}, nil
	case CreatedByKindUser:
		return &CreatedBy{Kind: CreatedByKindUser, Username: rest}, nil
	default:
		return nil, fmt.Errorf("created-by value %q has unknown identity kind %q", value, kind)
	}
}

// splitServiceAccountUsername decomposes a canonical service-account username
// (system:serviceaccount:<namespace>:<name>) into its parts. Malformed values
// that merely start with the prefix are reported as not-a-service-account so
// they fall back to the verbatim user encoding.
func splitServiceAccountUsername(username string) (namespace, name string, ok bool) {
	rest, found := strings.CutPrefix(username, serviceAccountUsernamePrefix)
	if !found {
		return "", "", false
	}
	namespace, name, found = strings.Cut(rest, ":")
	if !found || namespace == "" || name == "" || strings.Contains(name, ":") {
		return "", "", false
	}
	return namespace, name, true
}
