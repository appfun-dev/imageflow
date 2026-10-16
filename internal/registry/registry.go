// Package registry implements ImageFlow's read-only access to external image
// registries over the standard OCI Distribution / Docker Registry HTTP API V2.
//
// # Frozen seam S2
//
// The Client interface in this file is the frozen test seam S2 (spec issue #1,
// Testing Decision 3; ADR-007). Downstream consumers — starting with the T7
// Import controller — MUST access registries only through Client. The three
// operations are frozen:
//
//   - manifest fetch   → FetchManifest
//   - digest resolve   → ResolveDigest
//   - tag list         → ListTags
//
// Changing a signature or adding a method to Client is a breaking change to
// the seam and requires an ADR update first.
//
// # Constraints (ADR-007, spec Implementation Decision 10)
//
//   - Standard OCI / Docker Registry v2 API only; zero Quay-proprietary API
//     dependency. The same implementation covers both Quay forms (quay.io SaaS
//     and self-hosted Quay); they differ only in the registry host carried by
//     Reference and in the Credentials used.
//   - Read-only: push is never performed; blob/layer data is never fetched or
//     stored. Only manifests and their metadata cross this seam.
//   - Credentials are parsed read-only from dockerconfigjson pull secrets
//     (see credentials.go) and never appear in any output surface: error
//     messages redact credential material, including material echoed back by
//     a compromised or misbehaving registry.
//   - Registry endpoints are gated by a hostname allowlist (SSRF guard, fed
//     from the --registry-allowlist flag). Denied hosts fail closed with
//     *DeniedError before any network I/O.
//
// # Error taxonomy
//
// Errors are stable for consumers; classify with errors.Is against the
// sentinels or errors.As against the concrete types:
//
//   - *DeniedError / ErrDenied — allowlist rejection; no network I/O happened.
//   - *InvalidReferenceError / ErrInvalidReference — malformed reference; no
//     network I/O happened.
//   - *NotFoundError / ErrNotFound — HTTP 404 (MANIFEST_UNKNOWN, NAME_UNKNOWN).
//   - *UnauthorizedError / ErrUnauthorized — HTTP 401/403 after the auth
//     attempt; carries no server-provided detail by design.
//   - *DigestMismatchError / ErrDigestMismatch — payload digest differs from
//     the requested digest or the server-reported Docker-Content-Digest.
//   - *HTTPError — any other non-2xx status (429 rate limits, 5xx, ...).
//   - Timeouts satisfy errors.Is(err, ErrTimeout) or
//     errors.Is(err, context.DeadlineExceeded); other transport failures wrap
//     the underlying net error.
//
// No error produced by this package ever contains credential material.
package registry

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Manifest media types understood by the client. The registry decides which
// one a given reference serves; the client sends all of them in Accept.
const (
	MediaTypeDockerSchema2    = "application/vnd.docker.distribution.manifest.v2+json"
	MediaTypeDockerSchema1    = "application/vnd.docker.distribution.manifest.v1+json"
	MediaTypeDockerSchemaList = "application/vnd.docker.distribution.manifest.list.v2+json"
	MediaTypeOCIManifest      = "application/vnd.oci.image.manifest.v1+json"
	MediaTypeOCIIndex         = "application/vnd.oci.image.index.v1+json"
)

// manifestAcceptTypes is the Accept header value sent on manifest requests.
// A registry serves the stored manifest for a reference when its media type is
// accepted, so the order does not select content; all known types are listed.
var manifestAcceptTypes = []string{
	MediaTypeDockerSchema2,
	MediaTypeOCIManifest,
	MediaTypeDockerSchemaList,
	MediaTypeOCIIndex,
	MediaTypeDockerSchema1,
}

// Sentinel errors for errors.Is classification. The concrete types carry
// structured detail; the sentinels give consumers a stable, cheap check.
var (
	// ErrDenied marks allowlist rejections (SSRF guard, fail closed).
	ErrDenied = errors.New("registry endpoint denied by allowlist")
	// ErrNotFound marks HTTP 404 responses from the registry.
	ErrNotFound = errors.New("registry object not found")
	// ErrUnauthorized marks authentication/authorization failures (401/403).
	ErrUnauthorized = errors.New("registry authentication failed")
	// ErrDigestMismatch marks content whose computed digest differs from the
	// expected digest.
	ErrDigestMismatch = errors.New("manifest digest mismatch")
	// ErrInvalidReference marks malformed image references.
	ErrInvalidReference = errors.New("invalid image reference")
	// ErrTimeout marks registry operations that exceeded a deadline. Errors
	// caused by an expired caller context also satisfy
	// errors.Is(err, context.DeadlineExceeded).
	ErrTimeout = errors.New("registry request timed out")
	// ErrNoCredentials marks a pull secret that holds no entry for the
	// requested registry (see CredentialsFromSecret).
	ErrNoCredentials = errors.New("no credentials for registry in pull secret")
)

// Client is the frozen S2 seam: the only interface through which ImageFlow
// controllers access external registries. Implementations are safe for
// concurrent use.
//
// creds carries the read-only authentication material for the target registry,
// typically obtained via CredentialsFromSecret from the dockerconfigjson pull
// secret referenced by the requesting object. The zero Credentials value
// requests anonymous access. Credentials are never echoed back in results or
// errors.
type Client interface {
	// ResolveDigest resolves ref.Tag to the immutable content digest
	// ("sha256:<64 hex>") via a manifest HEAD request, returning the
	// server-reported Docker-Content-Digest. ref.Tag must be set and
	// ref.Digest must be empty.
	ResolveDigest(ctx context.Context, ref Reference, creds Credentials) (string, error)

	// FetchManifest retrieves a manifest by tag or by digest and verifies its
	// content digest. When ref.Digest is set it addresses the manifest and the
	// computed digest must match (otherwise *DigestMismatchError); ref.Tag is
	// then ignored. When only ref.Tag is set, the digest is computed from the
	// payload and cross-checked against the server-reported
	// Docker-Content-Digest header when present. At least one of ref.Tag or
	// ref.Digest must be set.
	//
	// Only manifest metadata crosses this seam: the returned Manifest carries
	// the raw manifest bytes plus derived metadata. Blob/layer content is
	// never fetched.
	FetchManifest(ctx context.Context, ref Reference, creds Credentials) (*Manifest, error)

	// ListTags returns all tags of the repository named by ref.Registry and
	// ref.Repository, following registry pagination to completion. ref.Tag and
	// ref.Digest must be empty.
	ListTags(ctx context.Context, ref Reference, creds Credentials) ([]string, error)
}

// Reference identifies an image repository — and optionally a tag or digest
// within it — on a specific registry. It is the frozen value type of the S2
// seam; the string form parsed from user input is
// "<registry>/<repository>[:<tag>][@<digest>]" (see ParseReference).
type Reference struct {
	// Registry is the registry host, optionally with port ("quay.io",
	// "quay.prod.example.com:8443"). No scheme, no path.
	Registry string
	// Repository is the repository path ("team/app"). Lowercase OCI grammar.
	Repository string
	// Tag is the mutable reference ("latest"). Empty when addressing by digest.
	Tag string
	// Digest is the immutable reference ("sha256:<64 hex>"). Empty when
	// addressing by tag.
	Digest string
}

// String renders the canonical string form of the reference. Both Tag and
// Digest may appear ("repo:tag@digest"); the digest then wins for addressing.
func (r Reference) String() string {
	s := r.Registry + "/" + r.Repository
	if r.Tag != "" {
		s += ":" + r.Tag
	}
	if r.Digest != "" {
		s += "@" + r.Digest
	}
	return s
}

// Descriptor is an OCI content descriptor: metadata pointer to content. The
// client only ever exposes descriptors of manifests and image configs, never
// of blobs it fetched.
type Descriptor struct {
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
}

// Manifest is the result of FetchManifest: the manifest payload plus derived
// metadata. ImageFlow stores only this metadata and the digest (ADR-007:
// never blob/layer data).
type Manifest struct {
	// Digest is the "sha256:<64 hex>" digest computed from Raw. It is the
	// authoritative identity of the manifest.
	Digest string
	// MediaType is the manifest media type: the response Content-Type when it
	// is a known manifest type, otherwise sniffed from the payload.
	MediaType string
	// Size is len(Raw) in bytes.
	Size int64
	// Raw is the manifest payload as served.
	Raw []byte
	// Config is the image config descriptor (the "image ID") for
	// single-platform manifests (Docker schema2 / OCI image manifest). It is
	// nil for manifest lists, image indexes and schema1 manifests.
	Config *Descriptor
}

// Credentials carries read-only registry authentication material, normally
// parsed from a dockerconfigjson pull secret. The zero value means anonymous
// access.
//
// Credentials redacts itself in every fmt output (String and GoString), so
// accidental "%v"/"%#v" logging of a Credentials value cannot leak secrets.
// Error construction in this package additionally scrubs credential material
// from server-provided text.
type Credentials struct {
	// Username for basic/token authentication (e.g. a Quay robot account
	// "org+robot"). Not treated as secret.
	Username string
	// Password or robot token paired with Username. Secret.
	Password string
	// IdentityToken is an OAuth-style refresh/identity token (docker config
	// "identitytoken"). Secret.
	IdentityToken string
	// RegistryToken is a ready-to-use bearer token (docker config
	// "registrytoken") sent as-is without a token exchange. Secret.
	RegistryToken string
}

// Anonymous reports whether the credentials carry no authentication material.
func (c Credentials) Anonymous() bool {
	return c.Username == "" && c.Password == "" &&
		c.IdentityToken == "" && c.RegistryToken == ""
}

// String implements fmt.Stringer with all secret material redacted.
func (c Credentials) String() string { return c.redacted() }

// GoString implements fmt.GoStringer so that %#v output is redacted too.
func (c Credentials) GoString() string { return c.redacted() }

func (c Credentials) redacted() string {
	var b strings.Builder
	b.WriteString("registry.Credentials{Username:")
	b.WriteString(fmt.Sprintf("%q", c.Username))
	b.WriteString(" Password:")
	b.WriteString(maskSecret(c.Password))
	b.WriteString(" IdentityToken:")
	b.WriteString(maskSecret(c.IdentityToken))
	b.WriteString(" RegistryToken:")
	b.WriteString(maskSecret(c.RegistryToken))
	b.WriteString("}")
	return b.String()
}

func maskSecret(v string) string {
	if v == "" {
		return `""`
	}
	return `"[redacted]"`
}

// DeniedError reports a registry endpoint rejected by the allowlist. The
// check happens before any network I/O (fail closed, SSRF guard).
type DeniedError struct {
	// Registry is the denied registry host.
	Registry string
}

func (e *DeniedError) Error() string {
	return fmt.Sprintf("registry %q is not in the registry allowlist", e.Registry)
}

// Is matches errors.Is(err, ErrDenied).
func (e *DeniedError) Is(target error) bool { return target == ErrDenied }

// NotFoundError reports an HTTP 404 from the registry (unknown manifest, tag
// or repository).
type NotFoundError struct {
	// Registry is the registry host that answered.
	Registry string
	// Target is the reference or repository that was not found.
	Target string
	// Code is the registry error code (e.g. MANIFEST_UNKNOWN, NAME_UNKNOWN)
	// when the response carried one.
	Code string
	// Detail is the scrubbed, truncated server message, if any.
	Detail string
}

func (e *NotFoundError) Error() string {
	msg := fmt.Sprintf("registry %q returned not found for %s", e.Registry, e.Target)
	if e.Code != "" {
		msg += fmt.Sprintf(" (code %s)", e.Code)
	}
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	return msg
}

// Is matches errors.Is(err, ErrNotFound).
func (e *NotFoundError) Is(target error) bool { return target == ErrNotFound }

// UnauthorizedError reports an authentication or authorization failure
// (HTTP 401/403) after the client's auth attempt. By design it carries no
// server-provided detail: an auth failure is the one path where a compromised
// registry could try to echo credential material back into user-visible
// output, so nothing from the response body is surfaced.
type UnauthorizedError struct {
	// Registry is the registry host that answered.
	Registry string
	// StatusCode is 401 or 403.
	StatusCode int
	// Anonymous records whether the request carried no credentials at all.
	Anonymous bool
}

func (e *UnauthorizedError) Error() string {
	if e.Anonymous {
		return fmt.Sprintf("registry %q requires authentication (HTTP %d); no credentials were provided",
			e.Registry, e.StatusCode)
	}
	return fmt.Sprintf("registry %q rejected the provided credentials (HTTP %d); check the referenced pull secret",
		e.Registry, e.StatusCode)
}

// Is matches errors.Is(err, ErrUnauthorized).
func (e *UnauthorizedError) Is(target error) bool { return target == ErrUnauthorized }

// DigestMismatchError reports manifest content whose computed digest differs
// from the expected digest.
type DigestMismatchError struct {
	// Expected is the digest the manifest was expected to have: the digest
	// requested by the caller, or the server-reported Docker-Content-Digest.
	Expected string
	// Actual is the digest computed from the served payload.
	Actual string
	// Registry is the registry host that served the payload.
	Registry string
	// Target is the reference that was fetched.
	Target string
}

func (e *DigestMismatchError) Error() string {
	return fmt.Sprintf("manifest digest mismatch from registry %q for %s: expected %s, computed %s",
		e.Registry, e.Target, e.Expected, e.Actual)
}

// Is matches errors.Is(err, ErrDigestMismatch).
func (e *DigestMismatchError) Is(target error) bool { return target == ErrDigestMismatch }

// HTTPError reports a non-2xx registry response outside the dedicated classes
// (429 rate limits, 5xx server errors, protocol violations).
type HTTPError struct {
	// Registry is the registry host that answered.
	Registry string
	// Target is the reference or repository targeted by the request.
	Target string
	// StatusCode is the HTTP status returned.
	StatusCode int
	// Code is the registry error code from the response body, if any.
	Code string
	// Detail is the scrubbed (credential material removed) and truncated
	// server message, if any.
	Detail string
}

func (e *HTTPError) Error() string {
	msg := fmt.Sprintf("registry %q returned HTTP %d for %s", e.Registry, e.StatusCode, e.Target)
	if e.Code != "" {
		msg += fmt.Sprintf(" (code %s)", e.Code)
	}
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	return msg
}

// InvalidReferenceError reports a reference that violates the ImageFlow
// reference grammar. The check happens before any network I/O.
type InvalidReferenceError struct {
	// Reference is the offending reference as provided.
	Reference Reference
	// Field is the offending field ("registry", "repository", "tag",
	// "digest", or "tag/digest" for combination violations).
	Field string
	// Reason explains the violation.
	Reason string
}

func (e *InvalidReferenceError) Error() string {
	return fmt.Sprintf("invalid image reference %q: %s %s", e.Reference.String(), e.Field, e.Reason)
}

// Is matches errors.Is(err, ErrInvalidReference).
func (e *InvalidReferenceError) Is(target error) bool { return target == ErrInvalidReference }

func (r Reference) invalid(field, reason string) error {
	return &InvalidReferenceError{Reference: r, Field: field, Reason: reason}
}

// Reference grammar (OCI Distribution). The registry must be an explicit
// host[:port]; ImageFlow never infers a default registry.
var (
	registryHostRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*(:[0-9]+)?$`)
	repositoryRe   = regexp.MustCompile(`^[a-z0-9]+((\.|__|_|-+)[a-z0-9]+)*(/[a-z0-9]+((\.|__|_|-+)[a-z0-9]+)*)*$`)
	tagRe          = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)
	digestRe       = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	errorCodeRe    = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)
)

const maxRepositoryLength = 255

// validateCommon checks the fields every operation requires: an explicit,
// well-formed registry host and repository.
func (r Reference) validateCommon() error {
	host := strings.ToLower(strings.TrimSpace(r.Registry))
	if host == "" {
		return r.invalid("registry", "must be a non-empty host[:port]")
	}
	if !registryHostRe.MatchString(host) || !isExplicitHost(host) {
		return r.invalid("registry",
			`must be an explicit host[:port] (no scheme, no path; "localhost", a dotted name, or host:port)`)
	}
	if r.Repository == "" {
		return r.invalid("repository", "must be non-empty")
	}
	if len(r.Repository) > maxRepositoryLength {
		return r.invalid("repository", fmt.Sprintf("exceeds %d characters", maxRepositoryLength))
	}
	if !repositoryRe.MatchString(r.Repository) {
		return r.invalid("repository", "must follow the lowercase OCI repository grammar")
	}
	return nil
}

// isExplicitHost reports whether the normalized host is unambiguous: a port,
// a dotted name, or "localhost". Bare single-label names other than
// "localhost" are rejected because Docker would read them as Docker Hub
// repositories; ImageFlow references always carry an explicit registry.
func isExplicitHost(host string) bool {
	if host == "localhost" || strings.HasPrefix(host, "localhost:") {
		return true
	}
	return strings.Contains(host, ".") || strings.Contains(host, ":")
}

func validateTag(r Reference) error {
	if !tagRe.MatchString(r.Tag) {
		return r.invalid("tag", "must match [A-Za-z0-9_][A-Za-z0-9_.-]{0,127}")
	}
	return nil
}

func validateDigest(r Reference) error {
	if !digestRe.MatchString(r.Digest) {
		return r.invalid("digest", `must be "sha256:<64 lowercase hex>"`)
	}
	return nil
}

// validateForResolve checks a ResolveDigest call: tag addressing only.
func (r Reference) validateForResolve() error {
	if err := r.validateCommon(); err != nil {
		return err
	}
	if r.Digest != "" {
		return r.invalid("tag/digest", "digest resolution requires a tag and no digest")
	}
	if r.Tag == "" {
		return r.invalid("tag", "must be set for digest resolution")
	}
	return validateTag(r)
}

// validateForFetch checks a FetchManifest call: digest wins when both are set.
func (r Reference) validateForFetch() error {
	if err := r.validateCommon(); err != nil {
		return err
	}
	switch {
	case r.Digest != "":
		return validateDigest(r)
	case r.Tag != "":
		return validateTag(r)
	default:
		return r.invalid("tag/digest", "either tag or digest must be set")
	}
}

// validateForListTags checks a ListTags call: repository addressing only.
func (r Reference) validateForListTags() error {
	if err := r.validateCommon(); err != nil {
		return err
	}
	if r.Tag != "" || r.Digest != "" {
		return r.invalid("tag/digest", "must be empty for tag listing")
	}
	return nil
}

// ParseReference parses the canonical image reference grammar
// "<registry>/<repository>[:<tag>][@<digest>]". The registry element must be
// an explicit host (a dotted name, host:port, or localhost); bare Docker Hub
// shorthands are rejected. The result is validated with the same grammar the
// Client operations enforce.
func ParseReference(s string) (Reference, error) {
	var ref Reference
	rest := s
	if i := strings.Index(rest, "@"); i >= 0 {
		ref.Digest = rest[i+1:]
		rest = rest[:i]
	}
	slash := strings.Index(rest, "/")
	if slash <= 0 || slash == len(rest)-1 {
		return Reference{}, &InvalidReferenceError{
			Reference: Reference{Registry: s},
			Field:     "registry/repository",
			Reason:    `expected "<registry>/<repository>[:<tag>][@<digest>]" with an explicit registry host`,
		}
	}
	ref.Registry = rest[:slash]
	rest = rest[slash+1:]
	if i := strings.LastIndex(rest, ":"); i >= 0 && !strings.Contains(rest[i+1:], "/") {
		ref.Tag = rest[i+1:]
		rest = rest[:i]
	}
	ref.Repository = rest
	if ref.Tag == "" && ref.Digest == "" {
		return ref, ref.invalid("tag/digest", "reference must carry a tag, a digest, or both")
	}
	if ref.Digest != "" {
		if err := validateDigest(ref); err != nil {
			return ref, err
		}
	}
	if ref.Tag != "" {
		if err := validateTag(ref); err != nil {
			return ref, err
		}
	}
	if err := ref.validateCommon(); err != nil {
		return ref, err
	}
	return ref, nil
}

// PullScope returns the registry token scope granting pull access to the
// repository: "repository:<name>:pull".
func PullScope(repository string) string {
	return "repository:" + repository + ":pull"
}
