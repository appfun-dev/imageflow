package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/appfun-dev/imageflow/internal/clock"
)

// Shared fixtures and vocabulary (goconst-friendly).
const (
	tagLatest       = "latest"
	repoApp         = "team/app"
	repoMultiarch   = "team/multiarch"
	repoLegacy      = "team/legacy"
	repoPaged       = "team/paged"
	repoEmpty       = "team/empty"
	repoOctet       = "team/octet"
	deniedRegistry  = "evil-not-allowed.example.com"
	realmPath       = "/v2/auth"
	fakeServiceName = "fake-registry"
	resSuccess      = "success"
	resError        = "error"
	resDenied       = "denied"
	resNotFound     = "not_found"
	resUnauthorized = "unauthorized"
	resTimeout      = "timeout"
	pagedTagCount   = 250
)

// registryForm describes one Quay deployment form. Per ADR-007 the two forms
// differ only in base URL (registry host) and credentials; the exact same
// test suite must pass against both.
type registryForm struct {
	name        string
	registry    string
	username    string
	password    string
	secretKey   string
	secretEntry func(username, password string) map[string]any
}

func dualForms() []registryForm {
	return []registryForm{
		{
			name:      "quay.io SaaS",
			registry:  "quay.io",
			username:  "imageflow+robot",
			password:  "quayio-PW-MARKER-xK92",
			secretKey: "quay.io",
			// docker CLI shape: base64 "auth" field only.
			secretEntry: func(username, password string) map[string]any {
				return map[string]any{
					"auth":  basicAuthValue(username, password),
					"email": "robot@example.com",
				}
			},
		},
		{
			name:      "self-hosted Quay",
			registry:  "quay.prod.example.com:8443",
			username:  "pull-service",
			password:  "selfhosted-PW-MARKER-zQ47",
			secretKey: "https://quay.prod.example.com:8443/v1/",
			// explicit username/password fields.
			secretEntry: func(username, password string) map[string]any {
				return map[string]any{"username": username, "password": password}
			},
		},
	}
}

// harness bundles one fake registry, one client and the form's credentials.
type harness struct {
	t          *testing.T
	form       registryForm
	fake       *fakeRegistry
	client     *HTTPClient
	creds      Credentials
	secret     *corev1.Secret
	metricsReg *prometheus.Registry
}

func newHarness(t *testing.T, form registryForm, customize func(f *fakeRegistry, o *Options)) *harness {
	t.Helper()
	fake := newFakeRegistry(t)
	fake.users[form.username] = form.password
	fake.requireAuth = true
	seedStandardContent(t, fake)

	secret := formSecret(form)
	creds, err := CredentialsFromSecret(secret, form.registry)
	if err != nil {
		t.Fatalf("credentials from pull secret: %v", err)
	}
	reg := prometheus.NewPedanticRegistry()
	opts := Options{
		// Every suite run also exercises the allowlist accept path.
		Allowlist:      []string{form.registry},
		Transport:      rewriteTransport{target: fake.server.URL},
		Registerer:     reg,
		UserAgent:      "imageflow-t6-test",
		RequestTimeout: 5 * time.Second,
	}
	if customize != nil {
		customize(fake, &opts)
	}
	client, err := NewClient(opts)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return &harness{t: t, form: form, fake: fake, client: client, creds: creds, secret: secret, metricsReg: reg}
}

func formSecret(form registryForm) *corev1.Secret {
	configJSON, err := json.Marshal(map[string]any{
		"auths": map[string]any{form.secretKey: form.secretEntry(form.username, form.password)},
	})
	if err != nil {
		panic(err)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "imageflow-system"},
		Type:       corev1.SecretTypeDockerConfigJson,
		Data:       map[string][]byte{corev1.DockerConfigJsonKey: configJSON},
	}
}

func seedStandardContent(t *testing.T, fake *fakeRegistry) {
	t.Helper()
	schema2 := dockerSchema2Manifest(t)
	child := "sha256:" + sha256Hex(schema2)
	fake.putManifest(repoApp, tagLatest, MediaTypeDockerSchema2, schema2)
	fake.putManifest(repoApp, "v1.0", MediaTypeOCIManifest, ociManifest(t))
	fake.putManifest(repoMultiarch, "multi", MediaTypeDockerSchemaList, dockerManifestList(t, child))
	fake.putManifest(repoMultiarch, tagLatest, MediaTypeOCIIndex, ociIndex(t, child))
	fake.putManifest(repoLegacy, "v0", MediaTypeDockerSchema1, schema1Manifest(t))
	fake.putRawManifest(repoOctet, tagLatest, fakeManifest{
		mediaType:   MediaTypeOCIManifest,
		contentType: "application/octet-stream",
		body:        ociManifest(t),
	})
	fake.putTags(repoPaged, pagedTags(pagedTagCount))
	fake.putTags(repoEmpty, nil)
}

func pagedTags(count int) []string {
	tags := make([]string, count)
	for i := range tags {
		tags[i] = fmt.Sprintf("tag-%03d", i)
	}
	return tags
}

func (h *harness) ref(repo, tag string) Reference {
	return Reference{Registry: h.form.registry, Repository: repo, Tag: tag}
}

func (h *harness) digestRef(repo, digest string) Reference {
	return Reference{Registry: h.form.registry, Repository: repo, Digest: digest}
}

func (h *harness) repoRef(repo string) Reference {
	return Reference{Registry: h.form.registry, Repository: repo}
}

func schema2Digest(t *testing.T) string {
	t.Helper()
	return "sha256:" + sha256Hex(dockerSchema2Manifest(t))
}

// TestSeamS2SuiteRunsOnBothQuayForms is the acceptance suite of ticket T6:
// success, 404, 401, manifest schema variants, tag list pagination and
// timeout — the identical suite parameterized over both Quay forms (quay.io
// SaaS base URL + robot credentials, self-hosted base URL + pull secret).
// TestSeamS2SuiteRunsOnBothQuayForms is the acceptance suite of ticket T6:
// success, 404, 401, manifest schema variants, tag list pagination and
// timeout — the identical suite parameterized over both Quay forms (quay.io
// SaaS base URL + robot credentials, self-hosted base URL + pull secret).
// Each subtest body lives in a named suiteXxx function below.
func TestSeamS2SuiteRunsOnBothQuayForms(t *testing.T) {
	for _, form := range dualForms() {
		t.Run(form.name, func(t *testing.T) {
			t.Run("ResolveDigestSuccess", func(t *testing.T) { suiteResolveDigestSuccess(t, form) })
			t.Run("ResolveDigestUnknownTagNotFound", func(t *testing.T) { suiteResolveDigestUnknownTagNotFound(t, form) })
			t.Run("ResolveDigestUnknownRepoNotFound", func(t *testing.T) { suiteResolveDigestUnknownRepoNotFound(t, form) })
			t.Run("FetchManifestByTagSchema2", func(t *testing.T) { suiteFetchManifestByTagSchema2(t, form) })
			t.Run("FetchManifestByDigestSuccess", func(t *testing.T) { suiteFetchManifestByDigestSuccess(t, form) })
			t.Run("FetchManifestDigestWinsOverTag", func(t *testing.T) { suiteFetchManifestDigestWinsOverTag(t, form) })
			t.Run("FetchManifestByDigestMismatch", func(t *testing.T) { suiteFetchManifestByDigestMismatch(t, form) })
			t.Run("FetchManifestHeaderDigestMismatch", func(t *testing.T) { suiteFetchManifestHeaderDigestMismatch(t, form) })
			t.Run("ManifestSchemaVariants", func(t *testing.T) { suiteManifestSchemaVariants(t, form) })
			t.Run("FetchManifestUnknownTagNotFound", func(t *testing.T) { suiteFetchManifestUnknownTagNotFound(t, form) })
			t.Run("UnauthorizedBadPassword", func(t *testing.T) { suiteUnauthorizedBadPassword(t, form) })
			t.Run("UnauthorizedAnonymousIsRejected", func(t *testing.T) { suiteUnauthorizedAnonymousIsRejected(t, form) })
			t.Run("ListTagsPagination", func(t *testing.T) { suiteListTagsPagination(t, form) })
			t.Run("ListTagsSinglePage", func(t *testing.T) { suiteListTagsSinglePage(t, form) })
			t.Run("ListTagsEmptyRepository", func(t *testing.T) { suiteListTagsEmptyRepository(t, form) })
			t.Run("ListTagsUnknownRepoNotFound", func(t *testing.T) { suiteListTagsUnknownRepoNotFound(t, form) })
			t.Run("TimeoutSurfacesAsDeadlineExceeded", func(t *testing.T) { suiteTimeoutSurfacesAsDeadlineExceeded(t, form) })
			t.Run("ServerErrorWithCredentialEchoIsScrubbed", func(t *testing.T) { suiteServerErrorWithCredentialEchoIsScrubbed(t, form) })
			t.Run("RateLimit429MapsToHTTPError", func(t *testing.T) { suiteRateLimit429MapsToHTTPError(t, form) })
			t.Run("CredentialsRedactInAllFmtVerbs", func(t *testing.T) { suiteCredentialsRedactInAllFmtVerbs(t, form) })
		})
	}
}

func suiteResolveDigestSuccess(t *testing.T, form registryForm) {
	h := newHarness(t, form, nil)
	digest, err := h.client.ResolveDigest(context.Background(), h.ref(repoApp, tagLatest), h.creds)
	if err != nil {
		t.Fatalf("resolve digest: %v", err)
	}
	if want := schema2Digest(t); digest != want {
		t.Fatalf("digest = %q, want %q", digest, want)
	}
	assertBearerFlowUsed(t, h, repoApp, form.username)
}

func suiteResolveDigestUnknownTagNotFound(t *testing.T, form registryForm) {
	h := newHarness(t, form, nil)
	_, err := h.client.ResolveDigest(context.Background(), h.ref(repoApp, "missing"), h.creds)
	// HEAD 404 responses carry no body, so no registry code is
	// available; the error class must still be NotFoundError.
	assertNotFound(t, err, "")
}

func suiteResolveDigestUnknownRepoNotFound(t *testing.T, form registryForm) {
	h := newHarness(t, form, nil)
	_, err := h.client.ResolveDigest(context.Background(), h.ref("team/ghost", tagLatest), h.creds)
	assertNotFound(t, err, "")
}

func suiteFetchManifestByTagSchema2(t *testing.T, form registryForm) {
	h := newHarness(t, form, nil)
	body := dockerSchema2Manifest(t)
	manifest, err := h.client.FetchManifest(context.Background(), h.ref(repoApp, tagLatest), h.creds)
	if err != nil {
		t.Fatalf("fetch manifest: %v", err)
	}
	if manifest.Digest != "sha256:"+sha256Hex(body) {
		t.Fatalf("digest = %q", manifest.Digest)
	}
	if manifest.MediaType != MediaTypeDockerSchema2 {
		t.Fatalf("mediaType = %q", manifest.MediaType)
	}
	if manifest.Size != int64(len(body)) || string(manifest.Raw) != string(body) {
		t.Fatalf("size/raw mismatch: size=%d len(body)=%d", manifest.Size, len(body))
	}
	if manifest.Config == nil ||
		manifest.Config.Digest != fixtureConfigDigest() ||
		manifest.Config.MediaType != "application/vnd.docker.container.image.v1+json" ||
		manifest.Config.Size != 7023 {
		t.Fatalf("config descriptor = %+v", manifest.Config)
	}
}

func suiteFetchManifestByDigestSuccess(t *testing.T, form registryForm) {
	h := newHarness(t, form, nil)
	want := schema2Digest(t)
	manifest, err := h.client.FetchManifest(context.Background(), h.digestRef(repoApp, want), h.creds)
	if err != nil {
		t.Fatalf("fetch manifest by digest: %v", err)
	}
	if manifest.Digest != want {
		t.Fatalf("digest = %q, want %q", manifest.Digest, want)
	}
}

func suiteFetchManifestDigestWinsOverTag(t *testing.T, form registryForm) {
	h := newHarness(t, form, nil)
	ociDigest := "sha256:" + sha256Hex(ociManifest(t))
	ref := Reference{
		Registry:   h.form.registry,
		Repository: repoApp,
		Tag:        tagLatest, // would resolve to the schema2 manifest
		Digest:     ociDigest, // must win
	}
	manifest, err := h.client.FetchManifest(context.Background(), ref, h.creds)
	if err != nil {
		t.Fatalf("fetch manifest: %v", err)
	}
	if manifest.MediaType != MediaTypeOCIManifest || manifest.Digest != ociDigest {
		t.Fatalf("digest did not win: mediaType=%q digest=%q", manifest.MediaType, manifest.Digest)
	}
}

func suiteFetchManifestByDigestMismatch(t *testing.T, form registryForm) {
	h := newHarness(t, form, nil)
	requested := "sha256:" + sha256Hex([]byte("original-manifest-body"))
	h.fake.putRawManifest(repoApp, requested, fakeManifest{
		mediaType:    MediaTypeDockerSchema2,
		body:         []byte("corrupted-payload"),
		digest:       requested,
		servedDigest: requested,
	})
	_, err := h.client.FetchManifest(context.Background(), h.digestRef(repoApp, requested), h.creds)
	var mismatch *DigestMismatchError
	if !errors.As(err, &mismatch) || !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("expected DigestMismatchError, got %v", err)
	}
	if mismatch.Expected != requested {
		t.Fatalf("expected digest = %q, want %q", mismatch.Expected, requested)
	}
	if mismatch.Actual != computeDigest([]byte("corrupted-payload")) {
		t.Fatalf("actual digest = %q", mismatch.Actual)
	}
}

func suiteFetchManifestHeaderDigestMismatch(t *testing.T, form registryForm) {
	h := newHarness(t, form, nil)
	bogusHeader := "sha256:" + sha256Hex([]byte("something-else"))
	h.fake.putRawManifest(repoApp, "badheader", fakeManifest{
		mediaType:    MediaTypeDockerSchema2,
		body:         dockerSchema2Manifest(t),
		servedDigest: bogusHeader,
	})
	_, err := h.client.FetchManifest(context.Background(), h.ref(repoApp, "badheader"), h.creds)
	var mismatch *DigestMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("expected DigestMismatchError, got %v", err)
	}
	if mismatch.Expected != bogusHeader {
		t.Fatalf("expected = %q, want header value %q", mismatch.Expected, bogusHeader)
	}
}

func suiteManifestSchemaVariants(t *testing.T, form registryForm) {
	cases := []struct {
		name      string
		repo      string
		tag       string
		mediaType string
		hasConfig bool
	}{
		{"docker schema2", repoApp, tagLatest, MediaTypeDockerSchema2, true},
		{"oci image manifest", repoApp, "v1.0", MediaTypeOCIManifest, true},
		{"docker manifest list", repoMultiarch, "multi", MediaTypeDockerSchemaList, false},
		{"oci image index", repoMultiarch, tagLatest, MediaTypeOCIIndex, false},
		{"docker schema1", repoLegacy, "v0", MediaTypeDockerSchema1, false},
		// Served as application/octet-stream; media type must be sniffed.
		{"octet-stream sniffed", repoOctet, tagLatest, MediaTypeOCIManifest, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, form, nil)
			manifest, err := h.client.FetchManifest(context.Background(), h.ref(tc.repo, tc.tag), h.creds)
			if err != nil {
				t.Fatalf("fetch manifest: %v", err)
			}
			if manifest.MediaType != tc.mediaType {
				t.Fatalf("mediaType = %q, want %q", manifest.MediaType, tc.mediaType)
			}
			if tc.hasConfig && (manifest.Config == nil || manifest.Config.Digest == "") {
				t.Fatalf("expected config descriptor, got %+v", manifest.Config)
			}
			if !tc.hasConfig && manifest.Config != nil {
				t.Fatalf("expected nil config, got %+v", manifest.Config)
			}
			if manifest.Digest != computeDigest(manifest.Raw) {
				t.Fatalf("digest not computed from payload")
			}
		})
	}
}

func suiteFetchManifestUnknownTagNotFound(t *testing.T, form registryForm) {
	h := newHarness(t, form, nil)
	_, err := h.client.FetchManifest(context.Background(), h.ref(repoApp, "missing"), h.creds)
	assertNotFound(t, err, "MANIFEST_UNKNOWN")
}

func suiteUnauthorizedBadPassword(t *testing.T, form registryForm) {
	h := newHarness(t, form, func(f *fakeRegistry, _ *Options) {
		f.users[form.username] = "rotated-on-the-server"
	})
	_, err := h.client.FetchManifest(context.Background(), h.ref(repoApp, tagLatest), h.creds)
	var unauthorized *UnauthorizedError
	if !errors.As(err, &unauthorized) || !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected UnauthorizedError, got %v", err)
	}
	if unauthorized.StatusCode != http.StatusUnauthorized || unauthorized.Anonymous {
		t.Fatalf("unauthorized error = %+v", unauthorized)
	}
	if strings.Contains(err.Error(), form.password) {
		t.Fatalf("password leaked into error: %v", err)
	}
}

func suiteUnauthorizedAnonymousIsRejected(t *testing.T, form registryForm) {
	h := newHarness(t, form, nil)
	_, err := h.client.FetchManifest(context.Background(), h.ref(repoApp, tagLatest), Credentials{})
	var unauthorized *UnauthorizedError
	if !errors.As(err, &unauthorized) || !unauthorized.Anonymous {
		t.Fatalf("expected anonymous UnauthorizedError, got %v", err)
	}
}

func suiteListTagsPagination(t *testing.T, form registryForm) {
	h := newHarness(t, form, nil)
	tags, err := h.client.ListTags(context.Background(), h.repoRef(repoPaged), h.creds)
	if err != nil {
		t.Fatalf("list tags: %v", err)
	}
	want := pagedTags(pagedTagCount)
	if len(tags) != len(want) {
		t.Fatalf("got %d tags, want %d", len(tags), len(want))
	}
	for i := range want {
		if tags[i] != want[i] {
			t.Fatalf("tag[%d] = %q, want %q", i, tags[i], want[i])
		}
	}
	var pages []recordedRequest
	for _, req := range h.fake.requestsFor("/v2/" + repoPaged + "/tags/list") {
		if strings.HasPrefix(req.Auth, "Bearer ") {
			pages = append(pages, req)
		}
	}
	if len(pages) != 3 {
		t.Fatalf("expected 3 authenticated page fetches, got %d", len(pages))
	}
	if got := pages[1].Query.Get("last"); got != "tag-099" {
		t.Fatalf("page 2 last = %q", got)
	}
	if got := pages[2].Query.Get("last"); got != "tag-199" {
		t.Fatalf("page 3 last = %q", got)
	}
}

func suiteListTagsSinglePage(t *testing.T, form registryForm) {
	h := newHarness(t, form, nil)
	tags, err := h.client.ListTags(context.Background(), h.repoRef(repoApp), h.creds)
	if err != nil {
		t.Fatalf("list tags: %v", err)
	}
	if len(tags) != 2 || tags[0] != tagLatest || tags[1] != "v1.0" {
		t.Fatalf("tags = %v", tags)
	}
}

func suiteListTagsEmptyRepository(t *testing.T, form registryForm) {
	h := newHarness(t, form, nil)
	tags, err := h.client.ListTags(context.Background(), h.repoRef(repoEmpty), h.creds)
	if err != nil {
		t.Fatalf("list tags: %v", err)
	}
	if tags == nil || len(tags) != 0 {
		t.Fatalf("tags = %v, want empty non-nil", tags)
	}
}

func suiteListTagsUnknownRepoNotFound(t *testing.T, form registryForm) {
	h := newHarness(t, form, nil)
	_, err := h.client.ListTags(context.Background(), h.repoRef("team/ghost"), h.creds)
	assertNotFound(t, err, "NAME_UNKNOWN")
}

func suiteTimeoutSurfacesAsDeadlineExceeded(t *testing.T, form registryForm) {
	block := make(chan struct{})
	h := newHarness(t, form, func(f *fakeRegistry, o *Options) {
		o.RequestTimeout = 150 * time.Millisecond
		f.setBlockManifests(block)
	})
	defer close(block)
	_, err := h.client.FetchManifest(context.Background(), h.ref(repoApp, tagLatest), h.creds)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	counters := collectCounterValues(t, h.metricsReg)
	if got := counters[counterKey(OpFetchManifest, resTimeout)]; got != 1 {
		t.Fatalf("timeout metric = %v, want 1 (counters: %v)", got, counters)
	}
}

func suiteServerErrorWithCredentialEchoIsScrubbed(t *testing.T, form registryForm) {
	h := newHarness(t, form, nil)
	echo := fmt.Sprintf("echo %s and %s", form.password, basicAuthValue(form.username, form.password))
	h.fake.setForce(http.StatusInternalServerError, "UNSUPPORTED", echo)
	defer h.fake.clearForce()
	_, err := h.client.FetchManifest(context.Background(), h.ref(repoApp, tagLatest), h.creds)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected HTTPError, got %v", err)
	}
	if httpErr.StatusCode != http.StatusInternalServerError || httpErr.Code != "UNSUPPORTED" {
		t.Fatalf("httpErr = %+v", httpErr)
	}
	if !strings.Contains(httpErr.Detail, "[redacted]") {
		t.Fatalf("detail not scrubbed: %q", httpErr.Detail)
	}
	if strings.Contains(err.Error(), form.password) {
		t.Fatalf("password leaked into error: %v", err)
	}
}

func suiteRateLimit429MapsToHTTPError(t *testing.T, form registryForm) {
	h := newHarness(t, form, nil)
	h.fake.setForce(http.StatusTooManyRequests, "TOOMANYREQUESTS", "anonymous pull rate limit exceeded")
	defer h.fake.clearForce()
	_, err := h.client.ResolveDigest(context.Background(), h.ref(repoApp, tagLatest), h.creds)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 HTTPError, got %v", err)
	}
}

func suiteCredentialsRedactInAllFmtVerbs(t *testing.T, form registryForm) {
	h := newHarness(t, form, nil)
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		rendered := fmt.Sprintf(verb, h.creds)
		if strings.Contains(rendered, form.password) {
			t.Fatalf("password leaked via %s: %s", verb, rendered)
		}
		if !strings.Contains(rendered, form.username) {
			t.Fatalf("username missing via %s: %s", verb, rendered)
		}
	}
}

// assertBearerFlowUsed verifies the token exchange behind a successful call:
// one token request with pull scope for the repo, then a Bearer request
// authenticated as the form's user.
func assertBearerFlowUsed(t *testing.T, h *harness, repo, username string) {
	t.Helper()
	tokenRequests := h.fake.requestsFor(realmPath)
	if len(tokenRequests) != 1 {
		t.Fatalf("expected 1 token request, got %d", len(tokenRequests))
	}
	if tokenRequests[0].Service != fakeServiceName {
		t.Fatalf("token service = %q", tokenRequests[0].Service)
	}
	if want := PullScope(repo); tokenRequests[0].Scope != want {
		t.Fatalf("token scope = %q, want %q", tokenRequests[0].Scope, want)
	}
	var sawBearer bool
	for _, req := range h.fake.recorded() {
		if strings.HasPrefix(req.Auth, "Bearer ") && req.User == username && strings.Contains(req.Path, "/manifests/") {
			sawBearer = true
		}
	}
	if !sawBearer {
		t.Fatalf("no Bearer-authenticated manifest request as %q: %+v", username, h.fake.recorded())
	}
}

func assertNotFound(t *testing.T, err error, code string) {
	t.Helper()
	var notFound *NotFoundError
	if !errors.As(err, &notFound) || !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected NotFoundError, got %v", err)
	}
	if code != "" && notFound.Code != code {
		t.Fatalf("code = %q, want %q", notFound.Code, code)
	}
}

// TestAllowlistDeniesUnlistedRegistry is the named acceptance case: registry
// endpoints outside the allowlist are rejected fail-closed, with zero network
// I/O, a distinct *DeniedError and result="denied" metrics.
func TestAllowlistDeniesUnlistedRegistry(t *testing.T) {
	for _, form := range dualForms() {
		t.Run(form.name, func(t *testing.T) {
			h := newHarness(t, form, nil)
			ctx := context.Background()
			denied := Reference{Registry: deniedRegistry, Repository: repoApp, Tag: tagLatest}
			before := h.fake.requestCount()

			_, errResolve := h.client.ResolveDigest(ctx, denied, h.creds)
			_, errFetch := h.client.FetchManifest(ctx, denied, h.creds)
			_, errList := h.client.ListTags(ctx, Reference{Registry: deniedRegistry, Repository: repoApp}, h.creds)

			for i, err := range []error{errResolve, errFetch, errList} {
				var deniedErr *DeniedError
				if !errors.As(err, &deniedErr) || !errors.Is(err, ErrDenied) {
					t.Fatalf("call %d: expected DeniedError, got %v", i, err)
				}
				if deniedErr.Registry != deniedRegistry {
					t.Fatalf("call %d: denied registry = %q", i, deniedErr.Registry)
				}
				if !strings.Contains(err.Error(), "allowlist") {
					t.Fatalf("call %d: error does not mention allowlist: %v", i, err)
				}
			}
			if calls := h.fake.requestCount() - before; calls != 0 {
				t.Fatalf("denied calls performed %d network requests (must fail closed)", calls)
			}
			counters := collectCounterValues(t, h.metricsReg)
			for _, op := range []string{OpResolveDigest, OpFetchManifest, OpListTags} {
				if got := counters[counterKey(op, resDenied)]; got != 1 {
					t.Fatalf("denied counter for %s = %v, want 1", op, got)
				}
			}
		})
	}
}

func TestAllowlistEmptyAllowsAllHosts(t *testing.T) {
	form := dualForms()[0]
	h := newHarness(t, form, func(_ *fakeRegistry, o *Options) { o.Allowlist = nil })
	other := Reference{Registry: "any-other-registry.example.com", Repository: repoApp, Tag: tagLatest}
	manifest, err := h.client.FetchManifest(context.Background(), other, h.creds)
	if err != nil {
		t.Fatalf("empty allowlist must allow all hosts: %v", err)
	}
	if manifest.Digest != schema2Digest(t) {
		t.Fatalf("digest = %q", manifest.Digest)
	}
}

func TestAllowlistRequiresExactPort(t *testing.T) {
	form := dualForms()[1] // self-hosted form runs on host:8443
	h := newHarness(t, form, func(_ *fakeRegistry, o *Options) {
		o.Allowlist = []string{"quay.prod.example.com"} // missing the port
	})
	_, err := h.client.ResolveDigest(context.Background(), h.ref(repoApp, tagLatest), h.creds)
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("expected denial for host:port mismatch, got %v", err)
	}
}

func TestAllowlistMatchesCaseInsensitively(t *testing.T) {
	form := dualForms()[0]
	h := newHarness(t, form, func(_ *fakeRegistry, o *Options) { o.Allowlist = []string{"QUAY.IO"} })
	if _, err := h.client.ResolveDigest(context.Background(), h.ref(repoApp, tagLatest), h.creds); err != nil {
		t.Fatalf("uppercase allowlist entry must match: %v", err)
	}
}

func TestNewClientRejectsMalformedAllowlist(t *testing.T) {
	for _, entry := range []string{"https://quay.io", "quay.io/path", "quay io", ":8443", "quay..io"} {
		if _, err := NewClient(Options{Allowlist: []string{entry}}); err == nil {
			t.Fatalf("allowlist entry %q must be rejected", entry)
		}
	}
}

func TestBasicAuthFlow(t *testing.T) {
	form := dualForms()[0]
	h := newHarness(t, form, func(f *fakeRegistry, _ *Options) { f.authStyle = authSchemeBasic })
	manifest, err := h.client.FetchManifest(context.Background(), h.ref(repoApp, tagLatest), h.creds)
	if err != nil {
		t.Fatalf("fetch over basic auth: %v", err)
	}
	if manifest.Digest != schema2Digest(t) {
		t.Fatalf("digest = %q", manifest.Digest)
	}
	if got := len(h.fake.requestsFor(realmPath)); got != 0 {
		t.Fatalf("basic flow must not hit the token endpoint (%d hits)", got)
	}
	wantAuth := "Basic " + basicAuthValue(form.username, form.password)
	var sawBasic bool
	for _, req := range h.fake.requestsFor("/v2/" + repoApp + "/manifests/" + tagLatest) {
		if req.Auth == wantAuth {
			sawBasic = true
		}
	}
	if !sawBasic {
		t.Fatalf("no request carried the expected Basic header")
	}
}

func TestRegistryTokenSentPreemptively(t *testing.T) {
	form := dualForms()[0]
	const registryToken = "rt-MARKER-preemptive-9"
	fake := newFakeRegistry(t)
	fake.users[form.username] = form.password
	fake.registryTokens[registryToken] = form.username
	fake.requireAuth = true
	seedStandardContent(t, fake)

	client, err := NewClient(Options{Transport: rewriteTransport{target: fake.server.URL}})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	creds := Credentials{RegistryToken: registryToken}
	manifest, err := client.FetchManifest(context.Background(),
		Reference{Registry: form.registry, Repository: repoApp, Tag: tagLatest}, creds)
	if err != nil {
		t.Fatalf("fetch with registry token: %v", err)
	}
	if manifest.Digest != schema2Digest(t) {
		t.Fatalf("digest = %q", manifest.Digest)
	}
	if got := len(fake.requestsFor(realmPath)); got != 0 {
		t.Fatalf("registry token must not trigger a token exchange (%d hits)", got)
	}
	for _, req := range fake.requestsFor("/v2/" + repoApp + "/manifests/" + tagLatest) {
		if req.Auth != "Bearer "+registryToken {
			t.Fatalf("manifest request auth = %q", req.Auth)
		}
	}
}

func TestIdentityTokenUsesRefreshGrant(t *testing.T) {
	form := dualForms()[0]
	// Characters that require URL escaping, proving the query is built and
	// matched correctly.
	const identityToken = "it-MARKER+special/chars=7"
	h := newHarness(t, form, func(f *fakeRegistry, _ *Options) {
		f.refreshTokens[identityToken] = form.username
	})
	creds := Credentials{IdentityToken: identityToken}
	if _, err := h.client.FetchManifest(context.Background(), h.ref(repoApp, tagLatest), creds); err != nil {
		t.Fatalf("fetch with identity token: %v", err)
	}
	tokenRequests := h.fake.requestsFor(realmPath)
	if len(tokenRequests) != 1 {
		t.Fatalf("expected 1 token request, got %d", len(tokenRequests))
	}
	if tokenRequests[0].Grant != "refresh_token" || tokenRequests[0].Refresh != identityToken {
		t.Fatalf("token request = grant %q refresh %q", tokenRequests[0].Grant, tokenRequests[0].Refresh)
	}
}

// TestInvalidReferencesRejected verifies grammar enforcement happens before
// any network I/O (URL/SSRF injection guard).
func TestInvalidReferencesRejected(t *testing.T) {
	transport := &countingTransport{}
	client, err := NewClient(Options{Transport: transport})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx := context.Background()
	creds := Credentials{}
	longTag := "t" + strings.Repeat("a", 128)
	cases := []struct {
		name string
		ref  Reference
		op   string
	}{
		{"registry with scheme", Reference{Registry: "https://quay.io", Repository: repoApp, Tag: tagLatest}, "fetch"},
		{"registry with path", Reference{Registry: "quay.io/proxy", Repository: "app", Tag: tagLatest}, "fetch"},
		{"registry with space", Reference{Registry: "quay io", Repository: repoApp, Tag: tagLatest}, "fetch"},
		{"registry not explicit", Reference{Registry: "dockerhub", Repository: "library/nginx", Tag: tagLatest}, "fetch"},
		{"empty repository", Reference{Registry: "quay.io", Repository: "", Tag: tagLatest}, "fetch"},
		{"uppercase repository", Reference{Registry: "quay.io", Repository: "Team/App", Tag: tagLatest}, "fetch"},
		{"repository traversal", Reference{Registry: "quay.io", Repository: "team/../etc", Tag: tagLatest}, "fetch"},
		{"repository double slash", Reference{Registry: "quay.io", Repository: "team//app", Tag: tagLatest}, "fetch"},
		{"repository leading slash", Reference{Registry: "quay.io", Repository: "/team/app", Tag: tagLatest}, "fetch"},
		{"tag with punctuation", Reference{Registry: "quay.io", Repository: repoApp, Tag: "a!b"}, "fetch"},
		{"tag too long", Reference{Registry: "quay.io", Repository: repoApp, Tag: longTag}, "fetch"},
		{"digest not sha256", Reference{Registry: "quay.io", Repository: repoApp, Digest: "sha512:" + strings.Repeat("a", 128)}, "fetch"},
		{"digest short hex", Reference{Registry: "quay.io", Repository: repoApp, Digest: "sha256:abc"}, "fetch"},
		{"fetch without selector", Reference{Registry: "quay.io", Repository: repoApp}, "fetch"},
		{"resolve without tag", Reference{Registry: "quay.io", Repository: repoApp}, "resolve"},
		{"resolve with digest", Reference{Registry: "quay.io", Repository: repoApp, Digest: schema2Literal()}, "resolve"},
		{"list with tag", Reference{Registry: "quay.io", Repository: repoApp, Tag: tagLatest}, "list"},
		{"list with digest", Reference{Registry: "quay.io", Repository: repoApp, Digest: schema2Literal()}, "list"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			switch tc.op {
			case "fetch":
				_, err = client.FetchManifest(ctx, tc.ref, creds)
			case "resolve":
				_, err = client.ResolveDigest(ctx, tc.ref, creds)
			case "list":
				_, err = client.ListTags(ctx, tc.ref, creds)
			}
			var invalid *InvalidReferenceError
			if !errors.As(err, &invalid) || !errors.Is(err, ErrInvalidReference) {
				t.Fatalf("expected InvalidReferenceError, got %v", err)
			}
		})
	}
	if transport.calls.Load() != 0 {
		t.Fatalf("invalid references performed %d network calls", transport.calls.Load())
	}
}

func schema2Literal() string {
	return "sha256:" + strings.Repeat("ab", 32)
}

type countingTransport struct {
	calls atomic.Int32
}

func (ct *countingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	ct.calls.Add(1)
	return nil, errors.New("network must not be reached")
}

func TestParseReference(t *testing.T) {
	digest := schema2Literal()
	valid := []struct {
		in   string
		want Reference
	}{
		{"quay.io/team/app:latest", Reference{Registry: "quay.io", Repository: "team/app", Tag: "latest"}},
		{"quay.prod.example.com:8443/org/img:v1.2.3", Reference{
			Registry: "quay.prod.example.com:8443", Repository: "org/img", Tag: "v1.2.3",
		}},
		{"localhost:5000/local/img:dev", Reference{Registry: "localhost:5000", Repository: "local/img", Tag: "dev"}},
		{"quay.io/team/app@" + digest, Reference{Registry: "quay.io", Repository: "team/app", Digest: digest}},
		{"quay.io/team/app:latest@" + digest, Reference{
			Registry: "quay.io", Repository: "team/app", Tag: "latest", Digest: digest,
		}},
	}
	for _, tc := range valid {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseReference(tc.in)
			if err != nil {
				t.Fatalf("ParseReference(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("ParseReference(%q) = %+v, want %+v", tc.in, got, tc.want)
			}
		})
	}
	invalid := []string{
		"team/app:latest", // bare first element is not an explicit registry
		"quay.io",
		"quay.io/",
		"/team/app:latest",
		"quay.io/team/app",
		"quay.io/team/app:",
		"quay.io/team/app@sha256:abc",
		"quay.io/team/../app:tag",
		"quay.io/team/app:v1:extra",
		"quay.io/Team/App:latest",
	}
	for _, in := range invalid {
		t.Run("invalid/"+in, func(t *testing.T) {
			if _, err := ParseReference(in); !errors.Is(err, ErrInvalidReference) {
				t.Fatalf("ParseReference(%q) error = %v, want ErrInvalidReference", in, err)
			}
		})
	}
}

func TestReferenceString(t *testing.T) {
	ref := Reference{Registry: "quay.io", Repository: "team/app", Tag: "latest", Digest: schema2Literal()}
	want := "quay.io/team/app:latest@" + schema2Literal()
	if got := ref.String(); got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

// ---- metrics ----

func counterKey(operation, result string) string {
	return fmt.Sprintf("imageflow_registry_requests_total{operation=%q,result=%q}", operation, result)
}

func collectCounterValues(t *testing.T, reg *prometheus.Registry) map[string]float64 {
	t.Helper()
	values := map[string]float64{}
	for _, family := range gatherFamilies(t, reg) {
		if family.GetType() != dto.MetricType_COUNTER {
			continue
		}
		for _, m := range family.GetMetric() {
			values[metricKey(family.GetName(), m)] = m.GetCounter().GetValue()
		}
	}
	return values
}

func collectHistogramStats(t *testing.T, reg *prometheus.Registry, name string) map[string][2]float64 {
	t.Helper()
	stats := map[string][2]float64{}
	for _, family := range gatherFamilies(t, reg) {
		if family.GetName() != name || family.GetType() != dto.MetricType_HISTOGRAM {
			continue
		}
		for _, m := range family.GetMetric() {
			stats[metricKey(name, m)] = [2]float64{
				float64(m.GetHistogram().GetSampleCount()),
				m.GetHistogram().GetSampleSum(),
			}
		}
	}
	return stats
}

func gatherFamilies(t *testing.T, reg *prometheus.Registry) []*dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	return families
}

func metricKey(name string, m *dto.Metric) string {
	parts := make([]string, 0, len(m.GetLabel()))
	for _, lp := range m.GetLabel() {
		parts = append(parts, fmt.Sprintf("%s=%q", lp.GetName(), lp.GetValue()))
	}
	sort.Strings(parts)
	return name + "{" + strings.Join(parts, ",") + "}"
}

func TestMetricsRequestsAndDurations(t *testing.T) {
	form := dualForms()[0]
	h := newHarness(t, form, nil)
	ctx := context.Background()

	if _, err := h.client.FetchManifest(ctx, h.ref(repoApp, tagLatest), h.creds); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if _, err := h.client.FetchManifest(ctx, h.ref(repoApp, "missing"), h.creds); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
	denied := Reference{Registry: deniedRegistry, Repository: repoApp, Tag: tagLatest}
	if _, err := h.client.FetchManifest(ctx, denied, h.creds); !errors.Is(err, ErrDenied) {
		t.Fatalf("expected denied, got %v", err)
	}
	if _, err := h.client.ResolveDigest(ctx, h.ref(repoApp, tagLatest), h.creds); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if _, err := h.client.ListTags(ctx, h.repoRef(repoApp), h.creds); err != nil {
		t.Fatalf("list: %v", err)
	}

	counters := collectCounterValues(t, h.metricsReg)
	want := map[string]float64{
		counterKey(OpFetchManifest, resSuccess):  1,
		counterKey(OpFetchManifest, resNotFound): 1,
		counterKey(OpFetchManifest, resDenied):   1,
		counterKey(OpResolveDigest, resSuccess):  1,
		counterKey(OpListTags, resSuccess):       1,
	}
	for key, wantValue := range want {
		if got := counters[key]; got != wantValue {
			t.Fatalf("counter %s = %v, want %v (all: %v)", key, got, wantValue, counters)
		}
	}
	stats := collectHistogramStats(t, h.metricsReg, "imageflow_registry_request_duration_seconds")
	if got := stats[`imageflow_registry_request_duration_seconds{operation="fetch_manifest",result="success"}`]; got[0] != 1 {
		t.Fatalf("duration histogram success count = %v, want 1 (stats: %v)", got[0], stats)
	}
	if got := stats[`imageflow_registry_request_duration_seconds{operation="fetch_manifest",result="denied"}`]; got[0] != 1 {
		t.Fatalf("duration histogram denied count = %v, want 1", got[0])
	}
}

// TestDurationMetricUsesClockSeam pins the observed duration to the injected
// clock: the operation starts at T0, the fake clock advances exactly 3s while
// the registry handler blocks, and the histogram must observe exactly 3.
func TestDurationMetricUsesClockSeam(t *testing.T) {
	form := dualForms()[0]
	clk := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	block := make(chan struct{})
	h := newHarness(t, form, func(f *fakeRegistry, o *Options) {
		o.Clock = clk
		f.setBlockManifests(block)
	})

	errCh := make(chan error, 1)
	go func() {
		_, err := h.client.FetchManifest(context.Background(), h.ref(repoApp, tagLatest), h.creds)
		errCh <- err
	}()
	select {
	case <-h.fake.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("manifest handler never entered the blocked state")
	}
	clk.Advance(3 * time.Second)
	close(block)
	if err := <-errCh; err != nil {
		t.Fatalf("fetch manifest: %v", err)
	}

	stats := collectHistogramStats(t, h.metricsReg, "imageflow_registry_request_duration_seconds")
	key := `imageflow_registry_request_duration_seconds{operation="fetch_manifest",result="success"}`
	got, ok := stats[key]
	if !ok || got[0] != 1 {
		t.Fatalf("expected exactly one success observation, got %v (stats: %v)", got, stats)
	}
	if got[1] != 3.0 {
		t.Fatalf("observed duration sum = %v seconds, want exactly 3 (fake clock)", got[1])
	}
}
