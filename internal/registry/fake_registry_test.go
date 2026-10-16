package registry

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeRegistry is an httptest double of a Docker Registry v2 endpoint with a
// token (bearer) or basic auth front door. It is the S2 test double required
// by spec Testing Decision 3; no test talks to a real network.
type fakeRegistry struct {
	t      *testing.T
	server *httptest.Server

	mu              sync.Mutex
	users           map[string]string // username -> password
	refreshTokens   map[string]string // identity token -> username
	registryTokens  map[string]string // ready-to-use bearer token -> username
	issuedTokens    map[string]string // token issued by the token endpoint -> username
	tokenCounter    int
	anonymousTokens bool // token endpoint serves anonymous tokens

	requireAuth bool   // repository endpoints require valid credentials
	authStyle   string // "bearer" (default) or "basic"
	realmPath   string // token endpoint path, default "/v2/auth"

	repos map[string]*fakeRepo

	forceStatus  int    // when > 0, repository endpoints answer with this status
	forceCode    string // registry error code for forced responses
	forceMessage string // registry error message for forced responses (may embed markers)

	blockManifests chan struct{} // manifest handlers wait for close (or request ctx done)
	blocked        chan struct{} // signals once that a handler entered the block

	requests []recordedRequest
}

// recordedRequest is one request seen by the fake (test-side observation).
type recordedRequest struct {
	Method  string
	Path    string
	Query   url.Values
	Auth    string // raw Authorization header ("" when absent)
	User    string // authenticated username when resolvable
	Service string // token endpoint: service query parameter
	Scope   string // token endpoint: scope query parameter
	Grant   string // token endpoint: grant_type query parameter
	Refresh string // token endpoint: refresh_token query parameter
}

type fakeRepo struct {
	tags      []string
	byTag     map[string]string       // tag -> digest
	manifests map[string]fakeManifest // digest or served key -> manifest
}

type fakeManifest struct {
	mediaType    string
	contentType  string // overrides the served Content-Type ("" = mediaType)
	body         []byte
	digest       string // canonical digest of body
	servedDigest string // overrides the Docker-Content-Digest header ("" = digest)
}

func newFakeRegistry(t *testing.T) *fakeRegistry {
	t.Helper()
	f := &fakeRegistry{
		t:              t,
		users:          map[string]string{},
		refreshTokens:  map[string]string{},
		registryTokens: map[string]string{},
		issuedTokens:   map[string]string{},
		repos:          map[string]*fakeRepo{},
		authStyle:      "bearer",
		realmPath:      "/v2/auth",
		blocked:        make(chan struct{}, 1),
	}
	f.server = httptest.NewServer(f)
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.record(r)
	switch {
	case r.URL.Path == f.realmPath:
		f.handleToken(w, r)
	case strings.HasPrefix(r.URL.Path, "/v2/"):
		f.handleV2(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeRegistry) record(r *http.Request) {
	entry := recordedRequest{
		Method:  r.Method,
		Path:    r.URL.Path,
		Query:   r.URL.Query(),
		Auth:    r.Header.Get("Authorization"),
		Service: r.URL.Query().Get("service"),
		Scope:   r.URL.Query().Get("scope"),
		Grant:   r.URL.Query().Get("grant_type"),
		Refresh: r.URL.Query().Get("refresh_token"),
	}
	entry.User, _ = f.authenticate(r)
	f.mu.Lock()
	f.requests = append(f.requests, entry)
	f.mu.Unlock()
}

// authenticate resolves the request Authorization header to a username.
func (f *fakeRegistry) authenticate(r *http.Request) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.authenticateLocked(r.Header.Get("Authorization"))
}

func (f *fakeRegistry) authenticateLocked(header string) (string, bool) {
	switch {
	case header == "":
		return "", false
	case strings.HasPrefix(header, "Bearer "):
		token := strings.TrimPrefix(header, "Bearer ")
		if user, ok := f.issuedTokens[token]; ok {
			return user, true
		}
		if user, ok := f.registryTokens[token]; ok {
			return user, true
		}
		return "", false
	case strings.HasPrefix(header, "Basic "):
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(header, "Basic "))
		if err != nil {
			return "", false
		}
		username, password, found := strings.Cut(string(decoded), ":")
		if !found {
			return "", false
		}
		if want, ok := f.users[username]; ok && want == password {
			return username, true
		}
		return "", false
	default:
		return "", false
	}
}

func (f *fakeRegistry) handleToken(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	query := r.URL.Query()
	if query.Get("grant_type") == "refresh_token" {
		if user, ok := f.refreshTokens[query.Get("refresh_token")]; ok {
			f.issueTokenLocked(w, user)
			return
		}
		f.tokenDenied(w)
		return
	}
	if _, _, ok := r.BasicAuth(); ok {
		username, password, _ := r.BasicAuth()
		if want, known := f.users[username]; known && want == password {
			f.issueTokenLocked(w, username)
			return
		}
		f.tokenDenied(w)
		return
	}
	if f.anonymousTokens {
		f.issueTokenLocked(w, "anonymous")
		return
	}
	f.tokenDenied(w)
}

func (f *fakeRegistry) issueTokenLocked(w http.ResponseWriter, user string) {
	f.tokenCounter++
	token := fmt.Sprintf("fake-token-%d", f.tokenCounter)
	f.issuedTokens[token] = user
	w.Header().Set("Content-Type", "application/json")
	payload, _ := json.Marshal(map[string]string{"token": token, "access_token": token})
	_, _ = w.Write(payload)
}

func (f *fakeRegistry) tokenDenied(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	payload, _ := json.Marshal(map[string]any{
		"errors": []map[string]string{{"code": "UNAUTHORIZED", "message": "invalid credentials or token"}},
	})
	_, _ = w.Write(payload)
}

func (f *fakeRegistry) handleV2(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v2/")
	if i := strings.LastIndex(rest, "/tags/list"); i >= 0 {
		f.handleTagsList(w, r, rest[:i])
		return
	}
	if i := strings.LastIndex(rest, "/manifests/"); i >= 0 {
		f.handleManifest(w, r, rest[:i], rest[i+len("/manifests/"):])
		return
	}
	f.writeError(w, http.StatusNotFound, "NAME_UNKNOWN", "unsupported v2 path "+r.URL.Path)
}

// authorized reports whether the request may proceed: valid credentials, or
// an anonymous request against a registry form that does not require auth.
func (f *fakeRegistry) authorized(r *http.Request) bool {
	_, ok := f.authenticate(r)
	if ok {
		return true
	}
	return !f.requireAuth && r.Header.Get("Authorization") == ""
}

func (f *fakeRegistry) challenge(w http.ResponseWriter, repo string) {
	if f.authStyle == authSchemeBasic {
		w.Header().Set("WWW-Authenticate", `Basic realm="fake-registry"`)
	} else {
		w.Header().Set("WWW-Authenticate", fmt.Sprintf(
			`Bearer realm="%s%s",service="fake-registry",scope="repository:%s:pull"`,
			f.server.URL, f.realmPath, repo))
	}
	f.writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "authentication required")
}

func (f *fakeRegistry) handleManifest(w http.ResponseWriter, r *http.Request, repo, ref string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		f.writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", r.Method)
		return
	}
	f.mu.Lock()
	block := f.blockManifests
	f.mu.Unlock()
	if block != nil {
		select {
		case f.blocked <- struct{}{}:
		default:
		}
		select {
		case <-block:
		case <-r.Context().Done():
			return
		}
	}
	if f.forced(w) {
		return
	}
	if !f.authorized(r) {
		f.challenge(w, repo)
		return
	}
	f.mu.Lock()
	entry, ok := f.lookupManifestLocked(repo, ref)
	f.mu.Unlock()
	if !ok {
		code := "MANIFEST_UNKNOWN"
		if _, repoKnown := f.repoExists(repo); !repoKnown {
			code = "NAME_UNKNOWN"
		}
		f.writeError(w, http.StatusNotFound, code, fmt.Sprintf("%s/%s not found", repo, ref))
		return
	}
	served := entry.servedDigest
	if served == "" {
		served = entry.digest
	}
	body := entry.body
	contentType := entry.contentType
	if contentType == "" {
		contentType = entry.mediaType
	}
	w.Header().Set(headerDockerContentDigest, served)
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, _ = w.Write(body)
}

func (f *fakeRegistry) lookupManifestLocked(repo, ref string) (fakeManifest, bool) {
	repository, ok := f.repos[repo]
	if !ok {
		return fakeManifest{}, false
	}
	if m, ok := repository.manifests[ref]; ok {
		return m, true
	}
	if digest, ok := repository.byTag[ref]; ok {
		if m, ok := repository.manifests[digest]; ok {
			return m, true
		}
	}
	return fakeManifest{}, false
}

func (f *fakeRegistry) repoExists(repo string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.repos[repo]
	return repo, ok
}

// forced answers with the configured forced error status when set.
func (f *fakeRegistry) forced(w http.ResponseWriter) bool {
	f.mu.Lock()
	status, code, message := f.forceStatus, f.forceCode, f.forceMessage
	f.mu.Unlock()
	if status <= 0 {
		return false
	}
	if code == "" {
		code = "UNKNOWN"
	}
	f.writeError(w, status, code, message)
	return true
}

func (f *fakeRegistry) handleTagsList(w http.ResponseWriter, r *http.Request, repo string) {
	if f.forced(w) {
		return
	}
	if !f.authorized(r) {
		f.challenge(w, repo)
		return
	}
	f.mu.Lock()
	repository, known := f.repos[repo]
	var tags []string
	if known {
		tags = append(tags, repository.tags...)
	}
	f.mu.Unlock()
	if !known {
		f.writeError(w, http.StatusNotFound, "NAME_UNKNOWN", "repository "+repo+" not found")
		return
	}
	query := r.URL.Query()
	size := len(tags)
	if n, err := strconv.Atoi(query.Get("n")); err == nil && n > 0 {
		size = n
	}
	start := 0
	if last := query.Get("last"); last != "" {
		found := false
		for i, tag := range tags {
			if tag == last {
				start, found = i+1, true
				break
			}
		}
		if !found {
			f.writeError(w, http.StatusBadRequest, "PAGINATION_INVALID", "unknown last tag")
			return
		}
	}
	if start > len(tags) {
		start = len(tags)
	}
	page := tags[start:min(start+size, len(tags))]
	if len(page) > 0 && start+len(page) < len(tags) {
		next := fmt.Sprintf("%s/v2/%s/tags/list?n=%d&last=%s",
			f.server.URL, repo, size, url.QueryEscape(page[len(page)-1]))
		w.Header().Set("Link", fmt.Sprintf(`<%s>; rel="next"`, next))
	}
	w.Header().Set("Content-Type", "application/json")
	payload, _ := json.Marshal(map[string]any{"name": repo, "tags": page})
	_, _ = w.Write(payload)
}

func (f *fakeRegistry) writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	payload, _ := json.Marshal(map[string]any{
		"errors": []map[string]string{{"code": code, "message": message}},
	})
	_, _ = w.Write(payload)
}

// ---- test-side mutation helpers (safe: called before/while client runs) ----

func (f *fakeRegistry) setForce(status int, code, message string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forceStatus, f.forceCode, f.forceMessage = status, code, message
}

func (f *fakeRegistry) clearForce() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forceStatus, f.forceCode, f.forceMessage = 0, "", ""
}

func (f *fakeRegistry) setBlockManifests(ch chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blockManifests = ch
}

func (f *fakeRegistry) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func (f *fakeRegistry) recorded() []recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedRequest(nil), f.requests...)
}

func (f *fakeRegistry) requestsFor(path string) []recordedRequest {
	var out []recordedRequest
	for _, req := range f.recorded() {
		if req.Path == path {
			out = append(out, req)
		}
	}
	return out
}

func (f *fakeRegistry) putTags(repo string, tags []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry := f.repos[repo]
	if entry == nil {
		entry = &fakeRepo{byTag: map[string]string{}, manifests: map[string]fakeManifest{}}
		f.repos[repo] = entry
	}
	entry.tags = append([]string(nil), tags...)
}

// putManifest stores a manifest body under its computed digest and maps the
// tag to it.
func (f *fakeRegistry) putManifest(repo, tag, mediaType string, body []byte) {
	digest := "sha256:" + sha256Hex(body)
	f.mu.Lock()
	defer f.mu.Unlock()
	entry := f.repos[repo]
	if entry == nil {
		entry = &fakeRepo{byTag: map[string]string{}, manifests: map[string]fakeManifest{}}
		f.repos[repo] = entry
	}
	entry.manifests[digest] = fakeManifest{mediaType: mediaType, body: body, digest: digest}
	if tag != "" {
		entry.byTag[tag] = digest
		found := false
		for _, existing := range entry.tags {
			if existing == tag {
				found = true
				break
			}
		}
		if !found {
			entry.tags = append(entry.tags, tag)
		}
	}
}

// putRawManifest stores a manifest under an explicit key with full control of
// served headers and body — used for digest-mismatch fixtures.
func (f *fakeRegistry) putRawManifest(repo, key string, m fakeManifest) {
	if m.digest == "" {
		m.digest = "sha256:" + sha256Hex(m.body)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	entry := f.repos[repo]
	if entry == nil {
		entry = &fakeRepo{byTag: map[string]string{}, manifests: map[string]fakeManifest{}}
		f.repos[repo] = entry
	}
	entry.manifests[key] = m
	if !strings.HasPrefix(key, "sha256:") {
		entry.byTag[key] = key // self-keyed: served as-is
	}
}

func sha256Hex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// rewriteTransport redirects every request to the httptest server, whatever
// registry host the client believes it is talking to. This is what lets the
// same test suite run against the "quay.io" and "self-hosted" forms without
// any network.
type rewriteTransport struct {
	target string
}

func (rt rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rewritten := req.Clone(req.Context())
	target, err := url.Parse(rt.target)
	if err != nil {
		return nil, err
	}
	rewritten.URL.Scheme = target.Scheme
	rewritten.URL.Host = target.Host
	return http.DefaultTransport.RoundTrip(rewritten)
}

// ---- manifest fixtures ----

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	body, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	return body
}

func fixtureConfigDigest() string {
	return "sha256:" + sha256Hex([]byte("image-config-blob"))
}

func dockerSchema2Manifest(t *testing.T) []byte {
	t.Helper()
	return mustJSON(t, map[string]any{
		"schemaVersion": 2,
		"mediaType":     MediaTypeDockerSchema2,
		"config": map[string]any{
			"mediaType": "application/vnd.docker.container.image.v1+json",
			"digest":    fixtureConfigDigest(),
			"size":      7023,
		},
		"layers": []any{
			map[string]any{
				"mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
				"digest":    "sha256:" + sha256Hex([]byte("layer-blob")),
				"size":      32654,
			},
		},
	})
}

func ociManifest(t *testing.T) []byte {
	t.Helper()
	return mustJSON(t, map[string]any{
		"schemaVersion": 2,
		"mediaType":     MediaTypeOCIManifest,
		"config": map[string]any{
			"mediaType": "application/vnd.oci.image.config.v1+json",
			"digest":    fixtureConfigDigest(),
			"size":      7023,
		},
		"layers": []any{
			map[string]any{
				"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
				"digest":    "sha256:" + sha256Hex([]byte("layer-blob")),
				"size":      32654,
			},
		},
	})
}

func dockerManifestList(t *testing.T, childDigest string) []byte {
	t.Helper()
	return mustJSON(t, map[string]any{
		"schemaVersion": 2,
		"mediaType":     MediaTypeDockerSchemaList,
		"manifests": []any{
			map[string]any{
				"mediaType": MediaTypeDockerSchema2,
				"digest":    childDigest,
				"size":      528,
				"platform":  map[string]string{"architecture": "amd64", "os": "linux"},
			},
			map[string]any{
				"mediaType": MediaTypeDockerSchema2,
				"digest":    childDigest,
				"size":      528,
				"platform":  map[string]string{"architecture": "arm64", "os": "linux"},
			},
		},
	})
}

func ociIndex(t *testing.T, childDigest string) []byte {
	t.Helper()
	return mustJSON(t, map[string]any{
		"schemaVersion": 2,
		"mediaType":     MediaTypeOCIIndex,
		"manifests": []any{
			map[string]any{
				"mediaType": MediaTypeOCIManifest,
				"digest":    childDigest,
				"size":      7143,
			},
		},
	})
}

func schema1Manifest(t *testing.T) []byte {
	t.Helper()
	return mustJSON(t, map[string]any{
		"schemaVersion": 1,
		"name":          "team/legacy",
		"tag":           "v0",
		"architecture":  "amd64",
		"history":       []any{map[string]string{"v1Compatibility": `{"id":"legacy"}`}},
		"signatures": []any{map[string]any{
			"header":    map[string]any{"jwk": map[string]string{"kty": "EC", "kid": "XYZ"}},
			"signature": "c2ln",
			"protected": "cHJvdA",
		}},
	})
}
