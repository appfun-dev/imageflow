package registry

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/appfun-dev/imageflow/internal/clock"
)

// Client defaults.
const (
	// DefaultRequestTimeout bounds one logical registry operation, including
	// the token exchange and the single auth retry.
	DefaultRequestTimeout = 30 * time.Second
	// DefaultUserAgent identifies ImageFlow registry traffic.
	DefaultUserAgent = "imageflow-registry-client"
	// DefaultTagListPageSize is the "n" query parameter of tag list requests.
	DefaultTagListPageSize = 100
	// DefaultMaxTagListPages caps pagination so a misbehaving registry cannot
	// trap the client in an endless loop.
	DefaultMaxTagListPages = 1000
)

const (
	headerDockerContentDigest = "Docker-Content-Digest"
	maxManifestBodyBytes      = 8 << 20 // 8 MiB, generous for any manifest
	maxErrorBodyBytes         = 8 << 10 // 8 KiB of registry error body
	maxTokenBodyBytes         = 64 << 10
	maxDetailLength           = 256
)

// Options configures a Client. The zero value is a working anonymous client
// with defaults: no allowlist (allow all), default transport, 30s request
// timeout, real clock, metrics disabled.
type Options struct {
	// Allowlist restricts which registry hosts the client may contact
	// (SSRF guard). Feed it from config.Config.RegistryAllowlist. Empty means
	// allow all. Entries are host[:port] compared exactly (case-insensitive);
	// malformed entries fail NewClient.
	Allowlist []string
	// Transport performs the HTTP round trips; nil means
	// http.DefaultTransport. Tests use it to redirect a registry host to an
	// httptest server.
	Transport http.RoundTripper
	// RequestTimeout bounds one logical operation (token exchange and auth
	// retry included). Values <= 0 select DefaultRequestTimeout.
	RequestTimeout time.Duration
	// Clock measures operation durations for metrics; nil means clock.Real.
	// Inject clock.Fake in tests for deterministic durations.
	Clock clock.Clock
	// Registerer receives the package metrics; nil disables metrics.
	// Pass sigs.k8s.io/controller-runtime/pkg/metrics.Registry in the manager.
	Registerer prometheus.Registerer
	// UserAgent for registry requests; empty means DefaultUserAgent.
	UserAgent string
	// TagListPageSize for tag list "n" parameters; <= 0 means default.
	TagListPageSize int
	// MaxTagListPages pagination cap; <= 0 means default.
	MaxTagListPages int
}

// HTTPClient is the production Client implementation: standard OCI /
// Docker Registry v2 API only, no third-party registry SDK. Safe for
// concurrent use.
type HTTPClient struct {
	httpClient      *http.Client
	allowlist       map[string]struct{}
	requestTimeout  time.Duration
	clock           clock.Clock
	metrics         *metrics
	userAgent       string
	tagListPageSize int
	maxTagListPages int
}

// NewClient builds a Client from opts. It fails fast on malformed allowlist
// entries or metric registration conflicts.
func NewClient(opts Options) (*HTTPClient, error) {
	allowlist := make(map[string]struct{}, len(opts.Allowlist))
	for _, entry := range opts.Allowlist {
		host := strings.ToLower(strings.TrimSpace(entry))
		if host == "" {
			continue
		}
		if !registryHostRe.MatchString(host) {
			return nil, fmt.Errorf("invalid registry allowlist entry %q: must be host[:port]", entry)
		}
		allowlist[host] = struct{}{}
	}
	m, err := newMetrics(opts.Registerer)
	if err != nil {
		return nil, err
	}
	clk := opts.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	transport := opts.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	timeout := opts.RequestTimeout
	if timeout <= 0 {
		timeout = DefaultRequestTimeout
	}
	pageSize := opts.TagListPageSize
	if pageSize <= 0 {
		pageSize = DefaultTagListPageSize
	}
	maxPages := opts.MaxTagListPages
	if maxPages <= 0 {
		maxPages = DefaultMaxTagListPages
	}
	userAgent := opts.UserAgent
	if userAgent == "" {
		userAgent = DefaultUserAgent
	}
	return &HTTPClient{
		httpClient:      &http.Client{Transport: transport},
		allowlist:       allowlist,
		requestTimeout:  timeout,
		clock:           clk,
		metrics:         m,
		userAgent:       userAgent,
		tagListPageSize: pageSize,
		maxTagListPages: maxPages,
	}, nil
}

var _ Client = (*HTTPClient)(nil)

// Operation label values for the registry metrics.
const (
	OpResolveDigest = "resolve_digest"
	OpFetchManifest = "fetch_manifest"
	OpListTags      = "list_tags"
)

// ResolveDigest implements Client.
func (c *HTTPClient) ResolveDigest(ctx context.Context, ref Reference, creds Credentials) (string, error) {
	start := c.clock.Now()
	digest, err := c.resolveDigest(ctx, ref, creds)
	c.metrics.observe(OpResolveDigest, classifyResult(err), c.clock.Since(start))
	return digest, err
}

// FetchManifest implements Client.
func (c *HTTPClient) FetchManifest(ctx context.Context, ref Reference, creds Credentials) (*Manifest, error) {
	start := c.clock.Now()
	manifest, err := c.fetchManifest(ctx, ref, creds)
	c.metrics.observe(OpFetchManifest, classifyResult(err), c.clock.Since(start))
	return manifest, err
}

// ListTags implements Client.
func (c *HTTPClient) ListTags(ctx context.Context, ref Reference, creds Credentials) ([]string, error) {
	start := c.clock.Now()
	tags, err := c.listTags(ctx, ref, creds)
	c.metrics.observe(OpListTags, classifyResult(err), c.clock.Since(start))
	return tags, err
}

func (c *HTTPClient) resolveDigest(ctx context.Context, ref Reference, creds Credentials) (string, error) {
	if err := ref.validateForResolve(); err != nil {
		return "", err
	}
	host := normalizeHost(ref.Registry)
	if err := c.checkAllowlist(host); err != nil {
		return "", err
	}
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	u := c.endpoint(host, fmt.Sprintf("/v2/%s/manifests/%s", ref.Repository, ref.Tag), nil)
	resp, err := c.do(ctx, http.MethodHead, u, host, creds, PullScope(ref.Repository), manifestAcceptTypes)
	if err != nil {
		return "", err
	}
	defer drainAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		return "", c.errorFromResponse(resp, host, ref.String(), creds)
	}
	digest := resp.Header.Get(headerDockerContentDigest)
	if !digestRe.MatchString(digest) {
		return "", fmt.Errorf("registry %q returned a missing or malformed %s header for %s",
			host, headerDockerContentDigest, ref.String())
	}
	return digest, nil
}

func (c *HTTPClient) fetchManifest(ctx context.Context, ref Reference, creds Credentials) (*Manifest, error) {
	if err := ref.validateForFetch(); err != nil {
		return nil, err
	}
	host := normalizeHost(ref.Registry)
	if err := c.checkAllowlist(host); err != nil {
		return nil, err
	}
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	selector := ref.Tag
	if ref.Digest != "" {
		selector = ref.Digest
	}
	u := c.endpoint(host, fmt.Sprintf("/v2/%s/manifests/%s", ref.Repository, selector), nil)
	resp, err := c.do(ctx, http.MethodGet, u, host, creds, PullScope(ref.Repository), manifestAcceptTypes)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer drainAndClose(resp)
		return nil, c.errorFromResponse(resp, host, ref.String(), creds)
	}
	body, err := readBodyLimit(resp, maxManifestBodyBytes)
	if err != nil {
		return nil, fmt.Errorf("registry %q served an unreadable manifest for %s: %w", host, ref.String(), err)
	}
	computed := computeDigest(body)
	if ref.Digest != "" && ref.Digest != computed {
		return nil, &DigestMismatchError{
			Expected: ref.Digest, Actual: computed, Registry: host, Target: ref.String(),
		}
	}
	if header := resp.Header.Get(headerDockerContentDigest); header != "" {
		if !digestRe.MatchString(header) {
			return nil, fmt.Errorf("registry %q returned a malformed %s header for %s",
				host, headerDockerContentDigest, ref.String())
		}
		if header != computed {
			return nil, &DigestMismatchError{
				Expected: header, Actual: computed, Registry: host, Target: ref.String(),
			}
		}
	}
	mediaType := resolveMediaType(resp.Header.Get("Content-Type"), body)
	return &Manifest{
		Digest:    computed,
		MediaType: mediaType,
		Size:      int64(len(body)),
		Raw:       body,
		Config:    parseConfigDescriptor(mediaType, body),
	}, nil
}

func (c *HTTPClient) listTags(ctx context.Context, ref Reference, creds Credentials) ([]string, error) {
	if err := ref.validateForListTags(); err != nil {
		return nil, err
	}
	host := normalizeHost(ref.Registry)
	if err := c.checkAllowlist(host); err != nil {
		return nil, err
	}
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	scope := PullScope(ref.Repository)
	query := url.Values{"n": {fmt.Sprintf("%d", c.tagListPageSize)}}
	next := c.endpoint(host, fmt.Sprintf("/v2/%s/tags/list", ref.Repository), query)
	tags := []string{}
	for page := 0; ; page++ {
		if page >= c.maxTagListPages {
			return nil, fmt.Errorf("registry %q tag list for %s exceeded %d pages",
				host, ref.Repository, c.maxTagListPages)
		}
		resp, err := c.do(ctx, http.MethodGet, next, host, creds, scope, nil)
		if err != nil {
			return nil, err
		}
		pageTags, link, err := c.decodeTagsPage(resp, host, ref, creds)
		if err != nil {
			return nil, err
		}
		tags = append(tags, pageTags...)
		if link == "" || len(pageTags) == 0 {
			return tags, nil
		}
		// SSRF guard: only path and query are taken from the Link header; the
		// request always goes back to the allowlisted registry host.
		next, err = c.linkURL(host, link)
		if err != nil {
			return nil, err
		}
	}
}

// decodeTagsPage reads one tags/list response and returns its tags plus the
// raw rel="next" Link target (empty when there is none).
func (c *HTTPClient) decodeTagsPage(resp *http.Response, host string, ref Reference, creds Credentials) ([]string, string, error) {
	if resp.StatusCode != http.StatusOK {
		defer drainAndClose(resp)
		return nil, "", c.errorFromResponse(resp, host, ref.Repository, creds)
	}
	body, err := readBodyLimit(resp, maxManifestBodyBytes)
	if err != nil {
		return nil, "", fmt.Errorf("registry %q served an unreadable tag list for %s: %w",
			host, ref.Repository, err)
	}
	var payload struct {
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, "", fmt.Errorf("registry %q served a malformed tag list for %s: %w",
			host, ref.Repository, redactJSONError(err, creds))
	}
	return payload.Tags, nextLinkTarget(resp.Header.Get("Link")), nil
}

var nextLinkRe = regexp.MustCompile(`<([^>]+)>\s*;\s*rel\s*=\s*"?next"?`)

// nextLinkTarget extracts the URL of the rel="next" Link header, if present.
func nextLinkTarget(header string) string {
	m := nextLinkRe.FindStringSubmatch(header)
	if m == nil {
		return ""
	}
	return m[1]
}

// linkURL turns a Link target into the next request URL, pinned to the
// registry host: scheme and authority always come from the allowlisted
// endpoint, never from the header value.
func (c *HTTPClient) linkURL(host, link string) (*url.URL, error) {
	parsed, err := url.Parse(link)
	if err != nil || parsed.Path == "" {
		return nil, fmt.Errorf("registry %q sent a malformed pagination link", host)
	}
	return &url.URL{Scheme: "https", Host: host, Path: parsed.Path, RawQuery: parsed.RawQuery}, nil
}

// checkAllowlist fails closed for hosts outside the configured allowlist.
// An empty allowlist allows every host.
func (c *HTTPClient) checkAllowlist(host string) error {
	if len(c.allowlist) == 0 {
		return nil
	}
	if _, ok := c.allowlist[host]; ok {
		return nil
	}
	return &DeniedError{Registry: host}
}

func normalizeHost(registry string) string {
	return strings.ToLower(strings.TrimSpace(registry))
}

func (c *HTTPClient) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.requestTimeout)
}

func (c *HTTPClient) endpoint(host, path string, query url.Values) *url.URL {
	u := &url.URL{Scheme: "https", Host: host, Path: path}
	if query != nil {
		u.RawQuery = query.Encode()
	}
	return u
}

// do performs one registry request with the Registry v2 auth dance: at most
// one challenge-driven retry (bearer token exchange or basic auth). The
// returned response is not drained; the caller owns it.
func (c *HTTPClient) do(
	ctx context.Context,
	method string,
	u *url.URL,
	host string,
	creds Credentials,
	scope string,
	accept []string,
) (*http.Response, error) {
	send := func(authHeader string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", c.userAgent)
		for _, mediaType := range accept {
			req.Header.Add("Accept", mediaType)
		}
		if authHeader != "" {
			req.Header.Set("Authorization", authHeader)
		}
		return c.httpClient.Do(req)
	}

	preemptive := ""
	if creds.RegistryToken != "" {
		preemptive = "Bearer " + creds.RegistryToken
	}
	resp, err := send(preemptive)
	if err != nil {
		return nil, transportError(err, host)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}
	challengeHeaders := resp.Header.Values("WWW-Authenticate")
	drainAndClose(resp)

	bearer, basic := parseAuthChallenge(challengeHeaders)
	switch {
	case bearer != nil:
		token, err := c.fetchToken(ctx, *bearer, creds, host, scope)
		if err != nil {
			return nil, err
		}
		resp, err = send("Bearer " + token)
		if err != nil {
			return nil, transportError(err, host)
		}
		return resp, nil
	case basic:
		if creds.Username == "" || creds.Password == "" {
			return nil, &UnauthorizedError{Registry: host, StatusCode: http.StatusUnauthorized, Anonymous: creds.Anonymous()}
		}
		resp, err = send("Basic " + basicAuthValue(creds.Username, creds.Password))
		if err != nil {
			return nil, transportError(err, host)
		}
		return resp, nil
	default:
		return nil, &UnauthorizedError{Registry: host, StatusCode: http.StatusUnauthorized, Anonymous: creds.Anonymous()}
	}
}

// bearerChallenge is a parsed "WWW-Authenticate: Bearer" challenge.
type bearerChallenge struct {
	Realm   string
	Service string
	Scope   string
}

// authSchemeBasic is the HTTP basic auth scheme token (shared with tests).
const authSchemeBasic = "basic"

var challengeParamRe = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_-]*)\s*=\s*"((?:[^"\\]|\\.)*)"`)

// parseAuthChallenge parses WWW-Authenticate header values into a bearer
// challenge and/or a basic-auth flag.
func parseAuthChallenge(headers []string) (*bearerChallenge, bool) {
	var bearer *bearerChallenge
	basic := false
	for _, header := range headers {
		header = strings.TrimSpace(header)
		space := strings.IndexAny(header, " \t")
		if space < 0 {
			continue
		}
		scheme := strings.ToLower(header[:space])
		params := header[space+1:]
		switch scheme {
		case "bearer":
			ch := &bearerChallenge{}
			for _, m := range challengeParamRe.FindAllStringSubmatch(params, -1) {
				value := unquoteChallengeParam(m[2])
				switch strings.ToLower(m[1]) {
				case "realm":
					ch.Realm = value
				case "service":
					ch.Service = value
				case "scope":
					ch.Scope = value
				}
			}
			if ch.Realm != "" {
				bearer = ch
			}
		case authSchemeBasic:
			basic = true
		}
	}
	return bearer, basic
}

func unquoteChallengeParam(v string) string {
	v = strings.ReplaceAll(v, `\"`, `"`)
	return strings.ReplaceAll(v, `\\`, `\`)
}

func basicAuthValue(username, password string) string {
	return base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
}

// fetchToken performs the Registry v2 token exchange against the challenge
// realm. Credentials travel as HTTP basic auth on the token request, or as a
// refresh_token grant when an identity token is configured. Errors never
// carry credential material: server text is scrubbed and the realm URL is
// reduced to scheme://host/path (the query may hold the refresh token).
func (c *HTTPClient) fetchToken(
	ctx context.Context,
	ch bearerChallenge,
	creds Credentials,
	registry string,
	fallbackScope string,
) (string, error) {
	realm, err := url.Parse(ch.Realm)
	if err != nil || (realm.Scheme != "https" && realm.Scheme != "http") || realm.Host == "" {
		return "", fmt.Errorf("registry %q sent an unusable token realm", registry)
	}
	query := realm.Query()
	if ch.Service != "" {
		query.Set("service", ch.Service)
	}
	scope := ch.Scope
	if scope == "" {
		scope = fallbackScope
	}
	if scope != "" {
		query.Set("scope", scope)
	}
	if creds.IdentityToken != "" {
		query.Set("grant_type", "refresh_token")
		query.Set("refresh_token", creds.IdentityToken)
	}
	realm.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, realm.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", c.userAgent)
	if creds.IdentityToken == "" && creds.Username != "" && creds.Password != "" {
		req.SetBasicAuth(creds.Username, creds.Password)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", tokenEndpointError(err, creds, realm, registry)
	}
	body, readErr := readBodyLimit(resp, maxTokenBodyBytes)
	drainAndClose(resp)
	if readErr != nil {
		return "", fmt.Errorf("registry %q token endpoint response unreadable: %w", registry, readErr)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		var payload struct {
			Token       string `json:"token"`
			AccessToken string `json:"access_token"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return "", fmt.Errorf("registry %q token endpoint returned a malformed token payload", registry)
		}
		token := payload.Token
		if token == "" {
			token = payload.AccessToken
		}
		if token == "" {
			return "", fmt.Errorf("registry %q token endpoint returned an empty token", registry)
		}
		return token, nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return "", &UnauthorizedError{Registry: registry, StatusCode: resp.StatusCode, Anonymous: creds.Anonymous()}
	default:
		code, detail := parseRegistryErrors(body, creds)
		return "", &HTTPError{
			Registry:   registry,
			Target:     "token endpoint",
			StatusCode: resp.StatusCode,
			Code:       code,
			Detail:     detail,
		}
	}
}

// tokenEndpointError sanitizes transport errors from the token endpoint. The
// realm query can carry the identity token, so the raw error text is scrubbed
// and the reported URL excludes the query.
func tokenEndpointError(err error, creds Credentials, realm *url.URL, registry string) error {
	safeRealm := realm.Scheme + "://" + realm.Host + realm.Path
	base := fmt.Sprintf("registry %q token endpoint %s failed: %s",
		registry, safeRealm, redactSecrets(err.Error(), creds))
	switch {
	case errors.Is(err, context.Canceled):
		return fmt.Errorf("%s: %w", base, context.Canceled)
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%s: %w", base, context.DeadlineExceeded)
	case isNetTimeout(err):
		return fmt.Errorf("%s: %w", base, ErrTimeout)
	default:
		return errors.New(base)
	}
}

// transportError wraps round-trip failures for registry endpoints. Their URLs
// never carry credential material (auth travels in headers), so the wrapped
// error is safe to surface.
func transportError(err error, host string) error {
	switch {
	case errors.Is(err, context.Canceled):
		return fmt.Errorf("registry %q request canceled: %w", host, err)
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("registry %q request timed out: %w", host, err)
	case isNetTimeout(err):
		return fmt.Errorf("registry %q request timed out: %w (%v)", host, ErrTimeout, err)
	default:
		return fmt.Errorf("registry %q request failed: %w", host, err)
	}
}

func isNetTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// errorFromResponse maps a non-2xx registry response onto the error
// taxonomy. It consumes and closes the response body.
func (c *HTTPClient) errorFromResponse(resp *http.Response, host, target string, creds Credentials) error {
	body, _ := readBodyLimit(resp, maxErrorBodyBytes)
	drainAndClose(resp)
	code, detail := parseRegistryErrors(body, creds)
	switch resp.StatusCode {
	case http.StatusNotFound:
		return &NotFoundError{Registry: host, Target: target, Code: code, Detail: detail}
	case http.StatusUnauthorized, http.StatusForbidden:
		// Deliberately no server detail: see UnauthorizedError docs.
		return &UnauthorizedError{Registry: host, StatusCode: resp.StatusCode, Anonymous: creds.Anonymous()}
	default:
		return &HTTPError{Registry: host, Target: target, StatusCode: resp.StatusCode, Code: code, Detail: detail}
	}
}

// registryErrorBody is the standard Registry v2 error payload.
type registryErrorBody struct {
	Errors []struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
}

// parseRegistryErrors extracts the first error code and a scrubbed,
// truncated message from a registry error body.
func parseRegistryErrors(body []byte, creds Credentials) (string, string) {
	var payload registryErrorBody
	if err := json.Unmarshal(body, &payload); err != nil || len(payload.Errors) == 0 {
		return "", ""
	}
	code := payload.Errors[0].Code
	if !errorCodeRe.MatchString(code) {
		code = ""
	}
	return code, truncateDetail(redactSecrets(payload.Errors[0].Message, creds))
}

func truncateDetail(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > maxDetailLength {
		return s[:maxDetailLength] + "..."
	}
	return s
}

// redactJSONError scrubs credential material from JSON decode error text
// (which can quote snippets of the payload).
func redactJSONError(err error, creds Credentials) error {
	return errors.New(redactSecrets(err.Error(), creds))
}

// redactSecrets removes every occurrence of the credential secrets — and
// their URL-escaped and docker "auth" base64 forms — from s.
func redactSecrets(s string, creds Credentials) string {
	secrets := []string{creds.Password, creds.IdentityToken, creds.RegistryToken}
	if creds.Username != "" && creds.Password != "" {
		secrets = append(secrets, basicAuthValue(creds.Username, creds.Password))
	}
	for _, secret := range secrets {
		if secret == "" {
			continue
		}
		s = strings.ReplaceAll(s, secret, "[redacted]")
		s = strings.ReplaceAll(s, url.QueryEscape(secret), "[redacted]")
	}
	return s
}

func readBodyLimit(resp *http.Response, limit int64) ([]byte, error) {
	defer drainAndClose(resp)
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("response body exceeds %d bytes", limit)
	}
	return body, nil
}

func drainAndClose(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBodyBytes))
	_ = resp.Body.Close()
}

func computeDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// resolveMediaType prefers a well-known Content-Type; otherwise it sniffs the
// payload ("mediaType" field, then schemaVersion 1) and falls back to the
// Content-Type as served.
func resolveMediaType(contentType string, body []byte) string {
	for _, known := range manifestAcceptTypes {
		if strings.EqualFold(contentType, known) {
			return known
		}
	}
	var sniffed struct {
		MediaType     string `json:"mediaType"`
		SchemaVersion int    `json:"schemaVersion"`
	}
	if err := json.Unmarshal(body, &sniffed); err == nil {
		for _, known := range manifestAcceptTypes {
			if strings.EqualFold(sniffed.MediaType, known) {
				return known
			}
		}
		if sniffed.SchemaVersion == 1 {
			return MediaTypeDockerSchema1
		}
	}
	return contentType
}

// parseConfigDescriptor extracts the image config descriptor from
// single-platform manifests; it returns nil for lists, indexes and schema1.
func parseConfigDescriptor(mediaType string, body []byte) *Descriptor {
	if mediaType != MediaTypeDockerSchema2 && mediaType != MediaTypeOCIManifest {
		return nil
	}
	var payload struct {
		Config Descriptor `json:"config"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.Config.Digest == "" {
		return nil
	}
	config := payload.Config
	return &config
}

// classifyResult maps an operation error onto the bounded "result" label
// vocabulary of the registry metrics.
func classifyResult(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, ErrDenied):
		return "denied"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	case errors.Is(err, ErrUnauthorized):
		return "unauthorized"
	case errors.Is(err, ErrTimeout), errors.Is(err, context.DeadlineExceeded), isNetTimeout(err):
		return "timeout"
	default:
		return "error"
	}
}
