// Package registrydouble provides an in-process double of an OCI Distribution /
// Docker Registry HTTP API V2 endpoint for ImageFlow tests.
//
// It exists because the frozen S2 seam (internal/registry) must be exercised
// against real registry semantics without a real network in unit and
// integration tests (spec issue #1, Testing Decision 3), and because the kind
// e2e needs a registry the manager pod can reach (ADR-007: standard OCI v2 API
// only, both Quay forms).
//
// # Fidelity
//
// The double implements the subset of the v2 API ImageFlow reads:
//
//	GET  /v2/                          ping
//	HEAD /v2/<repo>/manifests/<tag>    digest resolution (Docker-Content-Digest)
//	GET  /v2/<repo>/manifests/<ref>    manifest fetch by tag or by digest
//	GET  /v2/<repo>/tags/list          tag listing
//
// Digests are REAL: every served payload is a deterministic Docker schema2
// manifest and the digest is the sha256 of exactly those bytes, so digest
// verification, drift detection and content-addressed artifact naming behave as
// they do against quay.io or a self-hosted Quay. Authentication is HTTP basic
// with a v2-style WWW-Authenticate challenge, matching what the S2 client's
// auth dance expects; bearer/token flows are covered by the S2 package's own
// httptest suite and are not repeated here.
//
// Fault injection (SetFault) covers the failure classes the Import controller
// must diagnose: 401 Unauthorized, 404 MANIFEST_UNKNOWN, 5xx, and a hanging
// endpoint (client request timeout). Pointing a reference at a closed port
// covers "network unreachable"; no fault flag is needed for that.
//
// # DEVIATION from ticket T7's e2e wording (documented, orchestrator-approved)
//
// Ticket #7 (T7) asks for a real `projectquay/quay` container beside the kind
// cluster. The agent sandbox has no usable container runtime (docker is
// podman-aliased and the podman machine cannot start), so a real Quay could
// neither be started nor validated here; standing one up blind in CI would put
// the E2E workflow's reliability at risk (Quay needs Postgres + Redis + a
// config bundle and minutes of startup). The e2e therefore runs against THIS
// double beside the kind cluster by default, and the identical named cases can
// be pointed at a real Quay through the environment switch documented in
// test/e2e/import_e2e_test.go (IMAGEFLOW_E2E_REGISTRY_HOST and friends). The
// ticket's intent — validating against real OCI v2 semantics, per ADR-007's
// "one client, two Quay forms" — is preserved: no test-only behaviour branches
// exist beyond host and credentials.
package registrydouble

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/appfun-dev/imageflow/internal/registry"
)

// Descriptor media types of the synthetic manifests. The manifest media type
// itself comes from the S2 package so both sides cannot drift apart.
const (
	mediaTypeImageConfig = "application/vnd.docker.container.image.v1+json"
	mediaTypeLayerGzip   = "application/vnd.docker.image.rootfs.diff.tar.gzip"
)

// Fault is an injected failure mode of the double.
type Fault int

const (
	// FaultNone serves content normally.
	FaultNone Fault = iota
	// FaultUnauthorized answers every repository request with HTTP 401 and a
	// basic-auth challenge, even when correct credentials are presented.
	FaultUnauthorized
	// FaultNotFound answers every repository request with HTTP 404 and the
	// MANIFEST_UNKNOWN registry error code.
	FaultNotFound
	// FaultServerError answers every repository request with HTTP 503.
	FaultServerError
	// FaultHang blocks until the client gives up (request context cancelled),
	// which surfaces as a client-side timeout.
	FaultHang
)

// Options configures a Server.
type Options struct {
	// BindAddress of the listener; "" binds 127.0.0.1:0. Use "0.0.0.0:0" when
	// clients live in another network namespace (e.g. a kind cluster reaching
	// the test process through the container-network gateway).
	BindAddress string
	// TLSHosts are extra subject alternative names (DNS names or IP literals)
	// of the generated serving certificate. The loopback addresses are always
	// included.
	TLSHosts []string
	// Username, when non-empty, makes every /v2/ request require HTTP basic
	// authentication with Username/Password.
	Username string
	// Password paired with Username.
	Password string
}

// repoState is one repository of the double.
type repoState struct {
	tags      map[string]string // tag -> digest
	manifests map[string][]byte // digest -> payload
}

// Server is a running registry double. It is safe for concurrent use.
type Server struct {
	httpServer *httptest.Server
	host       string
	port       int
	caCertPEM  []byte
	client     *http.Client

	mu       sync.Mutex
	repos    map[string]*repoState
	fault    Fault
	username string
	password string
	requests int
}

// New starts a registry double serving TLS. The S2 client always speaks
// https://, so TLS is not optional; use HTTPClient() (or CACertPEM() for
// out-of-process clients) to trust the generated certificate.
func New(opts Options) (*Server, error) {
	bind := opts.BindAddress
	if bind == "" {
		bind = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", bind, err)
	}

	sans := append([]string{"localhost"}, opts.TLSHosts...)
	cert, caPEM, err := generateCertificate(sans)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}

	s := &Server{
		repos:     map[string]*repoState{},
		username:  opts.Username,
		password:  opts.Password,
		caCertPEM: caPEM,
	}
	s.httpServer = httptest.NewUnstartedServer(s)
	s.httpServer.Listener = listener
	s.httpServer.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	s.httpServer.StartTLS()

	host, portStr, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		s.httpServer.Close()
		return nil, fmt.Errorf("split listener address: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		s.httpServer.Close()
		return nil, fmt.Errorf("parse listener port: %w", err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		host = "127.0.0.1"
	}
	s.host = net.JoinHostPort(host, portStr)
	s.port = port

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		s.httpServer.Close()
		return nil, fmt.Errorf("the generated CA certificate is not usable")
	}
	s.client = &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
	}}
	return s, nil
}

// Host is the registry host[:port] as reachable from the test process.
func (s *Server) Host() string { return s.host }

// HostFor renders the registry host[:port] as reachable from another network
// (e.g. a pod inside a kind cluster reaching the test process through the
// container-network gateway address).
func (s *Server) HostFor(advertised string) string {
	return net.JoinHostPort(advertised, strconv.Itoa(s.port))
}

// Port is the listening port.
func (s *Server) Port() int { return s.port }

// URL is the base URL of the double.
func (s *Server) URL() string { return "https://" + s.host }

// CACertPEM is the PEM certificate that signs (and, being self-signed, also
// verifies) the serving certificate. Out-of-process clients trust the double by
// installing it (in the kind e2e: mounted into the manager pod and pointed at
// with SSL_CERT_FILE, which Go's x509 loader honours).
func (s *Server) CACertPEM() []byte { return append([]byte(nil), s.caCertPEM...) }

// HTTPClient returns a client that trusts the double's certificate. Its
// Transport is what tests hand to registry.NewClient(registry.Options{Transport}).
func (s *Server) HTTPClient() *http.Client { return s.client }

// Close shuts the double down.
func (s *Server) Close() { s.httpServer.Close() }

// Reference renders "<host>/<repo>:<tag>" for the double's in-process host.
func (s *Server) Reference(repo, tag string) string { return Reference(s.host, repo, tag) }

// ReferenceAt renders "<host>/<repo>:<tag>" for an advertised host.
func (s *Server) ReferenceAt(host, repo, tag string) string { return Reference(host, repo, tag) }

// DigestReferenceAt renders "<host>/<repo>@<digest>" for an advertised host.
func (s *Server) DigestReferenceAt(host, repo, digest string) string {
	return host + "/" + repo + "@" + digest
}

// Reference renders an image reference with an explicit registry host, the only
// form the S2 grammar accepts.
func Reference(host, repo, tag string) string { return host + "/" + repo + ":" + tag }

// PutTag stores a deterministic schema2 manifest derived from seed under
// (repo, tag) and returns its digest. Re-pointing an existing tag at a new seed
// is exactly the external drift the Import path must detect.
func (s *Server) PutTag(repo, tag, seed string) (string, error) {
	payload := Schema2Manifest(seed)
	digest := DigestOf(payload)

	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.repos[repo]
	if state == nil {
		state = &repoState{tags: map[string]string{}, manifests: map[string][]byte{}}
		s.repos[repo] = state
	}
	state.manifests[digest] = payload
	state.tags[tag] = digest
	return digest, nil
}

// MustPutTag is PutTag for table setup; it panics on the (impossible) error.
func (s *Server) MustPutTag(repo, tag, seed string) string {
	digest, err := s.PutTag(repo, tag, seed)
	if err != nil {
		panic(err)
	}
	return digest
}

// RemoveTag drops a tag, so the next request for it answers 404.
func (s *Server) RemoveTag(repo, tag string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state := s.repos[repo]; state != nil {
		delete(state.tags, tag)
	}
}

// DigestOf returns the digest currently served for (repo, tag), or "".
func (s *Server) DigestOf(repo, tag string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state := s.repos[repo]; state != nil {
		return state.tags[tag]
	}
	return ""
}

// SetFault injects (or clears, with FaultNone) a failure mode.
func (s *Server) SetFault(fault Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fault = fault
}

// Requests counts the HTTP requests the double has served.
func (s *Server) Requests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests
}

// ServeHTTP implements the v2 endpoints described in the package doc.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.requests++
	fault := s.fault
	s.mu.Unlock()

	if fault == FaultHang {
		<-r.Context().Done() // the client timed out; return without a response
		return
	}

	path := r.URL.Path
	if path != "/v2/" && path != "/v2" && !strings.HasPrefix(path, "/v2/") {
		http.NotFound(w, r)
		return
	}
	if !s.authorized(w, r, fault) {
		return
	}
	if path == "/v2/" || path == "/v2" {
		writeJSON(w, http.StatusOK, map[string]string{})
		return
	}

	switch fault {
	case FaultNotFound:
		writeRegistryError(w, http.StatusNotFound, "MANIFEST_UNKNOWN", "manifest unknown (injected)")
		return
	case FaultServerError:
		writeRegistryError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "injected server failure")
		return
	case FaultNone, FaultUnauthorized, FaultHang: // served normally
	}

	rest := strings.TrimPrefix(path, "/v2/")
	if repo, remainder, found := splitV2(rest, "/manifests/"); found {
		s.serveManifest(w, r, repo, remainder)
		return
	}
	if repo, _, found := splitV2(rest, "/tags/list"); found {
		s.serveTags(w, repo)
		return
	}
	writeRegistryError(w, http.StatusNotFound, "NAME_UNKNOWN", "unsupported v2 path "+path)
}

// authorized enforces the basic-auth front door and the injected 401 fault. It
// writes the challenge response and reports false when the request must stop.
func (s *Server) authorized(w http.ResponseWriter, r *http.Request, fault Fault) bool {
	challenge := func() {
		w.Header().Set("WWW-Authenticate", `Basic realm="imageflow-registry-double"`)
		writeRegistryError(w, http.StatusUnauthorized, "UNAUTHORIZED", "authentication required")
	}
	if fault == FaultUnauthorized {
		challenge()
		return false
	}
	if s.username == "" {
		return true
	}
	user, password, ok := r.BasicAuth()
	if !ok || user != s.username || password != s.password {
		challenge()
		return false
	}
	return true
}

func (s *Server) serveManifest(w http.ResponseWriter, r *http.Request, repo, selector string) {
	s.mu.Lock()
	var payload []byte
	var digest string
	if state := s.repos[repo]; state != nil {
		if strings.HasPrefix(selector, "sha256:") {
			digest = selector
			payload = state.manifests[selector]
		} else if found, ok := state.tags[selector]; ok {
			digest = found
			payload = state.manifests[found]
		}
	}
	s.mu.Unlock()

	if payload == nil {
		writeRegistryError(w, http.StatusNotFound, "MANIFEST_UNKNOWN",
			"no manifest for "+repo+" reference "+selector)
		return
	}
	w.Header().Set("Content-Type", registry.MediaTypeDockerSchema2)
	w.Header().Set("Docker-Content-Digest", digest)
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(payload)
}

func (s *Server) serveTags(w http.ResponseWriter, repo string) {
	s.mu.Lock()
	var tags []string
	if state := s.repos[repo]; state != nil {
		for tag := range state.tags {
			tags = append(tags, tag)
		}
	}
	s.mu.Unlock()
	sort.Strings(tags)
	if tags == nil {
		tags = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"name": repo, "tags": tags})
}

// splitV2 splits "<repo><sep><remainder>" where the repository may itself
// contain slashes, so the separator is matched from the right.
func splitV2(path, sep string) (repo, remainder string, found bool) {
	idx := strings.LastIndex(path, sep)
	if idx <= 0 {
		return "", "", false
	}
	return path[:idx], path[idx+len(sep):], true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	payload, err := json.Marshal(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(payload)
}

func writeRegistryError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"errors": []map[string]string{{"code": code, "message": message, "detail": ""}},
	})
}

// Schema2Manifest renders a deterministic Docker schema2 manifest for seed:
// the same seed always produces the same bytes, hence the same digest. Only
// metadata exists here — the double never serves blobs, mirroring ImageFlow's
// own boundary (ADR-007: never blob/layer data).
func Schema2Manifest(seed string) []byte {
	configBlob := []byte(`{"architecture":"amd64","os":"linux","seed":` + strconv.Quote(seed) + `}`)
	layerBlob := []byte("imageflow-registry-double-rootfs:" + seed)
	manifest := map[string]any{
		"schemaVersion": 2,
		"mediaType":     registry.MediaTypeDockerSchema2,
		"config":        descriptor(mediaTypeImageConfig, configBlob),
		"layers":        []any{descriptor(mediaTypeLayerGzip, layerBlob)},
	}
	payload, err := json.Marshal(manifest) // map keys are marshalled sorted: deterministic
	if err != nil {
		panic(err)
	}
	return payload
}

// DigestOf computes the "sha256:<64 hex>" digest of a payload.
func DigestOf(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func descriptor(mediaType string, blob []byte) map[string]any {
	return map[string]any{
		"mediaType": mediaType,
		"size":      len(blob),
		"digest":    DigestOf(blob),
	}
}

// generateCertificate builds a self-signed CA plus a leaf certificate whose
// subject alternative names cover the loopback addresses and the requested
// hosts, so both in-process and cross-network clients can verify the double.
func generateCertificate(hosts []string) (tls.Certificate, []byte, error) {
	notBefore := time.Now().Add(-time.Hour)
	notAfter := time.Now().Add(24 * time.Hour)

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("generate CA key: %w", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "imageflow-registry-double CA"},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("create CA certificate: %w", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("parse CA certificate: %w", err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("generate leaf key: %w", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "imageflow-registry-double"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	for _, host := range append([]string{"127.0.0.1", "::1"}, hosts...) {
		if ip := net.ParseIP(host); ip != nil {
			leafTemplate.IPAddresses = append(leafTemplate.IPAddresses, ip)
			continue
		}
		leafTemplate.DNSNames = append(leafTemplate.DNSNames, host)
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("create leaf certificate: %w", err)
	}

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("marshal leaf key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(leafPEM(leafDER), keyPEM)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("build serving certificate: %w", err)
	}
	return cert, caPEM, nil
}

func leafPEM(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// basicAuthValue renders the Authorization header value of a basic-auth pull
// secret, for tests that build dockerconfigjson payloads.
func basicAuthValue(user, password string) string {
	return base64.StdEncoding.EncodeToString([]byte(user + ":" + password))
}

// DockerConfigJSON renders a dockerconfigjson pull-secret payload for the given
// registry host, so tests can exercise the real credential path end to end.
func DockerConfigJSON(host, user, password string) []byte {
	payload, err := json.Marshal(map[string]any{"auths": map[string]any{
		host: map[string]any{
			"username": user,
			"password": password,
			"auth":     basicAuthValue(user, password),
		},
	}})
	if err != nil {
		panic(err)
	}
	return payload
}
