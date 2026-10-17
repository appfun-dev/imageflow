package registrydouble_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/appfun-dev/imageflow/internal/registry"
	"github.com/appfun-dev/imageflow/test/registrydouble"
)

// The double is only worth anything if the REAL S2 client accepts it, so these
// tests drive it through internal/registry exactly the way the Import
// controller does: digest resolution, manifest verification, tag listing, the
// basic-auth pull-secret path and every injected fault class. They are the
// local stand-in for the kind e2e leg (which cannot run in a sandbox without a
// container runtime).

const (
	testRepo = "imageflow/double-check"
	testTag  = "v1"
	testUser = "imageflow+double"
	testPass = "double-check-password"
	// advertisedHost stands in for the container-network gateway address a kind
	// cluster uses to reach the test process.
	advertisedHost = "172.18.0.1"
)

func newClientFor(t *testing.T, srv *registrydouble.Server, timeout time.Duration) registry.Client {
	t.Helper()
	client, err := registry.NewClient(registry.Options{
		Transport:      srv.HTTPClient().Transport,
		RequestTimeout: timeout,
	})
	if err != nil {
		t.Fatalf("build S2 client: %v", err)
	}
	return client
}

func referenceOf(srv *registrydouble.Server) registry.Reference {
	ref, err := registry.ParseReference(srv.Reference(testRepo, testTag))
	if err != nil {
		panic(err)
	}
	return ref
}

func TestDoubleServesRealDigestsThroughTheS2Seam(t *testing.T) {
	srv, err := registrydouble.New(registrydouble.Options{})
	if err != nil {
		t.Fatalf("start double: %v", err)
	}
	defer srv.Close()

	want := srv.MustPutTag(testRepo, testTag, "seed-one")
	if !strings.HasPrefix(want, "sha256:") || len(want) != len("sha256:")+64 {
		t.Fatalf("double produced a malformed digest %q", want)
	}
	client := newClientFor(t, srv, 5*time.Second)
	ctx := context.Background()

	got, err := client.ResolveDigest(ctx, referenceOf(srv), registry.Credentials{})
	if err != nil {
		t.Fatalf("ResolveDigest: %v", err)
	}
	if got != want {
		t.Errorf("ResolveDigest = %q, want %q", got, want)
	}

	manifest, err := client.FetchManifest(ctx, registry.Reference{
		Registry: srv.Host(), Repository: testRepo, Digest: want,
	}, registry.Credentials{})
	if err != nil {
		t.Fatalf("FetchManifest: %v", err)
	}
	if manifest.Digest != want {
		t.Errorf("FetchManifest digest = %q, want %q", manifest.Digest, want)
	}
	if manifest.Config == nil {
		t.Error("a schema2 manifest must expose its config descriptor (the image ID)")
	}

	tags, err := client.ListTags(ctx, registry.Reference{
		Registry: srv.Host(), Repository: testRepo,
	}, registry.Credentials{})
	if err != nil {
		t.Fatalf("ListTags: %v", err)
	}
	if len(tags) != 1 || tags[0] != testTag {
		t.Errorf("ListTags = %v, want [%s]", tags, testTag)
	}
}

func TestDoubleDriftChangesTheDigest(t *testing.T) {
	srv, err := registrydouble.New(registrydouble.Options{})
	if err != nil {
		t.Fatalf("start double: %v", err)
	}
	defer srv.Close()

	first := srv.MustPutTag(testRepo, testTag, "seed-one")
	second := srv.MustPutTag(testRepo, testTag, "seed-two")
	if first == second {
		t.Fatal("re-pointing a tag at new content must change the digest")
	}

	client := newClientFor(t, srv, 5*time.Second)
	got, err := client.ResolveDigest(context.Background(), referenceOf(srv), registry.Credentials{})
	if err != nil {
		t.Fatalf("ResolveDigest: %v", err)
	}
	if got != second {
		t.Errorf("resolved %q, want the drifted digest %q", got, second)
	}

	srv.RemoveTag(testRepo, testTag)
	if _, err := client.ResolveDigest(context.Background(), referenceOf(srv),
		registry.Credentials{}); !errors.Is(err, registry.ErrNotFound) {
		t.Errorf("a removed tag must answer 404, got %v", err)
	}
}

func TestDoubleBasicAuthMatchesThePullSecretPath(t *testing.T) {
	srv, err := registrydouble.New(registrydouble.Options{Username: testUser, Password: testPass})
	if err != nil {
		t.Fatalf("start double: %v", err)
	}
	defer srv.Close()
	want := srv.MustPutTag(testRepo, testTag, "seed-auth")
	client := newClientFor(t, srv, 5*time.Second)
	ctx := context.Background()

	// Anonymous access must be refused with the S2 unauthorized classification.
	if _, err := client.ResolveDigest(ctx, referenceOf(srv), registry.Credentials{}); !errors.Is(err, registry.ErrUnauthorized) {
		t.Fatalf("anonymous access must fail as unauthorized, got %v", err)
	}

	// The real credential path: dockerconfigjson secret -> CredentialsFromSecret.
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "catalog", Name: "double-pull-secret"},
		Type:       corev1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{
			corev1.DockerConfigJsonKey: registrydouble.DockerConfigJSON(srv.Host(), testUser, testPass),
		},
	}
	creds, err := registry.CredentialsFromSecret(secret, srv.Host())
	if err != nil {
		t.Fatalf("CredentialsFromSecret: %v", err)
	}
	got, err := client.ResolveDigest(ctx, referenceOf(srv), creds)
	if err != nil {
		t.Fatalf("authenticated ResolveDigest: %v", err)
	}
	if got != want {
		t.Errorf("resolved %q, want %q", got, want)
	}

	// Wrong credentials stay unauthorized and never leak into the error text.
	bad, err := registry.CredentialsFromSecret(&corev1.Secret{
		ObjectMeta: secret.ObjectMeta,
		Type:       corev1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{
			corev1.DockerConfigJsonKey: registrydouble.DockerConfigJSON(srv.Host(), testUser, "wrong"),
		},
	}, srv.Host())
	if err != nil {
		t.Fatalf("CredentialsFromSecret(bad): %v", err)
	}
	_, err = client.ResolveDigest(ctx, referenceOf(srv), bad)
	if !errors.Is(err, registry.ErrUnauthorized) {
		t.Fatalf("wrong credentials must fail as unauthorized, got %v", err)
	}
	if strings.Contains(err.Error(), "wrong") {
		t.Errorf("error text must not carry credential material: %v", err)
	}
}

func TestDoubleFaultsMapOntoTheS2Taxonomy(t *testing.T) {
	srv, err := registrydouble.New(registrydouble.Options{})
	if err != nil {
		t.Fatalf("start double: %v", err)
	}
	defer srv.Close()
	srv.MustPutTag(testRepo, testTag, "seed-faults")
	client := newClientFor(t, srv, 500*time.Millisecond)
	ctx := context.Background()

	cases := []struct {
		name  string
		fault registrydouble.Fault
		check func(error) bool
	}{
		{"unauthorized", registrydouble.FaultUnauthorized,
			func(err error) bool { return errors.Is(err, registry.ErrUnauthorized) }},
		{"not found", registrydouble.FaultNotFound,
			func(err error) bool { return errors.Is(err, registry.ErrNotFound) }},
		{"server error", registrydouble.FaultServerError, func(err error) bool {
			var httpErr *registry.HTTPError
			return errors.As(err, &httpErr) && httpErr.StatusCode == 503
		}},
		{"hang becomes a timeout", registrydouble.FaultHang, func(err error) bool {
			// S2 documents both shapes: its own request timeout wraps
			// context.DeadlineExceeded, a net-level timeout carries ErrTimeout.
			return errors.Is(err, registry.ErrTimeout) || errors.Is(err, context.DeadlineExceeded)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv.SetFault(tc.fault)
			defer srv.SetFault(registrydouble.FaultNone)
			_, err := client.ResolveDigest(ctx, referenceOf(srv), registry.Credentials{})
			if err == nil {
				t.Fatal("the injected fault must surface as an error")
			}
			if !tc.check(err) {
				t.Errorf("error %v does not match the expected S2 classification", err)
			}
		})
	}
}

func TestDoubleIsReachableUnderAnAdvertisedHost(t *testing.T) {
	// The kind e2e advertises the container-network gateway address: the
	// certificate must cover it and HostFor must render a reference the S2
	// grammar accepts.
	srv, err := registrydouble.New(registrydouble.Options{
		BindAddress: "0.0.0.0:0",
		TLSHosts:    []string{advertisedHost},
	})
	if err != nil {
		t.Fatalf("start double: %v", err)
	}
	defer srv.Close()
	srv.MustPutTag(testRepo, testTag, "seed-advertised")

	advertised := srv.HostFor(advertisedHost)
	ref, err := registry.ParseReference(srv.ReferenceAt(advertised, testRepo, testTag))
	if err != nil {
		t.Fatalf("advertised reference must parse: %v", err)
	}
	if ref.Registry != advertised {
		t.Errorf("registry = %q, want %q", ref.Registry, advertised)
	}
	if len(srv.CACertPEM()) == 0 {
		t.Error("the CA certificate must be exported for out-of-process clients")
	}
	if !strings.Contains(string(srv.CACertPEM()), "BEGIN CERTIFICATE") {
		t.Errorf("CA export is not PEM: %s", srv.CACertPEM())
	}

	// Prove the serving certificate covers the advertised host: dial the
	// loopback listener but verify the handshake against the advertised name,
	// exactly what an out-of-network client does.
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(srv.CACertPEM()) {
		t.Fatal("the exported CA does not parse")
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", srv.Host())
		},
		TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: advertisedHost,
			MinVersion: tls.VersionTLS12},
	}
	client, err := registry.NewClient(registry.Options{Transport: transport})
	if err != nil {
		t.Fatalf("build client: %v", err)
	}
	want := srv.DigestOf(testRepo, testTag)
	got, err := client.ResolveDigest(context.Background(), ref, registry.Credentials{})
	if err != nil {
		t.Fatalf("ResolveDigest over the advertised name: %v", err)
	}
	if got != want {
		t.Errorf("resolved %q, want %q", got, want)
	}
}
