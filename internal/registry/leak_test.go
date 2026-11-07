package registry

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

// failTokenTransport lets every request through except calls to the token
// endpoint; those fail with an error message that embeds the full URL —
// including the URL-escaped refresh token — exactly like a real net stack
// would. The client must sanitize such errors before they surface.
type failTokenTransport struct {
	target string
	calls  atomic.Int32
}

func (ft *failTokenTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Path == realmPath {
		ft.calls.Add(1)
		return nil, fmt.Errorf("dial tcp: cannot reach %s", req.URL.String())
	}
	return rewriteTransport{target: ft.target}.RoundTrip(req)
}

// TestCredentialsNeverSurfaceInErrors is the acceptance case for the
// credential-boundary guarantee: pull secret material must never appear in
// error messages or any output structure, on every failure path, even when
// the registry itself echoes the material back.
func TestCredentialsNeverSurfaceInErrors(t *testing.T) {
	for _, form := range dualForms() {
		t.Run(form.name, func(t *testing.T) {
			ctx := context.Background()
			authValue := basicAuthValue(form.username, form.password)
			markers := []string{
				form.password,
				authValue,
				credMarkerIdentity,
				url.QueryEscape(credMarkerIdentity),
				credMarkerRegistry,
			}

			var errs []error

			// 1. Server error whose message echoes the credentials sent with
			//    this call (username/password form).
			h := newHarness(t, form, nil)
			echo := fmt.Sprintf("echo %s %s", form.password, authValue)
			h.fake.setForce(http.StatusInternalServerError, "UNSUPPORTED", echo)
			_, err := h.client.FetchManifest(ctx, h.ref(repoApp, tagLatest), h.creds)
			errs = append(errs, err)
			h.fake.clearForce()

			// 1b. Server error echoing the token credentials sent with this
			//     call (identity/registry token form).
			echoTokens := fmt.Sprintf("echo %s %s", credMarkerIdentity, credMarkerRegistry)
			h.fake.setForce(http.StatusInternalServerError, "UNSUPPORTED", echoTokens)
			_, err = h.client.FetchManifest(ctx, h.ref(repoApp, tagLatest), Credentials{
				Username:      form.username,
				IdentityToken: credMarkerIdentity,
				RegistryToken: credMarkerRegistry,
			})
			errs = append(errs, err)
			h.fake.clearForce()

			// 2. Not-found with credentials configured.
			_, err = h.client.ResolveDigest(ctx, h.ref(repoApp, "missing-tag"), h.creds)
			errs = append(errs, err)

			// 3. Allowlist denial.
			_, err = h.client.FetchManifest(ctx,
				Reference{Registry: deniedRegistry, Repository: repoApp, Tag: tagLatest}, h.creds)
			errs = append(errs, err)

			// 4. Digest mismatch.
			requested := "sha256:" + sha256Hex([]byte("leak-test-original"))
			h.fake.putRawManifest(repoApp, requested, fakeManifest{
				mediaType:    MediaTypeDockerSchema2,
				body:         []byte("leak-test-corrupted"),
				digest:       requested,
				servedDigest: requested,
			})
			_, err = h.client.FetchManifest(ctx, h.digestRef(repoApp, requested), h.creds)
			errs = append(errs, err)

			// 5. Malformed Docker-Content-Digest header carrying a marker.
			h.fake.putRawManifest(repoApp, "leakyheader", fakeManifest{
				mediaType:    MediaTypeDockerSchema2,
				body:         dockerSchema2Manifest(t),
				servedDigest: "sha256-INVALID-" + credMarkerPassword,
			})
			_, err = h.client.ResolveDigest(ctx, h.ref(repoApp, "leakyheader"), h.creds)
			errs = append(errs, err)

			// 6. Unauthorized with server-side rotated password.
			rotated := newHarness(t, form, func(f *fakeRegistry, _ *Options) {
				f.users[form.username] = "rotated-server-side"
			})
			_, err = rotated.client.FetchManifest(ctx, rotated.ref(repoApp, tagLatest), rotated.creds)
			errs = append(errs, err)

			// 7. Token endpoint transport failure with the identity token in
			//    the realm URL query (raw and URL-escaped forms must vanish).
			tokenFail := newHarness(t, form, func(f *fakeRegistry, _ *Options) {
				f.refreshTokens[credMarkerIdentity] = form.username
			})
			transport := &failTokenTransport{target: tokenFail.fake.server.URL}
			client, err := NewClient(Options{Transport: transport})
			if err != nil {
				t.Fatalf("new client: %v", err)
			}
			_, err = client.FetchManifest(ctx, tokenFail.ref(repoApp, tagLatest),
				Credentials{IdentityToken: credMarkerIdentity})
			errs = append(errs, err)
			if transport.calls.Load() == 0 {
				t.Fatal("token endpoint was never reached; scenario invalid")
			}

			// 8. Pull secret parse failures carrying markers.
			badSecret := dockerConfigSecret(t, form.secretKey, map[string]any{
				"auth":          "!!!not-base64!!!",
				"password":      credMarkerPassword,
				"identitytoken": credMarkerIdentity,
			})
			_, err = CredentialsFromSecret(badSecret, form.registry)
			errs = append(errs, err)

			for i, e := range errs {
				if e == nil {
					t.Fatalf("case %d: expected an error, got nil", i)
				}
				text := e.Error()
				for _, marker := range markers {
					if marker != "" && strings.Contains(text, marker) {
						t.Fatalf("case %d: marker %q leaked into error %q", i, marker, text)
					}
				}
				if strings.Contains(text, "refresh_token=") &&
					!strings.Contains(text, "refresh_token=[redacted]") {
					t.Fatalf("case %d: unredacted refresh_token leaked into error %q", i, text)
				}
			}
		})
	}
}
