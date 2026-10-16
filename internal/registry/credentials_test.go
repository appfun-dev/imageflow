package registry

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	credMarkerPassword = "PW-MARKER-do-not-leak"
	credMarkerIdentity = "it-MARKER+special/chars=7"
	credMarkerRegistry = "rt-MARKER-registry-3"
)

func dockerConfigSecret(t *testing.T, key string, entry map[string]any) *corev1.Secret {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"auths": map[string]any{key: entry}})
	if err != nil {
		t.Fatalf("marshal secret payload: %v", err)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "pull-secret", Namespace: "imageflow-system"},
		Type:       corev1.SecretTypeDockerConfigJson,
		Data:       map[string][]byte{corev1.DockerConfigJsonKey: payload},
	}
}

func TestCredentialsFromSecretBothForms(t *testing.T) {
	for _, form := range dualForms() {
		t.Run(form.name, func(t *testing.T) {
			creds, err := CredentialsFromSecret(formSecret(form), form.registry)
			if err != nil {
				t.Fatalf("CredentialsFromSecret: %v", err)
			}
			if creds.Username != form.username || creds.Password != form.password {
				t.Fatalf("creds = %+v", creds)
			}
			if creds.Anonymous() {
				t.Fatal("credentials must not be anonymous")
			}
		})
	}
}

func TestCredentialsFromSecretPrefersExplicitFields(t *testing.T) {
	entry := map[string]any{
		"username": "explicit-user",
		"password": "explicit-pass",
		// Contradicting auth field must be ignored.
		"auth": basicAuthValue("auth-user", "auth-pass"),
	}
	creds, err := CredentialsFromSecret(dockerConfigSecret(t, "quay.io", entry), "quay.io")
	if err != nil {
		t.Fatalf("CredentialsFromSecret: %v", err)
	}
	if creds.Username != "explicit-user" || creds.Password != "explicit-pass" {
		t.Fatalf("creds = %+v", creds)
	}
}

func TestCredentialsFromSecretDecodesAuthField(t *testing.T) {
	entry := map[string]any{"auth": basicAuthValue("robot+user", "robot-pass")}
	creds, err := CredentialsFromSecret(dockerConfigSecret(t, "https://quay.io/v1/", entry), "quay.io")
	if err != nil {
		t.Fatalf("CredentialsFromSecret: %v", err)
	}
	if creds.Username != "robot+user" || creds.Password != "robot-pass" {
		t.Fatalf("creds = %+v", creds)
	}
}

func TestCredentialsFromSecretTokenFields(t *testing.T) {
	entry := map[string]any{
		"identitytoken": credMarkerIdentity,
		"registrytoken": credMarkerRegistry,
	}
	creds, err := CredentialsFromSecret(dockerConfigSecret(t, "quay.io", entry), "quay.io")
	if err != nil {
		t.Fatalf("CredentialsFromSecret: %v", err)
	}
	if creds.IdentityToken != credMarkerIdentity || creds.RegistryToken != credMarkerRegistry {
		t.Fatalf("token fields not passed through: %+v", creds)
	}
}

func TestLookupCredentialsKeyNormalization(t *testing.T) {
	payload := []byte(`{"auths":{
		"https://quay.io/v1/": {"username":"a","password":"p1"},
		"HTTPS://Quay.Example.COM:8443/v2/": {"username":"b","password":"p2"}
	}}`)
	cfg, err := ParseDockerConfigJSON(payload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	creds, found, err := cfg.LookupCredentials("quay.io")
	if err != nil || !found || creds.Username != "a" {
		t.Fatalf("quay.io lookup = %+v, %v, %v", creds, found, err)
	}
	creds, found, err = cfg.LookupCredentials("quay.example.com:8443")
	if err != nil || !found || creds.Username != "b" {
		t.Fatalf("self-hosted lookup = %+v, %v, %v", creds, found, err)
	}
	_, found, err = cfg.LookupCredentials("registry.other.example.com")
	if err != nil || found {
		t.Fatalf("unknown registry must not match: found=%v err=%v", found, err)
	}
}

func TestCredentialsFromSecretErrors(t *testing.T) {
	good := dockerConfigSecret(t, "quay.io", map[string]any{
		"username": "u", "password": credMarkerPassword,
	})

	t.Run("nil secret", func(t *testing.T) {
		if _, err := CredentialsFromSecret(nil, "quay.io"); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("unsupported type dockercfg", func(t *testing.T) {
		secret := good.DeepCopy()
		secret.Type = corev1.SecretTypeDockercfg
		secret.Data = map[string][]byte{corev1.DockerConfigKey: []byte("{}")}
		_, err := CredentialsFromSecret(secret, "quay.io")
		assertNoMarker(t, err, "unsupported type")
	})

	t.Run("opaque without key", func(t *testing.T) {
		secret := good.DeepCopy()
		secret.Type = corev1.SecretTypeOpaque
		secret.Data = map[string][]byte{"other": []byte("{}")}
		assertNoMarker(t, mustErr(t, func() error {
			_, err := CredentialsFromSecret(secret, "quay.io")
			return err
		}), "missing key")
	})

	t.Run("empty data", func(t *testing.T) {
		secret := good.DeepCopy()
		secret.Data = map[string][]byte{corev1.DockerConfigJsonKey: nil}
		assertNoMarker(t, mustErr(t, func() error {
			_, err := CredentialsFromSecret(secret, "quay.io")
			return err
		}), "empty data")
	})

	t.Run("malformed json", func(t *testing.T) {
		secret := good.DeepCopy()
		secret.Data = map[string][]byte{corev1.DockerConfigJsonKey: []byte(
			`{"auths":{"quay.io":{"password":"` + credMarkerPassword + `"`)}
		assertNoMarker(t, mustErr(t, func() error {
			_, err := CredentialsFromSecret(secret, "quay.io")
			return err
		}), "malformed json")
	})

	t.Run("no entry for registry", func(t *testing.T) {
		_, err := CredentialsFromSecret(good, "registry.other.example.com")
		if !errors.Is(err, ErrNoCredentials) {
			t.Fatalf("expected ErrNoCredentials, got %v", err)
		}
		assertNoMarker(t, err, "no entry")
	})

	t.Run("invalid base64 auth", func(t *testing.T) {
		secret := dockerConfigSecret(t, "quay.io", map[string]any{
			"auth":     "!!!not-base64!!!",
			"password": credMarkerPassword,
		})
		assertNoMarker(t, mustErr(t, func() error {
			_, err := CredentialsFromSecret(secret, "quay.io")
			return err
		}), "invalid base64")
	})

	t.Run("auth without colon", func(t *testing.T) {
		secret := dockerConfigSecret(t, "quay.io", map[string]any{
			"auth": base64.StdEncoding.EncodeToString([]byte("justusername")),
		})
		assertNoMarker(t, mustErr(t, func() error {
			_, err := CredentialsFromSecret(secret, "quay.io")
			return err
		}), "auth without colon")
	})
}

func TestOpaqueSecretWithDockerConfigJsonKeyIsAccepted(t *testing.T) {
	secret := dockerConfigSecret(t, "quay.io", map[string]any{"username": "u", "password": "p"})
	secret.Type = corev1.SecretTypeOpaque
	creds, err := CredentialsFromSecret(secret, "quay.io")
	if err != nil {
		t.Fatalf("opaque secret with key must work: %v", err)
	}
	if creds.Username != "u" || creds.Password != "p" {
		t.Fatalf("creds = %+v", creds)
	}
}

func TestCredentialsAnonymous(t *testing.T) {
	if !(Credentials{}).Anonymous() {
		t.Fatal("zero credentials must be anonymous")
	}
	if (Credentials{Username: "u"}).Anonymous() {
		t.Fatal("username alone must not count as anonymous")
	}
}

func TestCredentialsAndEntryRedaction(t *testing.T) {
	creds := Credentials{
		Username:      "imageflow+robot",
		Password:      credMarkerPassword,
		IdentityToken: credMarkerIdentity,
		RegistryToken: credMarkerRegistry,
	}
	entry := DockerConfigEntry{
		Username:      "imageflow+robot",
		Password:      credMarkerPassword,
		Email:         "robot@example.com",
		Auth:          basicAuthValue("imageflow+robot", credMarkerPassword),
		IdentityToken: credMarkerIdentity,
		RegistryToken: credMarkerRegistry,
	}
	rendered := []string{
		fmt.Sprintf("%v", creds), fmt.Sprintf("%+v", creds), fmt.Sprintf("%#v", creds),
		fmt.Sprintf("%v", entry), fmt.Sprintf("%+v", entry), fmt.Sprintf("%#v", entry),
	}
	for _, out := range rendered {
		for _, marker := range []string{credMarkerPassword, credMarkerIdentity, credMarkerRegistry,
			basicAuthValue("imageflow+robot", credMarkerPassword), "robot@example.com"} {
			if strings.Contains(out, marker) {
				t.Fatalf("marker %q leaked into %q", marker, out)
			}
		}
		if !strings.Contains(out, "imageflow+robot") {
			t.Fatalf("username must stay visible for diagnostics: %q", out)
		}
		if !strings.Contains(out, "[redacted]") {
			t.Fatalf("expected redaction marker in %q", out)
		}
	}
}

func mustErr(t *testing.T, fn func() error) error {
	t.Helper()
	err := fn()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	return err
}

// assertNoMarker fails when any credential marker appears in err.
func assertNoMarker(t *testing.T, err error, context string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected error", context)
	}
	for _, marker := range []string{
		credMarkerPassword, credMarkerIdentity, credMarkerRegistry,
		basicAuthValue("imageflow+robot", credMarkerPassword),
	} {
		if strings.Contains(err.Error(), marker) {
			t.Fatalf("%s: marker %q leaked into error %q", context, marker, err.Error())
		}
	}
}
