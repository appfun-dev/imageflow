package registry

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// DockerConfig is the read-only in-memory form of a dockerconfigjson pull
// secret payload (the ".dockerconfigjson" key of a
// kubernetes.io/dockerconfigjson Secret). ImageFlow only ever reads pull
// secrets, never writes them, and never surfaces their content.
type DockerConfig struct {
	Auths map[string]DockerConfigEntry `json:"auths"`
}

// DockerConfigEntry is one registry entry of a docker config. All secret
// fields are redacted in fmt output (String/GoString).
type DockerConfigEntry struct {
	Username      string `json:"username,omitempty"`
	Password      string `json:"password,omitempty"`
	Email         string `json:"email,omitempty"`
	Auth          string `json:"auth,omitempty"`
	IdentityToken string `json:"identitytoken,omitempty"`
	RegistryToken string `json:"registrytoken,omitempty"`
}

// String implements fmt.Stringer with all secret material redacted.
func (e DockerConfigEntry) String() string { return e.redacted() }

// GoString implements fmt.GoStringer so %#v output is redacted too.
func (e DockerConfigEntry) GoString() string { return e.redacted() }

func (e DockerConfigEntry) redacted() string {
	var b strings.Builder
	b.WriteString("registry.DockerConfigEntry{Username:")
	b.WriteString(fmt.Sprintf("%q", e.Username))
	for _, f := range []struct {
		name  string
		value string
	}{
		{"Password", e.Password},
		{"Email", e.Email},
		{"Auth", e.Auth},
		{"IdentityToken", e.IdentityToken},
		{"RegistryToken", e.RegistryToken},
	} {
		b.WriteString(" " + f.name + ":")
		b.WriteString(maskSecret(f.value))
	}
	b.WriteString("}")
	return b.String()
}

// ParseDockerConfigJSON parses a dockerconfigjson payload read-only. Error
// messages never contain the payload content.
func ParseDockerConfigJSON(data []byte) (*DockerConfig, error) {
	var cfg DockerConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse dockerconfigjson: %w", redactJSONError(err, credentialsFromRawConfig(data)))
	}
	return &cfg, nil
}

// credentialsFromRawConfig best-effort extracts secret-looking values from a
// raw config payload so that even parse errors can be scrubbed. It is a
// defense-in-depth helper for malformed JSON where structured parsing fails.
func credentialsFromRawConfig(data []byte) Credentials {
	var raw struct {
		Auths map[string]DockerConfigEntry `json:"auths"`
	}
	// Malformed JSON: fall back to scrubbing every quoted string longer than
	// a threshold is not attempted; json error text quotes at most one
	// character, so the zero credentials are acceptable here.
	_ = json.Unmarshal(data, &raw)
	for _, entry := range raw.Auths {
		if entry.Password != "" || entry.IdentityToken != "" || entry.RegistryToken != "" {
			return Credentials{
				Username:      entry.Username,
				Password:      entry.Password,
				IdentityToken: entry.IdentityToken,
				RegistryToken: entry.RegistryToken,
			}
		}
	}
	return Credentials{}
}

// LookupCredentials returns the credentials whose auths key matches the
// registry host. Keys are matched after normalization (scheme and path
// stripped, lowercased), so "https://quay.io/v1/" matches "quay.io".
// The bool is false when no entry matches; an error is returned only when a
// matching entry is malformed (e.g. invalid base64 in "auth").
func (c *DockerConfig) LookupCredentials(registry string) (Credentials, bool, error) {
	target := normalizeRegistryKey(registry)
	for key, entry := range c.Auths {
		if normalizeRegistryKey(key) != target {
			continue
		}
		creds, err := entry.credentials()
		if err != nil {
			return Credentials{}, true, fmt.Errorf("dockerconfigjson entry for registry %q: %w", registry, err)
		}
		return creds, true, nil
	}
	return Credentials{}, false, nil
}

// credentials converts one docker config entry into Credentials. Explicit
// username/password fields win; otherwise the base64 "auth" field is decoded
// as "username:password" (docker CLI behavior).
func (e DockerConfigEntry) credentials() (Credentials, error) {
	creds := Credentials{
		Username:      e.Username,
		Password:      e.Password,
		IdentityToken: e.IdentityToken,
		RegistryToken: e.RegistryToken,
	}
	if creds.Username == "" && e.Auth != "" {
		decoded, err := base64.StdEncoding.DecodeString(e.Auth)
		if err != nil {
			return Credentials{}, fmt.Errorf(`decode base64 "auth" field: %w`, err)
		}
		username, password, found := strings.Cut(string(decoded), ":")
		if !found {
			return Credentials{}, fmt.Errorf(`"auth" field is not in "username:password" form`)
		}
		creds.Username = username
		creds.Password = password
	}
	return creds, nil
}

// normalizeRegistryKey reduces a docker config auths key or a registry host
// to a comparable "host[:port]" form: trimmed, lowercased, scheme and path
// components stripped.
func normalizeRegistryKey(key string) string {
	k := strings.ToLower(strings.TrimSpace(key))
	if i := strings.Index(k, "://"); i >= 0 {
		k = k[i+3:]
	}
	if i := strings.LastIndex(k, "@"); i >= 0 {
		k = k[i+1:]
	}
	if i := strings.Index(k, "/"); i >= 0 {
		k = k[:i]
	}
	return k
}

// CredentialsFromSecret parses a dockerconfigjson pull secret read-only and
// returns the credentials for the given registry host.
//
// The Secret must be of type kubernetes.io/dockerconfigjson (or Opaque) and
// carry the ".dockerconfigjson" key; the legacy kubernetes.io/dockercfg type
// is rejected. When the secret holds no entry for the registry, the error
// wraps ErrNoCredentials. No error message ever contains secret material —
// only the secret namespace/name and the registry host are referenced.
func CredentialsFromSecret(secret *corev1.Secret, registry string) (Credentials, error) {
	if secret == nil {
		return Credentials{}, fmt.Errorf("pull secret is nil")
	}
	where := fmt.Sprintf("pull secret %s/%s", secret.Namespace, secret.Name)
	if secret.Type != corev1.SecretTypeDockerConfigJson && secret.Type != corev1.SecretTypeOpaque {
		return Credentials{}, fmt.Errorf("%s has unsupported type %q: only %q is supported",
			where, secret.Type, corev1.SecretTypeDockerConfigJson)
	}
	data, ok := secret.Data[corev1.DockerConfigJsonKey]
	if !ok || len(data) == 0 {
		return Credentials{}, fmt.Errorf("%s has no %q data", where, corev1.DockerConfigJsonKey)
	}
	cfg, err := ParseDockerConfigJSON(data)
	if err != nil {
		return Credentials{}, fmt.Errorf("%s: %w", where, err)
	}
	creds, found, err := cfg.LookupCredentials(registry)
	if err != nil {
		return Credentials{}, fmt.Errorf("%s: %w", where, err)
	}
	if !found {
		return Credentials{}, fmt.Errorf("%s has no entry for registry %q: %w",
			where, registry, ErrNoCredentials)
	}
	return creds, nil
}
