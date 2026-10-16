package controller

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	testStream   = "payments-api"
	testHexA     = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testDigestA  = "sha256:" + testHexA
	longTagUpper = "RELEASE_Candidate-1.0"
)

// adversarialTags is the corpus used for injectivity and DNS-1123 property
// checks: mostly valid OCI grammar ([a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}) plus a
// few defensive garbage inputs (the encoder is total). The set contains pairs
// that collide under naive normalization (case folding, underscore mapping,
// dot-adjacency repair) and length-boundary cases.
var adversarialTags = []string{
	"stable", "STABLE", "Stable", "s-table", "s_table",
	"a_b", "a-b", "A-B", "a.b", "a.-b", "a-.b", "a..b",
	"_x", "x_", "_x_", "__", "_", "v1_0", "v1.0", "v1-0", "V1.0",
	"latest", "LATEST", "1", "1.0", "0_0", ".hidden", "-dash",
	strings.Repeat("t", 128),
	strings.Repeat("T", 128),
	strings.Repeat("u_", 64),
	"a--b", "a---b", "--lead", "trail--",
}

func assertDNS1123Subdomain(t *testing.T, name string) {
	t.Helper()
	if errs := validation.IsDNS1123Subdomain(name); len(errs) != 0 {
		t.Fatalf("encoded name %q is not a DNS-1123 subdomain: %v", name, errs)
	}
}

func TestEncodeImageStreamTagNameCleanPassthrough(t *testing.T) {
	got := EncodeImageStreamTagName(testStream, "stable")
	if want := testStream + "--stable"; got != want {
		t.Fatalf("clean tag must encode verbatim: got %q, want %q", got, want)
	}
	// Digits, dots and hyphens inside a tag are already DNS-1123-safe.
	if got := EncodeImageStreamTagName(testStream, "v1.0-rc1"); got != testStream+"--v1.0-rc1" {
		t.Fatalf("dns-safe tag must pass through, got %q", got)
	}
	// A tag that legally contains "--" keeps the first-split injective: the
	// stream part can never contain "--" (ADR-002 admission, T3).
	if got := EncodeImageStreamTagName(testStream, "a--b"); got != testStream+"--a--b" {
		t.Fatalf("tag with double hyphen must pass through, got %q", got)
	}
}

func TestEncodeImageStreamTagNameNormalization(t *testing.T) {
	cases := []struct {
		tag       string
		wantIndex string // tag index prefix before the "-<hash>" suffix
	}{
		{"STABLE", "stable"},
		{"v1_0", "v1-0"},
		{"a.-b", "a--b"},
		{"a..b", "a--b"},
		{"a-.b", "a--b"},
		{"_x_", "x"},
		{"_", "tag"},
		{"__", "tag"},
		{"-dash", "dash"},
		{".hidden", "hidden"},
	}
	for _, tc := range cases {
		got := EncodeImageStreamTagName(testStream, tc.tag)
		wantPrefix := testStream + "--" + tc.wantIndex + "-"
		if !strings.HasPrefix(got, wantPrefix) {
			t.Fatalf("tag %q: got %q, want prefix %q + 16 hex hash", tc.tag, got, wantPrefix)
		}
		hash := strings.TrimPrefix(got, wantPrefix)
		if len(hash) != mirrorHashHexLen || !isLowerHex(hash) {
			t.Fatalf("tag %q: suffix %q is not %d lowercase hex chars", tc.tag, hash, mirrorHashHexLen)
		}
		assertDNS1123Subdomain(t, got)
		// Determinism: the encoding is a pure function of (stream, tag).
		if again := EncodeImageStreamTagName(testStream, tc.tag); again != got {
			t.Fatalf("tag %q: encoding is not deterministic: %q vs %q", tc.tag, got, again)
		}
	}
}

func TestEncodeImageStreamTagNameCollisionAware(t *testing.T) {
	// Tags that normalize onto the same DNS-safe form must encode to distinct
	// names (collision-awareness by construction: the hash covers the
	// ORIGINAL tag, no sibling context needed).
	colliding := [][]string{
		{"stable", "STABLE", "Stable"},
		{"a_b", "a-b"},
		{"v1_0", "v1-0", "V1.0"},
		{"a.b", "A.B"},
		{"_", "__", "_x_", "x"},
	}
	for _, group := range colliding {
		seen := map[string]string{}
		for _, tag := range group {
			name := EncodeImageStreamTagName(testStream, tag)
			if prev, dup := seen[name]; dup {
				t.Fatalf("tags %q and %q collide on encoded name %q", prev, tag, name)
			}
			seen[name] = tag
			assertDNS1123Subdomain(t, name)
		}
	}
}

func TestEncodeImageStreamTagNameInjectiveOverCorpus(t *testing.T) {
	streams := []string{
		testStream,
		"a",
		strings.Repeat("s", 200),
		strings.Repeat("x", 250) + "abc", // exactly 253, the DNS-1123 maximum
		"with.dots-1",
	}
	for _, stream := range streams {
		seen := map[string]string{}
		for _, tag := range adversarialTags {
			name := EncodeImageStreamTagName(stream, tag)
			if prev, dup := seen[name]; dup {
				t.Fatalf("stream %q: tags %q and %q collide on %q", stream, prev, tag, name)
			}
			seen[name] = tag
			assertDNS1123Subdomain(t, name)
			if len(name) > mirrorNameMaxLen {
				t.Fatalf("stream %q tag %q: name exceeds 253 chars (%d)", stream, tag, len(name))
			}
			// First "--" split recovers the stream part exactly for streams
			// that leave a tag budget (stream names contain no "--" per
			// ADR-002 admission; ultra-long streams truncate and are asserted
			// separately).
			gotStream, index, ok := SplitMirrorName(name)
			if !ok {
				t.Fatalf("stream %q tag %q: name %q does not split", stream, tag, name)
			}
			if index == "" {
				t.Fatalf("stream %q tag %q: empty tag index in %q", stream, tag, name)
			}
			if len(stream) <= 200 && gotStream != stream {
				t.Fatalf("stream %q tag %q: split stream part = %q", stream, tag, gotStream)
			}
		}
	}
}

func TestEncodeImageStreamTagNameUltraLongStreamStaysValid(t *testing.T) {
	stream := strings.Repeat("s", 250) + "abc" // 253 chars: no room left for a tag
	n1 := EncodeImageStreamTagName(stream, "stable")
	n2 := EncodeImageStreamTagName(stream, "STABLE")
	assertDNS1123Subdomain(t, n1)
	assertDNS1123Subdomain(t, n2)
	if n1 == n2 {
		t.Fatal("distinct tags of an ultra-long stream must not collide")
	}
	if len(n1) > mirrorNameMaxLen {
		t.Fatalf("name exceeds 253 chars: %d", len(n1))
	}
	// The stream part carries the disambiguating hash in this edge case; the
	// split remains well-defined and the spec fields stay authoritative.
	if _, _, ok := SplitMirrorName(n1); !ok {
		t.Fatalf("ultra-long stream name %q does not split", n1)
	}
}

func TestEncodeImageStreamTagNameLongTagTruncation(t *testing.T) {
	stream := strings.Repeat("s", 200)
	tag := strings.Repeat("t", 128)
	name := EncodeImageStreamTagName(stream, tag)
	assertDNS1123Subdomain(t, name)
	if len(name) > mirrorNameMaxLen {
		t.Fatalf("name exceeds 253 chars: %d", len(name))
	}
	// Truncation is lossy, so the hash suffix must be present.
	if !strings.Contains(name, mirrorHashSuffix(stream, tag)) {
		t.Fatalf("truncated name %q must carry the disambiguation suffix", name)
	}
}

func TestEncodeImageTagNameCanonicalForm(t *testing.T) {
	got := EncodeImageTagName(testStream, testDigestA)
	want := testStream + "--sha256-" + testHexA
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	assertDNS1123Subdomain(t, got)

	stream, digest, ok := DecodeImageTagName(got)
	if !ok || stream != testStream || digest != testDigestA {
		t.Fatalf("decode failed: %q -> (%q, %q, %v)", got, stream, digest, ok)
	}
}

func TestEncodeImageTagNameLongStream(t *testing.T) {
	stream := strings.Repeat("x", 200) + "abc"
	name := EncodeImageTagName(stream, testDigestA)
	assertDNS1123Subdomain(t, name)
	if len(name) > mirrorNameMaxLen {
		t.Fatalf("name exceeds 253 chars: %d", len(name))
	}
	// The digest tail keeps its canonical form even when the stream part is
	// truncated and hashed, so digest decoding survives.
	_, digest, ok := DecodeImageTagName(name)
	if !ok || digest != testDigestA {
		t.Fatalf("digest decode must survive stream truncation: (%q, %v)", digest, ok)
	}
}

func TestEncodeImageTagNameMalformedDigestFallback(t *testing.T) {
	// Defensive totality: cannot occur behind the CRD pattern, but the
	// encoder must never produce an invalid name.
	name := EncodeImageTagName(testStream, "sha256:ZZZ")
	assertDNS1123Subdomain(t, name)
	if name != EncodeImageTagName(testStream, "sha256:ZZZ") {
		t.Fatal("fallback encoding must be deterministic")
	}
	if _, _, ok := DecodeImageTagName(name); ok {
		t.Fatal("malformed digest must not decode as canonical")
	}
}

func TestSplitMirrorName(t *testing.T) {
	cases := []struct {
		name       string
		wantStream string
		wantIndex  string
		wantOK     bool
	}{
		{"payments-api--stable", "payments-api", "stable", true},
		{"a--b--c", "a", "b--c", true}, // split on the FIRST "--"
		{"a--sha256-" + testHexA, "a", "sha256-" + testHexA, true},
		{"noseparator", "", "", false},
		{"--leading", "", "", false},
		{"trailing--", "", "", false},
		{"--", "", "", false},
		{"", "", "", false},
	}
	for _, tc := range cases {
		stream, index, ok := SplitMirrorName(tc.name)
		if ok != tc.wantOK || stream != tc.wantStream || index != tc.wantIndex {
			t.Fatalf("SplitMirrorName(%q) = (%q, %q, %v), want (%q, %q, %v)",
				tc.name, stream, index, ok, tc.wantStream, tc.wantIndex, tc.wantOK)
		}
	}
}

func TestDecodeImageStreamTagNameRoundTrip(t *testing.T) {
	// Clean tags round-trip exactly; lossy tags round-trip to their index.
	name := EncodeImageStreamTagName(testStream, "stable")
	stream, tagIndex, ok := DecodeImageStreamTagName(name)
	if !ok || stream != testStream || tagIndex != "stable" {
		t.Fatalf("round trip failed: (%q, %q, %v)", stream, tagIndex, ok)
	}
	name = EncodeImageStreamTagName(testStream, longTagUpper)
	stream, tagIndex, ok = DecodeImageStreamTagName(name)
	if !ok || stream != testStream || !strings.HasPrefix(tagIndex, "release-candidate-1.0-") {
		t.Fatalf("lossy round trip failed: (%q, %q, %v)", stream, tagIndex, ok)
	}
}

func TestDecodeImageTagNameNonCanonical(t *testing.T) {
	for _, name := range []string{
		"stream--sha256-tooshort",
		"stream--sha256-" + strings.Repeat("Z", 64),
		"stream--stable",
		"stream--sha256-" + strings.Repeat("a", 63),
	} {
		stream, digest, ok := DecodeImageTagName(name)
		if ok {
			t.Fatalf("DecodeImageTagName(%q) must not succeed, got (%q, %q)", name, stream, digest)
		}
	}
}

func TestArtifactName(t *testing.T) {
	if got := ArtifactName(testDigestA); got != "sha256-"+testHexA {
		t.Fatalf("ArtifactName = %q", got)
	}
	if got := ArtifactName("sha256:short"); got != "" {
		t.Fatalf("malformed digest must yield empty artifact name, got %q", got)
	}
	if got := ArtifactName(""); got != "" {
		t.Fatalf("empty digest must yield empty artifact name, got %q", got)
	}
}

func TestNormalizeTagForMirrorName(t *testing.T) {
	cases := map[string]string{
		"stable":     "stable",
		"STABLE":     "stable",
		"v1_0":       "v1-0",
		"a.b":        "a.b",
		"a.-b":       "a--b",
		"a-.b":       "a--b",
		"a..b":       "a--b",
		"_x":         "x",
		"x_":         "x",
		"_":          "",
		"a b":        "a-b", // non-OCI input is sanitized defensively
		"tag😀":       "tag", // multi-byte runes become one '-' then trimmed
		"v1.0-alpha": "v1.0-alpha",
	}
	for tag, want := range cases {
		if got := normalizeTagForMirrorName(tag); got != want {
			t.Fatalf("normalizeTagForMirrorName(%q) = %q, want %q", tag, got, want)
		}
	}
}

func TestSplitSubscriptionSourceName(t *testing.T) {
	cases := []struct {
		in         string
		wantStream string
		wantTag    string
		wantOK     bool
	}{
		{"payments-api:stable", "payments-api", "stable", true},
		{"a:b:c", "a", "b:c", true}, // split on the FIRST colon (defensive; OCI tags cannot contain ':')
		{":tag", "", "", false},
		{"stream:", "", "", false},
		{"noseparator", "", "", false},
	}
	for _, tc := range cases {
		stream, tag, ok := splitSubscriptionSourceName(tc.in)
		if ok != tc.wantOK || stream != tc.wantStream || tag != tc.wantTag {
			t.Fatalf("splitSubscriptionSourceName(%q) = (%q, %q, %v), want (%q, %q, %v)",
				tc.in, stream, tag, ok, tc.wantStream, tc.wantTag, tc.wantOK)
		}
	}
}
