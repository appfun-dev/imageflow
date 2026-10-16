package controller

// Mirror object name encoding (ADR-002 R4/R9, ticket T5).
//
// Mirror objects are ordinary CRD objects, so their names must be DNS-1123
// subdomains (RFC 1123, per ADR-002): lowercase alphanumerics, '-' and '.',
// starting and ending alphanumeric, at most 253 characters. The spec fields of
// a mirror are authoritative; the object name is only an index (ADR-002).
//
// # Scheme
//
// ImageStreamTag mirrors are named:
//
//	<stream>--<tag-index>
//
// ImageTag mirrors are named:
//
//	<stream>--sha256-<64hex>
//
// Splitting a mirror name on the FIRST occurrence of "--" is injective
// (stream, index) as long as stream names contain no "--"; that constraint is
// enforced by the T3 admission webhook per ADR-002, and stream names are
// DNS-1123 subdomains so they always end alphanumeric (a truncated stream part
// is additionally trimmed below, so the encoding itself never introduces a
// second "--" before the separator).
//
// # Tag normalization (OCI grammar -> DNS-1123)
//
// OCI tags (`[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}`) may contain uppercase and
// '_', and may place '.' or '-' adjacencies that DNS-1123 segments forbid
// (e.g. "a.-b"). Normalization is:
//
//  1. ASCII-lowercase; '_' and every character outside [a-z0-9.-] -> '-';
//  2. every '.' not flanked by alphanumerics on both sides -> '-';
//  3. leading/trailing '.' and '-' are trimmed.
//
// # Collision policy
//
// Normalization is lossy ("STABLE" and "stable" both normalize to "stable"),
// so whenever the normalized tag differs from the original tag — or the name
// had to be truncated — the tag index carries the disambiguating suffix
//
//	-<hash>   where <hash> = first 16 hex chars of SHA-256("<stream>\x00<tag>")
//
// over the ORIGINAL tag. This makes the encoding injective by construction
// without sibling context: the name of a mirror depends only on (stream, tag),
// so creating or deleting other tags can never rename an existing mirror, and
// two distinct tags of one stream can never share a name (a clean tag passing
// through unhashed would have to equal another tag's normalized form plus its
// 64-bit SHA-256 prefix). Tags that are already DNS-1123-safe encode verbatim:
// the common case keeps the exact ADR-002 shape <stream>--<tag>.
//
// # Length budget (253)
//
// If <stream>--<tag-index>(+suffix) would exceed 253 characters, the tag part
// is truncated (trailing separators trimmed) and the hash suffix is forced.
// In the pathological case of a stream name so long that no tag room remains,
// the stream part itself is truncated (trailing separators trimmed) — the hash
// suffix covers both inputs, so names stay deterministic and distinct.
// ImageTag names apply the same stream-part truncation when len(stream) + 73
// would exceed the budget (the digest tail "--sha256-<64hex>" is fixed).
//
// Decoding helpers below are best-effort index parses: a decoded stream part
// may carry a truncation suffix, and a decoded tag index may carry
// normalization plus a hash suffix. Programmatic consumers must read the
// mirror spec fields, never the name (spec fields are authoritative).

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const (
	// mirrorNameMaxLen is the DNS-1123 subdomain length limit for object names.
	mirrorNameMaxLen = 253
	// mirrorNameSep separates the stream part from the index part. Splitting on
	// the first occurrence is injective given stream names contain no "--".
	mirrorNameSep = "--"
	// mirrorHashHexLen is the number of hex characters of SHA-256 used in the
	// disambiguation suffix (64 bits: accidental collisions are impossible at
	// catalog scale, and chosen-prefix collisions are computationally out of
	// reach for the kubectl-scale actor model of ADR-003).
	mirrorHashHexLen = 16
	// mirrorHashSuffixLen is "-<hash>".
	mirrorHashSuffixLen = 1 + mirrorHashHexLen
	// minTagPartLen reserves room for the tag part in the ultra-long-stream
	// edge case so a tag index never collapses to nothing.
	minTagPartLen = 12
	// emptyTagFallback is used when normalization empties the tag (e.g. "_").
	emptyTagFallback = "tag"

	// digestHexPrefix is the DNS-safe rendering of the "sha256:" algorithm
	// prefix inside ImageTag mirror names.
	digestHexPrefix = "sha256-"
	// digestHexLen is the number of hex characters in a sha256 digest.
	digestHexLen = 64
)

// EncodeImageStreamTagName encodes the mirror object name of the
// ImageStreamTag view for (stream, tag): <stream>--<tag-index> per the scheme
// documented at the top of this file. The function is total and deterministic:
// equal inputs always produce the equal name, and distinct tags of one stream
// produce distinct names.
func EncodeImageStreamTagName(stream, tag string) string {
	norm := normalizeTagForMirrorName(tag)
	lossy := norm != tag
	if norm == "" {
		norm = emptyTagFallback
		lossy = true
	}

	suffix := ""
	if lossy || len(stream)+len(mirrorNameSep)+len(norm) > mirrorNameMaxLen {
		suffix = mirrorHashSuffix(stream, tag)
	}

	sPart := stream
	maxTag := mirrorNameMaxLen - len(sPart) - len(mirrorNameSep) - len(suffix)
	if maxTag < 1 {
		// The stream alone exhausts the budget (suffix is guaranteed non-empty
		// here because the overflow branch above triggered): truncate it.
		sBudget := mirrorNameMaxLen - len(mirrorNameSep) - len(suffix) - minTagPartLen
		if sBudget < 1 {
			sBudget = 1
		}
		sPart = strings.TrimRight(stream[:sBudget], ".-")
		maxTag = mirrorNameMaxLen - len(sPart) - len(mirrorNameSep) - len(suffix)
	}

	tPart := norm
	if len(tPart) > maxTag {
		tPart = strings.TrimRight(tPart[:maxTag], ".-")
	}
	if tPart == "" {
		// Unreachable for valid inputs (norm starts alphanumeric and maxTag >=
		// minTagPartLen after truncation); kept as a totality guard.
		tPart = "t"
	}
	return sPart + mirrorNameSep + tPart + suffix
}

// EncodeImageTagName encodes the mirror object name of the ImageTag view for
// (stream, digest): <stream>--sha256-<64hex> per ADR-002 R9. The digest must
// be in sha256:<64 lowercase hex> form (guaranteed by the CRD pattern of every
// field this encoder is fed from); malformed input falls back to a hashed
// index so the function stays total.
func EncodeImageTagName(stream, digest string) string {
	var tail string
	if hexPart, ok := splitDigestHex(digest); ok {
		tail = digestHexPrefix + hexPart
	} else {
		// Defensive fallback for inputs that cannot occur behind the CRD
		// pattern: keep the name deterministic and DNS-safe.
		tail = "digest" + mirrorHashSuffix(stream, digest)
	}

	sepTail := mirrorNameSep + tail
	sPart := stream
	if len(sPart)+len(sepTail) > mirrorNameMaxLen {
		sBudget := mirrorNameMaxLen - len(sepTail) - mirrorHashSuffixLen
		if sBudget < 1 {
			sBudget = 1
		}
		sPart = strings.TrimRight(stream[:sBudget], ".-") + mirrorHashSuffix(stream, digest)
	}
	return sPart + sepTail
}

// SplitMirrorName splits a mirror object name on the first "--" into its
// stream part and index part. ok is false when the name has no separator, an
// empty stream part or an empty index part. The split is injective under the
// ADR-002 admission rule that stream names contain no "--"; the returned
// parts are index values, not necessarily the original spec fields (see the
// scheme documentation at the top of this file).
func SplitMirrorName(name string) (stream, index string, ok bool) {
	i := strings.Index(name, mirrorNameSep)
	if i <= 0 || i+len(mirrorNameSep) >= len(name) {
		return "", "", false
	}
	return name[:i], name[i+len(mirrorNameSep):], true
}

// DecodeImageStreamTagName parses an ImageStreamTag mirror name into (stream,
// tag index). The tag index equals the original tag exactly when the tag was
// DNS-1123-safe and the name was not truncated; callers needing the tag must
// read spec.tag (names are indexes only).
func DecodeImageStreamTagName(name string) (stream, tagIndex string, ok bool) {
	return SplitMirrorName(name)
}

// DecodeImageTagName parses an ImageTag mirror name into (stream, digest) when
// the index part has the canonical sha256-<64hex> form. The stream part equals
// the original stream name unless the ultra-long-stream truncation applied;
// callers needing the stream must read spec.stream.
func DecodeImageTagName(name string) (stream, digest string, ok bool) {
	stream, index, splitOK := SplitMirrorName(name)
	if !splitOK {
		return "", "", false
	}
	if !strings.HasPrefix(index, digestHexPrefix) || len(index) != len(digestHexPrefix)+digestHexLen {
		return stream, "", false
	}
	hexPart := index[len(digestHexPrefix):]
	if !isLowerHex(hexPart) {
		return stream, "", false
	}
	return stream, "sha256:" + hexPart, true
}

// ArtifactName derives the cluster-scoped ImageArtifact object name for a
// digest: sha256-<64hex> (T2 convention, ADR-002 R2 content addressing).
// It returns "" for digests outside the sha256:<64hex> form, which cannot
// occur behind the CRD patterns of the fields this is fed from.
func ArtifactName(digest string) string {
	if hexPart, ok := splitDigestHex(digest); ok {
		return digestHexPrefix + hexPart
	}
	return ""
}

// normalizeTagForMirrorName maps an OCI tag onto a DNS-1123-safe index part
// per the scheme documented at the top of this file. The result is empty only
// when the tag consists purely of separator characters (e.g. "_").
func normalizeTagForMirrorName(tag string) string {
	var b strings.Builder
	b.Grow(len(tag))
	for _, r := range strings.ToLower(tag) {
		switch {
		case r == '_':
			b.WriteRune('-')
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}

	// DNS-1123 subdomain segments must start and end alphanumeric: replace
	// dots that are not flanked by alphanumerics on both sides.
	out := []byte(b.String())
	for i := range out {
		if out[i] != '.' {
			continue
		}
		before := i > 0 && isAlnumLower(out[i-1])
		after := i+1 < len(out) && isAlnumLower(out[i+1])
		if !before || !after {
			out[i] = '-'
		}
	}
	return strings.Trim(string(out), ".-")
}

// mirrorHashSuffix returns "-<hash>": the first mirrorHashHexLen hex
// characters of SHA-256 over both name inputs, NUL-separated.
func mirrorHashSuffix(a, b string) string {
	sum := sha256.Sum256([]byte(a + "\x00" + b))
	return "-" + hex.EncodeToString(sum[:])[:mirrorHashHexLen]
}

// splitDigestHex extracts the 64 lowercase hex characters from a
// sha256:<64hex> digest string.
func splitDigestHex(digest string) (string, bool) {
	const prefix = "sha256:"
	if !strings.HasPrefix(digest, prefix) || len(digest) != len(prefix)+digestHexLen {
		return "", false
	}
	hexPart := digest[len(prefix):]
	if !isLowerHex(hexPart) {
		return "", false
	}
	return hexPart, true
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return len(s) > 0
}

func isAlnumLower(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}
