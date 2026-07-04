package velwatch

import (
	"net/url"
	"os"
	"strings"
)

// OTel resource-attribute keys carrying release/commit metadata. They are read
// from OTEL_RESOURCE_ATTRIBUTES as a fallback source and emitted verbatim as
// OTLP resource attributes.
const (
	otelServiceVersion = "service.version"
	otelVCSRevision    = "vcs.ref.head.revision"
)

// resolveReleaseInfo resolves the release and commit SHA once at init. Each
// value is resolved independently with the precedence:
//
//	explicit config > VELWATCH_RELEASE / VELWATCH_COMMIT_SHA >
//	OTEL_RESOURCE_ATTRIBUTES (service.version / vcs.ref.head.revision).
//
// The passed values are the explicit Config fields (highest precedence).
func resolveReleaseInfo(release, commitSHA string) (string, string) {
	otel := parseOTELResourceAttributes(os.Getenv("OTEL_RESOURCE_ATTRIBUTES"))

	release = firstNonEmpty(release, os.Getenv("VELWATCH_RELEASE"), otel[otelServiceVersion])
	commitSHA = firstNonEmpty(commitSHA, os.Getenv("VELWATCH_COMMIT_SHA"), otel[otelVCSRevision])
	return release, commitSHA
}

// parseOTELResourceAttributes parses the standard OTEL_RESOURCE_ATTRIBUTES value
// ("key1=value1,key2=value2") into a map. Per the OTel spec values may be
// percent-encoded, so each value is percent-decoded (a decode failure leaves the
// raw value in place). Keys and values are trimmed of surrounding whitespace.
func parseOTELResourceAttributes(s string) map[string]string {
	attrs := make(map[string]string)
	if s == "" {
		return attrs
	}
	for _, pair := range strings.Split(s, ",") {
		key, value, ok := strings.Cut(pair, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		value = strings.TrimSpace(value)
		if decoded, err := url.PathUnescape(value); err == nil {
			value = decoded
		}
		attrs[key] = value
	}
	return attrs
}

// firstNonEmpty returns the first non-empty string, or "" if all are empty.
func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
