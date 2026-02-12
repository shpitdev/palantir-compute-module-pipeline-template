package redact

import (
	"regexp"
	"strings"
)

var (
	// Matches "Bearer <token>" (JWTs and opaque tokens).
	bearerTokenRe = regexp.MustCompile(`(?i)\bBearer\s+[^\s"']+`)

	// Common key=value formats that sometimes leak in error strings.
	apiKeyKVRe = regexp.MustCompile(`(?i)\b(api[_-]?key|gemini[_-]?api[_-]?key)\b\s*[:=]\s*[^\s"']+`)
)

// Secrets removes obvious secret-bearing substrings from error/log strings.
func Secrets(s string) string {
	if s == "" {
		return ""
	}
	out := s
	out = bearerTokenRe.ReplaceAllString(out, "Bearer <redacted>")
	out = apiKeyKVRe.ReplaceAllString(out, "<redacted_kv>")
	return strings.TrimSpace(out)
}
